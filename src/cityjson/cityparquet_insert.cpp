#include "cityjson/cityparquet_insert.hpp"

#include "cityjson/appearance_table_function.hpp"
#include "cityjson/column_types.hpp"
#include "cityjson/cityparquet_package.hpp"
#include "cityjson/cityparquet_reconcile.hpp"
#include "cityjson/lod_table.hpp"
#include "cityjson/reader.hpp"
#include "cityjson/table_function.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/keyword_helper.hpp"

#include <algorithm>
#include <map>
#include <set>

namespace duckdb {
namespace cityjson {

namespace {

//! StringUtil::Join takes duckdb::vector, which std::vector does not convert to.
std::string Join(const std::vector<std::string> &parts, const std::string &separator) {
	std::string out;
	for (idx_t i = 0; i < parts.size(); i++) {
		if (i > 0) {
			out += separator;
		}
		out += parts[i];
	}
	return out;
}

std::string Literal(const std::string &text) {
	return KeywordHelper::WriteQuoted(text, '\'');
}

std::string Quoted(const std::string &name) {
	return KeywordHelper::WriteOptionallyQuoted(name);
}

//! The staged relation, and one temp table per sidecar the source turns out to have.
const char *const kStage = "__cp_ins_src";

std::string StageTable(const std::string &sidecar) {
	return "__cp_ins_" + sidecar;
}

std::string OffsetTable(const std::string &sidecar) {
	return "__cp_ins_off_" + sidecar;
}

std::string OffsetExpr(const std::string &sidecar) {
	return "(SELECT off FROM " + OffsetTable(sidecar) + ")";
}

//! The table function that produces a given sidecar from a CityJSON file.
std::string SidecarFunction(const std::string &sidecar) {
	if (sidecar == "materials") {
		return "cityjson_materials";
	}
	if (sidecar == "textures") {
		return "cityjson_textures";
	}
	return "cityjson_geometry_templates";
}

struct ColumnInfo {
	std::string name;
	LogicalType type;
};

std::vector<ColumnInfo> TableColumns(ClientContext &context, const std::string &schema, const std::string &table) {
	std::vector<ColumnInfo> columns;
	// The non-templated GetEntry: Catalog::GetEntry<TableCatalogEntry> ODR-uses
	// TableCatalogEntry::Name and collides with DuckDB's own definition at link time.
	auto &entry = Catalog::GetEntry(context, CatalogType::TABLE_ENTRY, INVALID_CATALOG, schema, table);
	for (auto &column : entry.Cast<TableCatalogEntry>().GetColumns().Logical()) {
		columns.push_back({column.Name(), column.Type()});
	}
	return columns;
}

const ColumnInfo *FindColumn(const std::vector<ColumnInfo> &columns, const std::string &name) {
	for (const auto &column : columns) {
		if (StringUtil::Lower(column.name) == StringUtil::Lower(name)) {
			return &column;
		}
	}
	return nullptr;
}

//! BIGINT -> DOUBLE is a safe widening; anything else that disagrees falls back to
//! VARCHAR. An empty type means the destination already accommodates the source.
LogicalType WidenedType(const LogicalType &destination, const LogicalType &source) {
	if (destination == source) {
		return LogicalType(LogicalTypeId::INVALID);
	}
	const auto d = destination.id();
	const auto s = source.id();
	const bool d_int = d == LogicalTypeId::BIGINT || d == LogicalTypeId::INTEGER;
	const bool s_double = s == LogicalTypeId::DOUBLE || s == LogicalTypeId::FLOAT;
	if (d_int && s_double) {
		return LogicalType(LogicalTypeId::DOUBLE);
	}
	if (d == LogicalTypeId::DOUBLE && (s == LogicalTypeId::BIGINT || s == LogicalTypeId::INTEGER)) {
		return LogicalType(LogicalTypeId::INVALID);
	}
	return LogicalType(LogicalTypeId::VARCHAR);
}

//! Open the source with the same factory the named read function uses, so the schema
//! this derives is the schema that read will produce. Auto-detection is not a detail
//! that can be approximated here: read_cityjson and read_cityjsonseq disagree about a
//! .city.json on purpose.
std::unique_ptr<CityJSONReader> OpenFor(ClientContext &context, const std::string &reader_function,
                                        const std::string &path, size_t sample_lines) {
	if (reader_function == "read_cityjsonseq") {
		return OpenCityJSONSeqFile(context, path, sample_lines);
	}
	return OpenAnyCityJSONFile(context, path, sample_lines);
}

//! The generated call to the read function, with the options the caller passed through.
std::string ReadCall(const std::string &reader_function, const std::string &path, const InsertOptions &options,
                     bool sidecar_appearance) {
	std::string call = reader_function + "(" + Literal(path);
	if (sidecar_appearance) {
		call += ", appearance := 'sidecar'";
	}
	if (options.target_lod.has_value()) {
		call += ", lod := " + Literal(options.target_lod.value());
	}
	if (options.sample_lines != 100) {
		call += ", sample_lines := " + std::to_string(options.sample_lines);
	}
	return call + ")";
}

} // namespace

std::string BuildInsertSQL(ClientContext &context, const std::string &schema, const std::string &path,
                           const std::string &reader_function, const InsertOptions &options) {
	// read_flatcitybuf produces no appearance columns at all and does not accept the
	// `appearance` parameter, so asking for sidecar mode there is a bind error rather
	// than a no-op.
	const bool sidecar_appearance = reader_function != "read_flatcitybuf";

	CityJSONReadOptions read_options;
	read_options.sidecar_appearance = sidecar_appearance;
	read_options.target_lod = options.target_lod;
	read_options.use_wkb_encoding = options.target_lod.has_value();
	read_options.sample_lines = options.sample_lines;

	auto reader = OpenFor(context, reader_function, path, options.sample_lines);
	const auto facts = InspectCityJSONSource(*reader, read_options, reader_function == "read_cityjsonseq");

	// ---- Phase 0: routing, plan time, no data ------------------------------
	// Every type must resolve to a module. Routing is total by specification, so an
	// unplaceable type is an error -- dropping its rows would be a silent partial insert.
	std::map<std::string, std::vector<std::string>> types_by_module;
	for (const auto &object_type : facts.object_types) {
		const auto module = ModuleForObjectType(object_type);
		if (module.empty()) {
			throw BinderException("insert_cityjson: object type '%s' in '%s' belongs to no CityGML module. "
			                      "Extension types cannot be routed without their module declaration; "
			                      "load the file with read_cityjson and insert the rows explicitly",
			                      object_type, path);
		}
		types_by_module[module].push_back(CityGMLClassForCityJSONType(object_type));
	}
	if (!options.tables.empty()) {
		for (auto it = types_by_module.begin(); it != types_by_module.end();) {
			it = std::find(options.tables.begin(), options.tables.end(), it->first) == options.tables.end()
			         ? types_by_module.erase(it)
			         : std::next(it);
		}
	}
	if (types_by_module.empty()) {
		throw BinderException("insert_cityjson: '%s' contributes no rows to any selected table", path);
	}

	// The columns each sidecar the source has will be staged with. materials and textures
	// have a fixed shape; geometry_templates carries per-LoD columns and so its shape is
	// a property of the file -- asked of the sidecar reader rather than reconstructed.
	std::map<std::string, std::vector<ColumnInfo>> source_sidecar_columns;
	auto record_sidecar = [&](const std::string &sidecar, const std::vector<std::string> &names,
	                          const std::vector<LogicalType> &types) {
		std::vector<ColumnInfo> columns;
		for (idx_t i = 0; i < names.size(); i++) {
			columns.push_back({names[i], types[i]});
		}
		source_sidecar_columns[sidecar] = std::move(columns);
	};
	{
		std::vector<std::string> names;
		std::vector<LogicalType> types;
		// The fixed sidecars are asked for too, not left empty. A destination's
		// `materials` may be sparser than what a read produces -- loaded from a Parquet
		// file written before a column existed -- and INSERT ... BY NAME rejects a staged
		// column the destination has not got.
		if (facts.has_materials) {
			AppearanceSidecarColumns("materials", names, types);
			record_sidecar("materials", names, types);
		}
		if (facts.has_textures) {
			AppearanceSidecarColumns("textures", names, types);
			record_sidecar("textures", names, types);
		}
		if (!facts.geometry_templates.Empty()) {
			std::vector<std::string> lods;
			GeometryTemplateColumns(facts.geometry_templates, names, types, lods);
			record_sidecar("geometry_templates", names, types);
		}
	}

	std::vector<std::string> source_sidecars;
	for (const auto &sidecar : SidecarTableNames()) {
		if (source_sidecar_columns.count(sidecar) > 0) {
			source_sidecars.push_back(sidecar);
		}
	}

	const auto destination_tables = ObjectTablesInSchema(context, schema);
	const auto destination_sidecars = SidecarTablesInSchema(context, schema);

	std::string sql;

	// ---- Phase 1: staging ---------------------------------------------------
	// The read runs once, into a temp table, rather than once per module table: a scan
	// per module would re-parse the whole file for each one. object_type is rewritten
	// to the CityGML 3.0 class name here, at the boundary (spec
	// 02-object-table-schema.mdx), so every routed literal below matches the staged
	// value and the package stores the spec vocabulary.
	const std::string remap_expr = "CASE object_type"
	                               " WHEN 'TransportSquare' THEN 'Square'"
	                               " WHEN 'GenericCityObject' THEN 'GenericOccupiedSpace'"
	                               " WHEN 'BuildingStorey' THEN 'Storey'"
	                               " WHEN 'TunnelHollowSpace' THEN 'HollowSpace'"
	                               " ELSE object_type END";
	sql += "CREATE OR REPLACE TEMP TABLE " + std::string(kStage) + " AS SELECT * REPLACE (" + remap_expr +
	       " AS object_type) FROM " + ReadCall(reader_function, path, options, sidecar_appearance) + ";\n";
	for (const auto &sidecar : source_sidecars) {
		sql += "CREATE OR REPLACE TEMP TABLE " + StageTable(sidecar) + " AS SELECT * FROM " +
		       SidecarFunction(sidecar) + "(" + Literal(path) + ");\n";
	}

	// ---- Phase 2: preconditions, before any mutation ------------------------
	// The rows this insert will actually route somewhere. With `tables` restricting the
	// insert, the staged relation holds rows destined for nothing, and they must not be
	// judged by preconditions that only apply to rows being written.
	std::string routed;
	{
		std::vector<std::string> literals;
		for (const auto &entry : types_by_module) {
			for (const auto &object_type : entry.second) {
				literals.push_back(Literal(object_type));
			}
		}
		routed = "(SELECT * FROM " + std::string(kStage) + " WHERE object_type IN (" + Join(literals, ", ") + "))";
	}

	// Ids are identity and resolve by bare id across every file in the package, so
	// uniqueness is checked against the whole destination, not just the target module.
	{
		std::vector<std::string> destination_ids;
		for (const auto &table : destination_tables) {
			destination_ids.push_back("SELECT id FROM " + QualifiedName(schema, table));
		}
		sql += "SELECT error('insert_cityjson: duplicate id ' || id ||\n"
		       "  ' -- the destination already contains it; ids are identity, so the insert is refused rather '\n"
		       "  'than renaming silently') FROM " +
		       routed + " s WHERE s.id IN (" + Join(destination_ids, " UNION ALL ") + ");\n";
	}

	// A parent an incoming row names must exist, either among the rows being inserted or
	// already in the destination. Otherwise the reconcile would resolve feature_id to a
	// parent that is not there and commit a package that immediately fails validation --
	// which `tables` makes easy to do by accident, by excluding the module the parent
	// lives in.
	{
		std::vector<std::string> known;
		known.push_back("SELECT id FROM " + routed + " k");
		for (const auto &table : destination_tables) {
			known.push_back("SELECT id FROM " + QualifiedName(schema, table));
		}
		sql += "SELECT error('insert_cityjson: unresolved parent ' || p ||\n"
		       "  ' -- named by ' || id || ', but present neither in the rows being inserted nor in the '\n"
		       "  'destination; inserting would commit a package that fails its own hierarchy check') FROM (\n"
		       "  SELECT id, u.p AS p FROM " +
		       routed + " s, UNNEST(s.parents) AS u(p) WHERE u.p IS NOT NULL\n) WHERE p NOT IN (" +
		       Join(known, " UNION ALL ") + ");\n";
	}

	// One CRS per package. Skipped when the destination's footer is unknown, which is
	// what a hand-rolled read_parquet load leaves behind.
	if (facts.reference_system.has_value()) {
		// The CRS goes in as a quoted literal, never spliced into an open string. A
		// referenceSystem is source metadata, so an apostrophe in it would otherwise end
		// the literal early and let the file's own text continue the generated script.
		const auto crs = Literal(facts.reference_system.value());
		sql += "SELECT error('insert_cityjson: CRS mismatch -- the destination is ' || d || ' and the source is ' || " +
		       crs +
		       " || '; reprojection is not performed') FROM (SELECT\n"
		       "  (SELECT DISTINCT cityparquet_city_field(city, 'crs') FROM " +
		       QualifiedName(schema, "__cityparquet") +
		       " WHERE city IS NOT NULL) AS d\n"
		       ") WHERE d IS NOT NULL AND d <> " +
		       crs + ";\n";
	}

	// ---- Phase 3: schema evolution, before any INSERT -----------------------
	PendingTables pending;
	std::vector<std::string> incoming_geometry_columns;
	bool incoming_has_bbox = false;
	bool incoming_has_children_roles = false;
	for (const auto &column : facts.columns) {
		const auto lowered = StringUtil::Lower(column.name);
		if (MatchesLodSuffix(lowered, "geometry_lod")) {
			incoming_geometry_columns.push_back(column.name);
		} else if (lowered == "bbox") {
			incoming_has_bbox = true;
		} else if (lowered == "children_roles") {
			incoming_has_children_roles = true;
		}
	}

	for (const auto &entry : types_by_module) {
		const auto &table = entry.first;
		const auto exists =
		    std::find(destination_tables.begin(), destination_tables.end(), table) != destination_tables.end();
		if (!exists) {
			if (!options.create_tables) {
				throw BinderException("insert_cityjson: destination schema '%s' has no '%s' table and "
				                      "create_tables is false",
				                      schema, table);
			}
			// The module table and its bookkeeping row are created together: a module
			// table without one cannot later be written with valid footer metadata.
			sql += "CREATE TABLE IF NOT EXISTS " + QualifiedName(schema, table) + " AS SELECT * FROM " +
			       std::string(kStage) + " WHERE false;\n";
			// No FROM on the outer SELECT, deliberately. With `ANY_VALUE(city) FROM
			// __cityparquet` up here the statement is an aggregate query, which yields
			// exactly one row whatever the WHERE says -- so the NOT EXISTS guard never
			// fired and two pragmas batched in one submission each added a row for the
			// same table. The aggregate belongs in a scalar subquery.
			sql += "INSERT INTO " + QualifiedName(schema, "__cityparquet") +
			       " (table_name, file_name, role, city) SELECT " + Literal(table) + ", " +
			       Literal(table + ".parquet") + ", 'object', (SELECT ANY_VALUE(city) FROM " +
			       QualifiedName(schema, "__cityparquet") + ") WHERE NOT EXISTS (SELECT 1 FROM " +
			       QualifiedName(schema, "__cityparquet") + " WHERE table_name = " + Literal(table) + ");\n";
			// The CREATE above is IF NOT EXISTS, so when two inserts are batched in one
			// submission -- both seeing the pre-batch catalog, both taking this branch --
			// the second one's CREATE does nothing and its own columns would never be
			// added. Evolving unconditionally afterwards makes the branch idempotent.
			for (const auto &column : facts.columns) {
				sql += "ALTER TABLE " + QualifiedName(schema, table) + " ADD COLUMN IF NOT EXISTS " +
				       Quoted(column.name) + " " + ColumnTypeUtils::ToDuckDBType(column.kind).ToString() + ";\n";
			}
			// It does not exist for the reconcile that follows either, so its columns are
			// handed over rather than looked up.
			pending[table] = PendingTable {incoming_geometry_columns, incoming_has_bbox, incoming_has_children_roles};
			continue;
		}

		auto existing = TableColumns(context, schema, table);
		bool evolved = false;
		for (const auto &column : facts.columns) {
			const auto type = ColumnTypeUtils::ToDuckDBType(column.kind);
			const auto *match = FindColumn(existing, column.name);
			if (match == nullptr) {
				// IF NOT EXISTS because two inserts batched in one submission each see the
				// pre-batch catalog and would otherwise collide on the same new column.
				sql += "ALTER TABLE " + QualifiedName(schema, table) + " ADD COLUMN IF NOT EXISTS " +
				       Quoted(column.name) + " " + type.ToString() + ";\n";
				existing.push_back({column.name, type});
				evolved = true;
				continue;
			}
			const auto widened = WidenedType(match->type, type);
			if (widened.id() != LogicalTypeId::INVALID) {
				sql += "ALTER TABLE " + QualifiedName(schema, table) + " ALTER COLUMN " + Quoted(column.name) +
				       " SET DATA TYPE " + widened.ToString() + ";\n";
			}
		}
		if (evolved) {
			// An ALTER above is invisible to BuildReconcileSQL, which reads the pre-batch
			// catalog: it would compute the bbox from the table's *old* geometry columns
			// and so ignore the very column the incoming rows carry their geometry in,
			// setting their bbox to NULL. `existing` is the post-ALTER shape.
			PendingTable entry;
			entry.has_children_roles = false;
			for (const auto &column : existing) {
				const auto lowered = StringUtil::Lower(column.name);
				if (MatchesLodSuffix(lowered, "geometry_lod")) {
					entry.geometry_columns.push_back(column.name);
				} else if (lowered == "bbox") {
					entry.has_bbox = true;
				} else if (lowered == "children_roles") {
					entry.has_children_roles = true;
				}
			}
			pending[table] = std::move(entry);
		}
	}

	// ---- Phase 4: sidecars, created before any offset is measured -----------
	// The order matters and cost a build cycle: a package with no appearance has no
	// `materials` table, so measuring max(id) against it before creating it fails.
	for (const auto &sidecar : source_sidecars) {
		const auto in_destination =
		    std::find(destination_sidecars.begin(), destination_sidecars.end(), sidecar) != destination_sidecars.end();
		if (!in_destination) {
			if (!options.create_tables) {
				throw BinderException("insert_cityjson: destination schema '%s' has no '%s' table and "
				                      "create_tables is false",
				                      schema, sidecar);
			}
			sql += "CREATE TABLE IF NOT EXISTS " + QualifiedName(schema, sidecar) + " AS SELECT * FROM " +
			       StageTable(sidecar) + " WHERE false;\n";
			sql += "INSERT INTO " + QualifiedName(schema, "__cityparquet") +
			       " (table_name, file_name, role, city) SELECT " + Literal(sidecar) + ", " +
			       Literal(sidecar + ".parquet") + ", 'sidecar', NULL WHERE NOT EXISTS (SELECT 1 FROM " +
			       QualifiedName(schema, "__cityparquet") + " WHERE table_name = " + Literal(sidecar) + ");\n";
		} else {
			// A sidecar needs schema evolution just as a module table does. The
			// geometry_templates sidecar carries per-LoD columns, so a second file whose
			// templates use a different LoD brings columns the destination has never
			// seen, and INSERT ... BY NAME rejects a source column with no destination
			// match.
			const auto existing = TableColumns(context, schema, sidecar);
			for (const auto &column : source_sidecar_columns[sidecar]) {
				if (FindColumn(existing, column.name) == nullptr) {
					sql += "ALTER TABLE " + QualifiedName(schema, sidecar) + " ADD COLUMN IF NOT EXISTS " +
					       Quoted(column.name) + " " + column.type.ToString() + ";\n";
				}
			}
		}
		// dst_max + 1 - src_min, not dst_max + 1: a source id may be negative, and adding
		// dst_max + 1 alone could land back inside the destination's occupied range.
		// Computed by the generated SQL because it depends on row data, which a
		// generator's pre-batch view of the database cannot see.
		sql += "CREATE OR REPLACE TEMP TABLE " + OffsetTable(sidecar) +
		       " AS SELECT (SELECT coalesce(max(id), -1) FROM " + QualifiedName(schema, sidecar) +
		       ") + 1 - (SELECT coalesce(min(id), 0) FROM " + StageTable(sidecar) + ") AS off;\n";
	}

	// ---- Phase 5: routed inserts -------------------------------------------
	// INSERT ... BY NAME matches on column name and leaves unmatched destination columns
	// NULL, so neither side needs an explicit column list -- which is what keeps this
	// working when the destination carries LoDs or attributes the source does not.
	std::vector<std::string> replacements;
	for (const auto &column : facts.columns) {
		const auto lowered = StringUtil::Lower(column.name);
		const char *kind = nullptr;
		if (facts.has_materials && MatchesLodSuffix(lowered, "material_lod")) {
			kind = "material";
		} else if (facts.has_textures && MatchesLodSuffix(lowered, "texture_lod")) {
			kind = "texture";
		}
		if (kind == nullptr) {
			continue;
		}
		const std::string sidecar = std::string(kind) == "material" ? "materials" : "textures";
		replacements.push_back("cityjson_shift_appearance_ids(" + Quoted(column.name) + ", '" + kind + "', " +
		                       OffsetExpr(sidecar) + ") AS " + Quoted(column.name));
	}
	const std::string projection =
	    replacements.empty() ? "*" : "* REPLACE (" + Join(replacements, ", ") + ")";

	for (const auto &entry : types_by_module) {
		std::vector<std::string> literals;
		for (const auto &object_type : entry.second) {
			literals.push_back(Literal(object_type));
		}
		sql += "INSERT INTO " + QualifiedName(schema, entry.first) + " BY NAME SELECT " + projection + " FROM " +
		       std::string(kStage) + " WHERE object_type IN (" + Join(literals, ", ") + ");\n";
	}

	// Sidecar rows go in after the object rows, so the offsets are still in scope while
	// the references above are being rewritten with them.
	for (const auto &sidecar : source_sidecars) {
		std::vector<std::string> sidecar_replacements = {"id + " + OffsetExpr(sidecar) + " AS id"};
		// A geometry template holds appearance of its own, so its material and texture
		// references need the same shift the object rows got. Miss this and a template
		// silently renders with whichever definition the destination already had at that
		// id -- the sidecar rows would move while their references stayed behind.
		for (const auto &column : source_sidecar_columns[sidecar]) {
			const auto lowered = StringUtil::Lower(column.name);
			if (lowered == "id") {
				continue;
			}
			const char *kind = nullptr;
			if (facts.has_materials && MatchesLodSuffix(lowered, "material_lod")) {
				kind = "material";
			} else if (facts.has_textures && MatchesLodSuffix(lowered, "texture_lod")) {
				kind = "texture";
			}
			if (kind == nullptr) {
				continue;
			}
			const std::string target = std::string(kind) == "material" ? "materials" : "textures";
			sidecar_replacements.push_back("cityjson_shift_appearance_ids(" + Quoted(column.name) + ", '" + kind +
			                               "', " + OffsetExpr(target) + ") AS " + Quoted(column.name));
		}
		sql += "INSERT INTO " + QualifiedName(schema, sidecar) + " BY NAME SELECT * REPLACE (" +
		       Join(sidecar_replacements, ", ") + ") FROM " + StageTable(sidecar) + ";\n";
	}

	// ---- Phase 6: derived state ---------------------------------------------
	sql += BuildReconcileSQL(context, schema, {}, pending);

	sql += "DROP TABLE IF EXISTS " + std::string(kStage) + ";\n";
	for (const auto &sidecar : source_sidecars) {
		sql += "DROP TABLE IF EXISTS " + StageTable(sidecar) + ";\n";
		sql += "DROP TABLE IF EXISTS " + OffsetTable(sidecar) + ";\n";
	}
	return sql;
}

namespace {

InsertOptions OptionsFromParameters(const FunctionParameters &parameters) {
	InsertOptions options;
	auto create = parameters.named_parameters.find("create_tables");
	if (create != parameters.named_parameters.end()) {
		options.create_tables = BooleanValue::Get(create->second);
	}
	auto tables = parameters.named_parameters.find("tables");
	if (tables != parameters.named_parameters.end()) {
		for (const auto &value : ListValue::GetChildren(tables->second)) {
			options.tables.push_back(StringUtil::Lower(value.ToString()));
		}
	}
	auto lod = parameters.named_parameters.find("lod");
	if (lod != parameters.named_parameters.end()) {
		// Normalised exactly as ParseCityJSONReadOptions normalises it. Storing the raw
		// value would make plan-time inference reject `lod = '2'` for a source LoD of
		// '2.0' with "LOD '2' not found", while the generated read call -- which does
		// normalise -- would have accepted it.
		options.target_lod = LODTableUtils::NormalizeLOD(StringValue::Get(lod->second));
	}
	auto sample_lines = parameters.named_parameters.find("sample_lines");
	if (sample_lines != parameters.named_parameters.end()) {
		const auto value = BigIntValue::Get(sample_lines->second);
		if (value < 0) {
			throw BinderException("insert_cityjson: sample_lines must be non-negative");
		}
		options.sample_lines = static_cast<size_t>(value);
	}
	return options;
}

template <const char *READER>
std::string PragmaInsert(ClientContext &context, const FunctionParameters &parameters) {
	return BuildInsertSQL(context, parameters.values[0].ToString(), parameters.values[1].ToString(), READER,
	                      OptionsFromParameters(parameters));
}

const char kReadCityJSON[] = "read_cityjson";
const char kReadCityJSONSeq[] = "read_cityjsonseq";
const char kReadFlatCityBuf[] = "read_flatcitybuf";

void InsertSQLScalar(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    args.data[0], args.data[1], result, args.size(), [&](string_t schema, string_t path) {
		    return StringVector::AddString(
		        result, BuildInsertSQL(context, schema.GetString(), path.GetString(), "read_cityjson", InsertOptions()));
	    });
}

void RegisterOne(ExtensionLoader &loader, const char *name, pragma_query_t query) {
	auto pragma = PragmaFunction::PragmaCall(name, query,
	                                         {LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR)});
	pragma.named_parameters["create_tables"] = LogicalType(LogicalTypeId::BOOLEAN);
	pragma.named_parameters["tables"] = LogicalType::LIST(LogicalType(LogicalTypeId::VARCHAR));
	pragma.named_parameters["lod"] = LogicalType(LogicalTypeId::VARCHAR);
	pragma.named_parameters["sample_lines"] = LogicalType(LogicalTypeId::BIGINT);
	loader.RegisterFunction(pragma);
}

} // namespace

void RegisterCityParquetInsertFunctions(ExtensionLoader &loader) {
	RegisterOne(loader, "insert_cityjson", PragmaInsert<kReadCityJSON>);
	RegisterOne(loader, "insert_cityjsonseq", PragmaInsert<kReadCityJSONSeq>);
	RegisterOne(loader, "insert_flatcitybuf", PragmaInsert<kReadFlatCityBuf>);

	ScalarFunction insert_sql("insert_cityjson_sql",
	                          {LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR)},
	                          LogicalType(LogicalTypeId::VARCHAR), InsertSQLScalar);
	loader.RegisterFunction(insert_sql);
}

} // namespace cityjson
} // namespace duckdb
