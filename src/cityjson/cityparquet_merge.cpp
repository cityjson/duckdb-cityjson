#include "cityjson/cityparquet_merge.hpp"

#include "cityjson/cityparquet_package.hpp"
#include "cityjson/cityparquet_reconcile.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/keyword_helper.hpp"

#include <algorithm>
#include <map>

namespace duckdb {
namespace cityjson {

namespace {

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

struct ColumnInfo {
	std::string name;
	LogicalType type;
};

std::vector<ColumnInfo> TableColumns(ClientContext &context, const std::string &schema, const std::string &table) {
	std::vector<ColumnInfo> columns;
	auto &entry = Catalog::GetEntry(context, CatalogType::TABLE_ENTRY, INVALID_CATALOG, schema, table);
	for (auto &column : entry.Cast<TableCatalogEntry>().GetColumns().Logical()) {
		columns.push_back({column.Name(), column.Type()});
	}
	return columns;
}

bool HasColumn(const std::vector<ColumnInfo> &columns, const std::string &name) {
	return std::any_of(columns.begin(), columns.end(), [&](const ColumnInfo &c) {
		return StringUtil::Lower(c.name) == StringUtil::Lower(name);
	});
}

const ColumnInfo *FindColumn(const std::vector<ColumnInfo> &columns, const std::string &name) {
	for (const auto &column : columns) {
		if (StringUtil::Lower(column.name) == StringUtil::Lower(name)) {
			return &column;
		}
	}
	return nullptr;
}

//! The promotion lattice: BIGINT -> DOUBLE is a safe widening; anything else that
//! disagrees falls back to VARCHAR. Returns an empty type when no widening is needed.
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
		return LogicalType(LogicalTypeId::INVALID); // destination already wider
	}
	return LogicalType(LogicalTypeId::VARCHAR);
}

//! Offsets are named per sidecar so several can be in flight at once.
std::string OffsetTable(const std::string &sidecar) {
	return "__cp_off_" + sidecar;
}

//! dst_max + 1 - src_min, not dst_max + 1. A source id may be negative, in which case
//! adding dst_max + 1 alone can land back inside the destination's occupied range.
//! Subtracting the source minimum maps the incoming range to start immediately after the
//! destination's maximum, whatever its sign.
std::string OffsetSQL(const std::string &destination, const std::string &source, const std::string &sidecar) {
	return "CREATE OR REPLACE TEMP TABLE " + OffsetTable(sidecar) + " AS SELECT (SELECT coalesce(max(id), -1) FROM " +
	       QualifiedName(destination, sidecar) + ") + 1 - (SELECT coalesce(min(id), 0) FROM " +
	       QualifiedName(source, sidecar) + ") AS off;\n";
}

std::string OffsetExpr(const std::string &sidecar) {
	return "(SELECT off FROM " + OffsetTable(sidecar) + ")";
}

} // namespace

std::string BuildMergeSQL(ClientContext &context, const std::string &destination, const std::string &source,
                          const MergeOptions &options) {
	auto destination_tables = ObjectTablesInSchema(context, destination);
	auto source_tables = ObjectTablesInSchema(context, source);
	if (!options.tables.empty()) {
		std::vector<std::string> filtered;
		for (const auto &table : source_tables) {
			if (std::find(options.tables.begin(), options.tables.end(), table) != options.tables.end()) {
				filtered.push_back(table);
			}
		}
		source_tables = std::move(filtered);
	}

	const auto destination_sidecars = SidecarTablesInSchema(context, destination);
	const auto source_sidecars = SidecarTablesInSchema(context, source);

	std::string sql;

	// ---- Phase 1: preconditions, before any mutation -----------------------
	// Object ids must be unique across the WHOLE destination package, not merely the
	// target module: parents, children and feature_id all resolve by bare id across
	// files, so a Road colliding with a Building id is still a collision.
	{
		std::vector<std::string> destination_ids;
		for (const auto &table : destination_tables) {
			destination_ids.push_back("SELECT id FROM " + QualifiedName(destination, table));
		}
		std::vector<std::string> incoming_ids;
		for (const auto &table : source_tables) {
			incoming_ids.push_back("SELECT id FROM " + QualifiedName(source, table));
		}
		sql += "SELECT error('cityparquet_merge: duplicate id ' || id ||\n"
		       "  ' -- the destination already contains it; ids are identity, so the merge is refused rather '\n"
		       "  'than renaming silently') FROM (" +
		       Join(incoming_ids, " UNION ALL ") + ") s WHERE s.id IN (" + Join(destination_ids, " UNION ALL ") +
		       ");\n";
	}

	// One CRS per file, with no per-row escape hatch, so a mismatch is a hard error
	// rather than a silently mis-georeferenced package. Skipped when either side's
	// footer is unknown (a hand-rolled load leaves it NULL).
	sql += "SELECT error('cityparquet_merge: CRS mismatch -- the destination is ' || d || ' and the source is ' || s ||\n"
	       "  '; reprojection is not performed') FROM (SELECT\n"
	       "  (SELECT DISTINCT cityparquet_city_field(city, 'crs') FROM " +
	       QualifiedName(destination, "__cityparquet") + " WHERE city IS NOT NULL) AS d,\n"
	       "  (SELECT DISTINCT cityparquet_city_field(city, 'crs') FROM " +
	       QualifiedName(source, "__cityparquet") + " WHERE city IS NOT NULL) AS s\n"
	       ") WHERE d IS NOT NULL AND s IS NOT NULL AND d <> s;\n";

	// ---- Phase 2: schema evolution, before any INSERT ----------------------
	std::map<std::string, std::vector<ColumnInfo>> destination_columns;
	PendingTables pending;
	for (const auto &table : source_tables) {
		const auto exists =
		    std::find(destination_tables.begin(), destination_tables.end(), table) != destination_tables.end();
		if (!exists) {
			if (!options.create_tables) {
				throw BinderException("cityparquet_merge: destination schema '%s' has no '%s' table and "
				                      "create_tables is false",
				                      destination, table);
			}
			// Create the module table AND its bookkeeping row together: a module table
			// without one cannot be written with valid footer metadata.
			sql += "CREATE TABLE IF NOT EXISTS " + QualifiedName(destination, table) + " AS SELECT * FROM " +
			       QualifiedName(source, table) + " WHERE false;\n";
			sql += "INSERT INTO " + QualifiedName(destination, "__cityparquet") +
			       " (table_name, file_name, role, city) SELECT " + Literal(table) + ", " +
			       Literal(table + ".parquet") + ", 'object', ANY_VALUE(city) FROM " +
			       QualifiedName(destination, "__cityparquet") + " WHERE NOT EXISTS (SELECT 1 FROM " +
			       QualifiedName(destination, "__cityparquet") + " WHERE table_name = " + Literal(table) + ");\n";
			auto created = TableColumns(context, source, table);
			// It does not exist for the reconcile at the end of this script either -- a
			// generator sees the pre-batch catalog -- so its columns are handed over
			// rather than looked up. Without this a destination row that has just become
			// the parent of an incoming one never gets the reciprocal children entry.
			PendingTable entry;
			for (const auto &column : created) {
				const auto lowered = StringUtil::Lower(column.name);
				if (MatchesLodSuffix(lowered, "geometry_lod")) {
					entry.geometry_columns.push_back(column.name);
				} else if (lowered == "bbox") {
					entry.has_bbox = true;
				}
			}
			pending[table] = std::move(entry);
			destination_columns[table] = std::move(created);
			continue;
		}

		auto existing = TableColumns(context, destination, table);
		const auto incoming = TableColumns(context, source, table);
		for (const auto &column : incoming) {
			const auto *match = FindColumn(existing, column.name);
			if (match == nullptr) {
				// IF NOT EXISTS so that batching several merges in one submission -- where
				// each generator sees the pre-batch catalog -- cannot fail on a duplicate.
				sql += "ALTER TABLE " + QualifiedName(destination, table) + " ADD COLUMN IF NOT EXISTS " +
				       Quoted(column.name) + " " + column.type.ToString() + ";\n";
				existing.push_back(column);
				continue;
			}
			const auto widened = WidenedType(match->type, column.type);
			if (widened.id() != LogicalTypeId::INVALID) {
				sql += "ALTER TABLE " + QualifiedName(destination, table) + " ALTER COLUMN " + Quoted(column.name) +
				       " SET DATA TYPE " + widened.ToString() + ";\n";
			}
		}
		destination_columns[table] = existing;
	}

	// ---- Phase 3: sidecar merge with id remap ------------------------------
	for (const auto &sidecar : source_sidecars) {
		const auto in_destination =
		    std::find(destination_sidecars.begin(), destination_sidecars.end(), sidecar) != destination_sidecars.end();
		if (!in_destination) {
			sql += "CREATE TABLE IF NOT EXISTS " + QualifiedName(destination, sidecar) + " AS SELECT * FROM " +
			       QualifiedName(source, sidecar) + " WHERE false;\n";
			sql += "INSERT INTO " + QualifiedName(destination, "__cityparquet") +
			       " (table_name, file_name, role, city) SELECT " + Literal(sidecar) + ", " +
			       Literal(sidecar + ".parquet") + ", 'sidecar', NULL WHERE NOT EXISTS (SELECT 1 FROM " +
			       QualifiedName(destination, "__cityparquet") + " WHERE table_name = " + Literal(sidecar) + ");\n";
		} else {
			// A sidecar needs schema evolution just as a module table does. The
			// geometry_templates sidecar carries per-LoD columns, so two packages whose
			// templates use different LoDs have genuinely different sidecar schemas and
			// the INSERT below would name a column the destination has never had.
			auto existing = TableColumns(context, destination, sidecar);
			for (const auto &column : TableColumns(context, source, sidecar)) {
				if (FindColumn(existing, column.name) == nullptr) {
					sql += "ALTER TABLE " + QualifiedName(destination, sidecar) + " ADD COLUMN IF NOT EXISTS " +
					       Quoted(column.name) + " " + column.type.ToString() + ";\n";
				}
			}
		}
		// The offset depends on row data, so it is computed here rather than at plan
		// time -- a generator's view of the data is the state before the batch began.
		sql += OffsetSQL(destination, source, sidecar);
	}

	// ---- Phase 4: routed inserts -------------------------------------------
	for (const auto &table : source_tables) {
		const auto &columns = destination_columns[table];
		const auto incoming = TableColumns(context, source, table);

		std::vector<std::string> names;
		std::vector<std::string> values;
		for (const auto &column : columns) {
			names.push_back(Quoted(column.name));
			if (!HasColumn(incoming, column.name)) {
				// A column the destination has and the source does not: NULL, cast so the
				// INSERT's types line up.
				values.push_back("NULL::" + column.type.ToString());
				continue;
			}
			const auto lowered = StringUtil::Lower(column.name);
			// The anchored LoD grammar, not a bare prefix: `material_lodging` is an
			// ordinary source attribute and handing it to cityjson_shift_appearance_ids
			// would abort the merge.
			const bool is_material =
			    MatchesLodSuffix(lowered, "material_lod") &&
			    std::find(source_sidecars.begin(), source_sidecars.end(), "materials") != source_sidecars.end();
			const bool is_texture =
			    MatchesLodSuffix(lowered, "texture_lod") &&
			    std::find(source_sidecars.begin(), source_sidecars.end(), "textures") != source_sidecars.end();
			if (is_material) {
				values.push_back("cityjson_shift_appearance_ids(" + Quoted(column.name) + ", 'material', " +
				                 OffsetExpr("materials") + ")");
			} else if (is_texture) {
				values.push_back("cityjson_shift_appearance_ids(" + Quoted(column.name) + ", 'texture', " +
				                 OffsetExpr("textures") + ")");
			} else {
				values.push_back(Quoted(column.name));
			}
		}
		sql += "INSERT INTO " + QualifiedName(destination, table) + " (" + Join(names, ", ") + ") SELECT " +
		       Join(values, ", ") + " FROM " + QualifiedName(source, table) + ";\n";
	}

	// Sidecar rows go in after the object rows so the offsets above are still valid
	// while the references are rewritten.
	const bool has_materials =
	    std::find(source_sidecars.begin(), source_sidecars.end(), "materials") != source_sidecars.end();
	const bool has_textures =
	    std::find(source_sidecars.begin(), source_sidecars.end(), "textures") != source_sidecars.end();
	for (const auto &sidecar : source_sidecars) {
		auto columns = TableColumns(context, source, sidecar);
		std::vector<std::string> names;
		std::vector<std::string> values;
		for (const auto &column : columns) {
			names.push_back(Quoted(column.name));
			const auto lowered = StringUtil::Lower(column.name);
			if (lowered == "id") {
				values.push_back("id + " + OffsetExpr(sidecar));
				continue;
			}
			// A geometry template holds appearance of its own, so its material and
			// texture references need the same shift the object rows got. Moving the
			// sidecar rows while leaving their references behind would repoint every
			// template at whichever definition already occupied that id.
			const char *kind = nullptr;
			if (has_materials && MatchesLodSuffix(lowered, "material_lod")) {
				kind = "material";
			} else if (has_textures && MatchesLodSuffix(lowered, "texture_lod")) {
				kind = "texture";
			}
			if (kind == nullptr) {
				values.push_back(Quoted(column.name));
				continue;
			}
			values.push_back("cityjson_shift_appearance_ids(" + Quoted(column.name) + ", '" + kind + "', " +
			                 OffsetExpr(std::string(kind) == "material" ? "materials" : "textures") + ")");
		}
		sql += "INSERT INTO " + QualifiedName(destination, sidecar) + " (" + Join(names, ", ") + ") SELECT " +
		       Join(values, ", ") + " FROM " + QualifiedName(source, sidecar) + ";\n";
		sql += "DROP TABLE IF EXISTS " + OffsetTable(sidecar) + ";\n";
	}

	// ---- Phase 5: derived state --------------------------------------------
	sql += BuildReconcileSQL(context, destination, {}, pending);
	return sql;
}

namespace {

MergeOptions OptionsFromParameters(const FunctionParameters &parameters) {
	MergeOptions options;
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
	return options;
}

std::string PragmaMerge(ClientContext &context, const FunctionParameters &parameters) {
	return BuildMergeSQL(context, parameters.values[0].ToString(), parameters.values[1].ToString(),
	                     OptionsFromParameters(parameters));
}

void MergeSQLScalar(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    args.data[0], args.data[1], result, args.size(), [&](string_t destination, string_t source) {
		    return StringVector::AddString(
		        result, BuildMergeSQL(context, destination.GetString(), source.GetString(), MergeOptions()));
	    });
}

} // namespace

void RegisterCityParquetMergeFunctions(ExtensionLoader &loader) {
	auto pragma = PragmaFunction::PragmaCall(
	    "cityparquet_merge", PragmaMerge, {LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR)});
	pragma.named_parameters["create_tables"] = LogicalType(LogicalTypeId::BOOLEAN);
	pragma.named_parameters["tables"] = LogicalType::LIST(LogicalType(LogicalTypeId::VARCHAR));
	loader.RegisterFunction(pragma);

	ScalarFunction merge_sql("cityparquet_merge_sql",
	                         {LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR)},
	                         LogicalType(LogicalTypeId::VARCHAR), MergeSQLScalar);
	loader.RegisterFunction(merge_sql);
}

} // namespace cityjson
} // namespace duckdb
