#include "cityjson/cityparquet_insert.hpp"

#include "cityjson/function_docs.hpp"
#include "cityjson/appearance_table_function.hpp"
#include "cityjson/column_types.hpp"
#include "cityjson/cityparquet_extensions.hpp"
#include "cityjson/cityparquet_package.hpp"
#include "cityjson/cityparquet_reconcile.hpp"
#include "cityjson/cityparquet_sql_common.hpp"
#include "cityjson/crs_projjson.hpp"
#include "cityjson/json_utils.hpp"
#include "cityjson/lod_table.hpp"
#include "cityjson/reader.hpp"
#include "cityjson/table_function.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/scalar_function.hpp"

#include <algorithm>
#include <map>
#include <set>

namespace duckdb {
namespace cityjson {

namespace {

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
	return "cityjson_implicit_geometries";
}

//! Open the source with the same factory the named read function uses, so the schema
//! this derives is the schema that read will produce. Auto-detection is not a detail
//! that can be approximated here: read_cityjson and read_cityjsonseq disagree about a
//! .city.json on purpose.
std::unique_ptr<CityJSONReader> OpenFor(ClientContext &context, const std::string &reader_function,
                                        const std::string &path, size_t sample_lines) {
	return OpenCityJSONFileOfKind(context, ReaderKindForFunction(reader_function), path, sample_lines);
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

//! One name the source adds to a package, for the collision rule and its messages.
struct SourceName {
	const char *kind;
	std::string name;
};

//! Every name the source adds that is NOT an extension name: its attribute columns,
//! object types and semantic surface types without CityJSON's `+`.
std::vector<SourceName> CoreNames(const CityJSONSourceFacts &facts) {
	std::vector<SourceName> names;
	for (const auto &column : facts.columns) {
		if (!IsPlusName(column.name) && !IsReservedColumnName(column.name)) {
			names.push_back({"attribute", column.name});
		}
	}
	for (const auto &type : facts.object_types) {
		if (!IsPlusName(type)) {
			names.push_back({"object type", type});
		}
	}
	for (const auto &type : facts.surface_types) {
		if (!IsPlusName(type)) {
			names.push_back({"semantic surface type", type});
		}
	}
	return names;
}

//! An attribute name compared case-insensitively, because DuckDB's column names are;
//! a type is compared exactly.
std::string Comparable(const SourceName &entry) {
	return std::string(entry.kind) == "attribute" ? StringUtil::Lower(entry.name) : entry.name;
}

//! True when `entry` starts with the prefix form of `ns`.
bool StartsWithPrefix(const SourceName &entry, const std::string &ns) {
	const auto name = Comparable(entry);
	const auto prefix = ns + "_";
	return name.size() > prefix.size() && name.compare(0, prefix.size(), prefix) == 0;
}

//! SQL that is true when the package's object footers declare the namespace `ns`.
std::string DeclaresNamespaceExpr(const std::string &schema, const std::string &ns) {
	return "EXISTS (SELECT 1 FROM " + QualifiedName(schema, "__cityparquet") +
	       " WHERE role = 'object' AND cityparquet_city_field(cityparquet_city_field(city, 'extensions'), " +
	       Literal(ns) + ") IS NOT NULL)";
}

//! The CityJSON Extension side of an insert (spec 06-extensions.mdx): the extension
//! the source's `+` names belong to, checked and turned into its namespace.
struct SourceExtension {
	//! Empty when the source has no `+` name; otherwise the one declaration its names
	//! are attributed to.
	std::vector<ExtensionDeclaration> declarations;
	std::string ns;
};

SourceExtension PlanSourceExtension(const CityJSONSourceFacts &facts, const std::string &path) {
	std::string first_plus;
	for (const auto &column : facts.columns) {
		if (first_plus.empty() && IsPlusName(column.name)) {
			first_plus = column.name;
		}
	}
	for (const auto &type : facts.object_types) {
		if (first_plus.empty() && IsPlusName(type)) {
			first_plus = type;
		}
	}
	for (const auto &type : facts.surface_types) {
		if (first_plus.empty() && IsPlusName(type)) {
			first_plus = type;
		}
	}
	SourceExtension result;
	if (first_plus.empty()) {
		return result;
	}
	if (facts.extensions.empty()) {
		throw BinderException("insert_cityjson: '%s' in '%s' carries CityJSON's extension marker, but the source "
		                      "declares no extension; CityJSON requires every extension a document uses to be "
		                      "declared in its `extensions` member",
		                      first_plus, path);
	}
	if (facts.extensions.size() > 1) {
		std::vector<std::string> names;
		for (const auto &entry : facts.extensions) {
			names.push_back(entry.first);
		}
		throw BinderException("insert_cityjson: '%s' declares %llu extensions (%s), so its `+` names cannot be "
		                      "attributed by declaration alone; that needs the extensions' schema documents, and "
		                      "attribution from extension schema documents is not implemented",
		                      path, static_cast<uint64_t>(names.size()), Join(names, ", "));
	}
	result.declarations = DeclarationsFromCityJSON(facts.extensions, "insert_cityjson");
	result.ns = result.declarations.front().ns;

	// A core name that starts with the namespace's prefix would read back as an
	// extension name: refused, rather than turned into `+` on export.
	const auto &declaration = result.declarations.front();
	for (const auto &entry : CoreNames(facts)) {
		if (StartsWithPrefix(entry, declaration.ns)) {
			throw BinderException("insert_cityjson: the %s '%s' in '%s' is not an extension name, but it starts "
			                      "with '%s_', the prefix of the extension '%s' the source declares; on export it "
			                      "would be indistinguishable from an extension name",
			                      entry.kind, entry.name, path, declaration.ns, declaration.name);
		}
	}
	return result;
}

//! The REPLACE expression giving a geometry_properties struct column's extension
//! surface types the namespace prefix. Every other field is carried as it is.
std::string PrefixedSurfacesExpr(const std::string &column, const std::string &ns) {
	const auto q = Quoted(column);
	return "CASE WHEN " + q + " IS NULL THEN NULL ELSE {'type': " + q +
	       ".\"type\", 'surfaces': cityparquet_prefix_surface_types(" + q + ".surfaces, " + Literal(ns) +
	       "), 'face_semantics': " + q + ".face_semantics, 'shells': " + q + ".shells} END AS " + q;
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
	auto facts = InspectCityJSONSource(*reader, read_options, reader_function == "read_cityjsonseq");

	// ---- CityJSON Extensions (spec 06-extensions.mdx) -----------------------
	// Every `+` name the source adds is stored with its extension's namespace as a
	// prefix: attribute columns are renamed in the staged relation, object types and
	// semantic surface types rewritten as they are staged. From here on `facts.columns`
	// carries the package's names, so every ALTER below names the column as staged.
	const auto extension = PlanSourceExtension(facts, path);
	std::vector<std::pair<std::string, std::string>> renamed_columns;
	bool prefixes_types = false;
	bool prefixes_surfaces = false;
	if (!extension.ns.empty()) {
		for (auto &column : facts.columns) {
			if (IsPlusName(column.name)) {
				const auto renamed = PrefixPlusName(column.name, extension.ns);
				renamed_columns.emplace_back(column.name, renamed);
				column.name = renamed;
			}
		}
		for (const auto &type : facts.object_types) {
			prefixes_types = prefixes_types || IsPlusName(type);
		}
		for (const auto &type : facts.surface_types) {
			prefixes_surfaces = prefixes_surfaces || IsPlusName(type);
		}
	}

	// ---- Phase 0: routing, plan time, no data ------------------------------
	// Every type must resolve to a module. Routing is total by specification, so an
	// unplaceable type is an error -- dropping its rows would be a silent partial insert.
	std::map<std::string, std::vector<std::string>> types_by_module;
	for (const auto &object_type : facts.object_types) {
		// An extension class routes by its name without the `+` when that is a core
		// class (spec 06-extensions.mdx, "The ModuleKey"); it stays an extension class,
		// stored under its prefixed name. Any other extension class would need the
		// module its extension declares for it, which this extension does not read.
		const bool is_extension = IsPlusName(object_type);
		const auto module = ModuleForObjectType(is_extension ? object_type.substr(1) : object_type);
		if (module.empty()) {
			throw BinderException("insert_cityjson: object type '%s' in '%s' belongs to no CityGML module. "
			                      "Extension types cannot be routed without their module declaration; "
			                      "load the file with read_cityjson and insert the rows explicitly",
			                      object_type, path);
		}
		types_by_module[module].push_back(is_extension ? PrefixPlusName(object_type, extension.ns)
		                                               : CityGMLClassForCityJSONType(object_type));
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
	// have a fixed shape; implicit_geometries carries per-LoD columns and so its shape is
	// a property of the file -- asked of the sidecar reader rather than reconstructed.
	std::map<std::string, std::vector<ColumnInfo>> source_sidecar_columns;
	auto record_sidecar = [&](const std::string &sidecar, const std::vector<std::string> &names,
	                          const std::vector<LogicalType> &types) {
		std::vector<ColumnInfo> columns;
		columns.reserve(names.size());
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
			ImplicitGeometryColumns(facts.geometry_templates, names, types, lods);
			record_sidecar("implicit_geometries", names, types);
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
	std::string remap_expr = "CASE object_type"
	                         " WHEN 'TransportSquare' THEN 'Square'"
	                         " WHEN 'GenericCityObject' THEN 'GenericOccupiedSpace'"
	                         " WHEN 'BuildingStorey' THEN 'Storey'"
	                         " WHEN 'TunnelHollowSpace' THEN 'HollowSpace'"
	                         " ELSE object_type END";
	if (prefixes_types) {
		remap_expr = "CASE WHEN starts_with(object_type, '+') THEN " + Literal(extension.ns + "_") +
		             " || substr(object_type, 2) ELSE " + remap_expr + " END";
	}
	std::vector<std::string> stage_replacements = {remap_expr + " AS object_type"};
	if (prefixes_surfaces) {
		for (const auto &column : facts.columns) {
			if (column.kind == ColumnType::GeometryPropertiesStruct) {
				stage_replacements.push_back(PrefixedSurfacesExpr(column.name, extension.ns));
			}
		}
	}
	sql += "CREATE OR REPLACE TEMP TABLE " + std::string(kStage) + " AS SELECT * REPLACE (" +
	       Join(stage_replacements, ", ") + ") FROM " + ReadCall(reader_function, path, options, sidecar_appearance) +
	       ";\n";
	for (const auto &renamed : renamed_columns) {
		sql += "ALTER TABLE " + std::string(kStage) + " RENAME COLUMN " + Quoted(renamed.first) + " TO " +
		       Quoted(renamed.second) + ";\n";
	}
	for (const auto &sidecar : source_sidecars) {
		// A relative geometry's semantic surfaces are named like an object's.
		std::vector<std::string> sidecar_replacements;
		if (prefixes_surfaces) {
			for (const auto &column : source_sidecar_columns[sidecar]) {
				if (MatchesLodSuffix(StringUtil::Lower(column.name), "geometry_properties_lod")) {
					sidecar_replacements.push_back(PrefixedSurfacesExpr(column.name, extension.ns));
				}
			}
		}
		const auto projection =
		    sidecar_replacements.empty() ? std::string("*") : "* REPLACE (" + Join(sidecar_replacements, ", ") + ")";
		sql += "CREATE OR REPLACE TEMP TABLE " + StageTable(sidecar) + " AS SELECT " + projection + " FROM " +
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
		destination_ids.reserve(destination_tables.size());
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

	// One CRS per package: a footer states a single CRS for every row in its file, and
	// reprojection is never performed, so what goes in has to be in the CRS that is
	// already declared.
	//
	// Both sides must be spelled the same way before they can be compared. The
	// destination's is PROJJSON, sitting in the footer; the source's is a CityJSON
	// `metadata.referenceSystem`, which is an OGC URL or an EPSG spelling. This check
	// used to compare the two directly, which made EVERY insert into a package with a
	// known CRS a bogus mismatch -- invisible only because a hand-rolled destination
	// carries no footer to compare against. The source is therefore resolved through the
	// same ProjjsonForReferenceSystem the writers use, and re-dumped, so both sides are
	// the same canonical text for the same CRS.
	{
		std::string source_crs = "NULL::VARCHAR";
		if (facts.reference_system.has_value()) {
			auto projjson = ProjjsonForReferenceSystem(facts.reference_system.value());
			if (projjson.has_value()) {
				// Re-dumped rather than passed through: nlohmann orders object keys one
				// way, which is also how the footer's copy was written and how
				// cityparquet_city_field hands it back. A referenceSystem is source
				// metadata, so it goes in as a quoted literal either way -- an apostrophe
				// in it must not end the literal early and let the file's own text
				// continue the generated script.
				source_crs = Literal(json_utils::ParseJson(projjson.value()).dump());
			}
			// A referenceSystem this writer cannot resolve leaves the source CRS unknown,
			// which the rules below handle as an unknown rather than as a value.
		}
		// The tri-state rule, and the two ways of misreading a package's declared CRS,
		// live in cityparquet_sql_common alongside cityparquet_merge's use of them.
		// A CityJSON source always STATES something -- it has CRS-bearing coordinates, and
		// either a CRS this writer can resolve or an unknown one -- so unlike a package it
		// is never the "states nothing" case.
		CrsCheckWording wording;
		wording.function = "insert_cityjson";
		wording.source_noun = "the source";
		wording.source_unknown_hint = "give the source a metadata.referenceSystem, or insert into a "
		                              "package whose CRS is unknown too";
		wording.destination_unknown_hint = "give the package that CRS with cityparquet_write(..., crs => ...) "
		                                   "and reload it before inserting";
		sql += OneCrsPerPackageSQL(wording.function, schema, "destination");
		sql += CrsPreconditionSQL(wording, DeclaredCrsExpr(schema), CrsStatedExpr(schema), source_crs, "TRUE");
	}

	// The collision rule across the package (spec 06-extensions.mdx, "Collisions"):
	// what the package declares is row data, so these checks are generated rather
	// than decided here. First, an incoming core name must not start with the prefix
	// of a namespace the package already declares.
	{
		std::map<std::string, SourceName> by_candidate;
		for (const auto &entry : CoreNames(facts)) {
			const auto name = Comparable(entry);
			const auto underscore = name.find('_');
			if (underscore == std::string::npos || underscore == 0 || underscore + 1 == name.size()) {
				continue;
			}
			const auto candidate = name.substr(0, underscore);
			if (IsValidExtensionNamespace(candidate) && candidate != extension.ns) {
				by_candidate.emplace(candidate, entry);
			}
		}
		for (const auto &entry : by_candidate) {
			const auto message = "insert_cityjson: the " + std::string(entry.second.kind) + " '" + entry.second.name +
			                     "' in '" + path + "' is not an extension name, but it starts with '" + entry.first +
			                     "_', the prefix of an extension namespace the package declares; on export it "
			                     "would be indistinguishable from an extension name";
			sql += "SELECT error(" + Literal(message) + ") WHERE " + DeclaresNamespaceExpr(schema, entry.first) + ";\n";
		}
	}
	// Second, a namespace this source brings into the package must not be the prefix
	// of a core name already there -- unless the package already declares it, in which
	// case those names are this extension's own.
	if (!extension.ns.empty()) {
		const auto &declaration = extension.declarations.front();
		const auto prefix = declaration.ns + "_";
		const auto undeclared = "NOT " + DeclaresNamespaceExpr(schema, declaration.ns);
		const auto message = [&](const std::string &kind, const std::string &name_sql) {
			return Literal("insert_cityjson: the package holds the " + kind + " '") + " || " + name_sql + " || " +
			       Literal("', which is not an extension name but starts with '" + prefix +
			               "', the prefix of the extension '" + declaration.name + "' that '" + path +
			               "' declares; on export it would be indistinguishable from an extension name");
		};
		std::vector<std::string> types;
		std::vector<std::string> surfaces;
		for (const auto &table : destination_tables) {
			for (const auto &column : TableColumns(context, schema, table)) {
				const auto lowered = StringUtil::Lower(column.name);
				if (!IsReservedColumnName(column.name) && lowered.size() > prefix.size() &&
				    lowered.compare(0, prefix.size(), prefix) == 0) {
					sql +=
					    "SELECT error(" + message("attribute", Literal(column.name)) + ") WHERE " + undeclared + ";\n";
				}
				if (MatchesLodSuffix(lowered, "geometry_properties_lod") && HasSurfacesField(column.type)) {
					// Cast: a package loaded from a file that declares the JSON logical type
					// holds `surfaces` as JSON, which regexp_matches does not take.
					surfaces.push_back("SELECT CAST(" + Quoted(column.name) + ".surfaces AS VARCHAR) AS s FROM " +
					                   QualifiedName(schema, table));
				}
			}
			types.push_back("SELECT object_type AS t FROM " + QualifiedName(schema, table));
		}
		sql += "SELECT error(" + message("object type", "t") + ") FROM (" + Join(types, " UNION ALL ") +
		       ") WHERE starts_with(t, " + Literal(prefix) + ") AND " + undeclared + ";\n";
		if (!surfaces.empty()) {
			// `surfaces` is JSON text; its writers differ in whitespace, never in keys.
			sql += "SELECT error(" +
			       message("semantic surface type",
			               "regexp_extract(s, " + Literal("\"type\"\\s*:\\s*\"(" + prefix + "[^\"]*)\"") + ", 1)") +
			       ") FROM (" + Join(surfaces, " UNION ALL ") + ") WHERE regexp_matches(s, " +
			       Literal("\"type\"\\s*:\\s*\"" + prefix) + ") AND " + undeclared + ";\n";
		}
		// And the package must not already declare this namespace for another extension.
		// cityparquet_merge_extensions refuses that; running it here, before anything is
		// written, makes the refusal leave the package untouched.
		sql += "SELECT COUNT(cityparquet_merge_extensions(city, " +
		       Literal(DeclarationsToFooterJson(extension.declarations).dump()) + ")) FROM " +
		       QualifiedName(schema, "__cityparquet") + " WHERE role = 'object';\n";
	}

	// ---- Phase 3: schema evolution, before any INSERT -----------------------
	PendingTables pending;
	// Per destination table, the geometry columns it holds as DuckDB-native GEOMETRY
	// (a GeoParquet-declared column comes back that way from cityparquet_read) where the
	// staged source carries WKB BLOB. The column keeps the package's type; the routed
	// INSERT converts the staged WKB into it.
	std::map<std::string, std::vector<std::string>> geometry_from_wkb;
	std::vector<ColumnInfo> incoming_geometry_columns;
	bool incoming_has_bbox = false;
	bool incoming_has_children_roles = false;
	for (const auto &column : facts.columns) {
		const auto lowered = StringUtil::Lower(column.name);
		if (MatchesLodSuffix(lowered, "geometry_lod")) {
			// A source column freshly staged from CityJSON is always written as WKB
			// BLOB (ColumnTypeUtils::ToDuckDBType), never DuckDB-native GEOMETRY --
			// that promotion only happens on a `read_parquet` of a GeoParquet-declared
			// column -- but the type travels alongside the name regardless, so BboxPhase
			// never has to special-case where a PendingTable came from.
			incoming_geometry_columns.push_back({column.name, ColumnTypeUtils::ToDuckDBType(column.kind)});
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
			if (match->type.id() == LogicalTypeId::GEOMETRY && type.id() == LogicalTypeId::BLOB) {
				geometry_from_wkb[table].push_back(column.name);
				continue;
			}
			const auto widened = WidenedType(match->type, type, "insert_cityjson", column.name);
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
					entry.geometry_columns.push_back(column);
				} else if (lowered == "bbox") {
					entry.has_bbox = true;
				} else if (lowered == "children_roles") {
					entry.has_children_roles = true;
				}
			}
			pending[table] = std::move(entry);
		}
	}

	// The extension's declaration goes onto every object table's footer, those this
	// insert just registered included: each file that carries an extension name must
	// declare its namespace, and every file declares it the same way.
	if (!extension.ns.empty()) {
		sql += "UPDATE " + QualifiedName(schema, "__cityparquet") + " SET city = cityparquet_merge_extensions(city, " +
		       Literal(DeclarationsToFooterJson(extension.declarations).dump()) + ") WHERE role = 'object';\n";
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
			// implicit_geometries sidecar carries per-LoD columns, so a second file whose
			// relative geometries use a different LoD brings columns the destination has
			// never seen, and INSERT ... BY NAME rejects a source column with no
			// destination match.
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
		replacements.push_back("cityjson_shift_appearance_ids(" + Quoted(column.name) + ", " + OffsetExpr(sidecar) +
		                       ") AS " + Quoted(column.name));
	}
	// An object's implicit geometry references a relative geometry whose id the sidecar
	// insert below shifts, so the reference moves with it.
	if (!facts.geometry_templates.Empty()) {
		replacements.push_back(ShiftedImplicitGeometry(OffsetExpr("implicit_geometries")) + " AS implicit_geometry");
	}
	for (const auto &entry : types_by_module) {
		// ST_GeomFromWKB ships in DuckDB core, like the ST_AsWKB GeometryColumnRef emits.
		auto table_replacements = replacements;
		const auto promoted = geometry_from_wkb.find(entry.first);
		if (promoted != geometry_from_wkb.end()) {
			for (const auto &name : promoted->second) {
				table_replacements.push_back("ST_GeomFromWKB(" + Quoted(name) + ") AS " + Quoted(name));
			}
		}
		const std::string projection =
		    table_replacements.empty() ? "*" : "* REPLACE (" + Join(table_replacements, ", ") + ")";
		std::vector<std::string> literals;
		literals.reserve(entry.second.size());
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
		// A relative geometry holds appearance of its own, so its material and texture
		// references need the same shift the object rows got. Miss this and a relative
		// geometry silently renders with whichever definition the destination already
		// had at that id -- the sidecar rows would move while their references stayed
		// behind.
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
			sidecar_replacements.push_back("cityjson_shift_appearance_ids(" + Quoted(column.name) + ", " +
			                               OffsetExpr(target) + ") AS " + Quoted(column.name));
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
		    return StringVector::AddString(result, BuildInsertSQL(context, schema.GetString(), path.GetString(),
		                                                          "read_cityjson", InsertOptions()));
	    });
}

void RegisterOne(ExtensionLoader &loader, const char *name, pragma_query_t query, const char *source,
                 const char *example) {
	auto pragma = PragmaFunction::PragmaCall(
	    name, query, {LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR)});
	pragma.named_parameters["create_tables"] = LogicalType(LogicalTypeId::BOOLEAN);
	pragma.named_parameters["tables"] = LogicalType::LIST(LogicalType(LogicalTypeId::VARCHAR));
	pragma.named_parameters["lod"] = LogicalType(LogicalTypeId::VARCHAR);
	pragma.named_parameters["sample_lines"] = LogicalType(LogicalTypeId::BIGINT);
	RegisterDocumented(loader, std::move(pragma),
	                   {{"schema", "path"},
	                    std::string("Inserts a ") + source +
	                        " file into a CityParquet package schema, routing each object to its CityGML module table "
	                        "and renumbering sidecar ids; refuses an id collision or a CRS mismatch.",
	                    example,
	                    {"cityparquet", "package"}});
}

} // namespace

void RegisterCityParquetInsertFunctions(ExtensionLoader &loader) {
	RegisterOne(loader, "insert_cityjson", PragmaInsert<kReadCityJSON>, "CityJSON",
	            "PRAGMA insert_cityjson('delft', 'tile.city.json', create_tables = true);");
	RegisterOne(loader, "insert_cityjsonseq", PragmaInsert<kReadCityJSONSeq>, "CityJSONSeq",
	            "PRAGMA insert_cityjsonseq('delft', 'tile.city.jsonl', create_tables = true);");
	RegisterOne(loader, "insert_flatcitybuf", PragmaInsert<kReadFlatCityBuf>, "FlatCityBuf",
	            "PRAGMA insert_flatcitybuf('delft', 'tile.fcb', create_tables = true);");

	ScalarFunction insert_sql("insert_cityjson_sql",
	                          {LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR)},
	                          LogicalType(LogicalTypeId::VARCHAR), InsertSQLScalar);
	RegisterDocumented(loader, std::move(insert_sql),
	                   {{"schema", "path"},
	                    "Returns the SQL that PRAGMA insert_cityjson would run, without running it.",
	                    "insert_cityjson_sql('delft', 'tile.city.json')",
	                    {"cityparquet", "package"}});
}

} // namespace cityjson
} // namespace duckdb
