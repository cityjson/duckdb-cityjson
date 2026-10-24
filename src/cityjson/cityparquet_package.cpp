#include "cityjson/cityparquet_sql_common.hpp"
#include "cityjson/cityparquet_package.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/common/vector/vector_writer.hpp"

#include "cityjson/column_types.hpp"
#include "cityjson/function_docs.hpp"
#include "cityjson/json_utils.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/sql_identifier.hpp"

#include <algorithm>
#include <map>
#include <set>

namespace duckdb {
namespace cityjson {

const std::vector<std::string> &ModuleTableNames() {
	// The CityGML 3.0 modules that hold feature objects. CityObjectGroup is not a
	// thematic feature and gets no file of its own -- it folds into generics.
	static const std::vector<std::string> names = {"building",       "bridge",         "tunnel",  "construction",
	                                               "transportation", "vegetation",     "relief",  "water_body",
	                                               "land_use",       "city_furniture", "generics"};
	return names;
}

const std::vector<std::string> &SidecarTableNames() {
	static const std::vector<std::string> names = {"materials", "textures", "implicit_geometries"};
	return names;
}

std::string ModuleForObjectType(const std::string &object_type) {
	// The specification's by-module table. Keyed on the CityGML 3.0 class name, but the
	// four CityJSON spellings that differ are accepted too, because read_cityjson emits
	// the source spelling; insert_cityjson rewrites it to the CityGML class at the
	// staging boundary.
	static const std::map<std::string, std::string> modules = {
	    {"building", "building"},
	    {"buildingpart", "building"},
	    {"buildinginstallation", "building"},
	    {"buildingconstructiveelement", "building"},
	    {"buildingfurniture", "building"},
	    {"storey", "building"},
	    {"buildingstorey", "building"}, // CityJSON spelling of Storey
	    {"buildingroom", "building"},
	    {"buildingunit", "building"},
	    {"bridge", "bridge"},
	    {"bridgepart", "bridge"},
	    {"bridgeinstallation", "bridge"},
	    {"bridgeconstructiveelement", "bridge"},
	    {"bridgeroom", "bridge"},
	    {"bridgefurniture", "bridge"},
	    {"tunnel", "tunnel"},
	    {"tunnelpart", "tunnel"},
	    {"tunnelinstallation", "tunnel"},
	    {"tunnelconstructiveelement", "tunnel"},
	    {"hollowspace", "tunnel"},
	    {"tunnelhollowspace", "tunnel"}, // CityJSON spelling of HollowSpace
	    {"tunnelfurniture", "tunnel"},
	    {"otherconstruction", "construction"},
	    {"road", "transportation"},
	    {"railway", "transportation"},
	    {"waterway", "transportation"},
	    {"square", "transportation"},
	    {"transportsquare", "transportation"}, // CityJSON spelling of Square
	    {"plantcover", "vegetation"},
	    {"solitaryvegetationobject", "vegetation"},
	    {"tinrelief", "relief"},
	    {"waterbody", "water_body"},
	    {"landuse", "land_use"},
	    {"cityfurniture", "city_furniture"},
	    {"genericoccupiedspace", "generics"},
	    {"genericcityobject", "generics"}, // CityJSON spelling of GenericOccupiedSpace
	    {"cityobjectgroup", "generics"},
	};
	auto found = modules.find(StringUtil::Lower(object_type));
	// Routing is total by specification: a class that resolves to no module is a hard
	// error, never a silent drop. Extension types are not resolvable here -- doing so
	// needs the document's `extensions` declarations, which this mapping does not see.
	return found == modules.end() ? std::string() : found->second;
}

std::string CityGMLClassForCityJSONType(const std::string &cityjson_type) {
	static const std::map<std::string, std::string> remapped = {
	    {"TransportSquare", "Square"},
	    {"GenericCityObject", "GenericOccupiedSpace"},
	    {"BuildingStorey", "Storey"},
	    {"TunnelHollowSpace", "HollowSpace"},
	};
	auto found = remapped.find(cityjson_type);
	return found == remapped.end() ? cityjson_type : found->second;
}

std::string CityJSONTypeForCityGMLClass(const std::string &citygml_class) {
	static const std::map<std::string, std::string> remapped = {
	    {"Square", "TransportSquare"},
	    {"GenericOccupiedSpace", "GenericCityObject"},
	    {"Storey", "BuildingStorey"},
	    {"HollowSpace", "TunnelHollowSpace"},
	};
	auto found = remapped.find(citygml_class);
	return found == remapped.end() ? citygml_class : found->second;
}

std::string QualifiedName(const std::string &schema, const std::string &table) {
	return SQLIdentifier::ToString(schema) + "." + SQLIdentifier::ToString(table);
}

std::string QualifiedName(const std::string &catalog, const std::string &schema, const std::string &table) {
	return SQLIdentifier::ToString(catalog) + "." + SQLIdentifier::ToString(schema) + "." +
	       SQLIdentifier::ToString(table);
}

std::string Literal(const std::string &text) {
	return SQLString::ToString(text);
}

namespace {

//! Lower-cased names of the tables that exist in `schema`.
std::set<std::string> TablesInSchema(ClientContext &context, const std::string &schema) {
	std::set<std::string> present;
	auto &schema_entry = Catalog::GetSchema(context, Identifier::InvalidCatalog(), Identifier(schema));
	schema_entry.Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
		if (entry.type == CatalogType::TABLE_ENTRY) {
			present.insert(StringUtil::Lower(entry.name.GetIdentifierName()));
		}
	});
	return present;
}

std::vector<std::string> Intersect(const std::set<std::string> &present, const std::vector<std::string> &candidates) {
	std::vector<std::string> found;
	for (const auto &name : candidates) {
		if (present.count(name) > 0) {
			found.push_back(name);
		}
	}
	std::sort(found.begin(), found.end());
	return found;
}

} // namespace

bool MatchesLodSuffix(const std::string &name, const std::string &prefix) {
	static const std::string lod = "_lod";
	if (prefix.size() <= lod.size() || prefix.compare(prefix.size() - lod.size(), lod.size(), lod) != 0) {
		return false;
	}
	return IsPerLodColumnName(name, prefix.substr(0, prefix.size() - lod.size()));
}

namespace {

//! Column names of `schema.table` matching `prefix` + the LoD suffix grammar.
std::vector<std::string> ColumnsWithPrefix(ClientContext &context, const std::string &schema, const std::string &table,
                                           const std::string &prefix) {
	std::vector<std::string> found;
	auto &catalog_entry = LookupTable(context, schema, table);
	auto &entry = catalog_entry.Cast<TableCatalogEntry>();
	for (auto &column : entry.GetColumns().Logical()) {
		if (MatchesLodSuffix(StringUtil::Lower(column.Name().GetIdentifierName()), prefix)) {
			found.push_back(column.Name().GetIdentifierName());
		}
	}
	return found;
}

} // namespace

//! The tables `schema.__cityparquet` registers as object tables: the module tables, and
//! an extension's own files (06-extensions.mdx, "The ModuleKey"), whose names no fixed
//! list holds. Empty when the schema has no bookkeeping table.
static std::vector<std::string> RegisteredObjectTables(ClientContext &context, const std::string &schema,
                                                       const std::set<std::string> &present) {
	std::vector<std::string> registered;
	if (present.count("__cityparquet") == 0) {
		return registered;
	}
	Connection connection(DatabaseInstance::GetDatabase(context));
	auto result = connection.Query("SELECT DISTINCT lower(table_name) FROM " + QualifiedName(schema, "__cityparquet") +
	                               " WHERE role = 'object' AND table_name IS NOT NULL");
	if (result->HasError()) {
		return registered;
	}
	for (auto &row : result->Collection().GetRows()) {
		registered.push_back(row.GetValue(0).ToString());
	}
	return registered;
}

std::vector<std::string> ObjectTablesInSchema(ClientContext &context, const std::string &schema, bool allow_none) {
	const auto present = TablesInSchema(context, schema);
	auto candidates = ModuleTableNames();
	for (const auto &table : RegisteredObjectTables(context, schema, present)) {
		const auto &sidecars = SidecarTableNames();
		if (std::find(candidates.begin(), candidates.end(), table) == candidates.end() &&
		    std::find(sidecars.begin(), sidecars.end(), table) == sidecars.end()) {
			candidates.push_back(table);
		}
	}
	auto found = Intersect(present, candidates);
	if (found.empty() && !allow_none) {
		throw BinderException("cityparquet: schema '%s' has no CityParquet object table "
		                      "(expected one of building, bridge, tunnel, construction, transportation, "
		                      "vegetation, relief, water_body, land_use, city_furniture, generics)",
		                      schema);
	}
	return found;
}

std::vector<std::string> SidecarTablesInSchema(ClientContext &context, const std::string &schema) {
	return Intersect(TablesInSchema(context, schema), SidecarTableNames());
}

std::vector<std::string> GeometryLodColumns(ClientContext &context, const std::string &schema,
                                            const std::string &table) {
	// A plain prefix test is unambiguous here: the companion column is named
	// `geometry_properties_lod*`, which begins "geometry_p", not "geometry_lod".
	return ColumnsWithPrefix(context, schema, table, "geometry_lod");
}

std::vector<std::string> AppearanceLodColumns(ClientContext &context, const std::string &schema,
                                              const std::string &table, const std::string &prefix) {
	return ColumnsWithPrefix(context, schema, table, prefix);
}

bool HasColumn(ClientContext &context, const std::string &schema, const std::string &table, const std::string &column) {
	auto &entry = LookupTable(context, schema, table);
	for (auto &existing : entry.Cast<TableCatalogEntry>().GetColumns().Logical()) {
		if (StringUtil::Lower(existing.Name().GetIdentifierName()) == column) {
			return true;
		}
	}
	return false;
}

std::string AllObjectsCTE(ClientContext &context, const std::string &schema,
                          const std::vector<std::string> &object_tables,
                          const std::map<std::string, bool> &known_children_roles) {
	std::string cte = "all_objects AS (\n";
	for (idx_t i = 0; i < object_tables.size(); i++) {
		if (i > 0) {
			cte += "  UNION ALL\n";
		}
		// `children_roles` is optional and a table read in `lod =` mode does not carry it
		// at all. Substituting NULL keeps every consumer of this CTE working against
		// whatever shape the package really has, rather than the shape the wide reader
		// happens to produce.
		auto known = known_children_roles.find(object_tables[i]);
		const bool has_roles = known != known_children_roles.end()
		                           ? known->second
		                           : HasColumn(context, schema, object_tables[i], "children_roles");
		const auto roles = has_roles ? std::string("children_roles") : std::string("NULL::VARCHAR[] AS children_roles");
		cte += "  SELECT " + Literal(object_tables[i]) + " AS __tbl, id, feature_id, parents, children, " + roles +
		       " FROM " + QualifiedName(schema, object_tables[i]) + "\n";
	}
	cte += ")";
	return cte;
}

namespace {

std::string BuildInitSQL(ClientContext &context, const std::string &schema) {
	// An empty schema initialises too: it is how a package is started, and the first
	// insert creates the module tables it needs.
	auto object_tables = ObjectTablesInSchema(context, schema, true);
	auto sidecar_tables = SidecarTablesInSchema(context, schema);

	const auto bookkeeping = QualifiedName(schema, "__cityparquet");
	std::string sql;
	// `city` is VARCHAR holding JSON text, not the JSON type: the JSON type's name and
	// functions live in the json extension, which this one does not require, and
	// `city` is only ever read through cityparquet_city_field.
	sql += "CREATE TABLE IF NOT EXISTS " + bookkeeping +
	       " (table_name VARCHAR, file_name VARCHAR, role VARCHAR, city VARCHAR);\n";

	// Idempotent by construction: re-running must not duplicate rows, and must pick up
	// tables created since the last call. `city` stays NULL -- a hand-rolled load has
	// discarded the footer it would have come from, and inventing one would be worse
	// than admitting we do not know it.
	auto emit = [&](const std::string &table, const char *role) {
		sql += "INSERT INTO " + bookkeeping + " (table_name, file_name, role, city) SELECT " + Literal(table) + ", " +
		       Literal(table + ".parquet") + ", " + Literal(std::string(role)) +
		       ", NULL WHERE NOT EXISTS (SELECT 1 FROM " + bookkeeping + " WHERE table_name = " + Literal(table) +
		       ");\n";
	};
	for (const auto &table : object_tables) {
		emit(table, "object");
	}
	for (const auto &table : sidecar_tables) {
		emit(table, "sidecar");
	}
	return sql;
}

//! Read one field out of a stored `city` footer object as text. The JSON type and
//! json_extract live in the json extension, which this one does not require, so the
//! parsing is done here with the vendored nlohmann::json. A nested value is returned as
//! its compact JSON text, which is enough to compare two CRSs for equality.
//!
//! An absent field and an explicit JSON `null` both come back as SQL NULL. For `crs` the
//! two are formally different -- absent means OGC:CRS84, null means "unknown" (spec
//! 05-metadata.mdx, "CRS rules") -- but no caller needs to tell them apart, because
//! nothing in this extension consumes a CRS: it is compared for equality (the merge and
//! insert mismatch checks, which skip on NULL) and re-emitted, never used to transform a
//! coordinate. The day something does reproject, it must read the raw footer, not this.
void SetCityFieldFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto cities = args.data[0].Values<string_t>();
	auto fields = args.data[1].Values<string_t>();
	auto values = args.data[2].Values<string_t>();
	auto writer = FlatVector::Writer<string_t>(result, args.size());
	for (idx_t row = 0; row < args.size(); row++) {
		const auto city_entry = cities[row];
		const auto field = fields[row];
		const auto value_text = values[row];
		if (!field.IsValid() || !value_text.IsValid()) {
			writer.WriteNull();
			continue;
		}
		json city = json::object();
		if (city_entry.IsValid()) {
			try {
				auto parsed = json_utils::ParseJson(city_entry.GetValue().GetString());
				if (parsed.is_object()) {
					city = std::move(parsed);
				}
			} catch (const std::exception &) {
				// unreadable: start from an empty object
			}
		}
		json value;
		try {
			value = json_utils::ParseJson(value_text.GetValue().GetString());
		} catch (const std::exception &e) {
			throw InvalidInputException("cityparquet_set_city_field: the value is not JSON: %s", e.what());
		}
		city[field.GetValue().GetString()] = std::move(value);
		writer.WriteValue(string_t(city.dump()));
	}
}

void CityFieldFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    args.data[0], args.data[1], result, [&](string_t city, string_t field) -> optional<string_t> {
		    json parsed;
		    try {
			    parsed = json_utils::ParseJson(city.GetString());
		    } catch (const std::exception &) {
			    return {};
		    }
		    if (!parsed.is_object()) {
			    return {};
		    }
		    auto entry = parsed.find(field.GetString());
		    if (entry == parsed.end() || entry->is_null()) {
			    return {};
		    }
		    const auto text = entry->is_string() ? entry->get<std::string>() : entry->dump();
		    return StringVector::AddString(result, text);
	    });
}

std::string PragmaRead(ClientContext &context, const FunctionParameters &parameters) {
	return BuildReadSQL(context, parameters.values[0].ToString(), parameters.values[1].ToString());
}

std::string PragmaInit(ClientContext &context, const FunctionParameters &parameters) {
	return BuildInitSQL(context, parameters.values[0].ToString());
}

void InitSQLScalar(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t schema) {
		return StringVector::AddString(result, BuildInitSQL(context, schema.GetString()));
	});
}

} // namespace

namespace {

//! A Parquet file's top-level column names as the file spells them, in order: the
//! root's children in parquet_schema's depth-first listing, each nested subtree skipped.
std::vector<std::string> FileColumnNames(Connection &connection, const std::string &path) {
	auto result = connection.Query("SELECT name, COALESCE(num_children, 0) FROM parquet_schema(" + Literal(path) + ")");
	if (result->HasError()) {
		throw IOException("cityparquet_read: cannot read the schema of '%s': %s", path, result->GetError());
	}
	std::vector<std::pair<std::string, int64_t>> rows;
	for (auto &row : result->Collection().GetRows()) {
		rows.emplace_back(row.GetValue(0).ToString(), row.GetValue(1).GetValue<int64_t>());
	}
	// Past the subtree rooted at `at`, iteratively: a footer can declare nesting as deep
	// as it likes in its flat element list, so a recursion per level would hand the stack
	// to the file. `pending` counts the subtree's elements not yet stepped over.
	const auto skip = [&rows](size_t at) {
		size_t pending = 1;
		while (pending > 0 && at < rows.size()) {
			pending += static_cast<size_t>(std::max<int64_t>(rows[at].second, 0));
			pending--;
			at++;
		}
		return at;
	};
	std::vector<std::string> names;
	if (rows.empty()) {
		return names;
	}
	size_t at = 1;
	for (int64_t child = 0; child < rows[0].second && at < rows.size(); child++) {
		names.push_back(rows[at].first);
		at = skip(at);
	}
	return names;
}

//! Refuses an object table holding a column whose own name collides with a reserved
//! one but is not it -- `ID` beside `id`. A conforming writer stores such an attribute
//! escaped (`_ID`, spec 02-object-table-schema.mdx, "Column naming and reservation
//! rules"), and DuckDB, whose column names are case-insensitive, could only hold it
//! renamed, so it would be written back under a name the source never had.
void RefuseCollidingColumns(Connection &connection, const std::string &path) {
	for (const auto &name : FileColumnNames(connection, path)) {
		const auto lowered = StringUtil::Lower(name);
		if (name != lowered && IsReservedColumnName(name)) {
			throw InvalidInputException(
			    "cityparquet_read: '%s' is not a valid CityParquet object table: the column '%s' collides with the "
			    "reserved column '%s' (names compare case-insensitively); a writer stores a colliding attribute as "
			    "'_%s' (spec 02-object-table-schema.mdx, \"Column naming and reservation rules\")",
			    path, name, lowered, name);
		}
	}
}

//! True when the file `table`.parquet at `path` is an extension's object table
//! (06-extensions.mdx, "The ModuleKey"): its `city` footer declares an extension whose
//! namespace is the file's name (the extension's own file, `noise.parquet`) or prefixes
//! it (an extension module's file, `energy_building_physics.parquet`). A file this
//! specification does not name is left alone.
bool IsExtensionObjectFile(Connection &connection, const std::string &path, const std::string &table) {
	auto result = connection.Query("SELECT decode(value) FROM parquet_kv_metadata(" + Literal(path) +
	                               ") WHERE decode(key) = 'city'");
	if (result->HasError() || result->RowCount() == 0) {
		return false;
	}
	const auto value = result->Collection().GetValue(0, 0);
	if (value.IsNull()) {
		return false;
	}
	json city;
	try {
		city = json_utils::ParseJson(value.ToString());
	} catch (const std::exception &) {
		return false;
	}
	const auto extensions = city.find("extensions");
	if (!city.is_object() || extensions == city.end() || !extensions->is_object()) {
		return false;
	}
	for (const auto &entry : extensions->items()) {
		const auto &ns = entry.key();
		if (table == ns || (table.size() > ns.size() + 1 && table.compare(0, ns.size() + 1, ns + "_") == 0)) {
			return true;
		}
	}
	return false;
}

} // namespace

std::string BuildReadSQL(ClientContext &context, const std::string &directory_arg, const std::string &schema) {
	auto &fs = FileSystem::GetFileSystem(context);
	// `pkg/` and `pkg` name the same package; without this, the file paths below
	// would carry a doubled separator.
	auto directory = directory_arg;
	while (directory.size() > 1 && (directory.back() == '/' || directory.back() == '\\')) {
		directory.pop_back();
	}

	// The file list comes from a glob at plan time -- no SQL, no data read. A glob,
	// not a directory listing: under DuckDB-Wasm a package is a set of files
	// registered by name, with no directory entry to list or test for, and the glob
	// is what finds them. Only files this specification names are adopted; anything
	// else is left alone rather than guessed at.
	std::set<std::string> known;
	for (const auto &name : ModuleTableNames()) {
		known.insert(name);
	}
	for (const auto &name : SidecarTableNames()) {
		known.insert(name);
	}

	Connection connection(DatabaseInstance::GetDatabase(context));
	std::vector<std::string> found;
	std::set<std::string> extension_tables;
	for (const auto &info : fs.Glob(fs.JoinPath(directory, "*.parquet"))) {
		const auto name = fs.ExtractName(info.path);
		const auto suffix = std::string(".parquet");
		if (name.size() <= suffix.size() || name.compare(name.size() - suffix.size(), suffix.size(), suffix) != 0) {
			continue;
		}
		const auto table = StringUtil::Lower(name.substr(0, name.size() - suffix.size()));
		if (std::find(found.begin(), found.end(), table) != found.end()) {
			continue;
		}
		if (known.count(table) > 0) {
			found.push_back(table);
		} else if (IsExtensionObjectFile(connection, info.path, table)) {
			found.push_back(table);
			extension_tables.insert(table);
		}
	}
	if (found.empty()) {
		if (!fs.DirectoryExists(directory)) {
			throw BinderException("cityparquet_read: no such directory '%s'", directory_arg);
		}
		throw BinderException("cityparquet_read: '%s' contains no CityParquet object table or sidecar", directory_arg);
	}
	std::sort(found.begin(), found.end());

	const auto bookkeeping = QualifiedName(schema, "__cityparquet");
	std::string sql = "CREATE SCHEMA IF NOT EXISTS " + SQLIdentifier::ToString(schema) + ";\n";
	sql += "CREATE OR REPLACE TABLE " + bookkeeping +
	       " (table_name VARCHAR, file_name VARCHAR, role VARCHAR, city VARCHAR);\n";

	for (const auto &table : found) {
		const auto file = table + ".parquet";
		const auto path = fs.JoinPath(directory, file);
		const bool is_object =
		    std::find(ModuleTableNames().begin(), ModuleTableNames().end(), table) != ModuleTableNames().end() ||
		    extension_tables.count(table) > 0;
		if (is_object) {
			RefuseCollidingColumns(connection, path);
		}
		sql += "CREATE OR REPLACE TABLE " + QualifiedName(schema, table) + " AS SELECT * FROM read_parquet(" +
		       Literal(path) + ");\n";
		// decode(), not a cast: parquet_kv_metadata returns BLOB, and casting it to
		// VARCHAR escapes bytes so the JSON no longer parses.
		sql += "INSERT INTO " + bookkeeping + " (table_name, file_name, role, city) SELECT " + Literal(table) + ", " +
		       Literal(file) + ", " + Literal(std::string(is_object ? "object" : "sidecar")) +
		       ", (SELECT decode(value) FROM parquet_kv_metadata(" + Literal(path) + ") WHERE decode(key) = 'city');\n";
	}
	return sql;
}

void RegisterCityParquetPackageFunctions(ExtensionLoader &loader) {
	RegisterDocumented(
	    loader,
	    PragmaFunction::PragmaCall("cityparquet_read", PragmaRead,
	                               {LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR)}),
	    {{"path", "schema"},
	     "Loads a CityParquet package directory into a schema, creating it if needed and replacing any "
	     "same-named table, one table per file, with each file's Parquet footer in __cityparquet.",
	     "PRAGMA cityparquet_read('./data/delft', 'delft');",
	     {"cityparquet", "package"}});

	RegisterDocumented(
	    loader, PragmaFunction::PragmaCall("cityparquet_init", PragmaInit, {LogicalType(LogicalTypeId::VARCHAR)}),
	    {{"schema"},
	     "Registers a schema's CityParquet object and sidecar tables in its __cityparquet bookkeeping table; "
	     "idempotent, and unlike cityparquet_read it recovers no Parquet footers.",
	     "PRAGMA cityparquet_init('delft');",
	     {"cityparquet", "package"}});

	ScalarFunction init_sql("cityparquet_init_sql", {LogicalType(LogicalTypeId::VARCHAR)},
	                        LogicalType(LogicalTypeId::VARCHAR), InitSQLScalar);
	RegisterDocumented(loader, std::move(init_sql),
	                   {{"schema"},
	                    "Returns the SQL that PRAGMA cityparquet_init would run for a schema, without running it.",
	                    "cityparquet_init_sql('delft')",
	                    {"cityparquet", "package"}});

	ScalarFunction set_city_field(
	    "cityparquet_set_city_field",
	    {LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR)},
	    LogicalType(LogicalTypeId::VARCHAR), SetCityFieldFunction);
	set_city_field.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	RegisterDocumented(loader, std::move(set_city_field),
	                   {{"city", "field", "value"},
	                    "Returns a CityParquet city footer JSON string with one top-level field set to the JSON "
	                    "text `value`; a NULL or unreadable footer starts as an empty object.",
	                    R"(cityparquet_set_city_field(NULL, 'crs', '{"id": {"authority": "EPSG", "code": 7415}}'))",
	                    {"cityparquet", "metadata"}});

	ScalarFunction city_field("cityparquet_city_field",
	                          {LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR)},
	                          LogicalType(LogicalTypeId::VARCHAR), CityFieldFunction);
	RegisterDocumented(
	    loader, std::move(city_field),
	    {{"city", "field"},
	     "Returns one top-level field of a CityParquet city footer JSON string as text, or NULL when the "
	     "footer is not a JSON object or the field is absent or null.",
	     R"(cityparquet_city_field('{"source_format": "CityJSONSeq"}', 'source_format'))",
	     {"cityparquet", "metadata"}});
}

} // namespace cityjson
} // namespace duckdb
