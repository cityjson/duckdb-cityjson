#include "cityjson/cityparquet_package.hpp"

#include "cityjson/json_utils.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/keyword_helper.hpp"

#include <algorithm>
#include <map>
#include <set>

namespace duckdb {
namespace cityjson {

const std::vector<std::string> &ModuleTableNames() {
	// The CityGML 3.0 modules that hold feature objects. CityObjectGroup is not a
	// thematic feature and gets no file of its own -- it folds into generics.
	static const std::vector<std::string> names = {"building",      "bridge",         "tunnel",
	                                               "construction",  "transportation", "vegetation",
	                                               "relief",        "water_body",     "land_use",
	                                               "city_furniture", "generics"};
	return names;
}

const std::vector<std::string> &SidecarTableNames() {
	static const std::vector<std::string> names = {"materials", "textures", "geometry_templates"};
	return names;
}

std::string ModuleForObjectType(const std::string &object_type) {
	// The specification's by-module table. Keyed on the CityGML 3.0 class name, but the
	// four CityJSON spellings that differ are accepted too, because that is what the
	// reader emits -- it does not currently rewrite the type on import.
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

std::string QualifiedName(const std::string &schema, const std::string &table) {
	return KeywordHelper::WriteOptionallyQuoted(schema) + "." + KeywordHelper::WriteOptionallyQuoted(table);
}

std::string Literal(const std::string &text) {
	return KeywordHelper::WriteQuoted(text, '\'');
}

namespace {

//! Lower-cased names of the tables that exist in `schema`.
std::set<std::string> TablesInSchema(ClientContext &context, const std::string &schema) {
	std::set<std::string> present;
	auto &schema_entry = Catalog::GetSchema(context, INVALID_CATALOG, schema);
	schema_entry.Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
		if (entry.type == CatalogType::TABLE_ENTRY) {
			present.insert(StringUtil::Lower(entry.name));
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
	if (name.size() <= prefix.size() || name.compare(0, prefix.size(), prefix) != 0) {
		return false;
	}
	const auto rest = name.substr(prefix.size());
	size_t i = 0;
	auto digits = [&]() {
		const auto start = i;
		while (i < rest.size() && rest[i] >= '0' && rest[i] <= '9') {
			i++;
		}
		return i > start;
	};
	if (!digits()) {
		return false;
	}
	if (i < rest.size()) {
		if (rest[i] != '_') {
			return false;
		}
		i++;
		if (!digits()) {
			return false;
		}
	}
	return i == rest.size();
}

namespace {

//! Column names of `schema.table` matching `prefix` + the LoD suffix grammar.
std::vector<std::string> ColumnsWithPrefix(ClientContext &context, const std::string &schema, const std::string &table,
                                           const std::string &prefix) {
	// The non-templated GetEntry, deliberately: Catalog::GetEntry<TableCatalogEntry>
	// binds a reference to TableCatalogEntry::Name for its error message, which
	// ODR-uses that static member and emits a comdat definition colliding at link time
	// with DuckDB's own strong definition. Same trap as LogicalType::DOUBLE in
	// wkb_extent.cpp.
	std::vector<std::string> found;
	auto &catalog_entry =
	    Catalog::GetEntry(context, CatalogType::TABLE_ENTRY, INVALID_CATALOG, schema, table);
	auto &entry = catalog_entry.Cast<TableCatalogEntry>();
	for (auto &column : entry.GetColumns().Logical()) {
		if (MatchesLodSuffix(StringUtil::Lower(column.Name()), prefix)) {
			found.push_back(column.Name());
		}
	}
	return found;
}

} // namespace

std::vector<std::string> ObjectTablesInSchema(ClientContext &context, const std::string &schema) {
	auto found = Intersect(TablesInSchema(context, schema), ModuleTableNames());
	if (found.empty()) {
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

std::string AllObjectsCTE(const std::string &schema, const std::vector<std::string> &object_tables) {
	std::string cte = "all_objects AS (\n";
	for (idx_t i = 0; i < object_tables.size(); i++) {
		if (i > 0) {
			cte += "  UNION ALL\n";
		}
		cte += "  SELECT " + Literal(object_tables[i]) +
		       " AS __tbl, id, feature_id, parents, children, children_roles FROM " +
		       QualifiedName(schema, object_tables[i]) + "\n";
	}
	cte += ")";
	return cte;
}

namespace {

std::string BuildInitSQL(ClientContext &context, const std::string &schema) {
	auto object_tables = ObjectTablesInSchema(context, schema);
	auto sidecar_tables = SidecarTablesInSchema(context, schema);

	const auto bookkeeping = QualifiedName(schema, "__cityparquet");
	std::string sql;
	// `city` is VARCHAR holding JSON text, not the JSON type: the JSON type lives in
	// the json extension, which this one does not require, and the rest of this
	// extension already carries JSON as VARCHAR (material_lod*, texture_lod*, other).
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
void CityFieldFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	BinaryExecutor::ExecuteWithNulls<string_t, string_t, string_t>(
	    args.data[0], args.data[1], result, args.size(),
	    [&](string_t city, string_t field, ValidityMask &mask, idx_t idx) {
		    json parsed;
		    try {
			    parsed = json::parse(city.GetString());
		    } catch (const std::exception &) {
			    mask.SetInvalid(idx);
			    return string_t();
		    }
		    if (!parsed.is_object()) {
			    mask.SetInvalid(idx);
			    return string_t();
		    }
		    auto entry = parsed.find(field.GetString());
		    if (entry == parsed.end() || entry->is_null()) {
			    mask.SetInvalid(idx);
			    return string_t();
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

std::string BuildReadSQL(ClientContext &context, const std::string &directory, const std::string &schema) {
	auto &fs = FileSystem::GetFileSystem(context);
	if (!fs.DirectoryExists(directory)) {
		throw BinderException("cityparquet_read: no such directory '%s'", directory);
	}

	// The file list comes from a directory listing at plan time -- no SQL, no data read.
	// Only files this specification names are adopted; anything else in the directory is
	// left alone rather than guessed at.
	std::set<std::string> known;
	for (const auto &name : ModuleTableNames()) {
		known.insert(name);
	}
	for (const auto &name : SidecarTableNames()) {
		known.insert(name);
	}

	std::vector<std::string> found;
	fs.ListFiles(directory, [&](const std::string &name, bool) {
		const auto suffix = std::string(".parquet");
		if (name.size() <= suffix.size() || name.compare(name.size() - suffix.size(), suffix.size(), suffix) != 0) {
			return;
		}
		const auto table = StringUtil::Lower(name.substr(0, name.size() - suffix.size()));
		if (known.count(table) > 0) {
			found.push_back(table);
		}
	});
	if (found.empty()) {
		throw BinderException("cityparquet_read: '%s' contains no CityParquet object table or sidecar", directory);
	}
	std::sort(found.begin(), found.end());

	const auto bookkeeping = QualifiedName(schema, "__cityparquet");
	std::string sql = "CREATE SCHEMA IF NOT EXISTS " + KeywordHelper::WriteOptionallyQuoted(schema) + ";\n";
	sql += "CREATE OR REPLACE TABLE " + bookkeeping +
	       " (table_name VARCHAR, file_name VARCHAR, role VARCHAR, city VARCHAR);\n";

	for (const auto &table : found) {
		const auto file = table + ".parquet";
		const auto path = fs.JoinPath(directory, file);
		sql += "CREATE OR REPLACE TABLE " + QualifiedName(schema, table) + " AS SELECT * FROM read_parquet(" +
		       Literal(path) + ");\n";
		const bool is_object =
		    std::find(ModuleTableNames().begin(), ModuleTableNames().end(), table) != ModuleTableNames().end();
		// decode(), not a cast: parquet_kv_metadata returns BLOB, and casting it to
		// VARCHAR escapes bytes so the JSON no longer parses.
		sql += "INSERT INTO " + bookkeeping + " (table_name, file_name, role, city) SELECT " + Literal(table) + ", " +
		       Literal(file) + ", " + Literal(std::string(is_object ? "object" : "sidecar")) +
		       ", (SELECT decode(value) FROM parquet_kv_metadata(" + Literal(path) + ") WHERE decode(key) = 'city');\n";
	}
	return sql;
}

void RegisterCityParquetPackageFunctions(ExtensionLoader &loader) {
	loader.RegisterFunction(PragmaFunction::PragmaCall(
	    "cityparquet_read", PragmaRead,
	    {LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR)}));

	loader.RegisterFunction(
	    PragmaFunction::PragmaCall("cityparquet_init", PragmaInit, {LogicalType(LogicalTypeId::VARCHAR)}));

	ScalarFunction init_sql("cityparquet_init_sql", {LogicalType(LogicalTypeId::VARCHAR)},
	                        LogicalType(LogicalTypeId::VARCHAR), InitSQLScalar);
	loader.RegisterFunction(init_sql);

	ScalarFunction city_field("cityparquet_city_field",
	                          {LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR)},
	                          LogicalType(LogicalTypeId::VARCHAR), CityFieldFunction);
	loader.RegisterFunction(city_field);
}

} // namespace cityjson
} // namespace duckdb
