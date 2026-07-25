#include "cityjson/cityparquet_package.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/keyword_helper.hpp"

#include <algorithm>
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

//! Column names of `schema.table` beginning with `prefix`, in catalog order.
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
		if (StringUtil::StartsWith(StringUtil::Lower(column.Name()), prefix)) {
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

void RegisterCityParquetPackageFunctions(ExtensionLoader &loader) {
	loader.RegisterFunction(
	    PragmaFunction::PragmaCall("cityparquet_init", PragmaInit, {LogicalType(LogicalTypeId::VARCHAR)}));

	ScalarFunction init_sql("cityparquet_init_sql", {LogicalType(LogicalTypeId::VARCHAR)},
	                        LogicalType(LogicalTypeId::VARCHAR), InitSQLScalar);
	loader.RegisterFunction(init_sql);
}

} // namespace cityjson
} // namespace duckdb
