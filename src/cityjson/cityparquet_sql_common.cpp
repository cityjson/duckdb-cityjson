#include "cityjson/cityparquet_sql_common.hpp"

#include "cityjson/cityparquet_package.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {
namespace cityjson {

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

std::string Quoted(const std::string &name) {
	return KeywordHelper::WriteOptionallyQuoted(name);
}

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

namespace {

//! The object-table footers that actually declare a CRS -- the only rows either helper
//! below may read. See the header for why the sidecars and the NULLs have to go.
std::string DeclaringObjectFooters(const std::string &schema) {
	return " FROM " + QualifiedName(schema, "__cityparquet") +
	       " WHERE role = 'object' AND city IS NOT NULL AND cityparquet_city_field(city, 'crs') IS NOT NULL";
}

} // namespace

std::string DeclaredCrsExpr(const std::string &schema) {
	// max(), not DISTINCT: one row whatever the data says. With OneCrsPerPackageSQL run
	// first there is at most one distinct value for it to pick, so which one is moot.
	return "(SELECT max(cityparquet_city_field(city, 'crs'))" + DeclaringObjectFooters(schema) + ")";
}

std::string CrsStatedExpr(const std::string &schema) {
	return "(SELECT COUNT(*) > 0 FROM " + QualifiedName(schema, "__cityparquet") +
	       " WHERE role = 'object' AND city IS NOT NULL)";
}

std::string OneCrsPerPackageSQL(const std::string &function, const std::string &schema, const std::string &label) {
	return "SELECT error('" + function + ": the " + label +
	       " package declares more than one CRS -- its object-table footers disagree, and a package '\n"
	       "  'states one CRS for every row it holds; rewrite it with cityparquet_write(..., crs => ...)') FROM (\n"
	       "  SELECT COUNT(DISTINCT cityparquet_city_field(city, 'crs')) AS n" +
	       DeclaringObjectFooters(schema) + "\n) WHERE n > 1;\n";
}

std::string CrsPreconditionSQL(const CrsCheckWording &wording, const std::string &destination_crs_expr,
                               const std::string &destination_stated_expr, const std::string &source_crs_expr,
                               const std::string &source_stated_expr) {
	const auto &fn = wording.function;
	const auto &noun = wording.source_noun;
	return "SELECT error(CASE\n"
	       "  WHEN d IS NOT NULL AND s IS NOT NULL THEN\n"
	       "    '" +
	       fn + ": CRS mismatch -- the destination is ' || d || ' and " + noun +
	       " is ' || s || '; reprojection is not performed'\n"
	       "  WHEN d IS NOT NULL THEN\n"
	       "    '" +
	       fn + ": the destination is ' || d || ' but " + noun +
	       " declares no CRS this writer can resolve, so its CRS is unknown -- a package '\n"
	       "    'states one CRS for every row it holds, and an unknown cannot be shown to be that one; " +
	       wording.source_unknown_hint +
	       "'\n"
	       "  ELSE\n"
	       "    '" +
	       fn +
	       ": the destination package CRS is unknown (its footer declares crs: null, or carries no crs at '\n"
	       "    'all) and " +
	       noun + " is ' || s || ' -- a package states one CRS for every row it holds; " +
	       wording.destination_unknown_hint +
	       "'\n"
	       "END) FROM (SELECT\n  " +
	       destination_crs_expr + " AS d,\n  " + source_crs_expr + " AS s\n) WHERE " + destination_stated_expr +
	       " AND " + source_stated_expr + " AND d IS DISTINCT FROM s;\n";
}

} // namespace cityjson
} // namespace duckdb
