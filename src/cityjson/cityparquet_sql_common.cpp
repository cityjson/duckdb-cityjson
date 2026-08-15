#include "cityjson/cityparquet_sql_common.hpp"

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

} // namespace cityjson
} // namespace duckdb
