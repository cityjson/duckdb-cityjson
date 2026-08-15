#pragma once

#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"

#include <string>
#include <vector>

namespace duckdb {
namespace cityjson {

//! StringUtil::Join takes duckdb::vector, which std::vector does not convert to.
std::string Join(const std::vector<std::string> &parts, const std::string &separator);

//! Optionally-quoted identifier (KeywordHelper::WriteOptionallyQuoted).
std::string Quoted(const std::string &name);

//! One catalog column, as the SQL generators consume it.
struct ColumnInfo {
	std::string name;
	LogicalType type;
};

//! Catalog columns of schema.table, in order. Non-templated Catalog::GetEntry --
//! the templated form ODR-uses TableCatalogEntry::Name (see cityparquet_package.cpp).
std::vector<ColumnInfo> TableColumns(ClientContext &context, const std::string &schema, const std::string &table);

//! Case-insensitive lookup; nullptr when absent.
const ColumnInfo *FindColumn(const std::vector<ColumnInfo> &columns, const std::string &name);

//! The promotion lattice: BIGINT -> DOUBLE is a safe widening; anything else that
//! disagrees falls back to VARCHAR. INVALID means the destination already
//! accommodates the source.
LogicalType WidenedType(const LogicalType &destination, const LogicalType &source);

} // namespace cityjson
} // namespace duckdb
