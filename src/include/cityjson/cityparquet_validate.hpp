#pragma once

#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <string>

namespace duckdb {
namespace cityjson {

/**
 * Read-only consistency checks over a CityParquet package held in a DuckDB schema.
 *
 * A PRAGMA cannot be used as a subquery (`FROM (PRAGMA ...)` is a parser error), so
 * the generated script materialises its findings into a temp table and then selects
 * from it. The caller gets rows immediately *and* can filter the table afterwards:
 *
 *     PRAGMA cityparquet_validate('ams');
 *     SELECT * FROM cityparquet_validation WHERE severity = 'error';
 */

//! Name of the temp table cityparquet_validate materialises its findings into.
extern const char *const VALIDATION_TABLE;

//! Script producing (check, severity, table_name, object_id, message) — one row per
//! consistency violation in `schema`.
std::string BuildValidateSQL(ClientContext &context, const std::string &schema);

void RegisterCityParquetValidateFunctions(ExtensionLoader &loader);

} // namespace cityjson
} // namespace duckdb
