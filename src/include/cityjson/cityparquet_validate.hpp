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

//! Names of the temp tables the pragmas materialise their findings into.
extern const char *const VALIDATION_TABLE;
extern const char *const ORPHAN_TABLE;

//! A sub-SELECT yielding every id of `sidecar` currently referenced from the object
//! tables of `schema`. Shared by BuildOrphansSQL, which reports the complement, and
//! BuildVacuumSQL, which deletes it — so the reporter and the deleter cannot drift.
std::string ReferencedIds(ClientContext &context, const std::string &schema, const std::string &sidecar);

//! Script producing (table_name, id, reason) — one row per unreferenced sidecar row.
std::string BuildOrphansSQL(ClientContext &context, const std::string &schema);

//! DELETE statements removing every unreferenced sidecar row from `schema`.
std::string BuildVacuumSQL(ClientContext &context, const std::string &schema);

//! Script producing (check, severity, table_name, object_id, message) — one row per
//! consistency violation in `schema`.
std::string BuildValidateSQL(ClientContext &context, const std::string &schema);

void RegisterCityParquetValidateFunctions(ExtensionLoader &loader);

} // namespace cityjson
} // namespace duckdb
