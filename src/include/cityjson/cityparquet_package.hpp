#pragma once

#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <string>
#include <vector>

namespace duckdb {
namespace cityjson {

/**
 * A CityParquet package, as held in DuckDB, is a **schema** whose tables are named
 * exactly as the specification names the package's files: `building`,
 * `transportation`, … plus the `materials` / `textures` / `geometry_templates`
 * sidecars. Naming is the whole binding — there is no registration state to keep in
 * sync and nothing session-scoped to go stale.
 *
 * The one thing a hand-rolled `CREATE TABLE … read_parquet(…)` load cannot provide is
 * the per-file provenance the Parquet footer carried, so the schema also holds a
 * `__cityparquet` bookkeeping table: one row per package file, with `table_name`,
 * `file_name`, `role` ('object' | 'sidecar') and the `city` footer JSON. `city` is
 * VARCHAR holding JSON text rather than the JSON type, which lives in the json
 * extension this one does not require.
 */

//! The 11 CityGML 3.0 modules that hold feature objects, as snake_case table
//! basenames, in the order the specification lists them.
const std::vector<std::string> &ModuleTableNames();

//! materials, textures, geometry_templates.
const std::vector<std::string> &SidecarTableNames();

//! Object tables actually present in `schema`, sorted. Throws BinderException when the
//! schema contains none — that is not a CityParquet package.
std::vector<std::string> ObjectTablesInSchema(ClientContext &context, const std::string &schema);

//! Sidecar tables actually present in `schema`, sorted. May legitimately be empty: a
//! source with no appearance produces no sidecars.
std::vector<std::string> SidecarTablesInSchema(ClientContext &context, const std::string &schema);

//! Quoted "schema"."table", safe to concatenate into generated SQL.
std::string QualifiedName(const std::string &schema, const std::string &table);

//! geometry_lod* column names of one table, in catalog order. Never hard-code an LoD
//! set: which LoDs exist is a property of the dataset, not of the format.
std::vector<std::string> GeometryLodColumns(ClientContext &context, const std::string &schema,
                                            const std::string &table);

//! As above for appearance columns; `prefix` is "material_lod" or "texture_lod".
std::vector<std::string> AppearanceLodColumns(ClientContext &context, const std::string &schema,
                                              const std::string &table, const std::string &prefix);

//! The `all_objects AS (...)` CTE body: every object table's identity and hierarchy
//! columns, unioned. Those columns are common to every module — only attribute columns
//! differ — so consistency checks and re-derivations are written once rather than once
//! per module.
std::string AllObjectsCTE(const std::string &schema, const std::vector<std::string> &object_tables);

//! Registers cityparquet_init (pragma) and cityparquet_init_sql (scalar).
void RegisterCityParquetPackageFunctions(ExtensionLoader &loader);

} // namespace cityjson
} // namespace duckdb
