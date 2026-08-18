#pragma once

#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <map>
#include <string>
#include <vector>

namespace duckdb {
namespace cityjson {

//! The CityParquet format version this extension writes (footer `city.version`,
//! STAC `cityparquet:version`). One definition, shared by the package writer and
//! the COPY-path footer builder.
inline constexpr const char *CITYPARQUET_VERSION = "0.1.0-draft";

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

//! The module table one CityObject type belongs in, per the specification's by-module
//! table — `Building` and `BuildingPart` alike give "building", `Road` and `Square` alike
//! give "transportation". Both the CityGML 3.0 class names and the four CityJSON
//! spellings that differ are accepted, since the reader emits the latter.
//!
//! Returns the empty string for a type it cannot place. Routing is total by
//! specification, so callers must treat that as an error rather than dropping the rows.
std::string ModuleForObjectType(const std::string &object_type);

//! CityGML 3.0 class name for a CityJSON type. Exactly four spellings differ
//! (TransportSquare->Square, GenericCityObject->GenericOccupiedSpace,
//! BuildingStorey->Storey, TunnelHollowSpace->HollowSpace); everything else --
//! including extension types -- is returned unchanged. Spec
//! 02-object-table-schema.mdx "object_type vocabulary"; mirrors cityparquet-rs
//! encode.rs.
std::string CityGMLClassForCityJSONType(const std::string &cityjson_type);

//! The inverse, for export back to CityJSON. Identity for everything but the four
//! remapped classes; mirrors cityparquet-rs decode.rs.
std::string CityJSONTypeForCityGMLClass(const std::string &citygml_class);

//! Object tables actually present in `schema`, sorted. Throws BinderException when the
//! schema contains none — that is not a CityParquet package.
std::vector<std::string> ObjectTablesInSchema(ClientContext &context, const std::string &schema);

//! Sidecar tables actually present in `schema`, sorted. May legitimately be empty: a
//! source with no appearance produces no sidecars.
std::vector<std::string> SidecarTablesInSchema(ClientContext &context, const std::string &schema);

//! Quoted "schema"."table", safe to concatenate into generated SQL.
std::string QualifiedName(const std::string &schema, const std::string &table);

//! Single-quoted SQL string literal (KeywordHelper::WriteQuoted). SQLString is a
//! formatting wrapper, not a quoting function -- never use it for this.
std::string Literal(const std::string &text);

//! geometry_lod* column names of one table, in catalog order. Never hard-code an LoD
//! set: which LoDs exist is a property of the dataset, not of the format.
std::vector<std::string> GeometryLodColumns(ClientContext &context, const std::string &schema,
                                            const std::string &table);

//! As above for appearance columns; `prefix` is "material_lod" or "texture_lod".
std::vector<std::string> AppearanceLodColumns(ClientContext &context, const std::string &schema,
                                              const std::string &table, const std::string &prefix);

//! True when `name` is exactly `prefix` followed by the LoD suffix grammar: digits,
//! optionally `_` and more digits, and nothing else.
//!
//! A bare prefix test is not enough. `material_lodging` is a perfectly ordinary source
//! attribute, and the reader already takes care not to swallow it as an appearance
//! column (test/sql/cityjson_appearance.test). Misclassifying it hands arbitrary
//! attribute text to cityjson_appearance_ids or cityjson_shift_appearance_ids, neither
//! of which can parse it.
bool MatchesLodSuffix(const std::string &name, const std::string &prefix);

//! The `all_objects AS (...)` CTE body: every object table's identity and hierarchy
//! columns, unioned. Those columns are common to every module — only attribute columns
//! differ — so consistency checks and re-derivations are written once rather than once
//! per module.
//! `known_children_roles` overrides the catalog probe for `children_roles`, which is an
//! optional column a table read in `lod =` mode does not carry. A caller generating SQL
//! for a table the same script is about to create MUST supply an entry, because there is
//! no catalog row to probe.
std::string AllObjectsCTE(ClientContext &context, const std::string &schema,
                          const std::vector<std::string> &object_tables,
                          const std::map<std::string, bool> &known_children_roles = {});

//! True when `schema.table` has a column of this (lower-cased) name.
bool HasColumn(ClientContext &context, const std::string &schema, const std::string &table, const std::string &column);

//! Loads a CityParquet package directory into `schema`: one table per file found, plus
//! a `__cityparquet` row per file with the footer's `city` object recovered via
//! parquet_kv_metadata -- the one thing a hand-rolled read_parquet load throws away.
std::string BuildReadSQL(ClientContext &context, const std::string &directory, const std::string &schema);

//! Registers cityparquet_init (pragma) and cityparquet_init_sql (scalar).
void RegisterCityParquetPackageFunctions(ExtensionLoader &loader);

} // namespace cityjson
} // namespace duckdb
