# Agent Guide

This document provides guidance to coding agents working in the DuckDB CityJSON Extension repository.

## Repository Context

This is a **C++ DuckDB extension** that registers SQL table functions for reading CityJSON and CityJSONSeq files.
It is **not Rust** — the entire extension is written in C++17.

Key entry points:

- `src/cityjson_extension.cpp` — extension loader (`LoadInternal`), registers all functions
- `src/cityjson/` — all implementation files
- `src/include/cityjson/` — all headers
- `test/sql/` — SQL-based tests
- `test/data/` — sample `.city.json` and `.city.jsonl` files

## Architecture Overview

### Registered SQL Functions

| Function                     | File                                     | Description                      |
| ---------------------------- | ---------------------------------------- | -------------------------------- |
| `read_cityjson(path)`        | `bind_function.cpp`, `scan_function.cpp` | Read CityJSON (`.city.json`)     |
| `read_cityjsonseq(path)`     | `bind_function.cpp`, `scan_function.cpp` | Read CityJSONSeq (`.city.jsonl`) |
| `cityjson_metadata(path)`    | `metadata_table_function.cpp`            | Metadata for CityJSON            |
| `cityjsonseq_metadata(path)` | `metadata_table_function.cpp`            | Metadata for CityJSONSeq         |
| `cityjson_wkb_extent(blob)`  | `wkb_extent.cpp`                         | 3D extent of a WKB blob, solids included |
| `cityjson_appearance_ids(cell, kind)` | `cityparquet_appearance.cpp`    | Sidecar ids an appearance cell references |
| `cityjson_materials(path)`   | `appearance_table_function.cpp`          | materials.parquet rows, ids interned across the file |
| `cityjson_textures(path)`    | `appearance_table_function.cpp`          | textures.parquet rows |
| `cityjson_geometry_templates(path)` | `appearance_table_function.cpp`   | geometry_templates.parquet rows, local coordinates |

### CityParquet package mutation

A CityParquet package is a **DuckDB schema** whose tables are named by the spec's file
basenames, plus a `__cityparquet` bookkeeping table. These are **`PragmaFunction`s
registered with a `pragma_query_t`**: the function *returns SQL text*, which DuckDB
parses and runs in place of the pragma, inside the caller's transaction
(`duckdb/src/planner/statement_preprocessor.cpp:107-122`). Atomicity is DuckDB's, not
ours — the extension only generates text.

| Function | File | Description |
| -------- | ---- | ----------- |
| `PRAGMA cityparquet_init(schema)` | `cityparquet_package.cpp` | Create/refresh `__cityparquet` |
| `PRAGMA cityparquet_validate(schema)` | `cityparquet_validate.cpp` | Consistency checks → `cityparquet_validation` |
| `PRAGMA cityparquet_orphans(schema)` | `cityparquet_validate.cpp` | Unreferenced sidecar rows → `cityparquet_orphan_rows` |
| `PRAGMA cityparquet_vacuum(schema)` | `cityparquet_validate.cpp` | Delete unreferenced sidecar rows |
| `PRAGMA cityparquet_reconcile(schema [, checks = [...]])` | `cityparquet_reconcile.cpp` | Re-derive `feature_id`, hierarchy, `bbox` |
| `PRAGMA cityparquet_delete(schema, predicate [, cascade =] [, tables =])` | `cityparquet_delete.cpp` | Delete with cascade |
| `PRAGMA cityparquet_merge(dst, src [, create_tables =] [, tables =])` | `cityparquet_merge.cpp` | Merge one package into another |
| `PRAGMA cityparquet_read(dir, schema)` | `cityparquet_package.cpp` | Load a package directory into a schema |
| `cityparquet_write(schema, dir [, crs =])` | `cityparquet_write.cpp` | Write the package back out (**table function**, sees committed state) |

Each mutating pragma has a scalar `*_sql()` twin returning the same text without
running it.

**`cityparquet_write` is the one exception to the pragma rule.** `KV_METADATA` accepts
`getvariable()` (so a footer *value* can be computed in generated SQL) but cannot omit a
*key*: a NULL value writes the literal string `"NULL"`. The spec requires a solid-only
table to write no `geo` key at all, legality is data-dependent, and SQL cannot branch the
shape of a `COPY`. So it is a table function that assembles the metadata in C++ — at the
cost of running on an internal connection and seeing only committed state.

**Traps worth knowing before adding to this layer:**

- **Pragma expansion happens before execution**, for the *whole* submitted script. A
  generator's view of the catalog and data is pre-batch, so anything
  destination-dependent must be idempotent (`ADD COLUMN IF NOT EXISTS`) or deferred into
  the generated SQL.
- **A PRAGMA cannot be a subquery.** `FROM (PRAGMA x)` is a parser error, so
  result-returning pragmas materialise a temp table and select from it.
- **PRAGMA named parameters use `=`, not `:=`** (`transform_pragma.cpp:26-33`).
- **No subqueries inside lambda bodies.** `list_filter(l, x -> x IN (SELECT ...))` is
  rejected; hoist the set into a session variable and use `list_contains(getvariable(...), x)`.
- **The `JSON` type and `json_extract` are unavailable** — they live in the `json`
  extension, which this one does not require. JSON is carried as `VARCHAR` and parsed in
  C++ with the vendored nlohmann::json.
- **Two ODR link traps.** Binding a *reference* to a `static constexpr` member emits a
  comdat definition that collides with DuckDB's strong one: write
  `LogicalType(LogicalTypeId::DOUBLE)` rather than `LogicalType::DOUBLE` in
  `emplace_back`, and use the non-templated `Catalog::GetEntry(context,
  CatalogType::TABLE_ENTRY, ...)` rather than `Catalog::GetEntry<TableCatalogEntry>`,
  which ODR-uses `TableCatalogEntry::Name`.
- **`StringUtil::Join` takes `duckdb::vector`**, which `std::vector` does not convert to.
- **`parquet_kv_metadata` returns BLOB.** Use `decode(value)`, not `value::VARCHAR` — the
  cast escapes bytes and the JSON no longer parses.
- **`FileFlags::FILE_FLAGS_READ` is a third ODR trap** (it is a `static constexpr
  FileOpenFlags`). Use the scalar `FileOpenFlags::FILE_FLAGS_READ` instead, as
  `duckdb_fs_range_reader.cpp` already does.
- **`SQLString` is a formatting wrapper, not a quoting function**; use
  `KeywordHelper::WriteQuoted(text, '\'')` and `WriteOptionallyQuoted` for identifiers.

### Key Source Files

| File                           | Purpose                                                                               |
| ------------------------------ | ------------------------------------------------------------------------------------- |
| `cityjson_types.hpp/cpp`       | Core data types: `CityJSON`, `CityJSONFeature`, `CityObject`, `Geometry`, `Transform` |
| `reader.hpp`                   | Abstract `CityJSONReader` interface                                                   |
| `reader_factory.cpp`           | `OpenAnyCityJSONFile()` — auto-detects format by extension                            |
| `local_cityjsonreader.cpp`     | Reads `.city.json` (full CityJSON)                                                    |
| `local_cityjsonseq_reader.cpp` | Reads `.city.jsonl` (CityJSONSeq, line-delimited)                                     |
| `bind_function.cpp`            | `CityJSONBind` / `CityJSONSeqBind` — schema inference, chunk loading                  |
| `scan_function.cpp`            | `CityJSONScan` — iterates CityObjects, writes to DuckDB vectors                       |
| `city_object_utils.cpp`        | Attribute extraction, geometry encoding, schema inference                             |
| `lod_table.cpp`                | LOD-based schema inference for `lod=` mode                                            |
| `wkb_encoder.cpp`              | WKB geometry encoding                                                                 |
| `metadata_table_function.cpp`  | `cityjson_metadata` and `cityjsonseq_metadata` implementations                        |

### CityJSONSeq Format

- Line 1: CityJSON metadata header (`"type": "CityJSON"`) — used by `*_metadata` functions
- Line 2+: `CityJSONFeature` records (`"type": "CityJSONFeature"`) — each has its own local `"vertices"` array
- **Important**: per-feature `"vertices"` are local to that feature; geometry boundary indices reference them, not the global header vertices

### LOD / WKB Mode

When `lod='X'` is passed:

- Schema restricts to that one LoD but keeps the same suffixed column grammar as
  the wide layout: `geometry_lodX_Y` (BLOB/WKB) + `geometry_properties_lodX_Y`
  (STRUCT) + `material_lodX_Y` / `texture_lodX_Y` + `bbox`. There is no bare
  `geometry` column, so the LoD stays recoverable from the column name — which
  is what lets `COPY TO cityjson` re-emit it.
- Per-feature vertex pool is used for CityJSONSeq; global metadata vertices used for regular CityJSON
- `GetGeometryAtLOD()` finds the geometry matching the requested LOD string
- `lod` field in geometry objects is optional (not all features declare it)

## Build & Tooling

```sh
# Initial setup (once)
GEN=ninja make

# Incremental rebuild of extension + duckdb binary
cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb

# Or full rebuild
make release
```

The `duckdb` binary is statically linked with the extension. Always rebuild it after code changes to test interactively:

```sh
./build/release/duckdb -c "SELECT COUNT(*) FROM read_cityjson('test/data/minimal.city.json');"
```

## ⚠️ Always Run Tests After Changes

**Whenever you make code changes, you MUST run the tests before considering the task done.**

```sh
# Run all SQL tests
cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb
./build/release/duckdb -c "SELECT * FROM read_cityjson('test/data/minimal.city.json');"
./build/release/duckdb -c "SELECT COUNT(*) FROM read_cityjsonseq('test/data/sample.city.jsonl');"
```

Or run the full test suite:

```sh
make test
```

Tests live in `test/sql/*.test`. Always verify:

1. `read_cityjson` still works on `.city.json` files
2. `read_cityjsonseq` works on `.city.jsonl` files
3. `lod=` option works for both
4. `cityjson_metadata` / `cityjsonseq_metadata` return correct rows

## Common Patterns

### Adding a New Named Parameter

1. Add to `func.named_parameters["param_name"] = LogicalType::...` in `table_function_registration.cpp`
2. Parse it in `CityJSONBind` / `CityJSONSeqBind` in `bind_function.cpp`
3. Store on `CityJSONBindData`
4. Use in `CityJSONScan` in `scan_function.cpp`

### Adding a New Predefined Column

1. Add to `GetDefinedColumns()` in `column_types.cpp`
2. Handle in `CityObjectUtils::GetAttributeValue()` in `city_object_utils.cpp`
3. Add `IsPredefinedColumn()` check if needed

### Geometry Parsing

`Geometry::FromJson` in `cityjson_types.cpp`:

- `type` and `boundaries` are required
- `lod` is **optional** (default `""`)
- `semantics`, `material`, `texture` are optional

### CityJSONFeature Vertices

In CityJSONSeq, each feature line has its own `"vertices"` array. These are parsed into `CityJSONFeature::vertices` and used during WKB encoding. The scan resolves the correct vertex pool per-feature:

```cpp
// In scan_function.cpp
const std::vector<std::array<double, 3>> *vertex_pool = nullptr;
if (!feature.vertices.empty()) {
    vertex_pool = &feature.vertices; // CityJSONSeq: per-feature
} else if (bind_data.metadata.vertices.has_value()) {
    vertex_pool = &bind_data.metadata.vertices.value(); // CityJSON: global
}
```

## Future Features (Not Yet Implemented)

- **Simple `TableFilterSet` pushdown** — projection pushdown and complex-filter pushdown (equality on `id` / `feature_id` / `object_type`) are **already enabled** in `table_function_registration.cpp` (`projection_pushdown = true`, `pushdown_complex_filter = CityJSONPushdownComplexFilter`); only DuckDB's simple `filter_pushdown` / `TableFilterSet` path is left disabled, because the scan callback does not implement `TableFilterSet` handling yet.
- **Column Statistics** — implement column min/max statistics for query optimization
- **Spatial Indexing** — integrate with DuckDB spatial extension for spatial queries
- **Streaming** — support very large files without loading all data into memory during bind
- **Compression** — support compressed CityJSON files (.gz, .bz2)

## References

- CityJSON specification: <https://www.cityjson.org/specs/2.0.1/>
- CityJSONSeq specification: <https://www.cityjson.org/cityjsonseq/>
- DuckDB C++ API: <https://duckdb.org/docs/stable/clients/c/api>
- DuckDB Extension development: <https://duckdb.org/docs/stable/dev/extensions>
