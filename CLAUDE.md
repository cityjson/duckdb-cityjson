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

- Schema switches to `geometry` (BLOB/WKB) + `geometry_properties` (VARCHAR/JSON)
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

- **Filter Pushdown** — push WHERE clauses down to readers for better performance
- **Column Statistics** — implement column min/max statistics for query optimization
- **Spatial Indexing** — integrate with DuckDB spatial extension for spatial queries
- **Streaming** — support very large files without loading all data into memory during bind
- **Compression** — support compressed CityJSON files (.gz, .bz2)

## References

- CityJSON specification: <https://www.cityjson.org/specs/2.0.1/>
- CityJSONSeq specification: <https://www.cityjson.org/cityjsonseq/>
- DuckDB C++ API: <https://duckdb.org/docs/stable/clients/c/api>
- DuckDB Extension development: <https://duckdb.org/docs/stable/dev/extensions>
