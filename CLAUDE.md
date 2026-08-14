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
| `cityjson_geoparquet_geo(path [, geometry_encoding =])` | `geoparquet_table_function.cpp` | GeoParquet `geo` footer JSON, or NULL when no column qualifies |

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
| `PRAGMA insert_cityjson(schema, path [, create_tables =] [, tables =] [, lod =] [, sample_lines =])` | `cityparquet_insert.cpp` | Add a CityJSON file, routed by module (also `insert_cityjsonseq` / `insert_flatcitybuf`) |
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
- **Never re-derive what the reader emits.** A generator that needs the incoming column
  list must call `InferCityJSONColumns` / `InspectCityJSONSource`
  (`bind_function.cpp`), the same inference the bind runs. Reconstructing it from
  `GetDefinedColumns` + `InferAttributeColumns` + `InferGeometryColumns` gives a
  different answer, and the generated SQL then names a column the staged relation does
  not have.
- **`INSERT ... BY NAME` is not symmetric.** It leaves an unmatched *destination* column
  NULL, but a *source* column with no destination match is a binder error. Schema
  evolution must therefore run over sidecars too, not only module tables —
  `geometry_templates` carries per-LoD columns and so its shape varies by file.
- **`bbox` is an optional column.** A source with only template geometry produces neither
  `geometry_lod*` nor `bbox`, so anything generating `UPDATE ... SET bbox` must check the
  column exists first.
- **The CityJSONSeq reader is a forward stream.** `ReadAllChunks` and `ReadNFeatures`
  rewind so they can be asked more than once; `ReadNextFeature` deliberately does not,
  because it is the streaming scan's cursor.
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

### Arrow-native geometry encoding (experimental, `arrow-native-type` branch)

`read_cityjson[seq](..., geometry_encoding := 'wkb' | 'arrow-native')` selects the
physical geometry encoding; `'wkb'` is the default and is untouched. Design:
`docs/superpowers/specs/2026-07-25-arrow-native-geometry-design.md` in the parent
workspace repo. Under `'arrow-native'`:

- `geometry_lod*` becomes `INTEGER[][][][][]` — five LIST levels, solid → shell →
  face → ring → vertex-pool index — and gains a sibling
  `geometry_vertices_lod*` of `STRUCT(x, y, z DOUBLE)[]` holding that row's pool.
  The sibling is paired by name only, as `geometry_properties_lod*` already is.
- `geometry_properties_lod*` is **unchanged**. It stays the only thing that says
  whether a row is a `Solid` or a `MultiSurface`: the physical nesting is uniform
  across both families, so **never infer the CityJSON type from the shape**. A
  surface type pads the outer two levels to length 1; for a real `Solid` the shell
  level is genuine structure.
- The pool compacts **distinct source indices**, never coordinate values —
  CityJSON permits two indices to carry identical coordinates and they must stay
  two entries. `ArrowNativeEncoder` (`arrow_native_encoder.cpp`) does this; phase 1
  covers `MultiSurface`/`CompositeSurface`/`Solid`/`MultiSolid`/`CompositeSolid`
  and rejects anything else.
- Rings keep CityJSON's winding and do **not** repeat the closing vertex, unlike
  the WKB path, which reverses and closes them.
- `cityjson_geoparquet_geo` takes the same parameter and declares **no** column
  under `arrow-native`: legality is a property of the encoding, not the CM type.

Two traps specific to this layer:

- **The geometry column list is derived twice.** `LODTableUtils::GetGeometryColumns`
  serves only the `lod=` path; the wide layout goes through
  `CityObjectUtils::InferGeometryColumns`. The encoding is applied by rewriting the
  finished list in `InferCityJSONColumns` (`CityObjectUtils::ApplyGeometryEncoding`),
  which is the single point where either derivation becomes `bind_data.columns` —
  so neither derivation has to know about encodings.
- **A nested `ListVector` child's data pointer is only valid after the `Reserve`
  that sizes it.** Each level's writer therefore fetches its own child pointer
  rather than receiving one, and is fully reserved before its children are
  visited. Same rule for the vertex pool's `STRUCT` children.

Encoder assertions live in `test/cpp/` (not in `make test` — it needs a built
`build/release` and the flatcitybuf prefix):
`FCB_PREFIX=/path/to/prefix test/cpp/run_encoder_tests.sh`.

### FlatCityBuf dependency: fork registry + `.vendor/prefix`

`flatcitybuf` is a **released vcpkg port resolved from a git registry**, not a
repo-local overlay port any more. `vcpkg.json` carries a registry entry scoped to
the single package (`https://github.com/HideBa/vcpkg`, with a pinned `baseline`);
everything else stays on the builtin baseline. When the upstream microsoft/vcpkg PR
merges, the entry is deleted and the dependency resolves from upstream unchanged.
`vcpkg_ports/flatcitybuf` and `vcpkg_ports/flatbuffers` are gone, along with the
`flatbuffers` version override — the builtin baseline already carries flatbuffers
25.9.23, and the fork's port relaxes the generated headers' exact-version
`static_assert` to a major-version check.

- **`cpp-v<version>` tags are the C++ releases.** The bare `v<version>` tags in
  `cityjson/flatcitybuf` are the Rust crate cut from the same train, so `v0.7.7` and
  `cpp-v0.7.7` are *not* the same code. The port is at `cpp-v0.9.0`.
- **Local dev builds use `just vendor-fcb`**, which builds flatbuffers v25.9.23 and
  flatcitybuf `cpp-v0.9.0` into the gitignored `.vendor/prefix` with
  `-DCMAKE_POSITION_INDEPENDENT_CODE=ON` (the loadable extension is a shared object,
  so both static libs need PIC). Point CMake at it with `-Dflatcitybuf_DIR` /
  `-Dflatbuffers_DIR` or `CMAKE_PREFIX_PATH`; the `test/cpp` harnesses want
  `FCB_PREFIX="$(pwd)/.vendor/prefix"`. Never a session scratchpad — a
  garbage-collected prefix breaks every relink. The recipe re-checks out the pinned
  tag on every run, so a bump upgrades an existing `.vendor/src` clone instead of
  silently reusing the old one.
- **`FileInfo`'s schema-optional strings are `std::optional<std::string>` as of
  `cpp-v0.9.0`** (`identifier`, `title`, `reference_date`, and every `poc_*` bar the
  address members), because the Rust oracle distinguishes absent from
  present-but-empty: `Some("")` emits the key with an empty value, only `None` omits
  it. Gate on `.has_value()`, never on `.empty()`. We consume none of these directly —
  FCB metadata flows through upstream's `to_cityjson_metadata` — so the change was a
  no-op for us; `Header().info()` is read only for `.columns` and `.features_count`.
- **`FCB_WITH_CURL` stays off.** The HTTP transport is DuckDB's FileSystem/httpfs
  through `DuckDBRangeReader` (`duckdb_fs_range_reader.cpp`), so there is one HTTP
  stack and one credentials/secrets/proxy story.

### Selective deserialisation (`FcbFieldMask`)

`read_flatcitybuf` decodes only what the query projects. `FcbFieldMask`
(`fcb_selective_convert.hpp`) is `{bool geometry; optional<set<string>> attributes;}`,
defaulting to "everything", and `ComputeFcbFieldMask(columns, column_ids)` derives it
from the projection. Two conversion paths:

- **Full path** (`geometry == true`): today's `fcb::to_cityjson_feature` conversion,
  unchanged.
- **Light path** (`geometry == false`): `ConvertFeatureLight` builds a
  `CityJSONFeature` straight from `Feature::raw()` — ids, type, parents/children,
  geographical extent — with attributes decoded by `DecodeAttributesFiltered`, a
  filtered walk of the attribute blob that materialises only the masked-in columns and
  skips the rest by width. Geometry is never touched.

Invariant either way: one output row per CityObject, and every projected column's
value is byte-identical across the two paths.

Traps in this layer:

- **Honour the per-object column schema.** `to_cityjson_feature` decodes attributes
  against each CityObject's *own* schema when it declares one, falling back to the
  header's. The filtered walk selects the schema the same way; get it wrong and values
  decode as garbage. Records are not self-delimiting, so an unknown column index or
  width aborts the walk rather than guessing and desynchronising the blob.
- **The full path ignores `mask.attributes` by design.** The attribute blob is small
  next to geometry, and hand-rolling geometry conversion would duplicate upstream
  logic. So `ComputeFcbFieldMask` clears `attributes` to `nullopt` whenever the mask
  is a full-path one, rather than leaving a misleading subset.
- **`other` forces a full attribute decode.** `GetAttributeValue` builds it from
  *every* non-predefined, non-geometry attribute, so projecting it needs the whole
  blob even though it is one of `GetDefinedColumns()`'s structural names.
- **`bbox` counts as geometry-derived**, alongside `geometry_lod*` /
  `geometry_vertices_lod*` / `geometry_properties_lod*` / `material_lod*` /
  `texture_lod*` — it is computed from the geometry's vertices, not read from a stored
  field. `IsGeometryDerivedColumn` classifies by `ColumnType` first and falls back to
  the name grammar, erring towards decoding *more*: over-decoding is a lost saving,
  under-decoding is a wrong answer.
- **`read_flatcitybuf` materialises in its own `init_global`, not at bind.**
  Projection is only known at `TableFunctionInitInput::column_ids`, so
  `FlatCityBufBind` calls `BindCityJSONReadRaw(..., materialise = false)` and
  `FlatCityBufInitGlobal` runs the one real read into `CityJSONGlobalState`, setting
  `use_global_chunks`. **Anything reading `bind_data.chunks` / `bind_data.scan_plan`
  sees nothing for FCB** — `MaterializedScan` picks the global-state override when
  `use_global_chunks` is set, and `FlatCityBufCardinality` answers from the header's
  `features_count` (an estimate: it counts features, rows are CityObjects) because
  counting bind-time chunks would return zero. `FlatCityBufPushdownComplexFilter` only
  records filters on the reader; it no longer re-reads.

**`Byte` / `UByte` / `Binary`: divergence resolved in `cpp-v0.9.0`.** Up to
`cpp-v0.8.1` the full path threw `UnsupportedColumnType` on all three while our light
path already decoded them. `cpp-v0.9.0` lands the decode arms and settles the
semantics the way we did in `58ddbe4`: **`Byte` is UNSIGNED `u8`** — the writer stores
a raw `u8`, and both the value path (`reader/deserializer.rs`) and the index path
(`reader/attr_query.rs`, `key.cpp`'s `key_kind_for_column` → `KeyKind::UInt8`) read it
back as `u8`; decoding it as `i8` turned a stored 200 into -56. `UByte` is `u8`, and
`Binary` is a u32 LE length plus raw bytes. Our light path
(`fcb_selective_convert.cpp`) and the pushdown's `BuildKeyValue`
(`flatcitybuf_table_function.cpp`, `KeyValue::from_u8` for `Byte`, because
`compare_keys` *throws* when the supplied key's kind differs from the one
`select_attr` derives from the column) already agree, so both paths now match
everywhere.

**That parity is unverified by fixture, though.** `guess_type`
(`writer/attribute.cpp`) only ever emits `Bool`/`Long`/`ULong`/`Double`/`String`/
`DateTime`/`Json`, so nothing this stack writes can carry a `Byte`/`UByte`/`Binary`
column and `test_fcb_selective.cpp`'s T3 light-vs-full comparison never sees one.
Those three types are covered only by T5's synthetic blobs, which exercise the light
path alone. A real fixture would have to come from upstream.

### FCB test harnesses

- `test/cpp/run_fcb_selective_tests.sh` — assertions for `FcbFieldMask` /
  `ConvertFeatureLight` / `DecodeAttributesFiltered`, alongside
  `run_encoder_tests.sh`. Both are outside `make test` and both need
  `FCB_PREFIX` plus a built `build/release`:

  ```sh
  FCB_PREFIX="$(pwd)/.vendor/prefix" test/cpp/run_fcb_selective_tests.sh
  ```

  **Stale-`.so` trap:** they link against `build/release/src/libduckdb.so`, which the
  usual `cityjson_extension` / `cityjson_loadable_extension` / `duckdb` targets do
  **not** rebuild. Undefined `fcb::` / `nlohmann` symbols (or a `json_abi_v3_11_3`
  mangling mismatch) means the `.so` predates the sources — fix with
  `ninja -C build/release src/libduckdb.so`.
- `test/sql/cityjson_fcb_remote.test` — HTTP range reads through `DuckDBRangeReader`,
  gated on `require-env FCB_REMOTE_TEST_URL` so a plain `make test` skips it. Run it
  with `just test-fcb-remote` (optionally `url=…`). **Caveat:** the bbox is a 500 m
  square hard-wired to the default hosted 3DBAG subset (RD New / EPSG:7415), and
  sqllogictest `test-env` defaults are only overridable through `--test-config`, so a
  different URL needs a matching bbox — the reader still materialises every matching
  feature in one go.

## Build & Tooling

```sh
# Initial setup (once)
GEN=ninja make

# Incremental rebuild of extension + duckdb binary + test binary
cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb unittest

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
# Rebuild everything the tests run against — note `unittest`
cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb unittest
./build/release/duckdb -c "SELECT * FROM read_cityjson('test/data/minimal.city.json');"
./build/release/duckdb -c "SELECT COUNT(*) FROM read_cityjsonseq('test/data/sample.city.jsonl');"
```

**`unittest` is not optional in that target list.** `make test` runs
`build/release/test/unittest` but does **not** rebuild it, so omitting the target
silently tests a stale binary and reports green on code that never ran. `just rebuild`
(and therefore `just t`) already includes it.

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
