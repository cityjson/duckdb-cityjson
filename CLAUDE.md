# Agent Guide

This document provides guidance to coding agents working in the DuckDB CityJSON Extension repository.

## Repository Context

This is a **C++ DuckDB extension** that registers SQL table functions for reading CityJSON and CityJSONSeq files.
It is **not Rust** — the entire extension is written in C++20 (pinned via target_compile_features; std::span requires it).

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
| `cityjson_geoparquet_geo(path [, geometry_encoding =])` | `geoparquet_table_function.cpp` | The two COPY-path footer keys: `geo` (GeoParquet, NULL when no column qualifies) and the required `city` object |

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
- **The footer's `crs` is tri-state, and absent is not "unknown".** GeoParquet's
  convention, which CityParquet adopts (spec `05-metadata.mdx`, "CRS rules"): a PROJJSON
  object = known; explicit `null` = the file holds CRS-bearing coordinates whose CRS is
  unknown or unresolvable; **absent = OGC:CRS84**, so the key may be omitted only by a
  file with no CRS-bearing coordinate at all (the sidecars — geometry templates are in
  local, unplaced coordinates). Both writers therefore always emit the key for an object
  table, mirror the same value onto every `city.columns[]` / `geo.columns[]` entry, and
  never guess. An unresolvable CRS is a warning, not a conversion error; only an
  explicitly supplied `crs =>` that cannot be resolved still throws. On the read side
  `cityparquet_city_field` maps both absent and null to SQL NULL, so a footer that is
  missing and one that declares `"crs": null` look the same through it — `insert_cityjson`
  tells them apart by counting **object-table** footers separately (`role = 'object' AND
  city IS NOT NULL`), because only the latter is a stated unknown.
- **Comparing a CRS means comparing PROJJSON with PROJJSON.** A footer holds PROJJSON; a
  CityJSON source holds a `metadata.referenceSystem` URL. `insert_cityjson` resolves the
  source through `ProjjsonForReferenceSystem` and re-dumps it so both sides are the same
  canonical text (nlohmann orders object keys one way). Comparing the two raw made every
  insert into a known-CRS package a bogus mismatch.
- **The one-CRS-per-package precondition is shared** — `DeclaredCrsExpr` /
  `CrsStatedExpr` / `OneCrsPerPackageSQL` / `CrsPreconditionSQL` in
  `cityparquet_sql_common`, used by both `insert_cityjson` and `cityparquet_merge`, which
  had drifted into two subtly different (and both wrong) readings of the same footers.
  Read the declared CRS **only from object-table footers that declare one**: a sidecar
  carries no `crs` key, so a `DISTINCT` spanning every footer answers an ordinary package
  with two rows and the scalar subquery dies with "More than one row returned by a
  subquery" — a CRS bug that does not mention CRSs. Both then apply the tri-state rule:
  known vs known compares, known vs unknown is refused **either way round** (an unknown
  cannot be shown to be the package's one CRS), unknown vs unknown passes, and a side
  whose footers are missing entirely states nothing and is not checked.
- **The one diagnostic channel is `DUCKDB_LOG_WARNING(context, …)`** (`duckdb/logging/
  logger.hpp`). The CLI enables logging at `WARNING` with its own storage and prints it;
  a test asserts it with `SET enable_logging = true; SET logging_level = 'WARNING';` and a
  query on `duckdb_logs` (whose in-memory storage the CLI replaces, so the two views are
  not interchangeable). There is no other user-visible warning mechanism here — the
  package writer's result rows are a file inventory, not a report.

### COPY TO: what the rows cannot carry

Two kinds of content are **file-level**, not row-level, and were both silently lost
until the source path was made recoverable: the source's `metadata` header (the CRS
above all) and its `appearance` object (the material/texture *definitions*; only the
per-geometry references are columns).

- **`COPY` recovers its source from the parsed SELECT.** `FindCopySourceRef`
  (`copy_source_ref.cpp`) walks `CopyInfo::select_statement` for exactly one
  `read_cityjson[seq]` / `read_flatcitybuf` call. It survives to our bind:
  `bind_copy.cpp:97` takes a `Copy()` of it rather than moving it, and `:118` hands
  the whole `CopyInfo` to `CopyFunctionBindInput`. **An ambiguous query — no reader
  call, more than one, or a non-literal path — returns `nullopt`, never a guess**:
  stamping a wrong CRS onto georeferenced output is worse than stamping none.
  `COPY my_table TO …` is not discoverable, which is what the `metadata_from` option
  is for; a `DUCKDB_LOG_WARNING` fires when neither applies. Precedence:
  `crs` / `metadata_query` > `metadata_from` > discovered source.
- **Appearance blocks are per-feature and their refs are feature-local.** In
  CityJSONSeq every feature carries its own `appearance`, and its material/texture
  indices are local to that block, exactly as its boundary indices are local to its
  own `vertices` pool. `cityjson_materials`' `id` is a **global interning by
  concatenation order** across header + features — *not* the index any row's
  `material_lod*` uses. So the writer re-emits each block onto the feature it came
  from and never merges them: merging preserves every count and identity a test
  would assert while silently re-pointing every reference. Blocks are carried as raw
  `json` so fields our `Material` / `Texture` structs do not model survive untouched.
- **`ValueToJson`'s default arm is a data-loss trap.** It renders anything without an
  explicit arm through `Value::ToString()` — DuckDB's *display* form — which rewrote
  every timestamp attribute from ISO-8601 to `YYYY-MM-DD HH:MM:SS`. **The loss is
  invisible to any row-level comparison**: the written value re-parses to the same
  `TIMESTAMP`, so only a `read_text` assertion catches it. Any new type the reader
  infers needs a matching arm here, not the default.
- **The COPY sink omits NULL attributes entirely** (`copy_function.cpp`, the
  `if (!val.IsNull())` guard). Two consequences. A key that is present-but-null in
  the source is written as absent — irreducible, since the reader surfaces both as
  SQL NULL, so it is pinned as an accepted loss in `cityjson_equivalence.test`. And
  an attribute that is null in *every* object is invisible to `fcb::add_attributes`,
  whose `guess_type` could not have typed a JSON null anyway — so the FCB writer
  declares attributes from the **source relation's column list** instead, and
  `FlatCityBufReader` additionally honours the header's own column declarations
  (appending, so the feature sample stays authoritative on type and valued columns
  are not downgraded to the header's `String`). Fixing only one of the two leaves a
  74-column source reading back as 68.
- **`flatcitybuf_metadata` reports `features_count`, not `city_objects_count`.** The
  header counts features; a row is a CityObject, and one feature may carry several.
  Reporting the former as the latter contradicted both other metadata functions and
  our own reader by ~2x on Delft. `city_objects_count` is SQL NULL for FCB — the true
  count needs a full decode a metadata call should not pay for, and a NULL is honest
  where a plausible wrong number is not. All three metadata functions share the
  schema, which is why the column is NULLed rather than dropped.

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
  the WKB path, which also preserves source order but closes them.
- `cityjson_geoparquet_geo` takes the same parameter and declares **no** column
  in `geo` under `arrow-native`: legality is a property of the encoding, not the
  CM type. Its `city` column still declares every geometry column, with
  `encoding` set to `CityParquetArrowNative-v1`.

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
- `test/sql/cityjson_remote.test` — HTTP reads for `read_cityjson`,
  `read_cityjsonseq`, all three metadata functions, and a hosted CityParquet
  package (`read_parquet` + `cityparquet_city_field` on the footer). Gated on
  `require-env CITYJSON_REMOTE_TEST`, ~25 MB of downloads, run with
  `just test-remote`. It carries the same three load-bearing incantations as
  `cityjson_fcb_remote.test` — above all `set ignore_error_messages`, without
  which a transport failure reports as a **skip** and the test silently passes.
- `test/sql/cityjson_fcb_remote.test` — HTTP range reads through `DuckDBRangeReader`,
  gated on `require-env FCB_REMOTE_TEST_URL` so a plain `make test` skips it. Run it
  with `just test-fcb-remote` (optionally `url=…`). **Caveat:** the bbox is a 500 m
  square hard-wired to the default hosted 3DBAG subset (RD New / EPSG:7415), and
  sqllogictest `test-env` defaults are only overridable through `--test-config`, so a
  different URL needs a matching bbox — the reader still materialises every matching
  feature in one go.

### DuckDB-Wasm target

**CI has always built wasm.** `_extension_distribution.yml`'s wasm job builds
`wasm_mvp`, `wasm_eh` and `wasm_threads`
(`extension-ci-tools/config/distribution_matrix.json`) under emsdk 3.1.71 and the
`wasm32-emscripten` triplet, with a plain `make <arch>`. What was missing was a way to
reproduce it locally.

**Local flow.** `just wasm-setup` once — installs the pinned emsdk and a vcpkg checkout
into the gitignored `.vendor/` (~2 GB, ~10 min), including an explicit `git fetch` of
`vcpkg.json`'s `builtin-baseline` commit, which a plain shallow clone does not contain
and without which manifest resolution fails. Then `just wasm` sources
`.vendor/emsdk/emsdk_env.sh` and runs `make wasm_mvp` with `VCPKG_TOOLCHAIN_PATH`
pointing into `.vendor/vcpkg`. The artefact lands at
`build/wasm_mvp/extension/cityjson/cityjson.duckdb_extension.wasm` (with a
repository-layout copy under `build/wasm_mvp/repository/v1.5.4/wasm_mvp/`); the native
`build/release` tree is untouched. Budget ~4 min for a clean build. Both pins live in
justfile variables (`emsdk_version`, `vcpkg_baseline`) with their CI provenance in
comments. Only `wasm_mvp` is wired up so far.

- **No `GEN=ninja` here, unlike every other build recipe.** The wasm targets in
  `extension-ci-tools/makefiles/duckdb_extension.Makefile` hardcode the build step as
  `emmake make -j8 -Cbuild/wasm_mvp`, so a Ninja-generated tree dies with "No targets
  specified and no makefile found"; CI sets no generator for wasm either. Recovering
  from that mistake also needs `rm -rf build/wasm_mvp`, since CMake refuses to switch
  generator in an existing cache.

**The side-module linking trap.** `build_loadable_extension()` behaves differently under
Emscripten: the target is a **static library**, for which the `LINK_LIBRARIES` property
is inert, and the real artefact comes from a separate post-build
`emcc … -sSIDE_MODULE=2 … ${TO_BE_LINKED}` command. `-sSIDE_MODULE` leaves unresolved
symbols as *imports* rather than erroring, so the build reports success and the `.wasm`
is rejected only at `LOAD` time (`bad export type for '…fcb::RangeReader::read_batch…':
undefined`). The knob is `DUCKDB_EXTENSION_CITYJSON_LINKED_LIBS`, set in `CMakeLists.txt`
immediately before the `build_loadable_extension()` call that consumes it,
space-separated because the function runs `separate_arguments()` on it. Generator
expressions are fine there — they resolve at generate time, so naming targets that
`find_package(flatcitybuf)` only creates further down the file works. **Any future
native dependency has to be added there too, along with its own transitive
dependencies**: `flatbuffers` is listed explicitly beside `flatcitybuf` because nothing
transitive survives a plain `emcc` invocation.

**`just test-wasm`** runs `test/wasm/run_wasm_smoke.sh`, a Node harness against
`@duckdb/duckdb-wasm` (pinned `1.33.1-dev57.0`, which carries DuckDB `v1.5.4` — the same
version as the submodule). Opt-in like `test/cpp/*`: `make test` never runs it, and it
needs `just wasm` to have produced the artefact (override with `CITYJSON_WASM_EXT`). It
asserts `pragma_platform()` is `wasm_mvp`, that the extension loads, and that
`read_cityjson('test/data/minimal.city.json')` and
`read_flatcitybuf('test/data/fcb_bbox_attr.fcb')` return the **native build's oracle
values** (1 row; 3 rows with `min(height) = 10.0`) — each oracle sits beside the command
that produced it in `smoke.mjs`.

The loading incantation is not guessable, and every part of it is load-bearing:

- **Offer the `mvp` bundle only.** Given a choice, `selectBundle()` picks `eh` under
  Node, `pragma_platform()` then reports `wasm_eh`, and a `wasm_mvp` extension cannot
  load into it.
- **`allowUnsignedExtensions: true`** in `db.open()` — the artefact is not signed by
  DuckDB Labs.
- **`LOAD '<path>'` does not go through DuckDB's filesystem**, so `registerFileBuffer()`
  is useless for it. Under Node the loader treats the argument as a URL and looks for a
  cached copy under `os.homedir()/.duckdb/extensions/<last four path segments>`; on a
  **miss** it spawns a worker and blocks on an `Atomics.wait` that a rejected fetch never
  notifies, so it hangs forever instead of erroring. The harness points `HOME` at
  `test/wasm/.cache-home` and seeds that slot, so no fetch is ever attempted.
- **The `wasm_mvp` bundle cannot report errors unshimmed.** It references `_setThrew` /
  `___cxa_can_catch` without ever defining them, so the first C++ exception surfaces as
  `ReferenceError: _setThrew is not defined` rather than the real message — reproducible
  on a stock instance with no extension loaded, and absent from the `eh` bundle. The
  harness wraps `WebAssembly.instantiate` / `WebAssembly.Instance` and binds the missing
  globals from the module's own exports, and keeps a standing assertion that a bad table
  name yields a `Catalog Error` and not a `ReferenceError`. Without it every failure the
  harness printed would be a lie: the side-module diagnosis above was invisible until the
  shim was in place.

Both of those last two are upstream duckdb-wasm defects, neither reported yet.

**Remote reads are XFAIL under Node, and `httpfs` is not the reason.**
`AutoLoadExtension` does not throw; DuckDB-Wasm's *Node* runtime simply implements one
data protocol — its `openFile()` handles `NODE_FS` and answers `HTTP` / `S3` / the
browser protocols with "Unsupported data protocol", which reaches SQL as an opaque
`error: 4061000` (verified on a stock instance with no extension loaded). The **browser**
runtime does do HTTP, via synchronous `XMLHttpRequest` range requests, so the same
artefact may well read remotely there; Node just cannot be where we prove it. The harness
therefore classifies rather than skips: only a failure matching that precise signature is
reported XFAIL, anything else is a hard failure, and the day the Node runtime learns HTTP
the assertion turns green on its own. `src/` carries no wasm-specific guard for any of
this.

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
