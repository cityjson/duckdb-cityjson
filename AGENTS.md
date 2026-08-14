
# C++ Agent Guide

This document provides guidance to coding agents focusing on C++ deliverables when working in the DuckDB CityJSON Extension repository.

## Repository Context

- The loadable extension itself is implemented in C++17 and exposes DuckDB SQL functions. (The only Rust in the tree is the vendored `cjseq` library under `src/external/` — a dependency, not the extension.)
- C++ work in this repository usually targets integration scenarios: embedding the extension in C++ applications, authoring DuckDB C++ tests or utilities, and validating the FFI boundary between the Rust extension and DuckDB's C++ API.
- CityJSON data layout mirrors the CityParquet schema exposed by the extension. Inspect the column set through `DESCRIBE` statements or the SQL tests under `test/sql/` to map values in C++.

## Architecture Overview

YOU SHOULD REFERENCE THE DESIGN_DOC.md FILE FOR THE ARCHITECTURAL OVERVIEW.

### CityParquet package mutation layer

`src/cityjson/cityparquet_*.cpp` implements mutation of a CityParquet package held as a
DuckDB schema. These are **`PragmaFunction`s registered with a `pragma_query_t`**: the
function *returns SQL text*, which DuckDB parses and runs in place of the pragma, inside
the caller's transaction (`duckdb/src/planner/statement_preprocessor.cpp:107-122`).
Atomicity is DuckDB's, not ours — the extension only generates text. Each mutating
pragma has a scalar `*_sql()` twin returning the same text without running it.

Functions: `cityparquet_init`, `cityparquet_validate`, `cityparquet_orphans`,
`cityparquet_vacuum`, `cityparquet_reconcile`, `cityparquet_delete`, `cityparquet_merge`,
`cityparquet_read`, `insert_cityjson` (and `insert_cityjsonseq` / `insert_flatcitybuf`),
plus the scalars `cityjson_wkb_extent`, `cityjson_wkb_geometry_type`,
`cityjson_appearance_ids`, `cityjson_shift_appearance_ids` and `cityparquet_city_field`.

**`cityparquet_write` is the one exception to the pragma rule.** `KV_METADATA` accepts
`getvariable()` (so a footer *value* can be computed in generated SQL) but cannot omit a
*key*: a NULL value writes the literal string `"NULL"`. The spec requires a solid-only
table to write no `geo` key at all, legality depends on the data, and SQL cannot branch
the shape of a `COPY`. So it is a **table function** assembling the metadata in C++ — at
the cost of an internal connection that sees only committed state. See `CLAUDE.md` for the full table
and `README.md` for usage.

**Traps worth knowing before adding to this layer:**

- **Pragma expansion happens before execution**, for the *whole* submitted script, so a
  generator's view of the catalog and data is pre-batch. Anything destination-dependent
  must be idempotent or deferred into the generated SQL.
- **A PRAGMA cannot be a subquery** — `FROM (PRAGMA x)` is a parser error. Result-returning
  pragmas materialise a temp table and select from it.
- **PRAGMA named parameters use `=`, not `:=`** (`transform_pragma.cpp:26-33`).
- **No subqueries inside lambda bodies.** Hoist the set into a session variable and use
  `list_contains(getvariable(...), x)`.
- **The `JSON` type and `json_extract` are unavailable** (they live in the `json`
  extension, which this one does not require). JSON is carried as `VARCHAR` and parsed in
  C++ with the vendored nlohmann::json.
- **Two ODR link traps.** Binding a *reference* to a `static constexpr` member emits a
  comdat definition that collides with DuckDB's strong one: write
  `LogicalType(LogicalTypeId::DOUBLE)` rather than `LogicalType::DOUBLE` in
  `emplace_back`, and use the non-templated `Catalog::GetEntry(context,
  CatalogType::TABLE_ENTRY, ...)` rather than `Catalog::GetEntry<TableCatalogEntry>`,
  which ODR-uses `TableCatalogEntry::Name`.
- **`StringUtil::Join` takes `duckdb::vector`**, which `std::vector` does not convert to.
- **`parquet_kv_metadata` returns BLOB.** Use `decode(value)`, not `value::VARCHAR`.
- **A third ODR trap: `FileFlags::FILE_FLAGS_READ`** is a `static constexpr FileOpenFlags`.
  Use the scalar `FileOpenFlags::FILE_FLAGS_READ`, as `duckdb_fs_range_reader.cpp` does.
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

### Appearance normalisation

`src/cityjson/appearance_normalise.cpp` and `appearance_table_function.cpp` turn
CityJSON's feature-local appearance indices into the dataset-global sidecar ids and
inlined UVs CityParquet requires. `cityjson_materials(path)` / `cityjson_textures(path)`
emit the sidecar tables; `read_cityjson*(path, appearance := 'sidecar')` rewrites the
object rows to match. `'local'` remains the default. `cityjson_geometry_templates(path)` emits the
template sidecar; template geometry is in **local** coordinates and exempt from the
dataset transform, so no transform is applied when encoding it.

**Do not assume the header holds every definition.** CityJSONSeq spreads them: the header
carries some and each feature carries the ones it uses under its *own* local indices, so
a feature's material `0` is not in general the header's material `0`. `AppearanceIndex`
interns across the whole file by structural equality, header entries first so their ids
stay their ordinal positions. Reading the header alone silently resolves references to
the wrong definitions.

**Do not assume a nesting depth.** Material `values` nest per shell and surface depending
on geometry type, and a texture ring sits one level deeper for a `Solid` than for a
`MultiSurface`. Recurse to the leaves; recognise a ring as the innermost array (elements
are scalars) rather than indexing at a fixed level.

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
  `run_encoder_tests.sh` (the arrow-native encoder assertions). Both are outside
  `make test` and both need `FCB_PREFIX` plus a built `build/release`:

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

1. Run `make` once to prepare the DuckDB build environment. To make use of cache, try to use `GEN=ninja make` instead.
2. Build debug binaries with `make test_debug`. The loadable module lands in `build/debug/extension/cityjson/`. To use DuckDB with loadable extension, exec `./build/release/duckdb`.
3. For release artifacts run `make`.
4. You only need CMake or other C++ build tools when producing auxiliary C++ binaries/tests.

## Using the Extension from C++

- Include DuckDB's header (`duckdb.hpp`) from the DuckDB submodule or your system installation.
- Load the extension dynamically at runtime; the module requires DuckDB 1.4.1 (matching the bundled submodule).
- Example snippet:

```cpp
#include "duckdb.hpp"

int main() {
    duckdb::DuckDB db;
    duckdb::Connection conn(db);
    conn.Query("LOAD './build/debug/extension/cityjson/cityjson.duckdb_extension';");
    auto result = conn.Query("SELECT * FROM read_cityjson('example.city.json');");
    if (result->HasError()) {
        throw std::runtime_error(result->GetError());
    }
    // Access result->GetValue(row, column) to inspect rows.
    return 0;
}
```

- Remember to start DuckDB with unsigned extensions enabled (`allow_unsigned_extensions=true`) when required.

## Interop Notes

- The Rust extension uses Arrow-style column buffers under the hood. When exchanging data with C++, prefer DuckDB logical types and `Value` helpers instead of manual buffer manipulation.
- Geometry coordinates are stored as `LIST<STRUCT<x DOUBLE, y DOUBLE, z DOUBLE>>` columns. Use the logical type metadata returned by DuckDB (e.g., via `PRAGMA table_info`) when reproducing decode logic in C++.
- Metadata such as transforms and CRS live in JSON columns; use DuckDB's JSON functions from C++ queries rather than custom parsers where possible.

## Testing

- Primary tests live under `test/sql/*.test`. Run them with `make test` or `make test_debug` after C++ changes that impact observable behaviour.
- **Rebuild `unittest` first.** `make test` runs `build/release/test/unittest` but does
  **not** rebuild it, so a change-verification loop must include that target:
  `cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb unittest`.
  Omit it and you test a stale binary and get a green report on code that never ran.
  `just rebuild` / `just t` already include it.
- For C++ unit tests, link against DuckDB's testing utilities (located in the DuckDB submodule). Keep them in `test/cpp/` if you introduce new suites.
- Always verify sample CityJSON files across `read_*` and `write_*` functions to ensure encoding parity with the Rust implementation.

## Performance & Memory Guidance

- Batch operations through SQL queries rather than row-by-row API calls; DuckDB's vectorised execution is far more efficient.
- When marshalling large geometries from C++, avoid unnecessary copies. Use `duckdb::Appender` for bulk inserts into staging tables.
- Be mindful of transform metadata; reapply the same scale/offset semantics as the Rust code when emitting raw coordinates.

## Contribution Workflow

- Mirror the Rust code style for documentation and naming when adding C++ bridging code. Use `clang-format` with DuckDB's style if you add new `.cc`/`.hh` files.
- Keep FFI boundaries minimal. Expose new Rust capabilities through SQL functions first; only add direct C/C++ hooks when unavoidable.
- Document any new SQL surface area in both `README.md` and the relevant `.test` files so Rust and C++ contributors share the same contract.

## References

- DuckDB C++ API: <https://duckdb.org/docs/stable/clients/c/api>
- CityJSON specification: <https://www.cityjson.org/specs/2.0.1/>
- DuckDB headers: `duckdb/src/include/duckdb.hpp`, `duckdb/src/include/duckdb/common/types.hpp`
