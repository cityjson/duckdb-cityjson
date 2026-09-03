# Traps

Things that are costly to rediscover. Read the section for a layer before changing it.

Behaviour lives in [FUNCTIONS.md](FUNCTIONS.md), architecture in
[DESIGN_DOC.md](DESIGN_DOC.md), working agreements in [../CLAUDE.md](../CLAUDE.md).

## Generated SQL and the pragma layer

The package-mutation functions are `PragmaFunction`s registered with a
`pragma_query_t`: the function *returns SQL text*, which DuckDB parses and runs in place
of the pragma, inside the caller's transaction
(`duckdb/src/planner/statement_preprocessor.cpp:107-122`). Atomicity is DuckDB's; the
extension only generates text.

- **Pragma expansion happens before execution**, for the *whole* submitted script. A
  generator's view of the catalog and data is pre-batch, so anything
  destination-dependent must be idempotent (`ADD COLUMN IF NOT EXISTS`) or deferred into
  the generated SQL. A schema and its first object table must therefore exist from an
  earlier statement, not the same one.
- **A PRAGMA cannot be a subquery.** `FROM (PRAGMA x)` is a parser error, so
  result-returning pragmas materialise a temp table and select from it.
- **PRAGMA named parameters use `=`, not `:=`** (`transform_pragma.cpp:26-33`).
- **No subqueries inside lambda bodies.** `list_filter(l, x -> x IN (SELECT ...))` is
  rejected; hoist the set into a session variable and use
  `list_contains(getvariable(...), x)`.
- **The `JSON` type and `json_extract` are unavailable.** They live in the `json`
  extension, which this one does not require. JSON is carried as `VARCHAR` and parsed in
  C++ with the vendored nlohmann::json. In tests, match with `LIKE`.
- **`parquet_kv_metadata` returns BLOB.** Use `decode(value)`, not `value::VARCHAR` —
  the cast escapes bytes and the JSON no longer parses.
- **`StringUtil::Join` takes `duckdb::vector`**, which `std::vector` does not convert to.
- **`SQLString` is a formatting wrapper, not a quoting function.** Use
  `KeywordHelper::WriteQuoted(text, '\'')`, and `WriteOptionallyQuoted` for identifiers.
- **Never re-derive what the reader emits.** A generator needing the incoming column list
  calls `InferCityJSONColumns` / `InspectCityJSONSource` (`bind_function.cpp`) — the same
  inference the bind runs. Reconstructing it from `GetDefinedColumns` +
  `InferAttributeColumns` + `InferGeometryColumns` gives a different answer, and the
  generated SQL then names a column the staged relation does not have.
- **`INSERT ... BY NAME` is not symmetric.** An unmatched *destination* column is left
  NULL, but a *source* column with no destination match is a binder error. Schema
  evolution must run over sidecars too, not only module tables — `geometry_templates`
  carries per-LoD columns, so its shape varies by file.
- **`bbox` is always present from this extension's own readers, but not from every
  table.** A source with only template geometry produces no `geometry_lod*` columns,
  but `bbox` is unconditional regardless (spec 02-object-table-schema.mdx, like
  `address`/`template`). A hand-rolled or foreign table can still lack it, so anything
  generating `UPDATE ... SET bbox` must check the column exists rather than assume a
  table this extension wrote.
- **`cityparquet_write` is a table function, not a pragma.** `KV_METADATA` accepts
  `getvariable()` (so a footer *value* can be computed in generated SQL) but cannot omit a
  *key*: a NULL value writes the literal string `"NULL"`. A solid-only table must write no
  `geo` key at all, legality is data-dependent, and SQL cannot branch the shape of a
  `COPY` — so the metadata is assembled in C++, at the cost of running on an internal
  connection and seeing only committed state.

### ODR link traps

Binding a *reference* to a `static constexpr` member emits a comdat definition that
collides with DuckDB's strong one.

- Write `LogicalType(LogicalTypeId::DOUBLE)`, not `LogicalType::DOUBLE`, in `emplace_back`.
- Use the non-templated `Catalog::GetEntry(context, CatalogType::TABLE_ENTRY, ...)`, not
  `Catalog::GetEntry<TableCatalogEntry>`, which ODR-uses `TableCatalogEntry::Name`.
- Use the scalar `FileOpenFlags::FILE_FLAGS_READ`, as `duckdb_fs_range_reader.cpp` does.

### Diagnostics

`DUCKDB_LOG_WARNING(context, …)` (`duckdb/logging/logger.hpp`) is the only user-visible
warning channel. The CLI enables logging at `WARNING` with its own storage and prints it;
a test asserts it with `SET enable_logging = true; SET logging_level = 'WARNING';` and a
query on `duckdb_logs`, whose in-memory storage the CLI replaces — the two views are not
interchangeable. The package writer's result rows are a file inventory, not a report.

## CRS

- **The footer's `crs` is tri-state, and absent is not "unknown".** GeoParquet's
  convention, which CityParquet adopts (spec `05-metadata.mdx`, "CRS rules"): a PROJJSON
  object means known; explicit `null` means the file holds CRS-bearing coordinates whose
  CRS is unknown or unresolvable; **absent means OGC:CRS84**. The key may be omitted only
  by a file with no CRS-bearing coordinate at all — the sidecars, since geometry templates
  are in local, unplaced coordinates. Both writers always emit the key for an object
  table, mirror the same value onto every `city.columns[]` / `geo.columns[]` entry, and
  never guess. An unresolvable CRS is a warning; only an explicitly supplied `crs =>` that
  cannot be resolved throws.
- **`cityparquet_city_field` maps both absent and null to SQL NULL**, so a missing footer
  and one declaring `"crs": null` look identical through it. `insert_cityjson` tells them
  apart by counting **object-table** footers separately (`role = 'object' AND city IS NOT
  NULL`), because only the latter is a stated unknown.
- **Comparing a CRS means comparing PROJJSON with PROJJSON.** A footer holds PROJJSON; a
  CityJSON source holds a `metadata.referenceSystem` URL. `insert_cityjson` resolves the
  source through `ProjjsonForReferenceSystem` and re-dumps it so both sides are the same
  canonical text — nlohmann orders object keys one way. Comparing the two raw makes every
  insert into a known-CRS package a bogus mismatch.
- **The one-CRS-per-package precondition is shared.** `DeclaredCrsExpr` / `CrsStatedExpr`
  / `OneCrsPerPackageSQL` / `CrsPreconditionSQL` in `cityparquet_sql_common` serve both
  `insert_cityjson` and `cityparquet_merge`; keep them there rather than growing a second
  reading. Read the declared CRS **only from object-table footers that declare one**: a
  sidecar carries no `crs` key, so a `DISTINCT` spanning every footer answers an ordinary
  package with two rows and the scalar subquery dies with "More than one row returned by a
  subquery" — a CRS bug whose message never mentions CRSs. Then apply the tri-state rule:
  known vs known compares, known vs unknown is refused **either way round** (an unknown
  cannot be shown to be the package's one CRS), unknown vs unknown passes, and a side
  whose footers are missing states nothing and is not checked.

## COPY: what the rows cannot carry

Two kinds of content are **file-level**, not row-level: the source's `metadata` header
(the CRS above all) and its `appearance` object — only the per-geometry *references* are
columns, never the definitions.

- **`COPY` recovers its source from the parsed SELECT.** `FindCopySourceRef`
  (`copy_source_ref.cpp`) walks `CopyInfo::select_statement` for exactly one
  `read_cityjson[seq]` / `read_flatcitybuf` / `read_obj` call; it survives to our bind
  because `bind_copy.cpp:97` copies rather than moves it and `:118` hands the whole
  `CopyInfo` to `CopyFunctionBindInput`. **An ambiguous query — no reader call, more than
  one, or a non-literal path — returns `nullopt`, never a guess**: stamping a wrong CRS
  onto georeferenced output is worse than stamping none. `COPY my_table TO …` is not
  discoverable, which is what `metadata_from` is for; a `DUCKDB_LOG_WARNING` fires when
  neither applies. Precedence: `crs` / `metadata_query` > `metadata_from` > discovered
  source.
- **An OBJ source has no JSON block to lift `appearance` from.** `LoadSourceAppearance`
  builds it instead from the same `OBJReader` the bind already opened for metadata
  (`ReadMetadata`), re-emitted via `Appearance::ToJson`; a second `OBJReader` over the
  same path is never constructed, since the first one memoises its parse and
  `ReadAllChunks` on it is free. The reader never renumbers a face's texture-coordinate
  indices per object — they stay file-global into the file's one `vt` pool —
  so that whole pool travels verbatim onto the `vertices-texture` block of each feature
  whose geometry carries a `texture`, rather than being sliced: index *i* means the same
  UV coordinate in every copy. A feature with no texture is stamped with nothing; the
  pool is one size for every object, so stamping it on all of them inflates the output by
  the object count. The pool is **not compacted per feature** — a textured feature
  carries the whole file's UVs, including the ones it never indexes; writer-side
  compaction (a per-feature pool with the refs renumbered against it) is a follow-up.
- **Appearance blocks are per-feature and their refs are feature-local.** In CityJSONSeq
  every feature carries its own `appearance`, and its material/texture indices are local
  to that block, exactly as its boundary indices are local to its own `vertices` pool.
  `cityjson_materials`' `id` is a **global interning by concatenation order** across
  header + features — *not* the index any row's `material_lod*` uses. The writer re-emits
  each block onto the feature it came from and never merges them: merging preserves every
  count and identity a test would assert while silently re-pointing every reference.
  Blocks are carried as raw `json` so fields the `Material` / `Texture` structs do not
  model survive untouched.
- **`ValueToJson`'s default arm is a data-loss trap.** It renders anything without an
  explicit arm through `Value::ToString()` — DuckDB's *display* form, which is not valid
  ISO-8601 for temporal types. **The loss is invisible to any row-level comparison**,
  because the written value re-parses to the same `TIMESTAMP`; only a `read_text`
  assertion catches it. Any new type the reader infers needs a matching arm, not the
  default.
- **The COPY sink omits NULL attributes entirely** (`copy_function.cpp`, the
  `if (!val.IsNull())` guard), with two consequences. A key that is present-but-null in
  the source is written as absent — irreducible, since the reader surfaces both as SQL
  NULL, and pinned as an accepted loss in `cityjson_equivalence.test`. And an attribute
  null in *every* object is invisible to `fcb::add_attributes`, whose `guess_type` cannot
  type a JSON null — so the FCB writer declares attributes from the **source relation's
  column list**, and `FlatCityBufReader` additionally honours the header's own column
  declarations, appending so the feature sample stays authoritative on type and valued
  columns are not downgraded to the header's `String`. Both halves are needed; one alone
  leaves a 74-column source reading back as 68.
- **`flatcitybuf_metadata` reports `features_count`, not `city_objects_count`.** The
  header counts features; a row is a CityObject, and one feature may carry several.
  `city_objects_count` is SQL NULL for FCB — the true count needs a full decode a metadata
  call should not pay for, and a NULL is honest where a plausible wrong number is not. All
  three metadata functions share the schema, which is why the column is NULLed rather than
  dropped.
- **Local and sidecar material cells are indistinguishable.** Only texture cells
  differ (`[id, 5, 6]` vs `[id, [u,v], …]`). Mesh COPY is therefore *told* the
  form: `materials_query`/`textures_query` present means sidecar. A source read
  with `appearance := 'sidecar'` is refused without them; `FindCopySourceRef`
  carries that flag. The refusal only fires for a **discovered** source — a
  materialised table (`COPY my_table TO … (FORMAT obj)`) hides the
  `read_cityjson[seq](…, appearance := 'sidecar')` call that produced it, so the
  form cannot be told and the cells silently resolve as local-form instead of
  being refused.
- **A material/texture cell also comes in two *shapes*, orthogonally to form.**
  This extension's own reader always re-serialises a row's material/texture
  column as the nested, per-shell form; the CityParquet spec defines the flat,
  per-WKB-face form. `BuildMeshModel` accepts both by inspecting nesting depth,
  because a mesh COPY source is not always this extension's own reader — a
  spec-conformant Parquet writer emits the flat shape, and both must colour the
  same faces.
- **Sidecar files and the temp-rename.** `Finalize` writes the main file to
  DuckDB's temp path, which is renamed afterwards. The `.mtl` and copied images
  (`obj`), and the `.bin` buffer and images (`gltf`), are not covered: they are
  written directly under the *final* stem, and `mtllib` / `buffers[0].uri` /
  each image's `uri` reference the final basename. Naming them from the temp
  path produces a file that references a name that never exists. A rename
  failure after a successful finalize therefore leaves the sidecars sitting
  beside a final path that was never created. For `obj`, images are copied
  lazily, inside the face loop, so a failure part-way through the write can
  leave copied images in the output directory with no `.obj` and no `.mtl`
  beside them: `WriteOBJ` closes the `.obj` after that loop and writes the
  `.mtl` after that again. `gltf`'s window is smaller but not absent:
  `WriteGltfSceneToFile` builds the whole `tinygltf::Model` in memory first,
  then writes the `.bin`, then every image, and only then opens the main file
  — so a main file that cannot be opened, or an image that fails after an
  earlier one succeeded, leaves those sidecars orphaned in the output
  directory.
- **tinygltf without stb.** Every translation unit that includes
  `tiny_gltf.h` must see `TINYGLTF_NO_STB_IMAGE` and
  `TINYGLTF_NO_STB_IMAGE_WRITE` (compile definitions on both targets); one TU
  without them makes the `TinyGLTF` constructor reference the stb-backed
  default callbacks and the link fails. With them, the default image writer
  is absent, so the `.gltf` path installs its own (`WriteRawImage`).
  `WriteRawImage`'s signature is tinygltf 2.9.x's eight-argument
  `WriteImageDataFunction`; it is not a virtual override and nothing checks it
  beyond the `SetImageWriter` call compiling, so re-check it against the
  typedef on any tinygltf version bump.
- **A `.gltf`'s sidecars.** tinygltf writes `buffers[0].uri` and image files
  relative to the *output path's directory*; that is DuckDB's temp path, which
  shares the final file's directory, so the names come from the final stem and
  the temp name never leaks.

## Readers

- **The CityJSONSeq reader is a forward stream.** `ReadAllChunks` and `ReadNFeatures`
  rewind so they can be asked more than once; `ReadNextFeature` deliberately does not,
  because it is the streaming scan's cursor.
- **Per-feature vertices are local.** In CityJSONSeq each line carries its own `vertices`,
  and geometry boundary indices reference those, not the header's.
- **`lod=` keeps the suffixed column grammar** (`geometry_lod2_2`, not a bare `geometry`),
  which is what keeps the LoD recoverable on export.
- **The OBJ and MTL parser is this extension's own** (`obj_parser.cpp`, a pure
  kernel with no DuckDB types, so `test/cpp/run_obj_parser_tests.sh` can compile
  it alone). Its rules, where OBJ leaves room: every real goes through `strtod`,
  so a decimal literal becomes the correctly-rounded double rather than a
  digit-by-digit accumulation one ULP off; a line whose last non-blank character
  is `\` continues on the next, joined with a single space before anything
  tokenises it — **except a comment**, which never continues, because the `#` on
  the joined line would then swallow whatever followed and an exporter writing a
  Windows path into its header would silently lose a line; a `mtllib` naming
  several files loads every one of them, in order, and a name it has already
  attempted is skipped; `o`, `usemtl` and `newmtl` name whatever the rest of the
  line holds, trimmed, while `g` takes the *last* whitespace token (`g a b` is
  group `b`); `map_Kd` drops the texture options before the file name and keeps
  the rest of the line, so a name holding spaces survives. The MTL is read line
  by line with no continuation: a `map_Kd` naming a Windows path ends in a
  backslash often enough that joining there would cost more than it buys.
- **A face's indices are raw as written** — 1-based, negative = relative to the
  end of the list so far, 0 or unparseable = absent — and resolving them is the
  parser's job. The count is checked first: a two-corner `f` is reported as a
  face with fewer than three vertices, not as a bad index.
- **OBJ state persists across `o`.** A `usemtl` or `g` before an `o` still
  governs the faces after it. `cube.obj`'s slab pins this.
- **A repeated `o` resumes its object**, so the reader keeps a name → index map
  beside the object list. Resolving each `o` by scanning the objects seen so far
  makes the parse quadratic in the object count, which a national tile
  (hundreds of thousands of `o` lines) feels as tens of seconds.
- **A material record must be able to say a directive was absent.** Every field
  of `MtlMaterial` that a `.mtl` may omit is an `optional`, because a
  default-initialised one cannot tell an unstated `Ks` from a black one, and the
  sidecar row's answer to "what does this material say about its specular
  colour?" is SQL NULL rather than a value the file never gave. Everything the
  mapping does not cover — `illum`, `Ni`, `map_Ks`, `map_bump`, whatever else —
  goes into `other` verbatim, key and rest of line. A block is created only by a
  `newmtl` with a name, so a `.mtl` that is empty or nothing but comments
  declares no material at all rather than one with an empty name; that is the
  case `mtllib '…' declares no material` warns about. A `usemtl` that matches
  nothing declared warns too, once per distinct name — the parse carries on with
  the face unmaterialled, since a missing `.mtl` must not cost the geometry.

## FlatCityBuf

`flatcitybuf` is a released vcpkg port resolved from a git registry scoped to that single
package (`https://github.com/HideBa/vcpkg`, pinned `baseline`); everything else stays on
the builtin baseline. The entry is removed once the upstream microsoft/vcpkg PR merges.

- **`cpp-v<version>` tags are the C++ releases.** The bare `v<version>` tags in
  `cityjson/flatcitybuf` are the Rust crate cut from the same train, so `v0.7.7` and
  `cpp-v0.7.7` are *not* the same code. The port is at `cpp-v0.9.0`.
- **`just vendor-fcb`** builds flatbuffers v25.9.23 and flatcitybuf `cpp-v0.9.0` into the
  gitignored `.vendor/prefix` with `-DCMAKE_POSITION_INDEPENDENT_CODE=ON` — the loadable
  extension is a shared object, so both static libs need PIC. Point CMake at it with
  `-Dflatcitybuf_DIR` / `-Dflatbuffers_DIR` or `CMAKE_PREFIX_PATH`. **Never a session
  scratchpad**: a garbage-collected prefix breaks every relink. The recipe re-checks out
  the pinned tag each run, so a bump upgrades an existing `.vendor/src` clone.
- **`FileInfo`'s schema-optional strings are `std::optional<std::string>`** (`identifier`,
  `title`, `reference_date`, every `poc_*` bar the address members), because the Rust
  oracle distinguishes absent from present-but-empty: `Some("")` emits the key with an
  empty value, only `None` omits it. Gate on `.has_value()`, never `.empty()`.
- **`FCB_WITH_CURL` stays off.** HTTP goes through DuckDB's FileSystem/httpfs via
  `DuckDBRangeReader` (`duckdb_fs_range_reader.cpp`), so there is one HTTP stack and one
  credentials/secrets/proxy story.

### Selective deserialisation (`FcbFieldMask`)

`read_flatcitybuf` decodes only what the query projects. Invariant either way: one output
row per CityObject, and every projected column's value byte-identical across both paths.

- **Honour the per-object column schema.** `to_cityjson_feature` decodes attributes
  against each CityObject's *own* schema when it declares one, falling back to the
  header's; the filtered walk selects the schema the same way. Records are not
  self-delimiting, so an unknown column index or width aborts the walk rather than
  guessing and desynchronising the blob.
- **The full path ignores `mask.attributes` by design.** The attribute blob is small next
  to geometry, and hand-rolling geometry conversion would duplicate upstream logic, so
  `ComputeFcbFieldMask` clears `attributes` to `nullopt` for a full-path mask rather than
  leaving a misleading subset.
- **`other` forces a full attribute decode.** `GetAttributeValue` builds it from every
  non-predefined, non-geometry attribute.
- **`bbox` counts as geometry-derived**, alongside `geometry_lod*` /
  `geometry_vertices_lod*` / `geometry_properties_lod*` / `material_lod*` /
  `texture_lod*` — it is computed from vertices, not read from a stored field.
  `IsGeometryDerivedColumn` classifies by `ColumnType` first, falling back to the name
  grammar, and errs towards decoding *more*: over-decoding is a lost saving,
  under-decoding is a wrong answer.
- **`read_flatcitybuf` materialises in `init_global`, not at bind.** Projection is known
  only at `TableFunctionInitInput::column_ids`, so `FlatCityBufBind` calls
  `BindCityJSONReadRaw(..., materialise = false)` and `FlatCityBufInitGlobal` runs the one
  real read, setting `use_global_chunks`. **Anything reading `bind_data.chunks` /
  `bind_data.scan_plan` sees nothing for FCB** — `MaterializedScan` picks the global-state
  override, and `FlatCityBufCardinality` answers from the header's `features_count` (an
  estimate: it counts features, rows are CityObjects).
- **`Byte` is UNSIGNED `u8`**, `UByte` is `u8`, `Binary` is a u32 LE length plus raw
  bytes. Decoding `Byte` as `i8` turns a stored 200 into -56. `BuildKeyValue` uses
  `KeyValue::from_u8` for `Byte`, because `compare_keys` *throws* when the supplied key's
  kind differs from the one `select_attr` derives from the column.
- **`Byte` / `UByte` / `Binary` parity is unverified by fixture.** `guess_type`
  (`writer/attribute.cpp`) emits only `Bool`/`Long`/`ULong`/`Double`/`String`/`DateTime`/
  `Json`, so nothing this stack writes can carry one, and `test_fcb_selective.cpp`'s T3
  light-vs-full comparison never sees one. They are covered only by T5's synthetic blobs,
  which exercise the light path alone. A real fixture would have to come from upstream.

## DuckDB-Wasm

CI builds `wasm_mvp`, `wasm_eh` and `wasm_threads`
(`extension-ci-tools/config/distribution_matrix.json`) under emsdk 3.1.71 and the
`wasm32-emscripten` triplet, with a plain `make <arch>`.

Locally: `just wasm-setup` once — installs the pinned emsdk and a vcpkg checkout into the
gitignored `.vendor/` (~2 GB, ~10 min), including an explicit `git fetch` of `vcpkg.json`'s
`builtin-baseline` commit, which a shallow clone does not contain and without which
manifest resolution fails. Then `just wasm <flavour>` sources `.vendor/emsdk/emsdk_env.sh`
and runs `make <flavour>` with `VCPKG_TOOLCHAIN_PATH` pointing into `.vendor/vcpkg`. The
artefact lands at `build/<flavour>/extension/cityjson/cityjson.duckdb_extension.wasm`; the
native `build/release` tree is untouched. ~4 min clean. Pins live in justfile variables
(`emsdk_version`, `vcpkg_baseline`).

`flavour` defaults to `wasm_mvp`, which is what `just test-wasm` asserts against. **A
browser needs `just wasm wasm_eh`**: `selectBundle()` picks the `eh` bundle wherever
native wasm exceptions are available, an `eh` instance can only load `eh` extensions,
and — per the error-reporting trap below — `eh` is the only flavour that surfaces a
DuckDB error message rather than a `ReferenceError`. The `eh` artefact loads into a
browser DuckDB-Wasm instance from a local extension repository:

```sql
SET custom_extension_repository='http://localhost:8080/ext';  -- <repo>/v1.5.4/wasm_eh/
INSTALL cityjson; LOAD cityjson;
```

with `allowUnsignedExtensions: true` in `db.open()`, since the artefact is not signed by
DuckDB Labs. The build also writes that layout itself, at
`build/<flavour>/repository/<version>/<flavour>/`.

- **No `GEN=ninja` here, unlike every other build recipe.** The wasm targets in
  `extension-ci-tools/makefiles/duckdb_extension.Makefile` hardcode
  `emmake make -j8 -Cbuild/wasm_mvp`, so a Ninja-generated tree dies with "No targets
  specified and no makefile found". Recovering also needs `rm -rf build/wasm_mvp`, since
  CMake refuses to switch generator in an existing cache.
- **The side-module linking trap.** Under Emscripten `build_loadable_extension()` makes
  the target a **static library**, for which `LINK_LIBRARIES` is inert; the real artefact
  comes from a post-build `emcc … -sSIDE_MODULE=2 … ${TO_BE_LINKED}`. `-sSIDE_MODULE`
  leaves unresolved symbols as *imports* rather than erroring, so the build reports
  success and the `.wasm` is rejected only at `LOAD` time (`bad export type for
  '…fcb::RangeReader::read_batch…': undefined`). The knob is
  `DUCKDB_EXTENSION_CITYJSON_LINKED_LIBS`, set in `CMakeLists.txt` immediately before the
  `build_loadable_extension()` call, space-separated because the function runs
  `separate_arguments()`. Generator expressions resolve at generate time, so naming
  targets `find_package(flatcitybuf)` creates further down works. **Any new native
  dependency goes there too, with its own transitive dependencies** — `flatbuffers` is
  listed explicitly because nothing transitive survives a plain `emcc` invocation.

`just test-wasm` runs `test/wasm/run_wasm_smoke.sh`, a Node harness against
`@duckdb/duckdb-wasm` (pinned `1.33.1-dev57.0`, carrying DuckDB `v1.5.4`, the submodule's
version). It needs `just wasm` to have produced the artefact (override with
`CITYJSON_WASM_EXT`) and asserts the native build's oracle values. Every part of the
loading incantation is load-bearing:

- **Offer the `mvp` bundle only.** Given a choice, `selectBundle()` picks `eh` under Node,
  `pragma_platform()` then reports `wasm_eh`, and a `wasm_mvp` extension cannot load into
  it.
- **`allowUnsignedExtensions: true`** in `db.open()` — the artefact is not signed by
  DuckDB Labs.
- **`LOAD '<path>'` does not go through DuckDB's filesystem**, so `registerFileBuffer()`
  is useless for it. Under Node the loader treats the argument as a URL and looks for a
  cached copy under `os.homedir()/.duckdb/extensions/<last four path segments>`; on a
  **miss** it spawns a worker and blocks on an `Atomics.wait` a rejected fetch never
  notifies, hanging forever instead of erroring. The harness points `HOME` at
  `test/wasm/.cache-home` and seeds that slot, so no fetch is attempted.
- **The `wasm_mvp` bundle cannot report errors unshimmed.** It references `_setThrew` /
  `___cxa_can_catch` without defining them, so the first C++ exception surfaces as
  `ReferenceError: _setThrew is not defined` rather than the real message — reproducible
  on a stock instance with no extension loaded, and absent from the `eh` bundle. The
  harness wraps `WebAssembly.instantiate` / `WebAssembly.Instance`, binds the missing
  globals from the module's own exports, and keeps a standing assertion that a bad table
  name yields a `Catalog Error` and not a `ReferenceError`. Without it, every failure the
  harness prints is a lie.
- **Remote reads are XFAIL under Node, and `httpfs` is not the reason.** DuckDB-Wasm's
  *Node* runtime implements one data protocol: `openFile()` handles `NODE_FS` and answers
  `HTTP` / `S3` / the browser protocols with "Unsupported data protocol", reaching SQL as
  an opaque `error: 4061000`. The **browser** runtime does do HTTP via synchronous
  `XMLHttpRequest` range requests, so the same artefact may read remotely there; Node
  cannot be where we prove it. The harness classifies rather than skips: only that precise
  signature is XFAIL, anything else is a hard failure, so the day Node learns HTTP the
  assertion turns green on its own. `src/` carries no wasm-specific guard.
- **Mesh output**: prefer GLB; `.gltf` and OBJ write sidecar files, which MEMFS holds but
  the browser cannot hand to the user as a set.

The `_setThrew` shim and the `Atomics.wait` hang are upstream duckdb-wasm defects,
neither reported.

## Test fixtures

- **`test/sql/cityjson_corpus_parity.test` uses upstream-produced fixtures.** The hosted
  corpus (`https://cityjson.open3d.city/corpus/`) holds the same three 3DBAG features as
  `small.city.jsonl`, `small.city.json`, `small.fcb` and the CityParquet package
  `small/building.parquet` + `metadata.json`, none written by this extension — so a
  disagreement between two of our readers is evidence about us rather than a circular
  oracle. All three CityJSON-family readers agree at 68 columns with a zero name+type
  diff; CityParquet is a superset by the reserved `address` and `template` columns,
  plus `other_attributes` -- an ordinary attribute column that fixture's writer
  happens to carry, not this format's reserved `other` escape hatch.
- **Do not assert WKB byte-equality between the two JSON readers on that corpus**, however
  much sharing `wkb_encoder.cpp` invites it. Its two files are quantised against different
  transform origins (`translate` `[85088.390625, 446394.25, 45.648…]` vs
  `[84593.249625, 446459.603, -0.304…]`), so the same real-world coordinate is stored as
  different integers and dequantises one ULP apart — the blobs differ while the geometry
  does not. Structure is asserted through WKB byte *length*; coordinates get a 1e-9
  tolerance.
- **`test/sql/cityjson_fcb_remote.test`'s bbox is hard-wired** to a 500 m square inside
  the default hosted 3DBAG subset (RD New / EPSG:7415). sqllogictest `test-env` defaults
  are overridable only through `--test-config`, so a different `FCB_REMOTE_TEST_URL` needs
  a matching bbox — and the reader materialises every matching feature in one go.
