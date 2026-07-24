# FlatCityBuf: migrate to native C++ reader/writer, add bbox + attribute pushdown, add COPY TO write path

Date: 2026-07-24
Status: approved for planning (design phase complete)

## 1. Motivation

`duckdb-cityjson`'s optional FlatCityBuf (`.fcb`) support is currently built on a Rust
`fcb_cpp` crate compiled with the `cxx` bridge, distributed as a prebuilt static library
downloaded per-platform/arch from GitHub releases. This has real costs:

- Requires OpenSSL + a Rust runtime shape baked into the binary.
- Disabled on musl and glibc < 2.32 Linux, and on macOS x86_64 — no valid prebuilt exists.
- No bbox or attribute query capability at all: `read_flatcitybuf` always does a full
  `fcb_reader_select_all` scan, decoding every feature to JSON regardless of what the
  query actually needs.
- Writing requires building a `CityJSONSeq`-shaped JSON document with an identity/zero
  transform hack and feeding it through the FFI writer one feature at a time — but does
  work today via `fcb::fcb_writer_new/add_feature/write`.

Upstream (`cityjson/flatcitybuf`) merged PR #45: a complete **native C++17
reimplementation** for C++ consumers, deleting the Rust/cxx bridge entirely. It adds:
a synchronous `RangeReader` abstraction (pluggable transport — file, HTTP, or a host's
own I/O), R-tree bbox queries (`select_bbox`), B+tree attribute queries (`select_attr`,
`Eq/Ne/Gt/Ge/Lt/Le`), and a genuine native writer (`FcbWriter`) with configurable
per-column attribute indices and R-tree tuning. This is the basis for this migration.

## 2. Upstream dependency

- Repo: `github.com/cityjson/flatcitybuf`, `src/cpp/` subtree.
- **Pin to commit `72e5b68d469aa00a75ccba23780e2063751e3cff`** (current `main` HEAD as of
  2026-07-24). `src/cpp/CMakeLists.txt` declares `project(flatcitybuf VERSION 0.8.0 ...)`
  but **no `v0.8.0` tag exists yet** — this is unreleased. Revisit the pin once a tag
  lands; until then, treat the API as more likely to shift than a tagged release would
  be (see Risks, §9).
- Ships as a proper CMake package: `find_package(flatcitybuf CONFIG REQUIRED)` +
  `target_link_libraries(... flatcitybuf::flatcitybuf)`. Has its own `vcpkg.json`
  (deps: `flatbuffers`, `nlohmann-json`; optional vcpkg features `curl`, `tests`).
- Build options relevant to us: `FCB_WITH_JSON` (default ON — CityJSON (de)serialization,
  needs `nlohmann-json >= 3.2.0`, already satisfied by this repo), `FCB_WITH_CURL`
  (default OFF — **we will leave this OFF**, see §5.2), `FCB_BUILD_TESTS`/
  `FCB_BUILD_EXAMPLES` (OFF for our vendored build — we don't need flatcitybuf's own
  test/example binaries).
- No `flatc` compile step needed — generated FlatBuffers headers
  (`include/fcb/generated/*.h`) are checked into the upstream repo.

### Public API surface we depend on (verified by reading the actual headers)

```
fcb::RangeReader            // abstract: total_size(), read(offset,length), read_batch(...)
fcb::FileRangeReader         // local-file adapter (not used directly by us — see §5.2)
fcb::BufferedRangeReader     // caching/prefetch decorator, per-query, wraps any RangeReader

fcb::FcbReader::open_file(path)
fcb::FcbReader::open(shared_ptr<RangeReader>)
  .header() -> const HeaderView&
  .select_all()               -> FeatureIterator
  .select_bbox(BBox)          -> FeatureIterator   // R-tree, 2D only
  .select_attr(AttrQuery, AttrQueryOptions{})       -> FeatureIterator   // B+tree

fcb::FeatureIterator::next() / .current() -> const Feature&
fcb::Feature                  // owns its decoded bytes; per-CityObject attribute/columns/extent accessors

fcb::HeaderView::info() -> const FileInfo&          // features_count, columns, crs, transform, extent, ...
fcb::HeaderView::attr_indices() -> const vector<AttrIndexInfo>&   // which columns have a B+tree index

fcb::BBox{min_x,min_y,max_x,max_y}                  // 2D query rectangle
fcb::Operator{Eq,Ne,Gt,Ge,Lt,Le}
fcb::AttrCondition{field, Operator, KeyValue}
fcb::AttrQuery = vector<AttrCondition>               // AND-combined
fcb::KeyValue::from_i8/u8/i16/u16/i32/u32/i64/u64/f32/f64/bool/datetime/string(kind, text)
fcb::KeyKind                                         // per-column key encoding, from key_kind_for_column(ColumnInfo.type)

fcb::to_cityjson_metadata(HeaderView)      -> nlohmann::json   // CityJSONSeq metadata-line shape
fcb::to_cityjson_feature(Feature, HeaderView) -> nlohmann::json // CityJSONFeature shape

fcb::FcbWriterOptions{
  write_index = true,
  index_node_size = kDefaultNodeSize,
  attribute_indices: vector<pair<string /*column*/, optional<uint16_t> /*branching factor*/>>,
  geographical_extent: optional<array<double,6>>,
}
fcb::FcbWriter(nlohmann::ordered_json cj_metadata, FcbWriterOptions,
               AttributeSchema attr_schema, optional<AttributeSchema> semantic_attr_schema)
  .add_feature(nlohmann::ordered_json city_json_feature)
  .write(std::ostream&)          // streaming finalize, bounded memory — USE THIS ONE
  .write() -> vector<uint8_t>    // convenience, materializes everything — do not use for real writes

fcb::add_attributes(AttributeSchema&, const nlohmann::ordered_json& attrs)   // two-pass schema building
fcb::Error : std::runtime_error, .code() -> ErrorCode{..., AttributeIndexNotFound, ...}
```

Not carried over from the old integration: attribute values must be typed correctly
against the column's on-disk type before building a `KeyValue` (get it wrong and you get
silently wrong comparisons, not a throw) — mirror `query_attributes.cpp`'s `make_value()`
dispatch, keyed off `ColumnInfo.type`.

## 3. Non-goals

- We do not implement `FCB_WITH_CURL`/libcurl HTTP (see §5.2 — DuckDB `FileSystem`
  covers this instead).
- We do not expose per-column branching factors in `COPY TO` (one global
  `branching_factor` applied to every indexed column — see §5.5).
- We do not implement appearance (material/texture) round-tripping through FlatCityBuf
  beyond whatever the existing JSON-based writer path already does — this migration is
  about the transport/query layer, not appearance semantics.
- We do not change `read_cityjson`/`read_cityjsonseq` behavior at all.

## 4. Current state (files touched)

All FlatCityBuf-specific code is `#ifdef CITYJSON_HAS_FCB`-guarded in:

| File | Role today |
|---|---|
| `CMakeLists.txt` (lines ~59-61, 96-286) | Downloads prebuilt `libfcb_cpp.a`/`.lib` per OS/arch, disables on musl/old-glibc/unsupported arch, links `lib.rs.cc` (cxx-generated bridge source) |
| `src/include/cityjson/flatcitybuf_reader.hpp`, `src/cityjson/flatcitybuf_reader.cpp` | `FlatCityBufReader : CityJSONReader`, calls `fcb::fcb_reader_open/select_all/metadata` |
| `src/include/cityjson/flatcitybuf_table_function.hpp`, `src/cityjson/flatcitybuf_table_function.cpp` | Registers `read_flatcitybuf`, `flatcitybuf_metadata` |
| `src/cityjson/reader_factory.cpp` | `OpenAnyCityJSONFile` dispatches `.fcb` to `FlatCityBufReader` |
| `src/include/cityjson/cityjson_writer.hpp`, `src/cityjson/cityjson_writer.cpp` (`WriteFlatCityBuf`) | Builds CityJSONSeq-shaped JSON, feeds `fcb::fcb_writer_new/add_feature/write` |
| `src/cityjson/copy_function.cpp` (`is_fcb`, `RegisterFlatCityBufCopyFunction`) | `COPY ... TO (FORMAT flatcitybuf)` |
| `src/cityjson_extension.cpp` | Registration under `#ifdef CITYJSON_HAS_FCB` |
| `vcpkg.json`, `README.md` | No FCB-specific vcpkg deps today (binary is downloaded directly, not through vcpkg) |

Every one of these needs to change. `src/include/cityjson/reader.hpp` (the abstract
`CityJSONReader` interface) and the generic bind/scan machinery
(`src/cityjson/bind_function.cpp`, `src/include/cityjson/table_function.hpp`) are
extended, not replaced — see §5.3/§5.4.

## 5. Design

### 5.1 Vendoring

`extension-ci-tools/` (including `extension-ci-tools/vcpkg_ports/`) is a **git
submodule** pinned to `duckdb/extension-ci-tools@v1.5.4` — per repo convention we do
not edit submodule content from here. Instead:

- Add a new, repo-owned overlay-port directory: `vcpkg_ports/flatcitybuf/` (root of
  `duckdb-cityjson`, sibling to `extension-ci-tools/`), containing a `portfile.cmake`
  (`vcpkg_from_github(REPO cityjson/flatcitybuf REF 72e5b68d469aa00a75ccba23780e2063751e3cff ...)`
  configuring `SOURCE_PATH/src/cpp` as the CMake project, `-DFCB_WITH_CURL=OFF
  -DFCB_BUILD_TESTS=OFF -DFCB_BUILD_EXAMPLES=OFF`) and a `vcpkg.json` for the port
  (depends on `flatbuffers`, `nlohmann-json`).
- Root `vcpkg.json`: add a second entry to `vcpkg-configuration.overlay-ports` —
  `["./extension-ci-tools/vcpkg_ports", "./vcpkg_ports"]` — and add `"flatbuffers"` to
  `dependencies`.
- Root `CMakeLists.txt`: replace the entire prebuilt-binary-download block
  (~lines 96-286) with:
  ```cmake
  option(CITYJSON_ENABLE_FCB "Enable FlatCityBuf (.fcb) support" ON)
  if(CITYJSON_ENABLE_FCB)
    find_package(flatcitybuf CONFIG REQUIRED)
    target_link_libraries(${EXTENSION_NAME} PRIVATE flatcitybuf::flatcitybuf)
    target_link_libraries(${LOADABLE_EXTENSION_NAME} PRIVATE flatcitybuf::flatcitybuf)
    target_compile_definitions(${EXTENSION_NAME} PRIVATE CITYJSON_HAS_FCB)
    target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE CITYJSON_HAS_FCB)
  endif()
  ```
  No more per-OS/arch/glibc branches — `CITYJSON_ENABLE_FCB` defaults **ON** on every
  platform, since this is now portable C++ source built by the same toolchain as the
  rest of the extension. `CITYJSON_HAS_FCB` keeps meaning what it means today, so
  every existing `#ifdef CITYJSON_HAS_FCB` guard in the six files above stays valid —
  only what's inside most of them changes.
- `test/sql/cityjson_e2e_fcb.test`'s `require notmusl` line is deleted (no longer a
  real restriction) — CI is expected to exercise FCB on every runner going forward.

### 5.2 Reader replacement + HTTP transport

`FlatCityBufReader` (`flatcitybuf_reader.cpp/.hpp`) is rewritten against the new API.
Shape:

```cpp
class FlatCityBufReader : public CityJSONReader {
public:
  FlatCityBufReader(ClientContext &context, const std::string &name,
                     const std::string &file_path, size_t sample_lines = 100);

  void SetBBoxFilter(std::array<double, 4> bbox);              // min_x,min_y,max_x,max_y
  void SetAttrQueryFilter(fcb::AttrQuery query, bool exact_index_only = false);
  std::vector<std::string> IndexedAttributeColumns() const;    // for pushdown eligibility checks
  fcb::HeaderView Header() const;                              // by value: HeaderView owns its own
                                                                // backing buffer (shared_ptr), and this
                                                                // reader reopens a fresh fcb::FcbReader
                                                                // per call rather than keeping one alive

  // CityJSONReader overrides unchanged in signature: ReadMetadata/ReadNthChunk/
  // ReadAllChunks/ReadNFeatures/Columns — internally now open a fresh fcb::FcbReader
  // per call (matching today's per-call-reopen pattern) and dispatch to select_bbox/
  // select_attr/select_all depending on which filters are set, decoding each hit via
  // fcb::to_cityjson_feature() -> CityJSONFeature::FromJson (unchanged downstream).
private:
  std::shared_ptr<fcb::RangeReader> OpenTransport() const;     // see below
  ...
};
```

**Transport.** A new small adapter, `DuckDBRangeReader : public fcb::RangeReader`
(new files `src/cityjson/duckdb_fs_range_reader.cpp` /
`src/include/cityjson/duckdb_fs_range_reader.hpp`), backed by
`duckdb::FileSystem::GetFileSystem(context).OpenFile(path, FileFlags::FILE_FLAGS_READ)`
+ `Read(handle, buffer, length, offset)` + `GetFileSize(handle)`. This serves local
paths *and* http(s)/s3/gcs URLs uniformly through the already-autoloaded `httpfs`
extension (same auto-load call already used in `json_utils.cpp`/
`local_cityjsonseq_reader.cpp`), giving FlatCityBuf remote reads the same
credentials/secrets/proxy story as `read_cityjson`/`read_cityjsonseq` already have. We
do **not** build with `FCB_WITH_CURL` — no libcurl dependency, no second HTTP stack.
`read_batch` can keep the default sequential-loop implementation from
`fcb::RangeReader` initially; a follow-up can override it against DuckDB's own
multi-range support if that ever becomes a measured bottleneck (not in scope now).

`flatcitybuf_metadata` (`FcbMetadataBind`/`FcbMetadataScan`) is updated to read
`features_count` from `reader->Header().info().features_count` — no more separate raw
`fcb::fcb_reader_open`/`fcb_reader_metadata` calls.

### 5.3 `read_flatcitybuf` bbox query

New named parameters on `read_flatcitybuf`: `min_x`, `min_y`, `max_x`, `max_y`
(all `DOUBLE`). Binder requires all four together or none (error otherwise — a partial
bbox is a user mistake, not a silently-ignored no-op).

`FlatCityBufBind` (in `flatcitybuf_table_function.cpp`) gains a dedicated bind-data
subclass:

```cpp
struct FlatCityBufBindData : public CityJSONBindData {
  std::optional<std::array<double, 4>> bbox;
  // Kept alive past Bind so FlatCityBufPushdownComplexFilter (§5.4) can re-query it.
  // shared_ptr, not unique_ptr: Copy() just shares it, which is safe because nothing
  // mutates it after the pushdown-filter step (see §5.4 point 4).
  std::shared_ptr<FlatCityBufReader> reader;
  unique_ptr<FunctionData> Copy() const override;
  bool Equals(const FunctionData &other) const override;
};
```

When `min_x`/`min_y`/`max_x`/`max_y` are given, `FlatCityBufBind` calls
`reader->SetBBoxFilter({min_x, min_y, max_x, max_y})` **before** invoking
`BindCityJSONRead` — so the very first `ReadMetadata()`/`ReadAllChunks()` call already
runs `select_bbox` internally and only decodes intersecting features. This is a real
R-tree-level skip, not a post-filter: non-intersecting features are never even byte-read
from the transport (beyond what `select_bbox`'s tree traversal itself touches).

### 5.4 Attribute-query pushdown from `WHERE`

`fcb::AttrQuery` only helps if the query is known *before* materialization. Bbox is
known immediately (it's a bind-time argument), but a `WHERE` clause is only available
after `pushdown_complex_filter` runs — which DuckDB invokes during query optimization,
**after** `Bind` has already completed. Since `read_flatcitybuf` currently
materializes fully during `Bind` (non-streaming path, `BindCityJSONRead`'s default
`streaming=false`), a `pushdown_complex_filter` callback that merely stashes the
condition (like today's `id`/`feature_id`/`object_type` equality filters do) would
gain nothing — the full unfiltered read already happened.

Design: `FlatCityBufPushdownComplexFilter` (new function, registered only on
`read_flatcitybuf`'s `TableFunction`, distinct from the existing
`CityJSONPushdownComplexFilter` used by `read_cityjson`/`read_cityjsonseq`) is allowed
to **redo the read** when it successfully consumes ≥1 condition:

1. Walk `filters` the same way `CityJSONPushdownComplexFilter` does (matching
   `BoundComparisonExpression` of a column ref against a constant, either operand
   order — flip the operator when the column is on the right, e.g. `5 < col` becomes
   `col > 5`).
2. For each candidate, look up the column in
   `flatcitybuf_bind_data.reader->IndexedAttributeColumns()` (built from
   `header().attr_indices()`). Only `=,!=,>,>=,<,<=` against an **indexed** column are
   eligible; anything else (unindexed column, `LIKE`, `IS NULL`, `OR`, ...) is left in
   `filters` for DuckDB to evaluate normally post-scan, exactly as happens today for
   filters `CityJSONPushdownComplexFilter` doesn't recognize.
3. Convert the constant to a `fcb::KeyValue` typed against the column's on-disk
   `ColumnInfo.type` (dispatch table mirroring `query_attributes.cpp`'s `make_value()`).
4. If ≥1 condition was consumed, call `reader->SetAttrQueryFilter(conditions)` and
   **re-run only `ReadAllChunks()`** (not `ReadMetadata()` — CRS/transform/extent are
   filter-independent, and metadata is already cached on the reader from the initial
   bind) against the *same* `FlatCityBufReader` instance held in
   `FlatCityBufBindData::reader`, replacing `bind_data.chunks` and
   `bind_data.scan_plan` (`= chunks.BuildScanPlan()`) in place. `bind_data.columns`
   (the schema) is untouched — filtering never changes the schema.
5. If a bbox was *also* set (§5.3), `SetAttrQueryFilter` combines with it by using
   `select_bbox` for the actual index traversal (the real spatial skip) and checking
   the attribute conditions per-candidate during decode, rather than trying to
   intersect two independent index traversals (`select_bbox` and `select_attr` are
   two separate queries with no combined form in the upstream API). Bbox-only and
   attribute-only cases each get their one real index-assisted query; the combined
   case still gets the bbox skip, just not a double index-assisted skip. This is
   documented as a known, deliberate limitation, not a bug.
6. Querying a column with no B+tree index is not an error at the SQL level — it's
   simply not eligible for pushdown (step 2 excludes it), and DuckDB filters it
   post-scan as it does for any predicate today.

### 5.5 `COPY TO ... (FORMAT flatcitybuf)` write path

New `COPY` options (parsed in `CityJSONCopyToBind`, alongside the existing
`version`/`crs`/`transform_scale`/`transform_translate`/`metadata_query`):

| Option | Type | Description |
|---|---|---|
| `attr_index` | VARCHAR | Comma-separated list of attribute column names to give a B+tree index |
| `branching_factor` | BIGINT | B+tree branching factor applied to every column in `attr_index` (upstream default if omitted) |
| `index_node_size` | BIGINT | R-tree node size (`FcbWriterOptions.index_node_size`; upstream default if omitted) |

`CityJSONCopyBindData` gains:
```cpp
std::vector<std::string> fcb_attr_index_columns;   // parsed from attr_index, empty = none
std::optional<uint16_t> fcb_branching_factor;
std::optional<uint16_t> fcb_index_node_size;
```

`CityJSONWriter::WriteFlatCityBuf` is reimplemented against `fcb::FcbWriter`. The
existing sink/combine/finalize flow already fully materializes `feature_objects`
(map of feature id -> `[(city_object_id, city_object_json)]`) and `feature_order`
before `Finalize` calls the writer, so the two-pass schema scan `FcbWriter` requires
is a natural fit at that point:

1. Pass 1: for every feature/object in `feature_order`, call `fcb::add_attributes`
   into an ordinary `AttributeSchema`, and separately into a semantic-surface
   `AttributeSchema` for each geometry's `semantics.surfaces[i]`'s non-`type`/
   `parent`/`children` members (mirrors upstream's own `write_cityjson.cpp` example).
2. Build `FcbWriterOptions`: `attribute_indices` = one `(column_name,
   branching_factor)` pair per entry in `fcb_attr_index_columns` that's actually
   present in the ordinary schema (silently skip a requested column that never
   appears in any feature's attributes — nothing to index); `index_node_size` from
   the option or upstream default.
3. Construct `fcb::FcbWriter(header_json, options, attr_schema,
   has_semantic_attrs ? semantic_attr_schema : nullopt)`, `add_feature(...)` once per
   feature in `feature_order`, then `write(ofstream)` (the bounded-memory streaming
   overload — never the vector-returning convenience overload).

Requesting `attr_index` on a column name that never appears in any feature is not an
error — it's simply not indexed (nothing to index). Requesting `branching_factor` or
`index_node_size` without `attr_index` for the former (or ever, for the latter) is
accepted and applied where relevant.

## 6. Testing strategy (TDD)

This repo has no C++ unit-test harness — behavior is specified and verified through
`test/sql/*.test` sqllogictest files. Implementation proceeds red/green/refactor at
that granularity, roughly in this phase order (each phase: write the failing test(s)
first, confirm the failure mode is the expected one — parse error / wrong row count /
wrong column — then implement, then confirm green):

1. **Vendoring + read/write parity** — existing `cityjson_e2e_fcb.test` (with
   `require notmusl` removed) must pass unchanged against the new native build, proving
   the swap didn't regress `read_flatcitybuf`, `flatcitybuf_metadata`, or
   `COPY ... TO (FORMAT flatcitybuf)` round-trips.
2. **Bbox query** — new test: a multi-feature `.fcb` fixture (built via phase-1's
   `COPY TO`), query with `min_x/min_y/max_x/max_y` covering a known subset, assert
   exact row count and IDs; assert an empty-intersection bbox returns zero rows (not
   a full scan); assert a missing-subset-of-four-params errors.
3. **Attribute pushdown** — new test: write a `.fcb` with `attr_index` set on a known
   column (phase 4 must land first, or a fixture pre-built for this test), query with
   `WHERE indexed_col > N`, assert correct rows; assert a `WHERE` on a **non**-indexed
   column still returns correct rows (falls back to normal filtering, proving
   pushdown-ineligibility doesn't break correctness); assert combined bbox + attribute
   WHERE returns the correct intersection.
4. **COPY TO options** — new tests: `attr_index`/`branching_factor`/`index_node_size`
   round-trip (write then read back row-for-row identical to a plain write); a
   `flatcitybuf_metadata`-equivalent inspection (or a follow-up query using the
   attribute pushdown from phase 3) confirming the requested column actually got an
   index the reader can query.
5. **HTTP transport** — new test gated behind whatever this repo's existing pattern is
   for remote-file tests (check for an existing local HTTP-serving test fixture
   pattern before inventing one), reading a `.fcb` over `http://` through
   `DuckDBRangeReader`/`httpfs` and asserting correct rows, ideally combined with a
   bbox filter to prove range-request behavior end-to-end at the SQL level (exact
   request-count assertions are out of reach from SQL tests — that level of
   verification, if wanted, is a manual/example-program check, not a sqllogictest).

`README.md` and (if present) `AGENTS.md` get updated to match: remove the
platform-restriction caveats, document `min_x/min_y/max_x/max_y`, document the new
`COPY TO` options, and update the "Optional: FlatCityBuf Support" build section to
describe the vcpkg overlay port instead of `-DCITYJSON_ENABLE_FCB=ON`'s old
prebuilt-binary download behavior (the flag name and default-on behavior stay, the
mechanism behind it changes).

## 7. Risks / open questions

- **Unreleased upstream commit.** Pinning to a `main`-branch commit rather than a tag
  means the API could still shift before `v0.8.0` is actually cut. If upstream tags a
  release during implementation, re-pin to it (should be a one-line `REF` change in
  the vendored portfile, assuming no breaking changes land alongside the tag).
  Otherwise, note the exact vetted commit in the port so a future bump is a deliberate,
  reviewed action, not silent drift.
- **vcpkg availability of `flatbuffers` in this project's baseline.** Not yet
  confirmed against this repo's pinned vcpkg baseline/registry — first implementation
  step should be a smoke build (`find_package(flatbuffers CONFIG REQUIRED)` alone)
  before writing any FlatCityBuf-specific code, to surface this early.
- **musl / cross-arch CI.** Dropping the old platform gate is only safe if the vcpkg
  overlay port actually builds flatcitybuf + its deps from source on every CI runner
  this repo targets (musl included) — first real test of this claim is CI itself, not
  local dev.
- **Combined bbox + attribute pushdown** intentionally does not get a double
  index-assisted skip (§5.4 point 5) — acceptable for v1, flagged rather than silently
  accepted as "fully optimal."

## 8. Process notes

- Design informed by an advisory pass from Fable (model `claude-fable-5`) on vendoring
  strategy, HTTP transport, bbox API shape, attribute pushdown scope, and `COPY TO`
  option shape; the HTTP-transport recommendation (DuckDB `FileSystem`-backed
  `RangeReader` over flatcitybuf's own libcurl option) was confirmed directly with the
  user.
- Per user request, implementation follows strict red/green/refactor TDD at the
  `test/sql/*.test` granularity (§6).
- Per user request, an independent review pass via `codex exec --model gpt-5.6-sol`
  runs over the finished diff before the branch is considered done.
