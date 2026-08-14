# FlatCityBuf: release-based dependency, HTTP verification, selective deserialisation

**Date:** 2026-08-14
**Status:** approved for autonomous execution (user pre-authorised; Fable plans/reviews, Opus executes, TDD)

## 1. Context — what the request asked for vs. what the tree already has

The request: replace the FlatCityBuf C++-bindings dependency with the official native C++
implementation (release v0.7.7), depend on it through the user's vcpkg fork
(`HideBa/vcpkg`, upstream PR to microsoft/vcpkg pending), gain HTTP range-request
reading, and add selective deserialisation (skip geometry / unneeded attributes).

What `develop` already contains (from the 2026-07-24 migration,
`docs/superpowers/specs/2026-07-24-flatcitybuf-native-cpp-migration-design.md`):

- `FlatCityBufReader` and `WriteFlatCityBuf` are **already native** — built on
  `fcb::FcbReader` / `fcb::FcbWriter` from `cityjson/flatcitybuf`, pinned to commit
  `72e5b68` via a repo-local vcpkg overlay port (mislabeled `0.8.0`).
- **HTTP range reads already work in principle**: the transport is `DuckDBRangeReader`
  (`duckdb_fs_range_reader.cpp`), an `fcb::RangeReader` over DuckDB's own FileSystem,
  auto-loading httpfs for remote paths. Verified 2026-08-14:
  `https://flatcitybuf.open3d.city/data/3dbag_subset.city.fcb` answers HTTP 206 with
  `accept-ranges: bytes`. What is missing is **tests and validation**, not capability.
- The local dev build is **broken right now**: `flatcitybuf_DIR` points into a
  garbage-collected session scratchpad (`/tmp/claude-1020/.../vendor/prefix`). Any
  relink fails until the library is rebuilt into a durable location.

So the real work is: (A) move the dependency to the released port through the fork's
registry and fix the local build durably, (B) prove and test HTTP reading, (C) add
selective deserialisation — which requires a structural change, because today
`BindCityJSONReadRaw` materialises **every feature at bind time**, before projection
is known.

## 2. Dependency: fork registry, released port `flatcitybuf@0.8.1`

C++ releases in `cityjson/flatcitybuf` are tagged `cpp-v<version>`; the bare `v0.7.7`
tag is the Rust-crate release cut from the same train. The fork's port
(`HideBa/vcpkg` `ports/flatcitybuf`) is version **0.8.1** → tag `cpp-v0.8.1`
(2026-08-13), which is the newest C++ release and a strict descendant of our pinned
`72e5b68`. The only API delta is additive: `Feature::raw()` is now public — the
supported access to encoded geometry, and exactly the hook §4 needs.

Changes:

- `vcpkg.json`: add a **git registry** entry —
  `{"kind": "git", "repository": "https://github.com/HideBa/vcpkg",
  "baseline": "40c2bbe2b6735f6ac01babfe0b6c13317c5c44e5", "packages": ["flatcitybuf"]}` —
  scoped to the one package, so everything else stays on the builtin baseline. When
  the microsoft/vcpkg PR merges, the entry is deleted and the dependency resolves
  from upstream unchanged.
- Delete `vcpkg_ports/flatcitybuf` and `vcpkg_ports/flatbuffers`, and the
  `flatbuffers` version override. The builtin baseline (`84bab45`) carries flatbuffers
  **25.9.23** — the exact version the pinned overlay exists to force — and the fork's
  port additionally patches the generated headers' exact-version `static_assert` down
  to a major-version check, so the pin is no longer needed even if the baseline moves.
- Keep `FCB_WITH_CURL` **off** (do not request the port's `curl` feature): the
  extension's HTTP stack is DuckDB's FileSystem/httpfs via `DuckDBRangeReader` — one
  HTTP implementation, one credentials/secrets/proxy story (§3).
- Local dev build: a `justfile` recipe (`just vendor-fcb`) that builds flatbuffers
  v25.9.23 + flatcitybuf `cpp-v0.8.1` (from the existing clone at
  `/data2/hideba/flatcitybuf` or a fresh shallow clone) into repo-local, gitignored
  `.vendor/prefix`, with `-DCMAKE_POSITION_INDEPENDENT_CODE=ON` (loadable extension is
  a shared object). Reconfigure the build against it. Never a session scratchpad again.
- Validate the registry path once for real: bootstrap vcpkg and run a manifest-mode
  install so the fork registry + port are proven to resolve and build, not assumed.

This phase is behaviour-preserving: the full SQL suite is the gate.

## 3. HTTP range-request reading: verify and test, don't rebuild

No transport change. Work items:

- An end-to-end manual verification against
  `https://flatcitybuf.open3d.city/data/3dbag_subset.city.fcb` (2.3 GB): a
  `flatcitybuf_metadata()` call (header-only fetch) and a bbox-restricted
  `read_flatcitybuf(min_x/min_y/max_x/max_y)` (R-tree walk + a handful of features).
  These exercise the point of the format — bytes fetched proportional to the query.
- SQL tests gated on `require-env FCB_REMOTE_TEST_URL` (the sqllogictest runner skips
  the file when unset, so CI without network/credentials is unaffected); the variable
  carries the URL so the dataset can be swapped. Documented in the justfile.
- The unfiltered-remote-scan caveat (bind downloads the whole file) is removed by §4's
  deferral, which also makes projection narrow what a remote scan fetches — noted here
  because it is the HTTP story's real payoff.

## 4. Selective deserialisation

FlatCityBuf stores each feature as a FlatBuffer: geometry lives in nested `Geometry`
tables, attributes in a per-object byte blob (`u16` column index + typed value,
documented in `fcb/attribute.hpp`). Nothing forces decoding either; today the reader
always converts the whole feature (`fcb::to_cityjson_feature` → full JSON →
`CityJSONFeature::FromJson`).

### 4.1 FieldMask on `FlatCityBufReader`

```
struct FcbFieldMask {
    bool geometry = true;                                  // decode geometry at all
    std::optional<std::set<std::string>> attributes;       // nullopt = all
};
```

- `need_geometry` is true iff any projected column is geometry-derived:
  `geometry_lod*`, `geometry_vertices_lod*`, `geometry_properties_lod*`,
  `material_lod*`, `texture_lod*`, or `bbox` (bbox is computed from geometry).
- `attributes` = projected non-predefined columns ∪ every field named in a pushed
  `AttrQuery` (the feature-level post-filter must still see its operands).
- Predefined structural columns (`id`, `object_type`, `parents`, `children`, …) are
  always materialised — they are cheap table-field reads.

### 4.2 Two conversion paths

- **Full path** (`geometry == true`): today's `to_cityjson_feature` conversion,
  unchanged. Attributes are all decoded even if masked — the attribute blob is small
  next to geometry, and hand-rolling geometry conversion (semantics, materials,
  textures, templates) would duplicate upstream logic we would then have to keep
  correct forever. Deliberate trade-off.
- **Light path** (`geometry == false`): build `CityJSONFeature` directly from
  `Feature::raw()` — object ids, `fcb::city_object_type_name`, parents/children, and
  attributes via a **filtered blob walk**: step the documented wire format, decode
  only masked-in columns, *skip* the bytes of the rest (fixed widths from
  `ColumnInfo`, length prefixes for string/json/binary). Geometry is never touched.
  **Trap:** `to_cityjson_feature` decodes attributes against each CityObject's *own*
  column schema when the object declares one, falling back to the header's — the
  filtered walk must select the schema the same way, or values decode as garbage.

Invariant either way: one output row per CityObject, and every projected column's
value is byte-identical to what the full path produces.

### 4.3 Defer materialisation from bind to init_global

Projection is only known at `init_global` (`TableFunctionInitInput::column_ids`), so
`read_flatcitybuf` stops materialising at bind:

- **Bind**: open header, `ReadMetadata()`, infer columns (samples `sample_lines`
  features as today), set any bbox filter — but no `ReadAllChunks()`.
- **`FlatCityBufPushdownComplexFilter`**: only translates filters and sets them on the
  reader — its current re-read-and-replace of `bind_data.chunks` is deleted.
- **New `FlatCityBufInitGlobal`**: computes the FieldMask from `column_ids` against
  `bind_data.columns`, runs the one real read with mask + filters, and stores the
  chunks/scan plan in the **global state**, not by mutating const bind data.
  `CityJSONScan` / `CityJSONInitLocal` learn to prefer a global-state override when
  present; the other readers' bind-time path is untouched.
- **Cardinality**: from the header's `features_count` (an estimate is expected);
  **statistics**: must tolerate empty bind-time chunks (return null stats for FCB if
  the current implementation walks them). Every consumer of `bind_data.chunks` /
  `scan_plan` gets audited for the FCB function.

This is also what §3 needs: a remote bind now fetches header + samples, and the scan
fetches only what filters and projection ask for.

### 4.4 Testing

- **C++ assertions** (the `test/cpp` harness from the arrow-native work, extended):
  red/green for the FieldMask paths — light-path features carry no geometry, filtered
  walk decodes exactly the masked-in attributes, per-object schema honoured,
  skip-bytes arithmetic exercised for every column type incl. string/json/binary.
- **SQL regression**: full existing FCB suite stays green; new tests assert projected
  queries (`SELECT id …`, `SELECT id, one_attr …`, `COUNT(*)`) return results
  identical to the wide scan, on both plain and attr-indexed files.
- **Benchmark evidence** (reported, not asserted): time attribute-only vs. full-row
  scans on a large local `.fcb`, before/after.

## 5. Out of scope

- Enabling the port's `curl` feature (DuckDB FS is the transport, permanently).
- Attribute schema inference from the FCB header instead of feature sampling —
  worthwhile follow-up, but it can shift inferred SQL types and deserves its own pass.
- Streaming (chunk-at-a-time) FCB scans; deferral keeps the one-shot materialise-then-
  scan shape, just later and narrower.

## 6. Execution model & review

Commander/planner/reviewer: Fable (this session). Executors: Opus subagents, one task
at a time, strict red/green/refactor. Commits land on `develop` per completed unit;
push at milestones. Fable reviews every diff; a `codex exec review -m gpt-5.6-sol
--base <pre-work ref>` pass runs before the final push, per repo practice.
