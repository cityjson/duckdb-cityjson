# FlatCityBuf Registry Dependency + Selective Deserialisation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Depend on the released flatcitybuf C++ library (`0.8.1`, tag `cpp-v0.8.1`) through the `HideBa/vcpkg` fork registry, prove HTTP range-request reading with tests, and add selective deserialisation so `read_flatcitybuf` skips geometry decoding and unneeded attribute decoding when the query's projection doesn't ask for them.

**Architecture:** Three phases. (1) Dependency: fix the broken local build with a durable vendored prefix and move the vcpkg manifest from repo-local overlay ports to a git registry entry scoped to the `flatcitybuf` package. (2) HTTP: characterisation tests gated on an env var, against the hosted 3DBAG file. (3) Selective deserialisation: a `FcbFieldMask` on `FlatCityBufReader`, a geometry-free "light path" conversion built on `fcb::Feature::raw()` with a filtered attribute-blob walk, and deferral of feature materialisation from bind to `init_global` (where DuckDB provides `column_ids`), storing chunks in the global state.

**Tech Stack:** C++17 DuckDB extension; flatcitybuf `cpp-v0.8.1` (flatbuffers 25.9.23, nlohmann-json); vcpkg manifest + git registry; DuckDB sqllogictest (`test/sql/*.test`) + the `test/cpp` g++ harness.

**Spec:** `docs/superpowers/specs/2026-08-14-fcb-registry-and-selective-deserialisation-design.md`

## Global Constraints

- The library clone is at `/data2/hideba/flatcitybuf`; the release tag is `cpp-v0.8.1` (NOT `v0.7.7` — that is the Rust-crate tag).
- Fork registry: `https://github.com/HideBa/vcpkg`, baseline commit `40c2bbe2b6735f6ac01babfe0b6c13317c5c44e5`, package `flatcitybuf` only.
- `FCB_WITH_CURL` stays OFF everywhere; the HTTP transport is `DuckDBRangeReader` (DuckDB FileSystem + httpfs). Do not request the port's `curl` feature.
- All flatcitybuf static libs must build with `-DCMAKE_POSITION_INDEPENDENT_CODE=ON` (they link into a loadable shared object).
- Every code change ends with the full suite green: `cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb && make test`. Commit per completed task on `develop`.
- ODR traps from CLAUDE.md apply (write `LogicalType(LogicalTypeId::X)` in `emplace_back`, scalar `FileOpenFlags::FILE_FLAGS_READ`, non-templated `Catalog::GetEntry`).
- Selective paths must preserve the invariant: one output row per CityObject, and every projected column's value identical to the full-decode path's.

---

### Task 1: Durable vendored flatcitybuf prefix at `cpp-v0.8.1`; unbreak the local build

The current `build/release` was configured with `flatcitybuf_DIR` pointing into a deleted session scratchpad, so any relink fails. Replace it with a repo-local, gitignored `.vendor/prefix` and a justfile recipe, at the new release tag.

**Files:**
- Modify: `justfile` (add `vendor-fcb` recipe)
- Modify: `.gitignore` (add `.vendor/`)
- No test file: the gate is the full SQL suite

**Interfaces:**
- Produces: `.vendor/prefix/lib/cmake/flatcitybuf` + `.vendor/prefix/lib/cmake/flatbuffers`, consumed by CMake configure; `FCB_PREFIX=.vendor/prefix` consumed by `test/cpp/run_encoder_tests.sh` and Task 4's harness.

- [ ] **Step 1: Confirm the breakage (red)**

Run: `cmake --build build/release --target cityjson_loadable_extension 2>&1 | tail -5`
Expected: failure referencing the missing `/tmp/claude-1020/.../vendor/prefix` (or a stale-cache error). If it unexpectedly succeeds, `grep flatcitybuf_DIR build/release/CMakeCache.txt` to confirm the stale path, and continue anyway — the prefix directory no longer exists.

- [ ] **Step 2: Add the `vendor-fcb` recipe to `justfile`**

Append (match the justfile's existing recipe style; check `head -20 justfile` for variable conventions first):

```just
# Build flatbuffers + flatcitybuf (tag cpp-v0.8.1) into .vendor/prefix.
# The loadable extension is a shared object, so both static libs need PIC.
vendor-fcb:
    #!/usr/bin/env bash
    set -euo pipefail
    PREFIX="$(pwd)/.vendor/prefix"
    SRC="$(pwd)/.vendor/src"
    mkdir -p "$SRC"
    if [ ! -d "$SRC/flatbuffers" ]; then
        git clone --depth 1 --branch v25.9.23 https://github.com/google/flatbuffers "$SRC/flatbuffers"
    fi
    cmake -S "$SRC/flatbuffers" -B "$SRC/flatbuffers/build" -DCMAKE_BUILD_TYPE=Release \
        -DFLATBUFFERS_BUILD_TESTS=OFF -DFLATBUFFERS_BUILD_FLATC=OFF -DFLATBUFFERS_BUILD_FLATHASH=OFF \
        -DFLATBUFFERS_BUILD_FLATLIB=ON -DFLATBUFFERS_BUILD_SHAREDLIB=OFF \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_INSTALL_PREFIX="$PREFIX"
    cmake --build "$SRC/flatbuffers/build" -j && cmake --install "$SRC/flatbuffers/build"
    if [ ! -d "$SRC/flatcitybuf" ]; then
        git clone --depth 1 --branch cpp-v0.8.1 https://github.com/cityjson/flatcitybuf "$SRC/flatcitybuf"
    fi
    cmake -S "$SRC/flatcitybuf/src/cpp" -B "$SRC/flatcitybuf/build" -DCMAKE_BUILD_TYPE=Release \
        -DFCB_WITH_JSON=ON -DFCB_WITH_CURL=OFF -DFCB_BUILD_TESTS=OFF -DFCB_BUILD_EXAMPLES=OFF \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_PREFIX_PATH="$PREFIX" -DCMAKE_INSTALL_PREFIX="$PREFIX"
    cmake --build "$SRC/flatcitybuf/build" -j && cmake --install "$SRC/flatcitybuf/build"
```

(Adjust flags only if the tag's `src/cpp/CMakeLists.txt` renamed an option — check with `git -C /data2/hideba/flatcitybuf show cpp-v0.8.1:src/cpp/CMakeLists.txt | grep -n "option("`.)

- [ ] **Step 3: Add `.vendor/` to `.gitignore`, run `just vendor-fcb`**

Expected: both installs succeed; `.vendor/prefix/lib/cmake/flatcitybuf/flatcitybuf-config.cmake` exists.

- [ ] **Step 4: Reconfigure and rebuild against the new prefix**

The existing cache pins the dead path, so re-point it:

```bash
cmake -B build/release -Dflatcitybuf_DIR="$(pwd)/.vendor/prefix/lib/cmake/flatcitybuf" \
      -Dflatbuffers_DIR="$(pwd)/.vendor/prefix/lib/cmake/flatbuffers" build/release
cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb
```

If reconfiguring the existing cache misbehaves, a clean `GEN=ninja make release` with `CMAKE_PREFIX_PATH="$(pwd)/.vendor/prefix"` exported is the fallback (slower but deterministic).

- [ ] **Step 5: Full suite green + smoke the new API surface**

Run: `make test` — expected: all pass (the pinned→0.8.1 delta is additive; if `to_cityjson_feature` output changed for appearance palettes — commit `73ac0e1` "emit the header appearance palette" is in the delta — investigate any diff and adjust expectations ONLY where upstream fixed a bug, noting it in the commit message).
Also run: `FCB_PREFIX="$(pwd)/.vendor/prefix" test/cpp/run_encoder_tests.sh` — expected: pass.

- [ ] **Step 6: Commit**

```bash
git add justfile .gitignore
git commit -m "build(fcb): durable .vendor prefix recipe, upgrade to flatcitybuf cpp-v0.8.1"
```

---

### Task 2: vcpkg manifest — fork registry entry, drop the overlay ports

**Files:**
- Modify: `vcpkg.json`
- Delete: `vcpkg_ports/flatcitybuf/`, `vcpkg_ports/flatbuffers/`
- Modify: `CMakeLists.txt:111-112` (comment says "vendored via vcpkg overlay port — see vcpkg_ports/flatcitybuf/" — update to name the registry)

**Interfaces:**
- Produces: CI resolves `flatcitybuf` 0.8.1 from `HideBa/vcpkg`; local dev keeps using Task 1's `.vendor/prefix`.

- [ ] **Step 1: Rewrite `vcpkg.json`**

```json
{
        "builtin-baseline": "84bab45d415d22042bd0b9081aea57f362da3f35",
        "dependencies": [
                "openssl",
                "nlohmann-json",
                "flatbuffers",
                "flatcitybuf"
        ],
        "vcpkg-configuration": {
                "registries": [
                        {
                                "kind": "git",
                                "repository": "https://github.com/HideBa/vcpkg",
                                "baseline": "40c2bbe2b6735f6ac01babfe0b6c13317c5c44e5",
                                "packages": ["flatcitybuf"]
                        }
                ],
                "overlay-ports": [
                        "./extension-ci-tools/vcpkg_ports"
                ],
                "overlay-triplets": [
                        "./extension-ci-tools/toolchains"
                ]
        }
}
```

Notes: the `flatbuffers` version override and the `./vcpkg_ports` overlay entry are gone. The builtin baseline already carries flatbuffers 25.9.23, and the fork's port patches the generated headers' exact-version `static_assert` to a major-only check, so no pin is needed. Keep the `flatbuffers` top-level dependency (the extension's own CMake does `find_package` on it transitively via the prefix).

- [ ] **Step 2: Delete the overlay ports**

```bash
git rm -r vcpkg_ports
```

- [ ] **Step 3: Validate the registry resolves and builds (red→green in one step; this IS the test)**

```bash
cd /tmp/claude-1020/-data2-hideba-cityparquet-paper-duckdb-cityjson/*/scratchpad 2>/dev/null || cd "$SCRATCHPAD"
git clone --depth 1 https://github.com/microsoft/vcpkg vcpkg-tool && ./vcpkg-tool/bootstrap-vcpkg.sh -disableMetrics
cd /data2/hideba/cityparquet-paper/duckdb-cityjson
"$SCRATCHPAD/vcpkg-tool/vcpkg" install --triplet x64-linux
```

Expected: `flatcitybuf:x64-linux@0.8.1` (from the git registry) and `flatbuffers:x64-linux@25.9.23` (builtin) install successfully. If the registry lookup fails on the `versions/f-/flatcitybuf.json` git-tree hash, report the exact error — that is a defect in the fork to surface, not to patch around locally.

- [ ] **Step 4: Update the CMakeLists comment, rebuild, suite green**

`CMakeLists.txt` line ~111: replace "vendored via vcpkg overlay port — see vcpkg_ports/flatcitybuf/" with "resolved from the HideBa/vcpkg git registry (flatcitybuf@0.8.1, tag cpp-v0.8.1) in CI; local dev uses `just vendor-fcb`'s .vendor/prefix". Then `cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb && make test`.

- [ ] **Step 5: Commit**

```bash
git add vcpkg.json CMakeLists.txt
git commit -m "build(fcb): resolve flatcitybuf@0.8.1 from the HideBa/vcpkg registry, drop overlay ports"
```

---

### Task 3: HTTP range-request characterisation tests

**Files:**
- Create: `test/sql/cityjson_fcb_remote.test`
- Modify: `justfile` (a `test-fcb-remote` convenience recipe)

**Interfaces:**
- Consumes: existing `DuckDBRangeReader` transport (no code change expected).
- Produces: env-gated proof that remote reads work; a named recipe the user can run.

- [ ] **Step 1: Write the test**

```
# name: test/sql/cityjson_fcb_remote.test
# description: HTTP range-request reads via DuckDBRangeReader + httpfs. Network test:
# runs only when FCB_REMOTE_TEST_URL is set, e.g.
#   FCB_REMOTE_TEST_URL=https://flatcitybuf.open3d.city/data/3dbag_subset.city.fcb
# group: [sql]

require cityjson

require httpfs

require-env FCB_REMOTE_TEST_URL

# Header-only fetch: metadata comes from the first few KB of the file.
query I
SELECT count(*) FROM flatcitybuf_metadata('${FCB_REMOTE_TEST_URL}');
----
1

# R-tree-guided fetch: a tiny bbox reads only matching feature ranges. The exact
# row count is data-dependent; assert the query executes and returns a count.
statement ok
SELECT count(*) FROM read_flatcitybuf('${FCB_REMOTE_TEST_URL}',
    min_x := 84000, min_y := 444000, max_x := 84500, max_y := 444500);
```

Check first (`grep -rn 'require-env' duckdb/test` and the sqllogictest docs in `duckdb/test/README.md`) whether `${VAR}` interpolation is supported in this DuckDB version's runner; if not, use the `require-env FCB_REMOTE_TEST_URL` + literal-URL form and gate with a second env var, adjusting the header comment. The 3DBAG subset file is RD New (EPSG:7415) — coordinates above are in its extent; verify with `flatcitybuf_metadata` output and adjust the bbox so the `statement ok` exercises a non-empty-but-small read.

- [ ] **Step 2: Run it gated-off (must skip cleanly)**

Run: `make test` (without the env var)
Expected: suite passes; the new file reports as skipped, not failed.

- [ ] **Step 3: Run it for real**

Run: `FCB_REMOTE_TEST_URL=https://flatcitybuf.open3d.city/data/3dbag_subset.city.fcb build/release/test/unittest "test/sql/cityjson_fcb_remote.test"` (find the exact unittest invocation `make test` uses in the Makefile first).
Expected: PASS. If it fails, this is the real work: debug `DuckDBRangeReader` (httpfs autoload, range-read path) with systematic-debugging before touching the test.

- [ ] **Step 4: Add the justfile recipe + commit**

```just
# Network-gated FlatCityBuf remote-read tests (HTTP range requests).
test-fcb-remote url="https://flatcitybuf.open3d.city/data/3dbag_subset.city.fcb":
    FCB_REMOTE_TEST_URL={{url}} build/release/test/unittest "test/sql/cityjson_fcb_remote.test"
```

```bash
git add test/sql/cityjson_fcb_remote.test justfile
git commit -m "test(fcb): env-gated HTTP range-request reads against hosted 3DBAG"
```

---

### Task 4: `FcbFieldMask` + geometry-free light path + filtered attribute walk (C++ TDD)

**Files:**
- Create: `src/include/cityjson/fcb_selective_convert.hpp`
- Create: `src/cityjson/fcb_selective_convert.cpp`
- Create: `test/cpp/test_fcb_selective.cpp`, `test/cpp/run_fcb_selective_tests.sh` (copy the compile-link pattern of `run_encoder_tests.sh`, adding `fcb_selective_convert.cpp` and `-I$FCB_PREFIX/include`)
- Modify: `src/include/cityjson/flatcitybuf_reader.hpp`, `src/cityjson/flatcitybuf_reader.cpp` (add `SetFieldMask`, route `ParseFeatures`)
- Modify: `CMakeLists.txt` (add `fcb_selective_convert.cpp` to the FCB source-list block at line ~62)

**Interfaces:**
- Consumes: `fcb::Feature::raw()` → `const ::CityFeature*` (include `<fcb/generated/feature_generated.h>`); `fcb::HeaderView::info().columns` (`std::vector<fcb::ColumnInfo>`); `fcb::city_object_type_name(uint8_t)`; the attribute wire format documented in `fcb/attribute.hpp` (records of `u16` LE column index + value; fixed-width types packed LE; String/DateTime/Json `u32` LE length + UTF-8).
- Produces (exact, used by Task 5):

```cpp
// fcb_selective_convert.hpp
struct FcbFieldMask {
	bool geometry = true;
	std::optional<std::set<std::string>> attributes; // nullopt = all
};

// Light path: mask.geometry must be false. One CityObject per raw object; fills
// id, type (via city_object_type_name / extension_type), attributes (filtered),
// parents, children, geographical_extent if the raw table carries it. Geometry
// vector stays empty.
CityJSONFeature ConvertFeatureLight(const fcb::Feature &feature, const fcb::HeaderView &header,
                                    const FcbFieldMask &mask);

// Filtered blob walk: decodes only columns named in `wanted` (nullopt = all),
// SKIPS the bytes of the rest without materialising strings/json. Throws
// CityJSONError on truncated records / unknown column index or type, matching
// fcb::decode_attributes's own error posture.
nlohmann::json DecodeAttributesFiltered(const uint8_t *data, size_t size,
                                        const std::vector<fcb::ColumnInfo> &schema,
                                        const std::optional<std::set<std::string>> &wanted);
```

and on the reader: `void FlatCityBufReader::SetFieldMask(FcbFieldMask mask);` — `ParseFeatures` uses `ConvertFeatureLight` when `!mask.geometry`, the existing `to_cityjson_feature` path otherwise (mask.attributes is IGNORED on the full path, by spec §4.2).

**Correctness traps to encode in tests, from the spec:**
1. Per-object schema: `to_cityjson_feature` decodes attributes against each CityObject's OWN `columns` table when the object declares one, header's otherwise. `ConvertFeatureLight` must do the same (the raw `::CityObject` table has a `columns` field — check `feature_generated.h`).
2. JSON semantics must match `fcb::attributes_to_json` exactly for decoded columns (numbers stay numbers, Json columns parse to JSON, DateTime stays string) — the SQL-visible values must be byte-identical to the full path's.
3. Skip arithmetic per `ColumnType`: fixed widths for Bool/Byte/UByte/Short/UShort/Int/UInt/Long/ULong/Float/Double; length-prefixed for String/DateTime/Json/Binary. An unknown `ColumnType` aborts the walk with an error (width unknown ⇒ rest of blob unparseable) — same posture as upstream.

- [ ] **Step 1: Write failing tests**

`test/cpp/test_fcb_selective.cpp`, using the checked-in fixture `test/data/fcb_bbox_attr.fcb` (features f1/f2/f3 with `height` numeric and `category` string attributes — read `test/data/fcb_bbox_attr.city.jsonl` for ground truth) via `fcb::FcbReader::open_file`. Follow `test_arrow_native_encoder.cpp`'s assertion style (plain `assert`/manual checks, no framework). Tests:

```cpp
// T1: light path yields no geometry, same ids/types as full path
// T2: light path with attributes={"height"} decodes height, omits category
// T3: light path with attributes=nullopt decodes all attrs, values == full path's
//     (compare against fcb::to_cityjson_feature output field-by-field)
// T4: attributes={} (empty set) decodes nothing, object still has id/type
// T5: DecodeAttributesFiltered on a hand-built blob covering every fixed-width
//     type + String + Json + Binary, wanted = subset -> only subset present
// T6: truncated blob throws CityJSONError
// T7 (per-object schema): if fcb_bbox_attr.fcb has no per-object columns, build
//     one in-test via the extension writer or fcb writer API; if that is
//     disproportionate, write the .fcb for this case with
//     build/release/duckdb COPY TO in the runner script before the binary runs
```

- [ ] **Step 2: Run to verify failure**

Run: `FCB_PREFIX="$(pwd)/.vendor/prefix" test/cpp/run_fcb_selective_tests.sh`
Expected: COMPILE FAILED (functions don't exist yet).

- [ ] **Step 3: Implement `fcb_selective_convert.cpp`**

Blob walk skeleton (verify field names against the actual generated header before trusting this):

```cpp
nlohmann::json DecodeAttributesFiltered(const uint8_t *data, size_t size,
                                        const std::vector<fcb::ColumnInfo> &schema,
                                        const std::optional<std::set<std::string>> &wanted) {
	nlohmann::json out = nlohmann::json::object();
	size_t pos = 0;
	auto need = [&](size_t n) {
		if (pos + n > size) throw CityJSONError("truncated attribute record");
	};
	while (pos < size) {
		need(2);
		uint16_t col_idx = static_cast<uint16_t>(data[pos]) | (static_cast<uint16_t>(data[pos + 1]) << 8);
		pos += 2;
		const fcb::ColumnInfo *col = nullptr;
		for (const auto &c : schema) if (c.index == col_idx) { col = &c; break; }
		if (!col) throw CityJSONError("attribute column index not in schema");
		bool want = !wanted.has_value() || wanted->count(col->name) > 0;
		// ... switch on static_cast<::ColumnType>(col->type): read LE fixed-width
		// or u32-length-prefixed payload; when !want, advance pos without
		// constructing values; when want, emit with the same JSON typing as
		// fcb::attributes_to_json (verify against src/cpp/src/attribute.cpp).
	}
	return out;
}
```

`ConvertFeatureLight` builds a `CityJSONFeature` directly (see `cityjson_types.hpp` for the struct): iterate `feature.raw()->objects()`, per object fill `id`, `type` (via `fcb::city_object_type_name`, honouring `extension_type` for extension objects — mirror upstream's `to_city_object` in `src/cpp/src/cityjson.cpp`, minus geometry), `attributes` via `DecodeAttributesFiltered` with the per-object-else-header schema, `children`/`parents`. Leave geometry empty. Reuse `CityJSONFeature`'s own construction conventions from `CityJSONFeature::FromJson` (e.g. how missing attributes are represented) so downstream scan code sees no difference.

- [ ] **Step 4: Iterate to green**

Run: `FCB_PREFIX="$(pwd)/.vendor/prefix" test/cpp/run_fcb_selective_tests.sh` until PASS.

- [ ] **Step 5: Wire `SetFieldMask` into the reader**

In `flatcitybuf_reader.hpp/.cpp`: member `FcbFieldMask field_mask_;` (default = full), setter, and in `ParseFeatures`:

```cpp
if (!field_mask_.geometry) {
	FcbFieldMask effective = field_mask_;
	if (attr_query_.has_value() && effective.attributes.has_value()) {
		for (const auto &cond : attr_query_.value()) effective.attributes->insert(cond.field);
	}
	feature = ConvertFeatureLight(it.current(), fcb_reader.header(), effective);
} else {
	/* existing to_cityjson_feature -> FromJson path, unchanged */
}
```

(The attr-query union lives HERE, not in Task 5's mask computation, so the post-filter can never be starved by a caller's mask.)

- [ ] **Step 6: Rebuild extension + full suite green (mask defaults to full ⇒ no behaviour change)**

Run: `cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb && make test`

- [ ] **Step 7: Commit**

```bash
git add src/include/cityjson/fcb_selective_convert.hpp src/cityjson/fcb_selective_convert.cpp \
        src/cityjson/flatcitybuf_reader.cpp src/include/cityjson/flatcitybuf_reader.hpp \
        test/cpp/test_fcb_selective.cpp test/cpp/run_fcb_selective_tests.sh CMakeLists.txt
git commit -m "feat(fcb): FcbFieldMask light path — skip geometry and unneeded attribute decoding"
```

---

### Task 5: Defer materialisation to init_global; compute the mask from the projection

**Files:**
- Modify: `src/cityjson/flatcitybuf_table_function.cpp` (bind stops materialising; new `FlatCityBufInitGlobal`; pushdown stops re-reading; FCB-specific cardinality)
- Modify: `src/include/cityjson/flatcitybuf_table_function.hpp` (declare `FlatCityBufInitGlobal`)
- Modify: `src/cityjson/bind_function.cpp` + `src/include/cityjson/table_function.hpp` (`BindCityJSONReadRaw` gains `bool materialise = true`; when false, skip `ReadAllChunks`/`BuildScanPlan` — schema inference still runs)
- Modify: `src/cityjson/scan_function.cpp:225-230` (`MaterializedScan` prefers global-state chunks when flagged)
- Modify: `src/include/cityjson/table_function.hpp` (`CityJSONGlobalState` gains `bool use_global_chunks = false;` — the existing `chunks`/`scan_plan` members are currently vestigial; verify with `grep -n "global_state.chunks" src/cityjson/*.cpp` and reuse them)
- Test: `test/sql/cityjson_fcb_projection.test`

**Interfaces:**
- Consumes: Task 4's `FcbFieldMask` + `FlatCityBufReader::SetFieldMask`; `TableFunctionInitInput::column_ids` (contains `COLUMN_IDENTIFIER_ROW_ID` entries — skip those); `bind_data.columns[idx].name`.
- Produces: `read_flatcitybuf` materialises once, in `FlatCityBufInitGlobal`, masked by projection.

**Geometry-derived column predicate** (the mask rule; `bbox` is computed from geometry):

```cpp
static bool IsGeometryDerivedColumn(const std::string &name) {
	return name == "bbox" || name.rfind("geometry_lod", 0) == 0 ||
	       name.rfind("geometry_vertices_lod", 0) == 0 ||
	       name.rfind("geometry_properties_lod", 0) == 0 ||
	       name.rfind("material_lod", 0) == 0 || name.rfind("texture_lod", 0) == 0;
}
```

Cross-check the actual grammar against `GetDefinedColumns()` (`column_types.cpp`) and `CityObjectUtils::InferGeometryColumns` before trusting the prefixes — the `lod=` mode keeps suffixed names (`geometry_lodX_Y`), so prefix matching covers it. Every column that is neither geometry-derived nor predefined is an attribute; define the second predicate as a static helper next to `IsGeometryDerivedColumn`:

```cpp
// Structural columns the light path always fills regardless of mask — the
// authoritative list is GetDefinedColumns() in column_types.cpp; build the set
// from that function at first use rather than hardcoding names here.
static bool IsPredefinedNonAttributeColumn(const std::string &name) {
	static const std::set<std::string> defined = [] {
		std::set<std::string> s;
		for (const auto &col : GetDefinedColumns()) s.insert(col.name);
		return s;
	}();
	return defined.count(name) > 0;
}
```

- [ ] **Step 1: Write failing SQL tests**

`test/sql/cityjson_fcb_projection.test` — these are green-on-correctness tests that will FAIL during the refactor if any consumer of bind-time chunks is missed, and they pin the invariant:

```
# name: test/sql/cityjson_fcb_projection.test
# description: projection-masked scans return values identical to wide scans
# group: [sql]

require cityjson

statement ok
COPY (SELECT * FROM read_cityjsonseq('test/data/fcb_bbox_attr.city.jsonl'))
TO '__TEST_DIR__/proj.fcb' (FORMAT flatcitybuf, attr_index 'height,category', branching_factor 4);

# attribute-only projection (light path)
query II
SELECT feature_id, height FROM read_flatcitybuf('__TEST_DIR__/proj.fcb') ORDER BY feature_id;
----
<copy expected values from test/data/fcb_bbox_attr.city.jsonl ground truth>

# COUNT(*) — empty projection, light path, still one row per CityObject
query I
SELECT count(*) FROM read_flatcitybuf('__TEST_DIR__/proj.fcb');
----
<ground-truth object count>

# geometry projected -> full path; bbox forces geometry too
query I
SELECT count(*) FROM read_flatcitybuf('__TEST_DIR__/proj.fcb') WHERE bbox IS NOT NULL;
----
<ground-truth>

# indexed attr filter + narrow projection: pushdown AND light path together
query I
SELECT feature_id FROM read_flatcitybuf('__TEST_DIR__/proj.fcb') WHERE height > 15 ORDER BY feature_id;
----
f2
f3

# bbox param + attr filter (the post-filter case) with attribute-only projection
query I
SELECT feature_id FROM read_flatcitybuf('__TEST_DIR__/proj.fcb',
    min_x := <fixture-min-x>, min_y := <fixture-min-y>, max_x := <fixture-max-x>, max_y := <fixture-max-y>)
WHERE category = 'A' ORDER BY feature_id;
----
<ground-truth>
```

Fill every `<...>` with real values computed from the fixture BEFORE writing the implementation (run the queries against the current build to capture ground truth — the current full-decode implementation is the oracle). These pass against the current build; they are the regression harness for the refactor.

- [ ] **Step 2: Run new tests against current build — must PASS (oracle capture)**

Run: `build/release/test/unittest "test/sql/cityjson_fcb_projection.test"`

- [ ] **Step 3: Implement the deferral**

In order:
1. `BindCityJSONReadRaw(..., bool materialise = true)`: wrap the existing `if (!streaming) { chunks = ReadAllChunks(); scan_plan = ... }` block in `if (!streaming && materialise)`. All existing callers unchanged (default true).
2. `FlatCityBufBind`: pass `materialise = false`. Keep `reader->SetBBoxFilter` where it is (before schema inference — the sample should respect the bbox exactly as today).
3. `FlatCityBufPushdownComplexFilter`: delete the tail (`SetAttrQueryFilter` stays; remove the `ReadAllChunks`/`BuildScanPlan` re-read and its try/catch).
4. New `FlatCityBufInitGlobal(ClientContext&, TableFunctionInitInput &input)`:

```cpp
unique_ptr<GlobalTableFunctionState> FlatCityBufInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto result_holder = CityJSONInitGlobal(context, input); // keeps has_filters etc.
	auto &state = result_holder->Cast<CityJSONGlobalState>();
	auto &bind_data = input.bind_data->Cast<FlatCityBufBindData>();

	FcbFieldMask mask;
	mask.geometry = false;
	mask.attributes = std::set<std::string>{};
	for (auto col_id : input.column_ids) {
		if (IsRowIdColumnId(col_id)) continue; // COLUMN_IDENTIFIER_ROW_ID — check duckdb's helper/constant
		const auto &name = bind_data.columns[col_id].name;
		if (IsGeometryDerivedColumn(name)) { mask.geometry = true; continue; }
		if (!IsPredefinedNonAttributeColumn(name)) mask.attributes->insert(name);
	}
	if (mask.geometry) mask.attributes.reset(); // full path ignores it anyway (spec §4.2)

	bind_data.reader->SetFieldMask(mask);
	try {
		state.chunks = bind_data.reader->ReadAllChunks();
	} catch (const CityJSONError &e) {
		throw InvalidInputException("Failed to read FlatCityBuf: %s", e.what());
	}
	state.scan_plan = state.chunks.BuildScanPlan();
	state.use_global_chunks = true;
	return result_holder;
}
```

Mind the bind-data constness: `input.bind_data` may be `optional_ptr<const FunctionData>` in this DuckDB version. `SetFieldMask` mutates only the reader (a `shared_ptr` — same object the pushdown callback already mutates post-bind); if the cast needs `const_cast`, take the same route `FlatCityBufPushdownComplexFilter` legitimises and comment why it is safe (single init_global per query, before any scan thread starts).
5. `MaterializedScan`: `const auto &active_chunks = global_state.use_global_chunks ? global_state.chunks : bind_data.chunks;` (same for the plan). Check every OTHER use of `bind_data.chunks`/`scan_plan` inside scan_function.cpp's filter path (lines ~301-336 use `active_chunks` already or `bind_data` directly — route them all through the `active_*` locals).
6. Registration: `func.init_global = FlatCityBufInitGlobal;` for `read_flatcitybuf` only; add an FCB-specific cardinality:

```cpp
static unique_ptr<NodeStatistics> FlatCityBufCardinality(ClientContext &, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<FlatCityBufBindData>();
	auto stats = make_uniq<NodeStatistics>();
	stats->has_estimated_cardinality = true;
	// features_count counts features; rows are CityObjects — an estimate is expected.
	stats->estimated_cardinality = bind_data.reader->Header().info().features_count;
	return stats;
}
```

(`CityJSONProgress` is not registered for read_flatcitybuf — verify with grep, and if it ever is, it must learn the global-state override too.)
7. Audit the remaining consumers: `grep -n "bind_data.chunks\|bind_data->chunks\|\.scan_plan" src/cityjson/*.cpp` — every hit reached from `read_flatcitybuf` must either be dead for FCB (bind-time chunks now empty — e.g. `CityJSONCardinality`, replaced in step 6) or routed through the override. List the audit result in the commit message.

- [ ] **Step 4: Rebuild + all FCB tests + full suite**

Run: `cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb && make test`
Expected: everything green — especially `cityjson_fcb_projection.test` (the oracle), `cityjson_fcb_attr_query.test` (pushdown now reads in init_global), `cityjson_fcb_bbox.test`, `cityjson_e2e_fcb.test` (insert_flatcitybuf goes through `InspectCityJSONSource`/`read_flatcitybuf` SQL — full projection ⇒ full path).

- [ ] **Step 5: Prove the skip actually happens**

Not SQL-observable, so assert at the C++ layer: add to `test_fcb_selective.cpp` a test that `FlatCityBufReader` (constructed directly, as the harness links the reader now — add `flatcitybuf_reader.cpp` + `duckdb_fs_range_reader.cpp` to the runner's source list, or simpler: test via `ParseFeatures` observable) — with `SetFieldMask({.geometry=false, .attributes=std::set<std::string>{}})`, every returned feature has empty `geometry` and empty `attributes` on all objects. If linking the reader into the harness drags in too much of DuckDB, assert instead on `ConvertFeatureLight` directly (already covered by Task 4 T1/T4) and note that the reader-level routing is covered by the SQL suite's correctness oracle. Run the harness green.

- [ ] **Step 6: Benchmark evidence (report, don't assert)**

Generate a large local `.fcb` (e.g. `COPY (FROM read_cityjsonseq(<largest .city.jsonl available, or a 3DBAG tile downloaded once>)) TO bench.fcb`), then time in `build/release/duckdb`:

```sql
.timer on
SELECT count(*) FROM read_flatcitybuf('bench.fcb');                     -- light path
SELECT max(height) FROM read_flatcitybuf('bench.fcb');                  -- light path, one attr
SELECT count(*) FROM read_flatcitybuf('bench.fcb') WHERE bbox IS NOT NULL; -- full path
```

Record before/after numbers (before = git stash or the Task 4 commit) in the final summary.

- [ ] **Step 7: Commit**

```bash
git add src/cityjson/flatcitybuf_table_function.cpp src/include/cityjson/flatcitybuf_table_function.hpp \
        src/cityjson/bind_function.cpp src/include/cityjson/table_function.hpp \
        src/cityjson/scan_function.cpp test/sql/cityjson_fcb_projection.test test/cpp/
git commit -m "feat(fcb): defer materialisation to init_global, mask decode by projection"
```

---

### Task 6: Documentation, cross-review, push

**Files:**
- Modify: `CLAUDE.md` + `AGENTS.md` (keep in sync): update the FCB section — registry dependency + `just vendor-fcb`, the FieldMask/light-path design and its two traps (per-object schema; full path ignores `attributes`), deferred materialisation (init_global owns chunks for FCB), `FCB_REMOTE_TEST_URL` gated tests.
- Modify: `README.md` if it mentions the FCB dependency mechanism.

- [ ] **Step 1: Write the doc updates** (both files, same content shape as the existing arrow-native section: what changed, the traps, how to run the harnesses)

- [ ] **Step 2: Commit docs**

```bash
git add CLAUDE.md AGENTS.md README.md
git commit -m "docs(fcb): registry dependency, selective deserialisation, remote test gate"
```

- [ ] **Step 3: Codex review over the whole branch**

Run: `codex exec review -m gpt-5.6-sol --base 345a68b` (the design-doc commit). Triage findings with superpowers:receiving-code-review — verify each against the code, fix real defects with the TDD loop, reject bad suggestions with reasons in the summary.

- [ ] **Step 4: Full suite + harnesses one last time, then push**

```bash
make test && FCB_PREFIX="$(pwd)/.vendor/prefix" test/cpp/run_encoder_tests.sh && \
  FCB_PREFIX="$(pwd)/.vendor/prefix" test/cpp/run_fcb_selective_tests.sh
git push origin develop
```
