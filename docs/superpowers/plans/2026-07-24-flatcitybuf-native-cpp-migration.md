# FlatCityBuf Native C++ Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `duckdb-cityjson`'s Rust/cxx-bridge FlatCityBuf integration with the new
native C++17 `flatcitybuf` library, add real bbox and attribute-query pushdown to
`read_flatcitybuf`, and add a native `COPY TO ... (FORMAT flatcitybuf)` write path with
`attr_index`/`branching_factor`/`index_node_size` options.

**Architecture:** Vendor `flatcitybuf`'s `src/cpp` (pinned to commit
`72e5b68d469aa00a75ccba23780e2063751e3cff`) through a new, repo-owned vcpkg overlay port.
Rewrite `FlatCityBufReader`/`WriteFlatCityBuf` against the native `fcb::` API, adding a
`DuckDBRangeReader : fcb::RangeReader` transport backed by DuckDB's own `FileSystem` (no
libcurl). Bbox is a bind-time argument dispatched to `fcb::FcbReader::select_bbox`.
Attribute-`WHERE` pushdown is a new `FlatCityBufPushdownComplexFilter` that re-runs the
read against `fcb::FcbReader::select_attr` when it can translate the predicate.

**Tech Stack:** C++17, DuckDB extension C API, nlohmann::json, flatbuffers, CMake + vcpkg,
sqllogictest (`test/sql/*.test`).

**Full design reference:** `docs/superpowers/specs/2026-07-24-flatcitybuf-native-cpp-migration-design.md`
— read it before starting; this plan implements it task by task and does not repeat its
rationale.

## Global Constraints

- Pin flatcitybuf to commit `72e5b68d469aa00a75ccba23780e2063751e3cff` (unreleased, no
  `v0.8.0` tag exists yet — see spec §2, §9).
- Do not edit anything under `extension-ci-tools/` — it is a git submodule pinned to
  `duckdb/extension-ci-tools@v1.5.4`. The new overlay port goes in a repo-owned
  `vcpkg_ports/` directory instead (spec §5.1).
- Build flatcitybuf with `-DFCB_WITH_CURL=OFF -DFCB_BUILD_TESTS=OFF -DFCB_BUILD_EXAMPLES=OFF`.
  No libcurl dependency anywhere in this migration (spec §5.2, confirmed with the user).
- `CITYJSON_ENABLE_FCB` stays the build option name; it now defaults **ON** on every
  platform (no more musl/glibc/arch restrictions).
- `CITYJSON_HAS_FCB` stays the compile define guarding all FCB-specific code — every
  existing `#ifdef CITYJSON_HAS_FCB` block keeps compiling, only what's inside changes.
- Every SQL-visible behavior change is proven by a `test/sql/*.test` file — this repo has
  no C++ unit-test harness. TDD happens at that granularity: write the test, run it,
  confirm the expected failure, implement, confirm green.
- Commit after every task (this plan's steps end each task with a commit step).
- Consult Fable (`Agent` tool, `model: "fable"`) for a design/implementation sanity check
  at the checkpoints called out below, and run `codex exec --model gpt-5.6-sol` as an
  independent review pass before the final task — both per explicit user instruction.
- If a step surfaces a decision only the user can make, note it in the task's commit
  message or a `NOTE:` comment and move on to the next task rather than blocking.

---

## File Structure

| File | Change |
|---|---|
| `vcpkg_ports/flatcitybuf/vcpkg.json`, `vcpkg_ports/flatcitybuf/portfile.cmake` | New — repo-owned overlay port |
| `vcpkg.json` (root) | Add `flatbuffers` dependency, add `./vcpkg_ports` to `overlay-ports` |
| `CMakeLists.txt` (root) | Replace prebuilt-binary-download block with `find_package(flatcitybuf CONFIG REQUIRED)` |
| `src/include/cityjson/duckdb_fs_range_reader.hpp`, `src/cityjson/duckdb_fs_range_reader.cpp` | New — `fcb::RangeReader` over DuckDB `FileSystem` |
| `src/include/cityjson/flatcitybuf_reader.hpp`, `src/cityjson/flatcitybuf_reader.cpp` | Rewritten against native `fcb::` API |
| `src/include/cityjson/flatcitybuf_table_function.hpp`, `src/cityjson/flatcitybuf_table_function.cpp` | `FlatCityBufBindData`, bbox params, `FlatCityBufPushdownComplexFilter`, metadata fn update |
| `src/cityjson/reader_factory.cpp` | `FlatCityBufReader` construction now takes `ClientContext&` |
| `src/include/cityjson/cityjson_writer.hpp`, `src/cityjson/cityjson_writer.cpp` | `WriteFlatCityBuf` reimplemented against `fcb::FcbWriter`, new options params |
| `src/include/cityjson/copy_function.hpp`, `src/cityjson/copy_function.cpp` | New `attr_index`/`branching_factor`/`index_node_size` options |
| `test/data/*.fcb` | New — committed binary fixtures (generated once, from the still-working old implementation, before it's removed) |
| `test/data/fcb_bbox_attr.city.jsonl` | New — small hand-built fixture for bbox/attribute tests |
| `test/sql/cityjson_e2e_fcb.test` | `require notmusl` line removed |
| `test/sql/cityjson_fcb_bbox.test`, `test/sql/cityjson_fcb_write_options.test`, `test/sql/cityjson_fcb_attr_query.test`, `test/sql/cityjson_fcb_http.test` | New |
| `README.md` | FCB sections updated (bbox params, new COPY options, build instructions) |

---

### Task 1: Generate and commit baseline `.fcb` fixtures from the current (soon-to-be-replaced) implementation

The current build still has the working Rust/cxx FlatCityBuf implementation. Before
touching any code, generate real `.fcb` files with it and commit them as fixtures — this
lets later tasks test the *new* reader against known-good files without needing the new
writer to exist yet, breaking the reader/writer chicken-and-egg dependency.

**Files:**
- Create: `test/data/fcb_bbox_attr.city.jsonl` (source fixture for later bbox/attribute tests)
- Create (binary, generated not hand-written): `test/data/sample.fcb`, `test/data/fcb_bbox_attr.fcb`

**Interfaces:**
- Produces: two `.fcb` fixture files other tasks read via `read_flatcitybuf('test/data/....fcb')`.

- [ ] **Step 1: Confirm the current build still has FCB enabled**

Run: `grep CITYJSON_ENABLE_FCB build/release/CMakeCache.txt`
Expected: `CITYJSON_ENABLE_FCB:BOOL=ON`. If the build directory doesn't exist or this is
OFF, run `GEN=ninja make` first (uses the current, not-yet-migrated CMakeLists.txt).

- [ ] **Step 2: Write the bbox/attribute-query source fixture**

Create `test/data/fcb_bbox_attr.city.jsonl` with exactly this content (3 features at
well-separated, round-number footprints so later bbox math is easy to reason about by
hand — f1 at x/y 0-10, f2 at 100-110, f3 at 200-210; `height`/`category` attributes for
attribute-query tests):

```
{"type":"CityJSON","version":"2.0","transform":{"scale":[1.0,1.0,1.0],"translate":[0.0,0.0,0.0]}}
{"type":"CityJSONFeature","id":"f1","CityObjects":{"b1":{"type":"Building","attributes":{"height":10.0,"category":"A"},"geometry":[{"type":"Solid","lod":"2.2","boundaries":[[[[0,1,2,3]]]]}]}},"vertices":[[0.0,0.0,0.0],[10.0,0.0,0.0],[10.0,10.0,0.0],[0.0,10.0,0.0]]}
{"type":"CityJSONFeature","id":"f2","CityObjects":{"b2":{"type":"Building","attributes":{"height":20.0,"category":"B"},"geometry":[{"type":"Solid","lod":"2.2","boundaries":[[[[0,1,2,3]]]]}]}},"vertices":[[100.0,100.0,0.0],[110.0,100.0,0.0],[110.0,110.0,0.0],[100.0,110.0,0.0]]}
{"type":"CityJSONFeature","id":"f3","CityObjects":{"b3":{"type":"Building","attributes":{"height":30.0,"category":"A"},"geometry":[{"type":"Solid","lod":"2.2","boundaries":[[[[0,1,2,3]]]]}]}},"vertices":[[200.0,200.0,0.0],[210.0,200.0,0.0],[210.0,210.0,0.0],[200.0,210.0,0.0]]}
```

Note the ground truth this fixture encodes, needed by later tasks:
- bbox `[50,50,150,150]` intersects only f2.
- `WHERE height > 15` matches f2, f3. `WHERE category = 'A'` matches f1, f3.
- bbox `[50,50,150,150]` AND `height > 15` matches only f2 (the deliberate combined case).

- [ ] **Step 3: Generate the two fixtures with the current (old) implementation**

Run:
```sh
./build/release/duckdb -c "COPY (SELECT * FROM read_cityjsonseq('test/data/sample.city.jsonl')) TO 'test/data/sample.fcb' (FORMAT flatcitybuf);"
./build/release/duckdb -c "COPY (SELECT * FROM read_cityjsonseq('test/data/fcb_bbox_attr.city.jsonl')) TO 'test/data/fcb_bbox_attr.fcb' (FORMAT flatcitybuf);"
```
Expected: both commands succeed with no output (COPY statements are silent on success).

- [ ] **Step 4: Verify both fixtures read back correctly**

Run:
```sh
./build/release/duckdb -c "SELECT COUNT(*) FROM read_flatcitybuf('test/data/sample.fcb');"
./build/release/duckdb -c "SELECT id FROM read_flatcitybuf('test/data/fcb_bbox_attr.fcb') ORDER BY id;"
```
Expected: `2` for the first; `f1`, `f2`, `f3` for the second.

- [ ] **Step 5: Commit**

```bash
git add test/data/fcb_bbox_attr.city.jsonl test/data/sample.fcb test/data/fcb_bbox_attr.fcb
git commit -m "test(fcb): commit baseline .fcb fixtures generated by the current implementation

Generated before the Rust/cxx FlatCityBuf backend is replaced, so the new
native reader has known-good files to test against independent of the new
writer landing first."
```

---

### Task 2: Add the flatcitybuf vcpkg overlay port

**Files:**
- Create: `vcpkg_ports/flatcitybuf/vcpkg.json`
- Create: `vcpkg_ports/flatcitybuf/portfile.cmake`
- Modify: `vcpkg.json` (root)

**Interfaces:**
- Produces: `find_package(flatcitybuf CONFIG REQUIRED)` resolvable once this port is
  registered as an overlay (Task 3 wires the CMake side that calls it).

- [ ] **Step 1: Write the port manifest**

Create `vcpkg_ports/flatcitybuf/vcpkg.json`:

```json
{
  "name": "flatcitybuf",
  "version": "0.8.0",
  "port-version": 0,
  "description": "Native C++ reader/writer for the FlatCityBuf cloud-optimized CityJSON format",
  "homepage": "https://github.com/cityjson/flatcitybuf",
  "license": "MIT",
  "dependencies": [
    "flatbuffers",
    "nlohmann-json",
    {
      "name": "vcpkg-cmake",
      "host": true
    },
    {
      "name": "vcpkg-cmake-config",
      "host": true
    }
  ]
}
```

- [ ] **Step 2: Write the portfile**

Create `vcpkg_ports/flatcitybuf/portfile.cmake`:

```cmake
vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO cityjson/flatcitybuf
    REF 72e5b68d469aa00a75ccba23780e2063751e3cff
    SHA512 0
)

vcpkg_cmake_configure(
    SOURCE_PATH "${SOURCE_PATH}/src/cpp"
    OPTIONS
        -DFCB_WITH_JSON=ON
        -DFCB_WITH_CURL=OFF
        -DFCB_BUILD_TESTS=OFF
        -DFCB_BUILD_EXAMPLES=OFF
)

vcpkg_cmake_install()
vcpkg_cmake_config_fixup(PACKAGE_NAME flatcitybuf CONFIG_PATH lib/cmake/flatcitybuf)
vcpkg_copy_pdbs()

file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")

file(INSTALL "${SOURCE_PATH}/LICENSE" DESTINATION "${CURRENT_PACKAGES_DIR}/share/${PORT}" RENAME copyright)
```

`SHA512 0` is a deliberate vcpkg placeholder — the first configure attempt fails and
prints the real hash, which Step 4 pastes in.

- [ ] **Step 3: Register the overlay port and the new dependency in the root manifest**

Read `vcpkg.json` first (Read tool), then edit its `dependencies` array to add
`"flatbuffers"` and its `vcpkg-configuration.overlay-ports` array to add
`"./vcpkg_ports"` as a second entry, so the result is:

```json
{
        "dependencies": [
                "openssl",
                "nlohmann-json",
                "flatbuffers"
        ],
        "vcpkg-configuration": {
                "overlay-ports": [
                        "./extension-ci-tools/vcpkg_ports",
                        "./vcpkg_ports"
                ],
                "overlay-triplets": [
                        "./extension-ci-tools/toolchains"
                ]
        }
}
```

- [ ] **Step 4: Resolve the real SHA512**

Run (from repo root, with `VCPKG_ROOT` set to wherever this project's vcpkg checkout
lives — check `build/release/CMakeCache.txt` for `VCPKG_ROOT` or `Z_VCPKG_ROOT_DIR` if
unsure):
```sh
"$VCPKG_ROOT/vcpkg" install flatcitybuf --overlay-ports=./vcpkg_ports --overlay-ports=./extension-ci-tools/vcpkg_ports 2>&1 | tail -30
```
Expected: fails with an "Actual hash" line. Copy that hash into `portfile.cmake`'s
`SHA512` field, replacing the `0` placeholder, then rerun the same command.
Expected: succeeds this time, ending with `flatcitybuf is installed`.

- [ ] **Step 5: Commit**

```bash
git add vcpkg_ports vcpkg.json
git commit -m "build(fcb): add repo-owned vcpkg overlay port for native flatcitybuf

Pinned to upstream commit 72e5b68d (unreleased — no v0.8.0 tag exists yet).
Lives outside extension-ci-tools/, which is a pinned duckdb submodule we
don't edit."
```

---

### Task 3: Wire root CMake to the vendored library and smoke-build

**Files:**
- Modify: `CMakeLists.txt:59-61` (source list comment), `CMakeLists.txt:96-286` (whole
  prebuilt-download block, replaced)

**Interfaces:**
- Produces: `CITYJSON_HAS_FCB` compile define + `flatcitybuf::flatcitybuf` linked into
  both `${EXTENSION_NAME}` and `${LOADABLE_EXTENSION_NAME}`, available to Task 4 onward.

- [ ] **Step 1: Read the current block to replace**

Read `CMakeLists.txt` lines 90-290 (Read tool) to get exact current text before editing
— the line numbers above are from the pre-migration file and will have shifted slightly
since Task 1/2 didn't touch this file.

- [ ] **Step 2: Replace the download block**

Replace the entire `option(CITYJSON_ENABLE_FCB ...)` through the matching `endif()` that
closes the "FCB support disabled" `file(WRITE ...)` no-op block (originally lines
~96-286) with:

```cmake
# Optional: FlatCityBuf support (native C++ library, vendored via vcpkg overlay port —
# see vcpkg_ports/flatcitybuf/). Portable C++ source, so no platform/arch restrictions.
option(CITYJSON_ENABLE_FCB "Enable FlatCityBuf (.fcb) support" ON)

if(CITYJSON_ENABLE_FCB)
  find_package(flatcitybuf CONFIG REQUIRED)
  target_link_libraries(${EXTENSION_NAME} PRIVATE flatcitybuf::flatcitybuf)
  target_link_libraries(${LOADABLE_EXTENSION_NAME} PRIVATE flatcitybuf::flatcitybuf)
  target_compile_definitions(${EXTENSION_NAME} PRIVATE CITYJSON_HAS_FCB)
  target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE CITYJSON_HAS_FCB)
  message(STATUS "FlatCityBuf support enabled (native C++, vendored)")
endif()
```

- [ ] **Step 3: Temporarily stub the FCB source files so the build can prove the link works**

Tasks 4-7 rewrite `flatcitybuf_reader.cpp`/`flatcitybuf_table_function.cpp`/
`cityjson_writer.cpp`'s FCB section for real. For this task only, add a minimal marker
so `cmake --build` actually exercises the new header/library: open
`src/cityjson/flatcitybuf_reader.cpp` and, directly after the `#include "fcb.h"` line
(the old FFI header include), change it to `#include <fcb/reader.hpp>` and add, right
after the existing includes, a throwaway smoke check inside the existing
`FlatCityBufReader::GetBBox()` method body's very first line:; the rest of this file's
old FFI code becomes dead weight for the next tasks to remove — that removal happens in
Task 5, not here. For this task, just confirm the header resolves and the library links:
add a translation-unit-scope static assertion right after the includes at the top of
`src/cityjson/flatcitybuf_reader.cpp`:

```cpp
#include "cityjson/flatcitybuf_reader.hpp"
#include "cityjson/city_object_utils.hpp"
#include "cityjson/json_utils.hpp"
#include <fcb/reader.hpp>

namespace {
// Task 3 smoke check: proves fcb/reader.hpp resolves and flatcitybuf::flatcitybuf
// actually links. Removed in Task 5 once the file is rewritten for real.
static_assert(sizeof(fcb::BBox) == 32, "fcb::BBox layout sanity check");
} // namespace
```
(Leave the rest of the file's old `fcb::fcb_reader_open(...)` FFI calls in place for
now — they'll fail to compile against the new header, which is expected and fixed in
Task 5. This step's only goal is proving the vendored library links; step 4 checks that
narrowly, not a full build.)

- [ ] **Step 4: Compile just the preprocessor+header-resolution step, not the full file**

Run: `cmake -B build/release -S . -DCMAKE_BUILD_TYPE=Release -GNinja 2>&1 | tail -40`
Expected: configure succeeds, ending with `-- FlatCityBuf support enabled (native C++,
vendored)` and no "Could not find flatcitybuf" error. (The full file will still fail to
*compile* past this point because of the old FFI calls further down — that's expected;
this step only proves configure-time `find_package` resolution succeeded.)

Run: `ninja -C build/release cityjson_extension 2>&1 | head -60`
Expected: fails, but specifically on the OLD `fcb::fcb_reader_open` calls further down
in the file (undeclared in the new `fcb::` namespace), not on `#include <fcb/reader.hpp>`
or the `static_assert` — confirming the header and library resolved correctly and the
only remaining problem is the (expected, not-yet-rewritten) old API calls.

- [ ] **Step 5: Remove the smoke check**

Revert the `static_assert` block added in Step 3 (Task 5 will replace the whole file's
FCB-facing includes properly). Keep the `#include <fcb/reader.hpp>` swap-in — Task 5
starts from there.

- [ ] **Step 6: Commit**

```bash
git add CMakeLists.txt src/cityjson/flatcitybuf_reader.cpp
git commit -m "build(fcb): link vendored flatcitybuf, drop prebuilt-binary download

Replaces the per-OS/arch/glibc prebuilt libfcb_cpp.a download with
find_package(flatcitybuf CONFIG REQUIRED) against the vcpkg overlay port
added in the previous commit. CITYJSON_ENABLE_FCB now defaults ON
everywhere -- no more platform restrictions."
```

---

### Task 4: Implement `DuckDBRangeReader`

**Files:**
- Create: `src/include/cityjson/duckdb_fs_range_reader.hpp`
- Create: `src/cityjson/duckdb_fs_range_reader.cpp`
- Modify: `CMakeLists.txt` (add the new `.cpp` to `EXTENSION_SOURCES`, inside the existing
  FCB section of the source list)

**Interfaces:**
- Produces: `duckdb::cityjson::DuckDBRangeReader`, constructible as
  `DuckDBRangeReader(ClientContext &context, const std::string &path)`, implementing
  `fcb::RangeReader`. Consumed by Task 5's `FlatCityBufReader`.

- [ ] **Step 1: Write the header**

Create `src/include/cityjson/duckdb_fs_range_reader.hpp`:

```cpp
#pragma once

#ifdef CITYJSON_HAS_FCB

#include <fcb/range_reader.hpp>
#include <cstdint>
#include <memory>
#include <string>

namespace duckdb {
class ClientContext;
class FileHandle;

namespace cityjson {

/**
 * fcb::RangeReader backed by DuckDB's own FileSystem, so local paths and any
 * http(s)/s3/gcs URL httpfs already supports are read the same way
 * read_cityjson/read_cityjsonseq read them -- one HTTP stack, one
 * credentials/secrets/proxy story. Auto-loads the httpfs extension for
 * remote paths, matching json_utils::ReadFileContent's own behavior.
 */
class DuckDBRangeReader : public fcb::RangeReader {
public:
	DuckDBRangeReader(duckdb::ClientContext &context, const std::string &path);
	~DuckDBRangeReader() override;

	std::uint64_t total_size() override;
	std::vector<std::uint8_t> read(std::uint64_t offset, std::uint64_t length) override;

private:
	std::string path_;
	std::unique_ptr<duckdb::FileHandle> handle_;
};

} // namespace cityjson
} // namespace duckdb

#endif // CITYJSON_HAS_FCB
```

- [ ] **Step 2: Write the implementation**

Create `src/cityjson/duckdb_fs_range_reader.cpp`:

```cpp
#ifdef CITYJSON_HAS_FCB

#include "cityjson/duckdb_fs_range_reader.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/extension_helper.hpp"

namespace duckdb {
namespace cityjson {

static bool IsRemotePath(const std::string &path) {
	return path.rfind("http://", 0) == 0 || path.rfind("https://", 0) == 0 || path.rfind("s3://", 0) == 0 ||
	       path.rfind("s3a://", 0) == 0 || path.rfind("s3n://", 0) == 0 || path.rfind("gcs://", 0) == 0 ||
	       path.rfind("gs://", 0) == 0 || path.rfind("r2://", 0) == 0 || path.rfind("hf://", 0) == 0;
}

DuckDBRangeReader::DuckDBRangeReader(duckdb::ClientContext &context, const std::string &path) : path_(path) {
	if (IsRemotePath(path_)) {
		duckdb::ExtensionHelper::AutoLoadExtension(context, "httpfs");
	}
	auto &fs = duckdb::FileSystem::GetFileSystem(context);
	try {
		handle_ = fs.OpenFile(path_, duckdb::FileOpenFlags::FILE_FLAGS_READ);
	} catch (const std::exception &e) {
		throw fcb::Error(fcb::ErrorCode::IoError, "Failed to open " + path_ + ": " + e.what());
	}
	if (!handle_) {
		throw fcb::Error(fcb::ErrorCode::IoError, "Failed to open " + path_);
	}
}

DuckDBRangeReader::~DuckDBRangeReader() = default;

std::uint64_t DuckDBRangeReader::total_size() {
	return static_cast<std::uint64_t>(handle_->GetFileSize());
}

std::vector<std::uint8_t> DuckDBRangeReader::read(std::uint64_t offset, std::uint64_t length) {
	auto total = total_size();
	if (offset >= total || length == 0) {
		return {};
	}
	// Contract (fcb/range_reader.hpp): clamp to what actually exists past `offset`
	// rather than throwing -- only a genuinely truncated transport read is an error.
	std::uint64_t clamped_length = std::min<std::uint64_t>(length, total - offset);
	std::vector<std::uint8_t> buffer(clamped_length);
	try {
		handle_->Read(buffer.data(), clamped_length, offset);
	} catch (const std::exception &e) {
		throw fcb::Error(fcb::ErrorCode::IoError,
		                 "Read failed at offset " + std::to_string(offset) + " of " + path_ + ": " + e.what());
	}
	return buffer;
}

} // namespace cityjson
} // namespace duckdb

#endif // CITYJSON_HAS_FCB
```

- [ ] **Step 3: Add the new source file to the build**

Read `CMakeLists.txt` around the existing `# FlatCityBuf (compiled always but guarded by
#ifdef CITYJSON_HAS_FCB)` comment (originally lines 59-61) and add the new file to that
same list:

```cmake
    # FlatCityBuf (compiled always but guarded by #ifdef CITYJSON_HAS_FCB)
    src/cityjson/duckdb_fs_range_reader.cpp
    src/cityjson/flatcitybuf_reader.cpp
```

- [ ] **Step 4: Build**

Run: `cmake --build build/release --target cityjson_extension 2>&1 | tail -60`
Expected: `duckdb_fs_range_reader.cpp` compiles cleanly. The overall target still fails
on `flatcitybuf_reader.cpp`'s old FFI calls — expected until Task 5.

- [ ] **Step 5: Commit**

```bash
git add src/include/cityjson/duckdb_fs_range_reader.hpp src/cityjson/duckdb_fs_range_reader.cpp CMakeLists.txt
git commit -m "feat(fcb): add DuckDBRangeReader, an fcb::RangeReader over DuckDB FileSystem

Serves local paths and any httpfs-supported http(s)/s3/gcs URL uniformly,
so FlatCityBuf's remote reads share read_cityjson/read_cityjsonseq's
credentials/secrets/proxy configuration instead of adding a second,
independent libcurl-based HTTP stack."
```

---

### Task 5: Rewrite `FlatCityBufReader` against the native API (read path, no query filters yet)

**Files:**
- Modify: `src/include/cityjson/flatcitybuf_reader.hpp` (full rewrite)
- Modify: `src/cityjson/flatcitybuf_reader.cpp` (full rewrite)

**Interfaces:**
- Consumes: `DuckDBRangeReader` (Task 4).
- Produces:
  ```cpp
  class FlatCityBufReader : public CityJSONReader {
  public:
    FlatCityBufReader(duckdb::ClientContext &context, const std::string &name,
                       const std::string &file_path, size_t sample_lines = 100);
    std::string Name() const override;
    CityJSON ReadMetadata() const override;
    CityJSONFeatureChunk ReadNthChunk(size_t n) const override;
    CityJSONFeatureChunk ReadAllChunks() const override;
    std::vector<CityJSONFeature> ReadNFeatures(size_t n) const override;
    std::vector<Column> Columns() const override;

    void SetBBoxFilter(std::array<double, 4> bbox);                       // Task 9
    void SetAttrQueryFilter(fcb::AttrQuery query, bool exact_index_only); // Task 11
    std::vector<std::string> IndexedAttributeColumns() const;            // Task 11
    std::optional<fcb::ColumnInfo> FindColumn(const std::string &name) const; // Task 11
    fcb::HeaderView Header() const;                                       // Task 11
  };
  ```
  Consumed by Task 6 (`flatcitybuf_metadata`), Task 8 (`reader_factory.cpp`), Task 9
  (bbox), Task 11 (attribute pushdown).

- [ ] **Step 1: Write the failing test (against Task 1's committed fixture)**

Create `test/sql/cityjson_fcb_reader_native.test`:

```
# name: test/sql/cityjson_fcb_reader_native.test
# description: read_flatcitybuf against the native C++ flatcitybuf reader
# group: [sql]

require cityjson

query I
SELECT COUNT(*) FROM read_flatcitybuf('test/data/sample.fcb');
----
2

query I
SELECT id FROM read_flatcitybuf('test/data/fcb_bbox_attr.fcb') ORDER BY id;
----
f1
f2
f3
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb 2>&1 | tail -40`
Expected: build failure (old FFI calls don't exist against the new header) — this IS the
expected red state; the test can't even run yet because the extension doesn't build.

- [ ] **Step 3: Replace the header**

Replace `src/include/cityjson/flatcitybuf_reader.hpp` entirely:

```cpp
#pragma once

#ifdef CITYJSON_HAS_FCB

#include "cityjson/reader.hpp"
#include <fcb/header.hpp>
#include <fcb/key.hpp>
#include <fcb/reader.hpp>
#include <fcb/stree.hpp>
#include <array>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace duckdb {
class ClientContext;

namespace cityjson {

/**
 * Reader for FlatCityBuf (.fcb) via the native flatcitybuf C++ library.
 * Opens a fresh fcb::FcbReader (over a fresh DuckDBRangeReader transport) per
 * call, matching this class's pre-existing per-call-reopen pattern; only
 * ReadMetadata()/Columns() results are cached across calls.
 */
class FlatCityBufReader : public CityJSONReader {
public:
	FlatCityBufReader(duckdb::ClientContext &context, const std::string &name, const std::string &file_path,
	                  size_t sample_lines = 100);

	std::string Name() const override;
	CityJSON ReadMetadata() const override;
	CityJSONFeatureChunk ReadNthChunk(size_t n) const override;
	CityJSONFeatureChunk ReadAllChunks() const override;
	std::vector<CityJSONFeature> ReadNFeatures(size_t n) const override;
	std::vector<Column> Columns() const override;

	/** Restrict reads to features whose 2D bbox intersects [min_x,min_y,max_x,max_y]. */
	void SetBBoxFilter(std::array<double, 4> bbox);

	/** Restrict reads to features matching every AND-combined condition. */
	void SetAttrQueryFilter(fcb::AttrQuery query, bool exact_index_only = false);

	/** Column names that have a B+tree attribute index, for pushdown eligibility checks. */
	std::vector<std::string> IndexedAttributeColumns() const;

	/** On-disk column info by name, for typing a KeyValue during pushdown. std::nullopt if absent. */
	std::optional<fcb::ColumnInfo> FindColumn(const std::string &name) const;

	/** File header, by value -- fcb::HeaderView owns its own backing buffer. */
	fcb::HeaderView Header() const;

private:
	duckdb::ClientContext &context_;
	std::string name_;
	std::string file_path_;
	size_t sample_lines_;

	std::optional<std::array<double, 4>> bbox_;
	std::optional<fcb::AttrQuery> attr_query_;
	bool attr_query_exact_index_only_ = false;

	mutable std::optional<CityJSON> cached_metadata_;
	mutable std::optional<std::vector<Column>> cached_columns_;

	fcb::FcbReader OpenFcbReader() const;
	fcb::FeatureIterator SelectIterator(fcb::FcbReader &reader) const;
	bool MatchesAttrQueryPostFilter(const CityJSONFeature &feature) const;
	std::vector<CityJSONFeature> ParseFeatures(std::optional<size_t> limit) const;
};

} // namespace cityjson
} // namespace duckdb

#endif // CITYJSON_HAS_FCB
```

- [ ] **Step 4: Replace the implementation**

Replace `src/cityjson/flatcitybuf_reader.cpp` entirely:

```cpp
#ifdef CITYJSON_HAS_FCB

#include "cityjson/flatcitybuf_reader.hpp"
#include "cityjson/city_object_utils.hpp"
#include "cityjson/duckdb_fs_range_reader.hpp"
#include "cityjson/json_utils.hpp"
#include <fcb/cityjson.hpp>

namespace duckdb {
namespace cityjson {

using namespace json_utils;

FlatCityBufReader::FlatCityBufReader(duckdb::ClientContext &context, const std::string &name,
                                     const std::string &file_path, size_t sample_lines)
    : context_(context), name_(name), file_path_(file_path), sample_lines_(sample_lines) {
}

std::string FlatCityBufReader::Name() const {
	return name_;
}

fcb::FcbReader FlatCityBufReader::OpenFcbReader() const {
	auto transport = std::make_shared<DuckDBRangeReader>(context_, file_path_);
	return fcb::FcbReader::open(transport);
}

void FlatCityBufReader::SetBBoxFilter(std::array<double, 4> bbox) {
	bbox_ = bbox;
}

void FlatCityBufReader::SetAttrQueryFilter(fcb::AttrQuery query, bool exact_index_only) {
	attr_query_ = std::move(query);
	attr_query_exact_index_only_ = exact_index_only;
}

fcb::FeatureIterator FlatCityBufReader::SelectIterator(fcb::FcbReader &reader) const {
	if (bbox_.has_value()) {
		const auto &b = bbox_.value();
		return reader.select_bbox(fcb::BBox {b[0], b[1], b[2], b[3]});
	}
	if (attr_query_.has_value()) {
		return reader.select_attr(attr_query_.value(), fcb::AttrQueryOptions {attr_query_exact_index_only_});
	}
	return reader.select_all();
}

namespace {

// fcb::KeyValue exposes no public numeric getter (by design -- see key.hpp: only
// kind()/original_string(), the latter populated for string kinds only). So a decoded
// JSON attribute value is turned into ANOTHER KeyValue of the same kind and compared
// via fcb::compare_keys, which gives the exact same ordering semantics the B+tree
// index itself uses (including its "ordered_float" float handling) instead of
// reimplementing comparison logic by hand.
std::optional<fcb::KeyValue> KeyValueFromJsonByKind(const json &value, fcb::KeyKind kind) {
	try {
		switch (kind) {
		case fcb::KeyKind::Int8:
			return fcb::KeyValue::from_i8(static_cast<int8_t>(value.get<int64_t>()));
		case fcb::KeyKind::UInt8:
			return fcb::KeyValue::from_u8(static_cast<uint8_t>(value.get<int64_t>()));
		case fcb::KeyKind::Int16:
			return fcb::KeyValue::from_i16(static_cast<int16_t>(value.get<int64_t>()));
		case fcb::KeyKind::UInt16:
			return fcb::KeyValue::from_u16(static_cast<uint16_t>(value.get<int64_t>()));
		case fcb::KeyKind::Int32:
			return fcb::KeyValue::from_i32(static_cast<int32_t>(value.get<int64_t>()));
		case fcb::KeyKind::UInt32:
			return fcb::KeyValue::from_u32(static_cast<uint32_t>(value.get<int64_t>()));
		case fcb::KeyKind::Int64:
			return fcb::KeyValue::from_i64(value.get<int64_t>());
		case fcb::KeyKind::UInt64:
			return fcb::KeyValue::from_u64(value.get<uint64_t>());
		case fcb::KeyKind::Float32:
			return fcb::KeyValue::from_f32(static_cast<float>(value.get<double>()));
		case fcb::KeyKind::Float64:
			return fcb::KeyValue::from_f64(value.get<double>());
		case fcb::KeyKind::Bool:
			return fcb::KeyValue::from_bool(value.get<bool>());
		case fcb::KeyKind::String20:
			return fcb::KeyValue::from_string(fcb::KeyKind::String20,
			                                  value.is_string() ? value.get<std::string>() : value.dump());
		case fcb::KeyKind::String50:
			return fcb::KeyValue::from_string(fcb::KeyKind::String50,
			                                  value.is_string() ? value.get<std::string>() : value.dump());
		case fcb::KeyKind::String100:
			return fcb::KeyValue::from_string(fcb::KeyKind::String100,
			                                  value.is_string() ? value.get<std::string>() : value.dump());
		default:
			return std::nullopt; // DateTime not needed for post-filter v1 -- no test exercises it
		}
	} catch (const json::type_error &) {
		return std::nullopt; // attribute's actual JSON type doesn't match the column's declared type
	}
}

} // namespace

bool FlatCityBufReader::MatchesAttrQueryPostFilter(const CityJSONFeature &feature) const {
	// Safety net for the "bbox AND attribute filter both set" case (spec §5.4 point 5):
	// select_bbox already ran the real index traversal, so this is a plain per-row
	// check, not a second index query. Harmless (redundant but correct) when
	// attr_query_ was itself the one used for SelectIterator's traversal too.
	if (!attr_query_.has_value()) {
		return true;
	}
	for (const auto &[obj_id, obj] : feature.city_objects) {
		bool all_match = true;
		for (const auto &cond : attr_query_.value()) {
			auto attr_it = obj.attributes.find(cond.field);
			if (attr_it == obj.attributes.end() || attr_it->second.is_null()) {
				all_match = false;
				break;
			}
			auto feature_value = KeyValueFromJsonByKind(attr_it->second, cond.value.kind());
			if (!feature_value.has_value()) {
				all_match = false;
				break;
			}
			int cmp = fcb::compare_keys(feature_value.value(), cond.value);
			bool ok = false;
			switch (cond.op) {
			case fcb::Operator::Eq:
				ok = (cmp == 0);
				break;
			case fcb::Operator::Ne:
				ok = (cmp != 0);
				break;
			case fcb::Operator::Gt:
				ok = (cmp > 0);
				break;
			case fcb::Operator::Ge:
				ok = (cmp >= 0);
				break;
			case fcb::Operator::Lt:
				ok = (cmp < 0);
				break;
			case fcb::Operator::Le:
				ok = (cmp <= 0);
				break;
			}
			if (!ok) {
				all_match = false;
				break;
			}
		}
		if (all_match) {
			return true;
		}
	}
	return false;
}

std::vector<CityJSONFeature> FlatCityBufReader::ParseFeatures(std::optional<size_t> limit) const {
	auto fcb_reader = OpenFcbReader();
	auto it = SelectIterator(fcb_reader);
	bool need_post_filter = bbox_.has_value() && attr_query_.has_value();

	std::vector<CityJSONFeature> features;
	while (it.next()) {
		if (limit.has_value() && features.size() >= limit.value()) {
			break;
		}
		json feature_json = fcb::to_cityjson_feature(it.current(), fcb_reader.header());
		try {
			CityJSONFeature feature = CityJSONFeature::FromJson(feature_json);
			if (need_post_filter && !MatchesAttrQueryPostFilter(feature)) {
				continue;
			}
			features.push_back(std::move(feature));
		} catch (const CityJSONError &) {
			// Skip malformed features, matching the previous FFI-based reader's behavior.
		}
	}
	return features;
}

CityJSON FlatCityBufReader::ReadMetadata() const {
	if (cached_metadata_.has_value()) {
		return cached_metadata_.value();
	}
	auto fcb_reader = OpenFcbReader();
	json meta_json = fcb::to_cityjson_metadata(fcb_reader.header());
	cached_metadata_ = CityJSON::FromJson(meta_json);
	return cached_metadata_.value();
}

CityJSONFeatureChunk FlatCityBufReader::ReadAllChunks() const {
	auto features = ParseFeatures(std::nullopt);
	return CityJSONFeatureChunk::CreateChunks(std::move(features), STANDARD_VECTOR_SIZE);
}

CityJSONFeatureChunk FlatCityBufReader::ReadNthChunk(size_t n) const {
	CityJSONFeatureChunk all_chunks = ReadAllChunks();
	if (n >= all_chunks.ChunkCount()) {
		return CityJSONFeatureChunk();
	}
	auto chunk_opt = all_chunks.GetChunk(n);
	if (!chunk_opt.has_value()) {
		return CityJSONFeatureChunk();
	}
	CityJSONFeatureChunk result;
	result.records = std::vector<CityJSONFeature>(chunk_opt->begin(), chunk_opt->end());
	result.chunks = {Range(0, result.records.size())};
	return result;
}

std::vector<CityJSONFeature> FlatCityBufReader::ReadNFeatures(size_t n) const {
	return ParseFeatures(n);
}

std::vector<Column> FlatCityBufReader::Columns() const {
	if (cached_columns_.has_value()) {
		return cached_columns_.value();
	}
	std::vector<Column> columns = GetDefinedColumns();
	std::vector<CityJSONFeature> sample_features = ReadNFeatures(sample_lines_);
	std::vector<Column> attr_columns = CityObjectUtils::InferAttributeColumns(sample_features, sample_lines_);
	std::vector<Column> geom_columns = CityObjectUtils::InferGeometryColumns(sample_features, sample_lines_);
	columns.insert(columns.end(), attr_columns.begin(), attr_columns.end());
	columns.insert(columns.end(), geom_columns.begin(), geom_columns.end());
	cached_columns_ = columns;
	return columns;
}

std::vector<std::string> FlatCityBufReader::IndexedAttributeColumns() const {
	auto fcb_reader = OpenFcbReader();
	const auto &header = fcb_reader.header();
	std::vector<std::string> result;
	for (const auto &idx_info : header.attr_indices()) {
		for (const auto &col : header.info().columns) {
			if (col.index == idx_info.column_index) {
				result.push_back(col.name);
				break;
			}
		}
	}
	return result;
}

std::optional<fcb::ColumnInfo> FlatCityBufReader::FindColumn(const std::string &name) const {
	auto fcb_reader = OpenFcbReader();
	for (const auto &col : fcb_reader.header().info().columns) {
		if (col.name == name) {
			return col;
		}
	}
	return std::nullopt;
}

fcb::HeaderView FlatCityBufReader::Header() const {
	return OpenFcbReader().header();
}

} // namespace cityjson
} // namespace duckdb

#endif // CITYJSON_HAS_FCB
```

- [ ] **Step 5: Build and run the new test**

Run: `cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb 2>&1 | tail -60`
Expected: builds cleanly (the rest of the codebase — `reader_factory.cpp`,
`flatcitybuf_table_function.cpp` — still references the old
`FlatCityBufReader(name, file_path, sample_lines)` two-string constructor and will fail;
that's expected and fixed in Task 8. For THIS step, build just the reader translation
unit in isolation to confirm it compiles:
`cmake --build build/release --target cityjson_extension 2>&1 | grep -A5 flatcitybuf_reader.cpp`
Expected: no errors reported against `flatcitybuf_reader.cpp` specifically (errors
against `flatcitybuf_table_function.cpp`/`reader_factory.cpp` calling the old
constructor signature are expected at this point).

- [ ] **Step 6: Commit**

```bash
git add src/include/cityjson/flatcitybuf_reader.hpp src/cityjson/flatcitybuf_reader.cpp test/sql/cityjson_fcb_reader_native.test
git commit -m "feat(fcb): rewrite FlatCityBufReader against native flatcitybuf C++ API

Replaces fcb::fcb_reader_open/select_all/metadata (Rust/cxx FFI) with
fcb::FcbReader::open + fcb::to_cityjson_metadata/to_cityjson_feature over
the new DuckDBRangeReader transport. Also lands SetBBoxFilter/
SetAttrQueryFilter/IndexedAttributeColumns/FindColumn/Header, consumed by
later tasks (bbox query, attribute pushdown) -- not wired to SQL yet."
```

---

### Task 6: Update `flatcitybuf_metadata` and the reader construction call site

**Files:**
- Modify: `src/cityjson/flatcitybuf_table_function.cpp` (`FlatCityBufBind`, `FcbMetadataBind`)
- Modify: `src/cityjson/reader_factory.cpp` (`.fcb` branch in `OpenAnyCityJSONFile`)

**Interfaces:**
- Consumes: `FlatCityBufReader(ClientContext&, name, file_path, sample_lines)` (Task 5).

- [ ] **Step 1: Write the failing test**

Create `test/sql/cityjson_fcb_metadata_native.test`:

```
# name: test/sql/cityjson_fcb_metadata_native.test
# description: flatcitybuf_metadata against the native C++ flatcitybuf reader
# group: [sql]

require cityjson

query I
SELECT city_objects_count FROM flatcitybuf_metadata('test/data/sample.fcb');
----
2
```

- [ ] **Step 2: Run it to confirm it fails to build**

Run: `cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb 2>&1 | tail -60`
Expected: fails — `FlatCityBufBind`/`FcbMetadataBind`/`OpenAnyCityJSONFile` still
construct `FlatCityBufReader` with the old 2-string-argument constructor, and
`FcbMetadataBind` still calls the removed `fcb::fcb_reader_open`/`fcb_reader_metadata`
free functions.

- [ ] **Step 3: Fix `reader_factory.cpp`**

In `src/cityjson/reader_factory.cpp`, change:
```cpp
	if (EndsWith(file_name, ".fcb")) {
		return std::make_unique<FlatCityBufReader>(file_name, file_name, sample_lines);
	}
```
to:
```cpp
	if (EndsWith(file_name, ".fcb")) {
		return std::make_unique<FlatCityBufReader>(context, file_name, file_name, sample_lines);
	}
```
(`context` is already a parameter of `OpenAnyCityJSONFile`, per its existing signature.)

- [ ] **Step 4: Fix `FlatCityBufBind`**

In `src/cityjson/flatcitybuf_table_function.cpp`, change:
```cpp
	auto reader = std::make_unique<FlatCityBufReader>(file_name, file_name, options.sample_lines);
```
to:
```cpp
	auto reader = std::make_unique<FlatCityBufReader>(context, file_name, file_name, options.sample_lines);
```

- [ ] **Step 5: Fix `FcbMetadataBind`**

In the same file, replace:
```cpp
	auto reader = std::make_unique<FlatCityBufReader>(result->file_name, result->file_name);

	try {
		result->metadata = reader->ReadMetadata();
	} catch (const CityJSONError &e) {
		throw BinderException("Failed to read FlatCityBuf metadata: " + std::string(e.what()));
	}

	// Use features_count from FCB metadata for a fast count without reading all features
	auto fcb_reader_raw = fcb::fcb_reader_open(result->file_name);
	auto fcb_meta = fcb::fcb_reader_metadata(*fcb_reader_raw);
	result->city_objects_count = fcb_meta.features_count;
```
with:
```cpp
	auto reader = std::make_unique<FlatCityBufReader>(context, result->file_name, result->file_name);

	try {
		result->metadata = reader->ReadMetadata();
	} catch (const CityJSONError &e) {
		throw BinderException("Failed to read FlatCityBuf metadata: " + std::string(e.what()));
	}

	// features_count comes straight from the header -- no separate reopen needed now
	// that FlatCityBufReader exposes Header() directly.
	result->city_objects_count = reader->Header().info().features_count;
```

Also remove the now-unused `#include "fcb.h"` at the top of this file if nothing else in
it references the old FFI header (check with `grep -n "fcb::" src/cityjson/flatcitybuf_table_function.cpp`
after this step — anything still referencing `fcb::` should be the new-API calls from
this task, not the old `fcb.h` free functions).

- [ ] **Step 6: Build and run**

Run: `cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb 2>&1 | tail -60`
Expected: builds cleanly now (writer path, `cityjson_writer.cpp`'s `WriteFlatCityBuf`,
is still on the old FFI API and will fail — that's Task 7).

Isolate just this task's two test files by building only what's needed and running them
directly (skip `make test`'s full suite, which needs the writer too):
```sh
./build/release/duckdb -unsigned -c "LOAD 'build/release/extension/cityjson/cityjson.duckdb_extension'; SELECT COUNT(*) FROM read_flatcitybuf('test/data/sample.fcb'); SELECT city_objects_count FROM flatcitybuf_metadata('test/data/sample.fcb');"
```
Expected: `2` then `2`.

- [ ] **Step 7: Commit**

```bash
git add src/cityjson/flatcitybuf_table_function.cpp src/cityjson/reader_factory.cpp test/sql/cityjson_fcb_metadata_native.test
git commit -m "fix(fcb): update flatcitybuf_metadata and reader construction for the native API

flatcitybuf_metadata's features_count now comes from FlatCityBufReader::
Header() instead of a second, separate fcb_reader_open/fcb_reader_metadata
FFI call -- that API no longer exists."
```

---

### Task 7: Reimplement `WriteFlatCityBuf` against `fcb::FcbWriter`

**Files:**
- Modify: `src/include/cityjson/cityjson_writer.hpp` (`WriteFlatCityBuf` signature —
  unchanged shape for this task; new options params land in Task 10)
- Modify: `src/cityjson/cityjson_writer.cpp` (`WriteFlatCityBuf` body, full rewrite)

**Interfaces:**
- Consumes: `feature_objects`/`feature_order` (existing shape, unchanged),
  `CityJSONWriteMetadata` (existing).
- Produces: `.fcb` files the Task 5 reader can read back (verified via round-trip test).

- [ ] **Step 1: Write the failing test**

Create `test/sql/cityjson_fcb_writer_native.test`:

```
# name: test/sql/cityjson_fcb_writer_native.test
# description: COPY TO (FORMAT flatcitybuf) against the native fcb::FcbWriter
# group: [sql]

require cityjson

statement ok
COPY (SELECT * FROM read_cityjsonseq('test/data/sample.city.jsonl'))
TO '__TEST_DIR__/native_write.fcb' (FORMAT flatcitybuf);

query I
SELECT COUNT(*) FROM read_flatcitybuf('__TEST_DIR__/native_write.fcb');
----
2

query I
SELECT id FROM read_flatcitybuf('__TEST_DIR__/native_write.fcb') ORDER BY id;
----
feature1
feature2
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb 2>&1 | tail -60`
Expected: fails — `cityjson_writer.cpp`'s `WriteFlatCityBuf` still calls the removed
`fcb::fcb_writer_new`/`fcb_writer_add_feature`/`fcb_writer_write` FFI functions.

- [ ] **Step 3: Rewrite `WriteFlatCityBuf`**

In `src/cityjson/cityjson_writer.cpp`, replace the whole `#ifdef CITYJSON_HAS_FCB` /
`WriteFlatCityBuf` block (previously the section starting at the `// WriteFlatCityBuf`
comment) with:

```cpp
#ifdef CITYJSON_HAS_FCB

namespace {

// Mirrors write_cityjson.cpp's semantic_surface_other_members(): everything on a
// semantic surface besides type/parent/children is an indexable "other" attribute.
nlohmann::ordered_json SemanticSurfaceOtherMembers(const nlohmann::ordered_json &surface) {
	nlohmann::ordered_json other = nlohmann::ordered_json::object();
	for (const auto &[key, val] : surface.items()) {
		if (key != "type" && key != "parent" && key != "children") {
			other[key] = val;
		}
	}
	return other;
}

} // namespace

void CityJSONWriter::WriteFlatCityBuf(const std::string &file_path, const CityJSONWriteMetadata &metadata,
                                      std::map<std::string, std::vector<std::pair<std::string, json>>> feature_objects,
                                      const std::vector<std::string> &feature_order) {

	// Build the metadata header (same shape as CityJSONSeq's line 1).
	json header;
	header["type"] = "CityJSON";
	header["version"] = metadata.version;
	header["CityObjects"] = json::object();
	header["vertices"] = json::array();

	auto meta_json = BuildMetadataJson(metadata);
	if (!meta_json.empty()) {
		header["metadata"] = meta_json;
	}

	// FcbWriter needs a transform to quantize/dequantize vertices -- identity if none given.
	{
		auto &t = metadata.transform;
		header["transform"] = json::object();
		header["transform"]["scale"] = json::array(
		    {t.has_value() ? t->scale[0] : 1.0, t.has_value() ? t->scale[1] : 1.0, t.has_value() ? t->scale[2] : 1.0});
		header["transform"]["translate"] =
		    json::array({t.has_value() ? t->translate[0] : 0.0, t.has_value() ? t->translate[1] : 0.0,
		                 t.has_value() ? t->translate[2] : 0.0});
	}

	// Build each feature's per-feature vertex pool exactly like WriteCityJSONSeq does,
	// and convert both header and features to nlohmann::ordered_json -- the concrete
	// type fcb::add_attributes/FcbWriter require. Since feature_objects' values are
	// plain (map-ordered, i.e. alphabetical) nlohmann::json, converting via dump()+
	// parse() yields alphabetical column-index assignment -- deterministic and
	// correctly self-consistent for round-tripping through our own reader/writer,
	// just not guaranteed byte-identical to what the upstream Rust CLI would produce
	// for the same input (which uses true document/insertion order). That's fine: we
	// don't need CLI byte-compatibility, only correct round-tripping.
	nlohmann::ordered_json ordered_header = nlohmann::ordered_json::parse(header.dump());

	std::vector<nlohmann::ordered_json> ordered_features;
	ordered_features.reserve(feature_order.size());
	for (const auto &fid : feature_order) {
		auto it = feature_objects.find(fid);
		if (it == feature_objects.end()) {
			continue;
		}
		auto &feature_objs = it->second;
		auto vertex_pool = BuildVertexPool(feature_objs, metadata.transform);

		json feature;
		feature["type"] = "CityJSONFeature";
		feature["id"] = fid;
		feature["CityObjects"] = json::object();
		for (const auto &[obj_id, obj_json] : feature_objs) {
			feature["CityObjects"][obj_id] = obj_json;
		}
		feature["vertices"] = json::array();
		for (const auto &v : vertex_pool) {
			feature["vertices"].push_back(json::array({v[0], v[1], v[2]}));
		}

		ordered_features.push_back(nlohmann::ordered_json::parse(feature.dump()));
	}

	// Pass 1: two-pass attribute schema scan, required before FcbWriter construction
	// because column numbering is assigned as names are first encountered.
	fcb::AttributeSchema attr_schema;
	fcb::AttributeSchema semantic_attr_schema;
	for (const auto &feature : ordered_features) {
		for (const auto &[obj_id, obj] : feature.at("CityObjects").items()) {
			if (auto attr_it = obj.find("attributes"); attr_it != obj.end()) {
				fcb::add_attributes(attr_schema, *attr_it);
			}
			auto geom_it = obj.find("geometry");
			if (geom_it == obj.end() || !geom_it->is_array()) {
				continue;
			}
			for (const auto &geometry : *geom_it) {
				auto sem_it = geometry.find("semantics");
				if (sem_it == geometry.end() || !sem_it->contains("surfaces")) {
					continue;
				}
				for (const auto &surface : sem_it->at("surfaces")) {
					auto other = SemanticSurfaceOtherMembers(surface);
					if (!other.empty()) {
						fcb::add_attributes(semantic_attr_schema, other);
					}
				}
			}
		}
	}
	const bool has_semantic_attrs = !semantic_attr_schema.empty();

	fcb::FcbWriterOptions options;
	// attr_index/branching_factor options land in Task 10; options.attribute_indices
	// stays empty here, matching the old FFI writer's behavior (no indices by default).

	fcb::FcbWriter writer(ordered_header, options, attr_schema,
	                      has_semantic_attrs ? std::optional(semantic_attr_schema) : std::nullopt);
	for (const auto &feature : ordered_features) {
		writer.add_feature(feature);
	}

	std::ofstream out(file_path, std::ios::binary);
	if (!out.is_open()) {
		throw CityJSONError::FileWrite("Failed to open output file: " + file_path);
	}
	writer.write(out); // streaming overload -- bounded memory, unlike write()'s vector return
	out.close();
	if (!out) {
		throw CityJSONError::FileWrite("Failed writing output file: " + file_path);
	}
}

#endif // CITYJSON_HAS_FCB
```

Add `#include <fcb/writer/fcb_writer.hpp>` and `#include <fcb/writer/attribute.hpp>` to
the top of `src/cityjson/cityjson_writer.cpp` (replacing the old `#include "fcb.h"`).

- [ ] **Step 4: Build and run**

Run: `cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb 2>&1 | tail -60`
Expected: builds cleanly.

Run: `./build/release/duckdb -unsigned -c "$(cat test/sql/cityjson_fcb_writer_native.test | sed -n '/^statement ok/,$p' | head -5)"` —
simpler, just run the full sqllogictest file:
`./build/release/test/unittest test/sql/cityjson_fcb_writer_native.test` (adjust path if
the unittest binary lives elsewhere in this build tree — check
`find build/release -maxdepth 2 -iname "*unittest*"` if unsure).
Expected: all queries pass (`2`, then `feature1`/`feature2`).

- [ ] **Step 5: Commit**

```bash
git add src/cityjson/cityjson_writer.cpp test/sql/cityjson_fcb_writer_native.test
git commit -m "feat(fcb): reimplement WriteFlatCityBuf against native fcb::FcbWriter

Replaces fcb::fcb_writer_new/add_feature/write (Rust/cxx FFI) with a
two-pass attribute-schema scan (fcb::add_attributes) followed by
fcb::FcbWriter::add_feature/write(ostream&). Column-index assignment is
now alphabetical (via nlohmann::json -> ordered_json conversion) rather
than Rust-CLI-matching insertion order -- deterministic and correct for
round-tripping through our own reader/writer, not byte-compatible with
the upstream CLI's own output, which we don't need."
```

---

### Task 8: Full regression — restore `cityjson_e2e_fcb.test`, drop `require notmusl`

**Files:**
- Modify: `test/sql/cityjson_e2e_fcb.test` (remove `require notmusl` line only)

**Interfaces:**
- Consumes: everything from Tasks 2-7.

- [ ] **Step 1: Remove the platform restriction**

In `test/sql/cityjson_e2e_fcb.test`, delete these two lines:
```
# FCB uses a pre-built glibc binary — skip on musl
require notmusl
```

- [ ] **Step 2: Run the full existing e2e test**

Run: `cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb 2>&1 | tail -40`
Expected: clean build.

Run the test (find the unittest binary path first if needed:
`find build/release -maxdepth 2 -iname "*unittest*"`), then:
`build/release/test/unittest test/sql/cityjson_e2e_fcb.test`
Expected: all phases pass (CityJSON→FlatCityBuf, CityJSONSeq→FlatCityBuf,
FlatCityBuf→CityJSON/CityJSONSeq/FlatCityBuf, and the full-chain conversions at the end
of that file).

- [ ] **Step 3: Run the complete `test/sql/` suite**

Run: `make test 2>&1 | tail -80` (or whatever this repo's actual `make test` invocation
resolves to — confirmed in the repo's own CLAUDE.md build section).
Expected: no regressions in `read_cityjson`/`read_cityjsonseq`/GeoParquet/appearance
tests — this task only touched FCB code, but a full run is the actual regression gate.

- [ ] **Step 4: Commit**

```bash
git add test/sql/cityjson_e2e_fcb.test
git commit -m "test(fcb): drop the musl skip -- native flatcitybuf has no platform restriction

The old require notmusl existed because the prebuilt libfcb_cpp.a needed
glibc >= 2.32. The vendored native build has no such constraint."
```

---

### Task 9: Bbox query on `read_flatcitybuf`

**Files:**
- Modify: `src/include/cityjson/flatcitybuf_table_function.hpp` (declare
  `FlatCityBufBindData`)
- Modify: `src/cityjson/flatcitybuf_table_function.cpp` (`FlatCityBufBindData`,
  `FlatCityBufBind`, `RegisterFlatCityBufTableFunction`)

**Interfaces:**
- Consumes: `FlatCityBufReader::SetBBoxFilter` (Task 5).
- Produces: `FlatCityBufBindData : public CityJSONBindData` with a `bbox` field and a
  `reader` field — the latter consumed by Task 11's pushdown filter.

- [ ] **Step 1: Write the failing test**

Create `test/sql/cityjson_fcb_bbox.test`:

```
# name: test/sql/cityjson_fcb_bbox.test
# description: bbox query pushdown on read_flatcitybuf via min_x/min_y/max_x/max_y
# group: [sql]

require cityjson

# Ground truth from test/data/fcb_bbox_attr.city.jsonl (Task 1): f1 at x/y 0-10,
# f2 at 100-110, f3 at 200-210.
statement ok
COPY (SELECT * FROM read_cityjsonseq('test/data/fcb_bbox_attr.city.jsonl'))
TO '__TEST_DIR__/bbox_query.fcb' (FORMAT flatcitybuf);

# bbox covering only f2
query I
SELECT id FROM read_flatcitybuf('__TEST_DIR__/bbox_query.fcb', min_x := 50, min_y := 50, max_x := 150, max_y := 150) ORDER BY id;
----
f2

# bbox covering nothing
query I
SELECT COUNT(*) FROM read_flatcitybuf('__TEST_DIR__/bbox_query.fcb', min_x := 500, min_y := 500, max_x := 600, max_y := 600);
----
0

# bbox covering everything
query I
SELECT COUNT(*) FROM read_flatcitybuf('__TEST_DIR__/bbox_query.fcb', min_x := 0, min_y := 0, max_x := 300, max_y := 300);
----
3

# no bbox at all still works (full scan)
query I
SELECT COUNT(*) FROM read_flatcitybuf('__TEST_DIR__/bbox_query.fcb');
----
3

# a partial bbox (missing max_y) is a binder error, not a silent no-op
statement error
SELECT COUNT(*) FROM read_flatcitybuf('__TEST_DIR__/bbox_query.fcb', min_x := 0, min_y := 0, max_x := 300);
----
read_flatcitybuf: min_x, min_y, max_x, and max_y must all be given together
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb 2>&1 | tail -40`
Expected: clean build (the test itself fails, not the build — `min_x` etc. aren't
recognized named parameters yet). Run the test file directly:
`build/release/test/unittest test/sql/cityjson_fcb_bbox.test`
Expected: FAIL — DuckDB rejects `min_x`/`min_y`/`max_x`/`max_y` as unknown named
parameters for `read_flatcitybuf`.

- [ ] **Step 3: Declare `FlatCityBufBindData`**

In `src/include/cityjson/flatcitybuf_table_function.hpp`, add (inside
`namespace duckdb { namespace cityjson {`, after the existing forward declarations):

```cpp
#include "cityjson/table_function.hpp"
#include <array>
#include <memory>
#include <optional>

namespace duckdb {
namespace cityjson {

class FlatCityBufReader;

/**
 * Bind data for read_flatcitybuf. Extends the generic CityJSONBindData with an
 * optional bbox and a live reference to the reader that produced `chunks`, so
 * FlatCityBufPushdownComplexFilter (Task 11) can re-query it once a WHERE
 * clause is known -- which happens after Bind, during filter pushdown.
 */
struct FlatCityBufBindData : public CityJSONBindData {
	std::optional<std::array<double, 4>> bbox;
	// shared_ptr, not unique_ptr: Copy() just shares it. Safe because nothing
	// mutates it after the pushdown-filter step (see FlatCityBufPushdownComplexFilter).
	std::shared_ptr<FlatCityBufReader> reader;

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other) const override;
};

void FlatCityBufPushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data,
                                      vector<unique_ptr<Expression>> &filters);

} // namespace cityjson
} // namespace duckdb
```

(Add this above the existing `RegisterFlatCityBufTableFunction`/
`RegisterFlatCityBufMetadataTableFunction` declarations, inside the same
`#ifdef CITYJSON_HAS_FCB` guard the file already has.)

- [ ] **Step 4: Implement `FlatCityBufBindData::Copy`/`Equals`, rewrite `FlatCityBufBind`**

In `src/cityjson/flatcitybuf_table_function.cpp`, add near the top (mirroring
`bind_data.cpp`'s pattern for the base struct):

```cpp
unique_ptr<FunctionData> FlatCityBufBindData::Copy() const {
	auto result = make_uniq<FlatCityBufBindData>();
	result->file_name = file_name;
	result->metadata = metadata;
	result->chunks = chunks;
	result->scan_plan = scan_plan;
	result->columns = columns;
	result->target_lod = target_lod;
	result->use_wkb_encoding = use_wkb_encoding;
	result->streaming = streaming;
	result->equality_filters = equality_filters;
	result->bbox = bbox;
	result->reader = reader;
	return result;
}

bool FlatCityBufBindData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<FlatCityBufBindData>();
	return file_name == other.file_name && target_lod == other.target_lod &&
	       use_wkb_encoding == other.use_wkb_encoding && streaming == other.streaming &&
	       equality_filters == other.equality_filters && bbox == other.bbox;
}
```

`BindCityJSONRead` (the existing shared bind helper) allocates its own
`make_uniq<CityJSONBindData>()` internally and takes ownership of the reader via
`unique_ptr<CityJSONReader>` — it can't hand back our `FlatCityBufBindData` subclass,
and `read_flatcitybuf` needs to keep its OWN `shared_ptr<FlatCityBufReader>` around
after bind (for Task 11's pushdown filter) rather than give up ownership. So this step
introduces `BindCityJSONReadRaw`, a reference-taking variant of the same logic that
returns a plain `CityJSONBindData` BY VALUE instead of an owning `unique_ptr`, and
refactors `BindCityJSONRead` to be a thin wrapper around it (no logic duplication).

In `src/include/cityjson/table_function.hpp`, add next to the existing
`BindCityJSONRead` declaration:
`BindCityJSONRead` to operate on a `CityJSONReader&` instead of taking ownership via
`unique_ptr` — needed because `FlatCityBufBindData` must keep its OWN `shared_ptr`
to the reader (for Task 11's pushdown filter) while also handing it to the shared bind
logic. In `src/include/cityjson/table_function.hpp`, add next to the existing
`BindCityJSONRead` declaration:

```cpp
/**
 * Same as BindCityJSONRead, but takes the reader by reference instead of by
 * unique_ptr, for callers (read_flatcitybuf) that need to keep their own
 * ownership handle to the reader after bind completes.
 */
CityJSONBindData BindCityJSONReadRaw(ClientContext &context, TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types, vector<string> &names,
                                     const std::string &function_name, CityJSONReader &reader,
                                     bool streaming = false);
```

In `src/cityjson/bind_function.cpp`, refactor `BindCityJSONRead` to delegate to this new
function instead of duplicating logic — replace the existing `BindCityJSONRead` body
with:

```cpp
CityJSONBindData BindCityJSONReadRaw(ClientContext &context, TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types, vector<string> &names,
                                     const std::string &function_name, CityJSONReader &reader,
                                     bool streaming) {
	CityJSONBindData result;

	if (input.inputs.empty()) {
		throw BinderException(function_name + " requires a file path");
	}
	result.file_name = StringValue::Get(input.inputs[0]);
	result.streaming = streaming;

	auto options = ParseCityJSONReadOptions(input, function_name);
	result.target_lod = options.target_lod;
	result.use_wkb_encoding = options.use_wkb_encoding;

	try {
		result.metadata = reader.ReadMetadata();
	} catch (const CityJSONError &e) {
		throw BinderException("Failed to read metadata: " + std::string(e.what()));
	}

	if (!streaming) {
		try {
			result.chunks = reader.ReadAllChunks();
		} catch (const CityJSONError &e) {
			throw BinderException("Failed to read data: " + std::string(e.what()));
		}
		result.scan_plan = result.chunks.BuildScanPlan();
	}

	InferSchema(result, reader, options.sample_lines);

	for (const auto &col : result.columns) {
		names.push_back(col.name);
		return_types.push_back(ColumnTypeUtils::ToDuckDBType(col.kind));
	}

	return result;
}

unique_ptr<FunctionData> BindCityJSONRead(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names,
                                          const std::string &function_name,
                                          std::unique_ptr<CityJSONReader> reader, bool streaming) {
	auto result = make_uniq<CityJSONBindData>(
	    BindCityJSONReadRaw(context, input, return_types, names, function_name, *reader, streaming));
	return result;
}
```

`InferSchema` currently takes `CityJSONBindData &bind_data` — check its exact signature
in `src/cityjson/bind_function.cpp` before this edit (it's a `static` free function in
that file, taking `(CityJSONBindData &bind_data, CityJSONReader &reader, size_t sample_lines)`)
and confirm it still compiles unchanged against a plain (non-pointer) `CityJSONBindData &result` —
it should, since `result` above is a local by-value `CityJSONBindData`, not a pointer.

With `BindCityJSONReadRaw` in place, `FlatCityBufBind` itself is now straightforward —
build the reader and bbox, call it for the shared schema-inference/metadata/chunks
logic, then assemble the `FlatCityBufBindData` from the result:

```cpp
static unique_ptr<FunctionData> FlatCityBufBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.empty()) {
		throw BinderException("read_flatcitybuf requires a file path");
	}
	std::string file_name = StringValue::Get(input.inputs[0]);
	auto options = ParseCityJSONReadOptions(input, "read_flatcitybuf");

	auto reader = std::make_shared<FlatCityBufReader>(context, file_name, file_name, options.sample_lines);

	bool has_min_x = input.named_parameters.count("min_x") > 0;
	bool has_min_y = input.named_parameters.count("min_y") > 0;
	bool has_max_x = input.named_parameters.count("max_x") > 0;
	bool has_max_y = input.named_parameters.count("max_y") > 0;
	std::optional<std::array<double, 4>> bbox;
	if (has_min_x || has_min_y || has_max_x || has_max_y) {
		if (!(has_min_x && has_min_y && has_max_x && has_max_y)) {
			throw BinderException("read_flatcitybuf: min_x, min_y, max_x, and max_y must all be given together");
		}
		bbox = std::array<double, 4> {DoubleValue::Get(input.named_parameters.at("min_x")),
		                              DoubleValue::Get(input.named_parameters.at("min_y")),
		                              DoubleValue::Get(input.named_parameters.at("max_x")),
		                              DoubleValue::Get(input.named_parameters.at("max_y"))};
		reader->SetBBoxFilter(bbox.value());
	}

	auto generic = BindCityJSONReadRaw(context, input, return_types, names, "read_flatcitybuf", *reader, false);

	// Field-by-field, matching CityJSONBindData::Copy()'s own pattern (bind_data.cpp) --
	// deliberately not a whole-object copy-assignment through a CityJSONBindData&
	// reference, to sidestep any question about FunctionData's (deprecated-but-legal)
	// implicitly-generated copy assignment operator.
	auto result = make_uniq<FlatCityBufBindData>();
	result->file_name = generic.file_name;
	result->metadata = generic.metadata;
	result->chunks = generic.chunks;
	result->scan_plan = generic.scan_plan;
	result->columns = generic.columns;
	result->target_lod = generic.target_lod;
	result->use_wkb_encoding = generic.use_wkb_encoding;
	result->streaming = false;
	result->bbox = bbox;
	result->reader = reader;
	return result;
}
```

- [ ] **Step 5: Register the new named parameters**

In `RegisterFlatCityBufTableFunction`, add:

```cpp
	func.named_parameters["min_x"] = LogicalType::DOUBLE;
	func.named_parameters["min_y"] = LogicalType::DOUBLE;
	func.named_parameters["max_x"] = LogicalType::DOUBLE;
	func.named_parameters["max_y"] = LogicalType::DOUBLE;
```
alongside the existing `sample_lines`/`lod` entries.

- [ ] **Step 6: Build and run**

Run: `cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb 2>&1 | tail -80`
Expected: clean build.

Run: `build/release/test/unittest test/sql/cityjson_fcb_bbox.test`
Expected: all pass.

Run the full FCB regression again to confirm no breakage:
`build/release/test/unittest test/sql/cityjson_e2e_fcb.test test/sql/cityjson_fcb_reader_native.test test/sql/cityjson_fcb_writer_native.test test/sql/cityjson_fcb_metadata_native.test`
Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add src/include/cityjson/table_function.hpp src/cityjson/bind_function.cpp src/include/cityjson/flatcitybuf_table_function.hpp src/cityjson/flatcitybuf_table_function.cpp test/sql/cityjson_fcb_bbox.test
git commit -m "feat(fcb): bbox query on read_flatcitybuf via min_x/min_y/max_x/max_y

Real R-tree-level skip (fcb::FcbReader::select_bbox), not a post-filter --
non-intersecting features are never decoded. Introduces
FlatCityBufBindData (bbox + a shared_ptr to the reader, the latter for
attribute-pushdown re-querying in a later commit) and BindCityJSONReadRaw,
a reference-taking refactor of BindCityJSONRead needed so
read_flatcitybuf can keep its own ownership handle on the reader after
bind."
```

---

### Task 10: `COPY TO ... (FORMAT flatcitybuf)` write options — `attr_index`, `branching_factor`, `index_node_size`

**Files:**
- Modify: `src/include/cityjson/copy_function.hpp` (`CityJSONCopyBindData` new fields)
- Modify: `src/cityjson/copy_function.cpp` (`CityJSONCopyToBind` option parsing, call site)
- Modify: `src/include/cityjson/cityjson_writer.hpp`, `src/cityjson/cityjson_writer.cpp`
  (`WriteFlatCityBuf` gains 3 new parameters)

**Interfaces:**
- Produces: `WriteFlatCityBuf(file_path, metadata, feature_objects, feature_order,
  attr_index_columns, branching_factor, index_node_size)` — consumed by Task 11's fixture
  generation and attribute-pushdown tests.

- [ ] **Step 1: Write the failing test**

Create `test/sql/cityjson_fcb_write_options.test`:

```
# name: test/sql/cityjson_fcb_write_options.test
# description: COPY TO flatcitybuf attr_index/branching_factor/index_node_size options
# group: [sql]

require cityjson

statement ok
COPY (SELECT * FROM read_cityjsonseq('test/data/fcb_bbox_attr.city.jsonl'))
TO '__TEST_DIR__/write_options.fcb' (FORMAT flatcitybuf, attr_index 'height,category', branching_factor 4, index_node_size 8);

# round-trips correctly
query I
SELECT COUNT(*) FROM read_flatcitybuf('__TEST_DIR__/write_options.fcb');
----
3

# a requested column that never appears in any feature's attributes is silently
# not indexed rather than an error
statement ok
COPY (SELECT * FROM read_cityjsonseq('test/data/fcb_bbox_attr.city.jsonl'))
TO '__TEST_DIR__/write_options_missing_col.fcb' (FORMAT flatcitybuf, attr_index 'height,does_not_exist');

query I
SELECT COUNT(*) FROM read_flatcitybuf('__TEST_DIR__/write_options_missing_col.fcb');
----
3
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `build/release/test/unittest test/sql/cityjson_fcb_write_options.test` (after a
build, which should still succeed since `attr_index` etc. are simply unrecognized COPY
options at this point — DuckDB's copy option parsing loop in `CityJSONCopyToBind`
currently just silently ignores unknown option names, so confirm the actual failure mode
first):

Run: `cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb 2>&1 | tail -20 && build/release/test/unittest test/sql/cityjson_fcb_write_options.test`
Expected: the COPY statements succeed (options silently ignored today) but produce a
file with NO attribute index — this test doesn't yet assert on index presence (that's
Task 11's job once pushdown can observe it), so this test as written would actually
currently PASS even without the feature. Fix this before moving on: add one more
assertion that only passes once indexing is real, using `flatcitybuf_metadata`'s
existing metadata output won't show index info directly, so instead defer the
"proves indexing actually happened" assertion to Task 11's attribute-query test (which
queries `WHERE height > 15` and can only return correct pushdown-filtered results if the
index exists) — note this explicitly in this test file with a comment so the next task
picks it up, by adding this line right after the `group: [sql]` header:

```
# NOTE: this file only proves attr_index/branching_factor/index_node_size are accepted
# and round-trip correctly. Proving the index is actually queryable is
# cityjson_fcb_attr_query.test's job (next task), since that's the first place a
# WHERE-clause pushdown can observe whether the B+tree index really exists.
```

- [ ] **Step 3: Add the new `CityJSONCopyBindData` fields**

In `src/include/cityjson/copy_function.hpp`, inside `struct CityJSONCopyBindData`, add
(near `is_fcb`):

```cpp
	// FlatCityBuf write-only options (COPY TO ... FORMAT flatcitybuf).
	std::vector<std::string> fcb_attr_index_columns; // parsed from attr_index, empty = none
	std::optional<uint16_t> fcb_branching_factor;
	std::optional<uint16_t> fcb_index_node_size;
```

- [ ] **Step 4: Parse the new options**

In `src/cityjson/copy_function.cpp`'s `CityJSONCopyToBind`, inside the `for (auto &option
: input.info.options)` loop, add branches alongside the existing `transform_scale`/
`transform_translate` ones:

```cpp
		} else if (loption == "attr_index") {
			auto columns_str = val.ToString();
			std::vector<std::string> columns;
			size_t start = 0;
			while (start <= columns_str.size()) {
				auto comma = columns_str.find(',', start);
				auto piece = columns_str.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
				// Trim surrounding whitespace so "a, b" and "a,b" behave the same.
				size_t first = piece.find_first_not_of(" \t");
				size_t last = piece.find_last_not_of(" \t");
				if (first != std::string::npos) {
					columns.push_back(piece.substr(first, last - first + 1));
				}
				if (comma == std::string::npos) {
					break;
				}
				start = comma + 1;
			}
			bind_data->fcb_attr_index_columns = columns;
		} else if (loption == "branching_factor") {
			bind_data->fcb_branching_factor = static_cast<uint16_t>(val.GetValue<int64_t>());
		} else if (loption == "index_node_size") {
			bind_data->fcb_index_node_size = static_cast<uint16_t>(val.GetValue<int64_t>());
		}
```

- [ ] **Step 5: Thread the options through to `WriteFlatCityBuf`**

In `src/include/cityjson/cityjson_writer.hpp`, change the `WriteFlatCityBuf` declaration
to:

```cpp
	static void WriteFlatCityBuf(const std::string &file_path, const CityJSONWriteMetadata &metadata,
	                             std::map<std::string, std::vector<std::pair<std::string, json>>> feature_objects,
	                             const std::vector<std::string> &feature_order,
	                             const std::vector<std::string> &attr_index_columns = {},
	                             std::optional<uint16_t> branching_factor = std::nullopt,
	                             std::optional<uint16_t> index_node_size = std::nullopt);
```

In `src/cityjson/cityjson_writer.cpp`, change the function signature to match, and
replace the `fcb::FcbWriterOptions options;` line (added in Task 7) with:

```cpp
	fcb::FcbWriterOptions options;
	if (index_node_size.has_value()) {
		options.index_node_size = index_node_size.value();
	}
	for (const auto &col_name : attr_index_columns) {
		if (attr_schema.count(col_name) == 0) {
			// Requested column never appeared in any feature's attributes -- nothing
			// to index, not an error.
			continue;
		}
		options.attribute_indices.emplace_back(col_name, branching_factor);
	}
```

- [ ] **Step 6: Update the call site in `copy_function.cpp`**

Find the existing `CityJSONWriter::WriteFlatCityBuf(output_path, write_meta,
gstate.feature_objects, gstate.feature_order);` call (in `CityJSONCopyToFinalize`) and
change it to:

```cpp
		CityJSONWriter::WriteFlatCityBuf(output_path, write_meta, gstate.feature_objects, gstate.feature_order,
		                                 bind_data.fcb_attr_index_columns, bind_data.fcb_branching_factor,
		                                 bind_data.fcb_index_node_size);
```

- [ ] **Step 7: Build and run**

Run: `cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb 2>&1 | tail -60`
Expected: clean build.

Run: `build/release/test/unittest test/sql/cityjson_fcb_write_options.test`
Expected: pass.

- [ ] **Step 8: Commit**

```bash
git add src/include/cityjson/copy_function.hpp src/cityjson/copy_function.cpp src/include/cityjson/cityjson_writer.hpp src/cityjson/cityjson_writer.cpp test/sql/cityjson_fcb_write_options.test
git commit -m "feat(fcb): COPY TO flatcitybuf attr_index/branching_factor/index_node_size options

attr_index takes a comma-separated column list; branching_factor applies
to every column in it; index_node_size tunes the R-tree. A requested
attr_index column that never appears in any feature is silently not
indexed rather than an error -- nothing to index."
```

---

### Task 11: Attribute-query `WHERE` pushdown

**Files:**
- Modify: `src/cityjson/flatcitybuf_table_function.cpp`
  (`FlatCityBufPushdownComplexFilter`, `RegisterFlatCityBufTableFunction`)

**Interfaces:**
- Consumes: `FlatCityBufReader::SetAttrQueryFilter`/`IndexedAttributeColumns`/
  `FindColumn` (Task 5), `FlatCityBufBindData::reader` (Task 9),
  `fcb::AttrQuery`/`Operator`/`KeyValue` (upstream API).

- [ ] **Step 1: Write the failing test**

Create `test/sql/cityjson_fcb_attr_query.test`:

```
# name: test/sql/cityjson_fcb_attr_query.test
# description: attribute-query pushdown on read_flatcitybuf via WHERE on indexed columns
# group: [sql]

require cityjson

statement ok
COPY (SELECT * FROM read_cityjsonseq('test/data/fcb_bbox_attr.city.jsonl'))
TO '__TEST_DIR__/attr_query.fcb' (FORMAT flatcitybuf, attr_index 'height,category', branching_factor 4);

# indexed numeric column, pushed down
query I
SELECT id FROM read_flatcitybuf('__TEST_DIR__/attr_query.fcb') WHERE height > 15 ORDER BY id;
----
f2
f3

# indexed string column, pushed down
query I
SELECT id FROM read_flatcitybuf('__TEST_DIR__/attr_query.fcb') WHERE category = 'A' ORDER BY id;
----
f1
f3

# a column with NO index still filters correctly (falls back to normal DuckDB filtering)
statement ok
COPY (SELECT * FROM read_cityjsonseq('test/data/fcb_bbox_attr.city.jsonl'))
TO '__TEST_DIR__/attr_query_noindex.fcb' (FORMAT flatcitybuf);

query I
SELECT id FROM read_flatcitybuf('__TEST_DIR__/attr_query_noindex.fcb') WHERE height > 15 ORDER BY id;
----
f2
f3

# combined bbox + attribute WHERE: only f2 is in both the bbox and height > 15
query I
SELECT id FROM read_flatcitybuf('__TEST_DIR__/attr_query.fcb', min_x := 50, min_y := 50, max_x := 150, max_y := 150) WHERE height > 15 ORDER BY id;
----
f2
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb 2>&1 | tail -20 && build/release/test/unittest test/sql/cityjson_fcb_attr_query.test`
Expected: the queries currently still return the CORRECT rows (DuckDB's own
post-scan filtering already handles `WHERE height > 15` correctly today, pushdown or
not — pushdown is a performance optimization, not a correctness requirement this test
can observe by row content alone). This is expected: **row-level correctness passes
today already**; what's NOT yet true is that it's pushed down. Since sqllogictest can't
directly assert "fewer bytes were read," treat this test as already green for
correctness and use it as the regression gate for the pushdown implementation (which
must not break these already-correct results) — the actual "did pushdown happen" signal
for a human reviewing this task is a manual check in Step 5, not this file.

- [ ] **Step 3: Implement `FlatCityBufPushdownComplexFilter`**

In `src/cityjson/flatcitybuf_table_function.cpp`, add (needs
`#include "duckdb/planner/expression/bound_columnref_expression.hpp"`,
`#include "duckdb/planner/expression/bound_comparison_expression.hpp"`,
`#include "duckdb/planner/expression/bound_constant_expression.hpp"`,
`#include "duckdb/planner/operator/logical_get.hpp"`, and `#include <fcb/stree.hpp>`):

```cpp
namespace {

std::optional<fcb::Operator> ToFcbOperator(ExpressionType type, bool column_on_right) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		return fcb::Operator::Eq;
	case ExpressionType::COMPARE_NOTEQUAL:
		return fcb::Operator::Ne;
	case ExpressionType::COMPARE_GREATERTHAN:
		return column_on_right ? fcb::Operator::Lt : fcb::Operator::Gt;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return column_on_right ? fcb::Operator::Le : fcb::Operator::Ge;
	case ExpressionType::COMPARE_LESSTHAN:
		return column_on_right ? fcb::Operator::Gt : fcb::Operator::Lt;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return column_on_right ? fcb::Operator::Ge : fcb::Operator::Le;
	default:
		return std::nullopt;
	}
}

// Types a DuckDB constant against the column's ON-DISK type, mirroring
// upstream's query_attributes.cpp make_value(). Getting this wrong doesn't
// throw -- the bytes are reinterpreted -- so every branch pulls the constant
// via the DuckDB getter that matches the on-disk type's own category.
std::optional<fcb::KeyValue> BuildKeyValue(const fcb::ColumnInfo &col, const Value &constant) {
	switch (static_cast<::ColumnType>(col.type)) {
	case ::ColumnType::Byte:
		return fcb::KeyValue::from_i8(static_cast<int8_t>(constant.GetValue<int64_t>()));
	case ::ColumnType::UByte:
		return fcb::KeyValue::from_u8(static_cast<uint8_t>(constant.GetValue<int64_t>()));
	case ::ColumnType::Bool:
		return fcb::KeyValue::from_bool(constant.GetValue<bool>());
	case ::ColumnType::Short:
		return fcb::KeyValue::from_i16(static_cast<int16_t>(constant.GetValue<int64_t>()));
	case ::ColumnType::UShort:
		return fcb::KeyValue::from_u16(static_cast<uint16_t>(constant.GetValue<int64_t>()));
	case ::ColumnType::Int:
		return fcb::KeyValue::from_i32(static_cast<int32_t>(constant.GetValue<int64_t>()));
	case ::ColumnType::UInt:
		return fcb::KeyValue::from_u32(static_cast<uint32_t>(constant.GetValue<int64_t>()));
	case ::ColumnType::Long:
		return fcb::KeyValue::from_i64(constant.GetValue<int64_t>());
	case ::ColumnType::ULong:
		return fcb::KeyValue::from_u64(static_cast<uint64_t>(constant.GetValue<int64_t>()));
	case ::ColumnType::Float:
		return fcb::KeyValue::from_f32(static_cast<float>(constant.GetValue<double>()));
	case ::ColumnType::Double:
		return fcb::KeyValue::from_f64(constant.GetValue<double>());
	case ::ColumnType::String:
		return fcb::KeyValue::from_string(fcb::KeyKind::String50, constant.ToString());
	case ::ColumnType::Json:
	case ::ColumnType::Binary:
		return fcb::KeyValue::from_string(fcb::KeyKind::String100, constant.ToString());
	default:
		return std::nullopt; // unsupported column type for pushdown -- leave unpushed
	}
}

} // namespace

void FlatCityBufPushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                      vector<unique_ptr<Expression>> &filters) {
	auto &bind_data = bind_data_p->Cast<FlatCityBufBindData>();
	if (!bind_data.reader) {
		return;
	}
	auto indexed_columns = bind_data.reader->IndexedAttributeColumns();
	if (indexed_columns.empty()) {
		return;
	}

	fcb::AttrQuery conditions;

	for (auto it = filters.begin(); it != filters.end();) {
		auto &expr = *it;
		bool consumed = false;

		if (expr->type >= ExpressionType::COMPARE_EQUAL && expr->type <= ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
			auto &comp = expr->Cast<BoundComparisonExpression>();

			BoundColumnRefExpression *col_ref = nullptr;
			BoundConstantExpression *constant = nullptr;
			bool column_on_right = false;

			if (comp.left->type == ExpressionType::BOUND_COLUMN_REF &&
			    comp.right->type == ExpressionType::VALUE_CONSTANT) {
				col_ref = &comp.left->Cast<BoundColumnRefExpression>();
				constant = &comp.right->Cast<BoundConstantExpression>();
			} else if (comp.right->type == ExpressionType::BOUND_COLUMN_REF &&
			           comp.left->type == ExpressionType::VALUE_CONSTANT) {
				col_ref = &comp.right->Cast<BoundColumnRefExpression>();
				constant = &comp.left->Cast<BoundConstantExpression>();
				column_on_right = true;
			}

			if (col_ref && constant && col_ref->binding.table_index == get.table_index &&
			    col_ref->binding.column_index < get.GetColumnIds().size()) {
				idx_t schema_idx = get.GetColumnIds()[col_ref->binding.column_index].GetPrimaryIndex();
				if (schema_idx < bind_data.columns.size()) {
					const auto &column_name = bind_data.columns[schema_idx].name;
					bool is_indexed =
					    std::find(indexed_columns.begin(), indexed_columns.end(), column_name) != indexed_columns.end();
					if (is_indexed) {
						auto op = ToFcbOperator(expr->type, column_on_right);
						auto col_info = bind_data.reader->FindColumn(column_name);
						if (op.has_value() && col_info.has_value()) {
							auto key_value = BuildKeyValue(col_info.value(), constant->value);
							if (key_value.has_value()) {
								conditions.push_back({column_name, op.value(), key_value.value()});
								consumed = true;
							}
						}
					}
				}
			}
		}

		if (consumed) {
			it = filters.erase(it);
		} else {
			++it;
		}
	}

	if (conditions.empty()) {
		return;
	}

	bind_data.reader->SetAttrQueryFilter(conditions, false);
	try {
		bind_data.chunks = bind_data.reader->ReadAllChunks();
	} catch (const CityJSONError &e) {
		throw BinderException("Failed to re-read FlatCityBuf with pushed-down attribute filter: " +
		                      std::string(e.what()));
	}
	bind_data.scan_plan = bind_data.chunks.BuildScanPlan();
}
```

- [ ] **Step 4: Register the pushdown callback**

In `RegisterFlatCityBufTableFunction`, add:

```cpp
	func.pushdown_complex_filter = FlatCityBufPushdownComplexFilter;
```

- [ ] **Step 5: Build, run, and manually confirm the pushdown actually fires**

Run: `cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb 2>&1 | tail -60`
Expected: clean build.

Run: `build/release/test/unittest test/sql/cityjson_fcb_attr_query.test`
Expected: all pass.

Manually confirm the FIRST query (`WHERE height > 15` against the indexed file) is
actually served via `select_attr` rather than a coincidental full-scan-then-filter, by
temporarily adding a `fprintf(stderr, ...)` inside `FlatCityBufPushdownComplexFilter`
right after `conditions.push_back(...)` printing the column name and operator, rebuilding,
and re-running the query interactively:
```sh
./build/release/duckdb -unsigned -c "LOAD 'build/release/extension/cityjson/cityjson.duckdb_extension'; SELECT id FROM read_flatcitybuf('__TEST_DIR__/attr_query.fcb') WHERE height > 15;" 2>&1 | grep -i "pushdown\|height"
```
(Adjust `__TEST_DIR__` to wherever the sqllogictest runner actually materializes it, or
just regenerate the fixture at a fixed path via the `COPY TO` statement directly first.)
Expected: the debug line prints, confirming the condition was recognized and consumed.
Remove the temporary `fprintf` once confirmed and rebuild clean.

- [ ] **Step 6: Full regression**

Run: `make test 2>&1 | tail -80`
Expected: no regressions anywhere in the suite.

- [ ] **Step 7: Commit**

```bash
git add src/cityjson/flatcitybuf_table_function.cpp test/sql/cityjson_fcb_attr_query.test
git commit -m "feat(fcb): attribute-query WHERE pushdown on read_flatcitybuf

Simple =,!=,>,>=,<,<= comparisons against B+tree-indexed columns are
translated into an fcb::AttrQuery and re-run via select_attr during
pushdown_complex_filter, which fires after Bind (once DuckDB knows the
WHERE clause) -- FlatCityBufBindData keeps a live reader reference
specifically so this can re-materialize chunks/scan_plan at that point.
Unindexed columns and unsupported operators are left for DuckDB's normal
post-scan filtering, unchanged from today's behavior."
```

---

### Task 12: HTTP transport test

**Files:**
- Create: `test/sql/cityjson_fcb_http.test`
- Check first: `grep -rln "python3 -m http.server\|http://localhost\|require httpfs" test/sql/*.test`
  for this repo's existing pattern (if any) for serving a local file over HTTP in a
  sqllogictest — reuse it if found rather than inventing a new one.

**Interfaces:**
- Consumes: `DuckDBRangeReader` (Task 4), bbox query (Task 9).

- [ ] **Step 1: Check for an existing local-HTTP-serving test pattern**

Run: `grep -rln "http.server\|require httpfs\|localhost" test/sql/*.test`
Read whatever matches to find this repo's established pattern for standing up a local
HTTP server inside a test (sqllogictest doesn't run arbitrary shell commands directly —
check whether there's a `test/python/` helper, a Makefile target that starts a fixture
server before `make test`, or whether remote-file tests in this repo are actually
skipped/marked `require-env` and only run in CI with network access). If no pattern
exists, this task's remaining steps assume the simplest viable option: a `require`
tag gating the test to only run when a helper server is already up (matching whatever
this repo's CI does for `httpfs`-dependent tests today, if anything) — read
`test/sql/*.test` for any existing httpfs/S3 test first (e.g. grep for `require httpfs`)
to confirm whether this repo already has ANY passing remote-file test to model this on.

- [ ] **Step 2: Write the test using whatever pattern Step 1 found**

If Step 1 finds an existing local-server pattern, mirror it exactly, adapting only the
target file and function names, asserting:
```
query I
SELECT COUNT(*) FROM read_flatcitybuf('http://localhost:<port>/fcb_bbox_attr.fcb', min_x := 50, min_y := 50, max_x := 150, max_y := 150);
----
1
```
(serving `test/data/fcb_bbox_attr.fcb`, produced by Task 1 or regenerated fresh via
`COPY TO` in this test's setup).

If Step 1 finds NO existing pattern for local-HTTP sqllogictests in this repo, do not
invent new test infrastructure here — instead write a narrower, manually-run smoke check
documented as a comment at the top of a skipped test file (`# group: [sql]` with a
`require skip_http_smoke` guard that's never satisfied in CI, or simply do not create
this test file at all and instead run the check once by hand via Bash during this task,
recording the exact commands and their output in the commit message as evidence, since
this repo has no established way to assert it automatically yet). Prefer the manual
verification path over inventing a new automated-test mechanism this repo doesn't
already have, per "follow existing patterns" — flag this gap rather than papering over
it.

- [ ] **Step 3: Manually verify (regardless of which path Step 2 took)**

Run:
```sh
cd test/data && python3 -m http.server 8931 &
SERVER_PID=$!
sleep 1
../../build/release/duckdb -unsigned -c "SELECT COUNT(*) FROM read_flatcitybuf('http://localhost:8931/fcb_bbox_attr.fcb', min_x := 50, min_y := 50, max_x := 150, max_y := 150);"
kill $SERVER_PID
```
Expected: `1` (only `f2` intersects that bbox).

- [ ] **Step 4: Commit**

```bash
git add test/sql/cityjson_fcb_http.test  # only if Step 2 produced a real committed test
git commit -m "test(fcb): verify read_flatcitybuf over HTTP via DuckDBRangeReader

Confirms remote .fcb reads (through httpfs, same as read_cityjson/
read_cityjsonseq) work end-to-end, combined with a bbox filter."
```
(If Step 2 concluded no automated test file should be added, adjust this commit to
whatever was actually changed — e.g. a comment-only note — and say so plainly rather
than claiming a test was added when it wasn't.)

---

### Task 13: Update `README.md` (and `AGENTS.md` if present)

**Files:**
- Modify: `README.md`
- Modify: `AGENTS.md` if it exists and mirrors `CLAUDE.md`/this repo's own
  `README.md` FCB sections (check with `ls AGENTS.md` first)

**Interfaces:** none (docs only).

- [ ] **Step 1: Update the FlatCityBuf sections**

Read `README.md` in full first. Update:
- The `read_flatcitybuf(path [, lod => 'X.Y'])` section: add `min_x`/`min_y`/`max_x`/
  `max_y` to the parameters list, with the same example style as the existing `lod`
  example, e.g.:
  ```sql
  -- Bbox query (real R-tree-level skip, not a post-filter)
  SELECT id FROM read_flatcitybuf('buildings.fcb', min_x := 84000, min_y := 445000, max_x := 85000, max_y := 446000);
  ```
- The `COPY TO` Options table: add rows for `attr_index`, `branching_factor`,
  `index_node_size`, matching the existing table's column format.
- The "Optional: FlatCityBuf Support" build section: replace the
  `EXT_FLAGS="-DCITYJSON_ENABLE_FCB=ON" GEN=ninja make` instructions (FCB is now
  default-on, so no special flag is needed) with a note that it's vendored via a vcpkg
  overlay port (`vcpkg_ports/flatcitybuf/`) and requires no manual flag; mention
  `-DCITYJSON_ENABLE_FCB=OFF` as the way to disable it if needed.
- Remove any mention of platform restrictions (musl, macOS x86_64, glibc version) —
  none apply anymore.
- The "Remote File Support" section: note that FlatCityBuf's remote reads go through the
  same `httpfs`-backed path as `read_cityjson`/`read_cityjsonseq`.

- [ ] **Step 2: Check for and update `AGENTS.md`**

Run: `ls AGENTS.md 2>&1`. If it exists and has a FlatCityBuf section mirroring
`README.md`'s (per this repo's own convention of keeping `CLAUDE.md`/`AGENTS.md` in
sync — check `CLAUDE.md` itself for whether it references FCB build instructions too),
apply the same edits there.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs(fcb): document bbox params, new COPY TO options, updated build instructions

Reflects the native C++ flatcitybuf backend: no more platform
restrictions, no special build flag needed (default-on), vendored via a
vcpkg overlay port instead of a prebuilt-binary download."
```

---

### Task 14: Fable implementation-review checkpoint

Per user instruction, get an advisory pass from Fable on the actual finished
implementation (not just the design, which it already reviewed) before the final code
review.

- [ ] **Step 1: Dispatch the advisory review**

Use the `Agent` tool with `model: "fable"`, `subagent_type: "Plan"` (matches the
brainstorming-phase advisory call), prompt summarizing: what was built (link/reference
this plan's task list), and ask specifically whether (a) the
`BindCityJSONReadRaw`/`FlatCityBufBindData::reader` shared-ownership approach for
re-querying during `pushdown_complex_filter` has any correctness hazard Fable can see
(e.g. around `Copy()` semantics or thread-safety of re-reading during optimization), (b)
whether the "bbox present -> skip select_attr's real index traversal, fall back to a
per-row post-filter for attribute conditions" tradeoff (Task 5's
`MatchesAttrQueryPostFilter`) is still the right call now that it's actually implemented,
and (c) anything else that looks off. Keep the prompt self-contained (Fable's earlier
advisory call is a different session with no memory of this one).

- [ ] **Step 2: Address anything actionable**

Any concrete, actionable finding gets fixed with its own small commit (write/adjust a
test first if the finding describes a behavior gap, per this plan's TDD discipline).
Anything that's a judgment call only the user could settle, note and move on rather than
blocking (per user's explicit "defer that task and work on later" instruction).

- [ ] **Step 3: Commit any fixes**

```bash
git add -A
git commit -m "fix(fcb): address Fable's implementation-review findings

<one line per concrete fix actually made>"
```
(Skip this commit entirely if Step 2 found nothing actionable.)

---

### Task 15: Codex review pass

- [ ] **Step 1: Run codex review**

Run: `codex exec --model gpt-5.6-sol --sandbox read-only review 2>&1 | tee /tmp/claude-1020/-data2-hideba-cityparquet-paper-duckdb-cityjson/0453cca0-c252-4df5-94d4-6ab1d07d6902/scratchpad/codex_review.txt`

(If `codex exec review` doesn't accept `--model`/`--sandbox` the way expected, fall back
to `codex exec --model gpt-5.6-sol "Review the diff between origin/develop and HEAD in
this repo for correctness, memory-safety, and DuckDB-extension-convention issues,
focusing on src/cityjson/flatcitybuf_reader.cpp, src/cityjson/flatcitybuf_table_function.cpp,
src/cityjson/cityjson_writer.cpp, src/cityjson/duckdb_fs_range_reader.cpp, and
src/cityjson/bind_function.cpp"` instead — check `codex exec review --help` first to
confirm the actual accepted flags before running either form.)

- [ ] **Step 2: Triage findings**

Read the review output. For each finding: verify it against the actual code (per this
plan's evidence-before-assertions discipline) before acting — a review comment is a
claim to check, not an instruction to blindly implement.

- [ ] **Step 3: Fix confirmed findings (TDD, one at a time)**

For each confirmed finding: write/adjust a failing test that demonstrates it (if the
finding describes a behavior bug, not a style nit), confirm it fails, fix, confirm it
passes, commit. Style-only nits with no behavior impact can be fixed directly without a
new test, noting so in the commit message.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "fix(fcb): address codex review findings

<one line per fix, or note if no actionable findings>"
```

---

### Task 16: Final full verification

- [ ] **Step 1: Full clean rebuild**

Run: `rm -rf build/release && GEN=ninja make 2>&1 | tail -100`
Expected: succeeds from scratch, including vcpkg resolving `flatcitybuf` fresh.

- [ ] **Step 2: Full test suite**

Run: `make test 2>&1 | tail -150`
Expected: all tests pass, including every new `test/sql/cityjson_fcb_*.test` file added
in this plan and the pre-existing `test/sql/cityjson_e2e_fcb.test`.

- [ ] **Step 3: Final status check**

Run: `git log --oneline origin/develop..HEAD` and `git status`
Expected: a clean sequence of the commits from Tasks 1-15 (plus any review-fix commits),
working tree clean.

- [ ] **Step 4: Report**

Summarize to the user: what shipped, any deferred/judgment-call items noted along the
way (Task 14/15's "note and move on" cases), and the current state of the branch —
no commit needed for this step, it's a status report.
