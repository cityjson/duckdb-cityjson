# OBJ Reader Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `read_obj(path, lod := …)` reads a Wavefront OBJ (+ `.mtl`) into the CityParquet object-table schema, with `obj_materials`, `obj_textures` and `obj_metadata` companions.

**Architecture:** A new `OBJReader : CityJSONReader` parses the OBJ through tinyobjloader's callback API into `CityJSONFeature` records (one per `o` group, own vertex pool), and the existing generic bind (`BindCityJSONRead`) and scan (`CityJSONScan`) turn those into WKB, `geometry_properties`, `bbox` and appearance columns unchanged. The sidecar table functions get a `TableFunctionInfo` carrying a reader-opener so the same bind/scan serves CityJSON and OBJ inputs.

**Tech Stack:** C++20 DuckDB extension, vcpkg (`tinyobjloader` with feature `double`, header-only use), nlohmann-json, sqllogictest.

**Spec:** `docs/superpowers/specs/2026-09-02-obj-gltf-interchange-design.md` — Part 1 and *Dependencies*.

## Global Constraints

- vcpkg baseline stays `84bab45d415d22042bd0b9081aea57f362da3f35`; new ports: `tinyobjloader` (feature `double`), `tinygltf` pinned `2.9.7` via `overrides`, `earcut-hpp`. (All three are declared in Task 1 so the manifest changes once.)
- All file bytes come through `duckdb::FileSystem` (`json_utils::ReadFileContent`), never `std::ifstream`.
- `read_obj` does **not** take `crs`; only `obj_metadata` does.
- British English in prose and docs. Document the present, never the past.
- sqllogictest header is exactly three lines (`# name:`, `# description:` single line, `# group:`).
- Build: `GEN=ninja make` once, then `just rebuild` before every `make test` (the test binary is not rebuilt by `make test`). Run one file with `./build/release/test/unittest "test/sql/<name>.test"`.
- Before each commit run `make format-fix` if `clang-format` 11.0.1 is installed (`just hooks` runs it automatically); tidy runs in the hook.
- Breaking changes are welcome; no shims.

---

## File structure

| File | Responsibility |
| --- | --- |
| `vcpkg.json` | Declare the three ports and the tinygltf pin |
| `CMakeLists.txt` | Include dirs for the three header-only uses; new sources |
| `src/cityjson/third_party_impl.cpp` | The one TU that defines `TINYOBJLOADER_IMPLEMENTATION` (and, in the glTF plan, `TINYGLTF_IMPLEMENTATION`) |
| `src/include/cityjson/obj_reader.hpp`, `src/cityjson/obj_reader.cpp` | `OBJReadOptions`, `OBJReader`: parse OBJ+MTL into `CityJSONFeature`s and a `CityJSON` header |
| `src/include/cityjson/obj_table_function.hpp`, `src/cityjson/obj_table_function.cpp` | `read_obj`, `obj_metadata` binds and registration |
| `src/include/cityjson/appearance_table_function.hpp`, `src/cityjson/appearance_table_function.cpp` | Refactor: reader-opener via `TableFunctionInfo`; registers `obj_materials` / `obj_textures` |
| `src/cityjson_extension.cpp` | Registration calls |
| `test/data/obj/cube.obj`, `cube.mtl`, `open_roof.obj`, `continuation.obj` | Hand-written fixtures |
| `test/sql/read_obj.test`, `test/sql/obj_sidecars.test`, `test/sql/obj_metadata.test` | Behaviour pins |
| `docs/FUNCTIONS.md`, `docs/DESIGN_DOC.md`, `docs/TRAPS.md` | Documentation |

---

### Task 1: Dependencies and build wiring

**Files:**
- Modify: `vcpkg.json`
- Modify: `CMakeLists.txt` (after `find_package(nlohmann_json REQUIRED)` at line 10; the `EXTENSION_SOURCES` list; the link block near line 266)
- Create: `src/cityjson/third_party_impl.cpp`

**Interfaces:**
- Produces: `#include <tiny_obj_loader.h>` compiles in any TU; `tinyobj::` symbols link (defined once in `third_party_impl.cpp`). `<mapbox/earcut.hpp>` and `<tiny_gltf.h>` are on the include path for the later plans.

- [ ] **Step 1: Declare the ports**

Replace the `"dependencies"` array in `vcpkg.json` and add the override:

```json
{
        "builtin-baseline": "84bab45d415d22042bd0b9081aea57f362da3f35",
        "dependencies": [
                "nlohmann-json",
                "flatbuffers",
                "flatcitybuf",
                { "name": "tinyobjloader", "features": ["double"] },
                "tinygltf",
                "earcut-hpp"
        ],
        "overrides": [
                { "name": "tinygltf", "version": "2.9.7" }
        ],
        "vcpkg-configuration": { … unchanged … }
}
```

Keep the existing `vcpkg-configuration` block byte-for-byte.

- [ ] **Step 2: Wire the include directories in CMake**

Immediately after `find_package(nlohmann_json REQUIRED)` add:

```cmake
# Three header-only uses. tinyobjloader's port also builds a static library, but the
# header carries the implementation behind TINYOBJLOADER_IMPLEMENTATION, and compiling
# it into our own objects is what keeps the wasm side-module link free of a second
# archive to hand over (see the flatcitybuf block below for what that costs). The port
# has already patched the header for double precision (feature `double`).
find_path(TINYOBJLOADER_INCLUDE_DIR tiny_obj_loader.h REQUIRED)
find_path(TINYGLTF_INCLUDE_DIR tiny_gltf.h REQUIRED)
find_path(EARCUT_INCLUDE_DIR mapbox/earcut.hpp REQUIRED)
include_directories(${TINYOBJLOADER_INCLUDE_DIR} ${TINYGLTF_INCLUDE_DIR} ${EARCUT_INCLUDE_DIR})
```

Add to `EXTENSION_SOURCES`, after the `# Reader layer` group:

```cmake
    # Third-party single-header implementations (one TU, see the file)
    src/cityjson/third_party_impl.cpp
```

- [ ] **Step 3: The implementation TU**

```cpp
// src/cityjson/third_party_impl.cpp
//
// The single translation unit that instantiates the header-only third-party
// libraries. Every other file includes the headers without the *_IMPLEMENTATION
// macro; defining one of them twice is an ODR violation that only shows up at link.
#define TINYOBJLOADER_IMPLEMENTATION
#include <tiny_obj_loader.h>
```

- [ ] **Step 4: Configure and build**

Run: `GEN=ninja make` (first configure resolves the new ports; expect several minutes for vcpkg) then `just rebuild`.
Expected: builds clean. If `find_path` fails, inspect `build/release/vcpkg_installed/<triplet>/include/` — the header names are `tiny_obj_loader.h`, `tiny_gltf.h`, `mapbox/earcut.hpp`.

- [ ] **Step 5: Smoke-test the symbol**

Temporarily add to `third_party_impl.cpp`:

```cpp
namespace { [[maybe_unused]] void Probe() { tinyobj::callback_t cb; (void)cb; } }
```

Run `just rebuild`; expected: compiles. Remove the probe.

- [ ] **Step 6: Commit**

```bash
git add vcpkg.json CMakeLists.txt src/cityjson/third_party_impl.cpp
git commit -m "build: add tinyobjloader, tinygltf (pinned 2.9.7) and earcut-hpp"
```

---

### Task 2: Fixtures

**Files:**
- Create: `test/data/obj/cube.obj`, `test/data/obj/cube.mtl`, `test/data/obj/open_roof.obj`, `test/data/obj/continuation.obj`
- Modify: `test/data/README.md` (append a section)

**Interfaces:**
- Produces: fixtures every later task asserts against. The expected values below are derived by hand from these files and are the oracle; do not regenerate them from the reader.

- [ ] **Step 1: `cube.obj`**

```
# Hand-written fixture: a closed unit cube and an open slab.
# Exercises: mtllib, o, g, usemtl (semantic-surface names and a plain material),
# v with a 4th w component, vt with v/vt faces, negative (relative) indices,
# a comment line and a blank line.
mtllib cube.mtl

o cube
v 0 0 0
v 1 0 0
v 1 1 0
v 0 1 0
v 0 0 1 1.0
v 1 0 1
v 1 1 1
v 0 1 1
vt 0 0
vt 1 0
vt 1 1
vt 0 1
g GroundSurface
usemtl GroundSurface
f 1 4 3 2
g WallSurface
usemtl brick
f 1/1 2/2 6/3 5/4
f 2/1 3/2 7/3 6/4
f 3/1 4/2 8/3 7/4
f 4/1 1/2 5/3 8/4
g RoofSurface
usemtl RoofSurface
f -4 -3 -2 -1
o slab
v 10 0 0
v 12 0 0
v 12 2 0
v 10 2 0
f 9 10 11 12
```

Hand-derived facts (the oracle):
- `cube`: 6 faces, every edge used by exactly two faces → closed → `Solid`, `shells` `[[6]]`. Surfaces in first-use order `GroundSurface`(0), `WallSurface`(1), `RoofSurface`(2); `face_semantics` `[0,1,1,1,1,2]`. Materials: `GroundSurface`(0), `brick`(1), `RoofSurface`(2) in MTL order; `material` values `[0,1,1,1,1,2]`. Only `brick` has `map_Kd` → texture id 0; walls carry `vt`, so texture rings are `[0, 0,1,2,3]` locally and `[0,[0,0],[1,0],[1,1],[0,1]]` in sidecar form; ground and roof rings are `[null]`. The roof face `-4 -3 -2 -1` resolves to vertices 5,6,7,8. `bbox` `(0,0,0)-(1,1,1)`.
- `slab`: 1 face, open → `MultiSurface`; no surface (no `g`/`usemtl` after `o slab`? — **no**: OBJ state persists across `o`, so the slab's face still has `usemtl RoofSurface` and `g RoofSurface` in force → surface `RoofSurface`, material 2, no texture). `bbox` `(10,0,0)-(12,2,0)`. This is deliberate: it pins that state persists across `o`, which is how cjio-written files (one `o` per geometry) behave.

- [ ] **Step 2: `cube.mtl`**

```
# Hand-written fixture for cube.obj.
newmtl GroundSurface
Kd 0.3 0.3 0.3

newmtl brick
Kd 0.7 0.3 0.2
Ks 0.1 0.1 0.1
d 0.9
Ns 250
map_Kd brick.png

newmtl RoofSurface
Kd 0.9 0.06 0.09
```

Facts: `brick` → `transparency` 0.1 (= 1 − d), `shininess` 0.25 (= 250/1000), `specularColor` `[0.1,0.1,0.1]`. `GroundSurface` → `transparency` 0.0, `shininess` 0.001 (tinyobjloader's MTL default `Ns` is 1). Textures: one row, `id` 0, `image_uri` `brick.png`, `image_type` `PNG`, `wrapMode` `wrap`, `textureType` `unknown`.

- [ ] **Step 3: `open_roof.obj`** (no mtllib, no `o`; a single open gable of two faces)

```
# Hand-written fixture: one gable of two faces, no `o` line, no materials.
v 0 0 0
v 4 0 0
v 4 0 3
v 0 0 3
v 0 2 4
v 4 2 4
f 1 2 3 4
f 4 3 6 5
```

Facts: one object named after the file stem `open_roof`; open → `MultiSurface`; no semantics, no material, no texture; `bbox` `(0,0,0)-(4,2,4)`.

- [ ] **Step 4: `continuation.obj`** (the documented unsupported feature)

```
v 0 0 0
v 1 0 0
v 1 1 \
0
f 1 2 3
```

- [ ] **Step 5: README section** — append to `test/data/README.md`:

```markdown
## obj/

Hand-written Wavefront OBJ fixtures for `read_obj`. `cube.obj` + `cube.mtl` is a closed
unit cube (one `o`, three `g`/`usemtl` semantic groups, one textured plain material,
one face with negative indices, one `v` with a `w` component) followed by an open slab
that inherits the cube's last `g`/`usemtl` state -- OBJ state persists across `o`, and
the fixture pins that. `open_roof.obj` has no `o` line and no materials. `continuation.obj`
ends a line with a backslash, which the reader refuses. The expected values in the tests
are derived by hand from these files, not from the reader.
```

- [ ] **Step 6: Commit**

```bash
git add test/data/obj test/data/README.md
git commit -m "test(obj): hand-written OBJ fixtures"
```

---

### Task 3: `OBJReader` parse and `read_obj` schema

**Files:**
- Create: `src/include/cityjson/obj_reader.hpp`, `src/cityjson/obj_reader.cpp`
- Create: `src/include/cityjson/obj_table_function.hpp`, `src/cityjson/obj_table_function.cpp`
- Modify: `CMakeLists.txt` (sources), `src/cityjson_extension.cpp` (registration)
- Test: `test/sql/read_obj.test`

**Interfaces:**
- Produces:
  ```cpp
  struct OBJReadOptions { std::string lod; std::string object_type = "Building";
                          std::string geometry_type = "auto"; std::optional<std::string> crs; };
  class OBJReader : public CityJSONReader {  // ctor (ClientContext&, std::string file_path, OBJReadOptions)
      // Name, ReadMetadata, ReadAllChunks, ReadNFeatures, Columns, CountCityObjects, CountFeatures
      static bool IsSemanticSurfaceType(const std::string &name); };
  OBJReadOptions ParseOBJReadOptions(const TableFunctionBindInput &input, const std::string &function_name);
  void RegisterOBJTableFunctions(ExtensionLoader &loader);   // read_obj (+ obj_metadata in Task 7)
  ```
- Consumes: `CityJSONReader` (reader.hpp), `BindCityJSONRead` and the `CityJSON*` scan callbacks (table_function.hpp), `LODTableUtils::NormalizeLOD`, `GetDefinedColumns`, `CityObjectUtils::InferGeometryColumns`, `LODTableUtils::GetTrailingColumns`, `CityJSONFeatureChunk::CreateChunks`, `json_utils::ReadFileContent`.

- [ ] **Step 1: Write the failing test**

`test/sql/read_obj.test`:

```
# name: test/sql/read_obj.test
# description: read_obj maps Wavefront OBJ objects, groups and materials onto the object-table schema
# group: [sql]

require cityjson

# --- Schema: the standard wide layout with exactly one LoD group and no attributes.
query II
SELECT column_name, column_type
FROM (DESCRIBE SELECT * FROM read_obj('test/data/obj/cube.obj', lod := '2.2'))
WHERE column_name LIKE 'geometry%' OR column_name LIKE 'material%' OR column_name LIKE 'texture%';
----
geometry_lod2_2	BLOB
geometry_properties_lod2_2	STRUCT("type" VARCHAR, surfaces VARCHAR, face_semantics INTEGER[], shells INTEGER[][])
material_lod2_2	VARCHAR
texture_lod2_2	VARCHAR

query I
SELECT COUNT(*) FROM (DESCRIBE SELECT * FROM read_obj('test/data/obj/cube.obj', lod := '2.2'))
WHERE column_name NOT IN ('id','feature_id','object_type','parents','children','children_roles','address','bbox',
                          'geometry_lod2_2','geometry_properties_lod2_2','material_lod2_2','texture_lod2_2',
                          'template','other');
----
0

# --- One row per `o`, in file order; id doubles as feature_id; object_type from the parameter.
query III
SELECT id, feature_id, object_type FROM read_obj('test/data/obj/cube.obj', lod := '2.2');
----
cube	cube	Building
slab	slab	Building

query I
SELECT DISTINCT object_type FROM read_obj('test/data/obj/cube.obj', lod := '2', object_type := 'BuildingPart');
----
BuildingPart

# --- lod is normalised the way every other reader normalises it.
query I
SELECT column_name FROM (DESCRIBE SELECT * FROM read_obj('test/data/obj/cube.obj', lod := '2'))
WHERE column_name LIKE 'geometry_lod%';
----
geometry_lod2_0

# --- lod is required: an OBJ carries none, and the value names the columns.
statement error
SELECT * FROM read_obj('test/data/obj/cube.obj');
----
read_obj: lod is required
```

- [ ] **Step 2: Run it and watch it fail**

Run: `just rebuild && ./build/release/test/unittest "test/sql/read_obj.test"`
Expected: FAIL — `Table Function with name read_obj does not exist`.

- [ ] **Step 3: The reader header**

```cpp
// src/include/cityjson/obj_reader.hpp
#pragma once

#include "cityjson/reader.hpp"

#include <array>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace duckdb {
class ClientContext;
}

namespace duckdb {
namespace cityjson {

//! What an OBJ cannot tell us and the caller must: the level of detail the one geometry
//! column is named after, the CityGML class every object gets, how faces become a
//! geometry, and -- for obj_metadata only -- the CRS.
struct OBJReadOptions {
	std::string lod;                    // normalised, e.g. "2.2"
	std::string object_type = "Building";
	std::string geometry_type = "auto"; // "auto" | "Solid" | "MultiSurface"
	std::optional<std::string> crs;     // surfaces as metadata.referenceSystem (OGC URL form)
};

/**
 * Wavefront OBJ reader.
 *
 * One `o` group is one city object with one geometry at `options.lod`; `usemtl` / `g`
 * names that are CityJSON semantic-surface types become the face's surface; the `.mtl`
 * materials become the appearance definitions, textured ones become textures, and the
 * `vt` list is the UV pool. Everything is parsed once, lazily, and served from a cache.
 * All bytes come through DuckDB's FileSystem, so remote paths work.
 */
class OBJReader : public CityJSONReader {
public:
	OBJReader(ClientContext &context, std::string file_path, OBJReadOptions options);
	~OBJReader() override;

	std::string Name() const override;
	CityJSON ReadMetadata() const override;
	CityJSONFeatureChunk ReadAllChunks() const override;
	std::vector<CityJSONFeature> ReadNFeatures(size_t n) const override;
	std::vector<Column> Columns() const override;
	size_t CountCityObjects() const override;
	size_t CountFeatures() const override;

	//! True for the CityJSON 2.0 semantic-surface vocabulary (RoofSurface, WallSurface,
	//! GroundSurface, ClosureSurface, OuterCeilingSurface, OuterFloorSurface, Window,
	//! Door, InteriorWallSurface, CeilingSurface, FloorSurface, WaterSurface,
	//! WaterGroundSurface, TrafficArea, AuxiliaryTrafficArea, TransportationMarking,
	//! TransportationHole) and for any `+`-prefixed extension name.
	static bool IsSemanticSurfaceType(const std::string &name);

	//! The `# origin x y z` comment the OBJ writer emits; nullopt when absent.
	static std::optional<std::array<double, 3>> ParseOriginComment(const std::string &content);

	//! Directory part of a path or URL ("a/b/c.obj" -> "a/b", "c.obj" -> "").
	static std::string DirectoryOf(const std::string &path);

private:
	struct Parsed;
	const Parsed &Load() const;

	ClientContext &context_;
	std::string file_path_;
	OBJReadOptions options_;
	mutable std::shared_ptr<const Parsed> parsed_;
	mutable std::optional<std::vector<Column>> cached_columns_;
};

} // namespace cityjson
} // namespace duckdb
```

- [ ] **Step 4: The reader implementation**

```cpp
// src/cityjson/obj_reader.cpp
#include "cityjson/obj_reader.hpp"

#include "cityjson/city_object_utils.hpp"
#include "cityjson/column_types.hpp"
#include "cityjson/crs_projjson.hpp"
#include "cityjson/error.hpp"
#include "cityjson/json_utils.hpp"
#include "cityjson/lod_table.hpp"

#include <tiny_obj_loader.h>

#include <algorithm>
#include <cctype>
#include <map>
#include <set>
#include <sstream>
#include <unordered_map>

namespace duckdb {
namespace cityjson {

namespace {

// ------------------------------------------------------------------
// Intermediate parse state, filled by the tinyobjloader callbacks
// ------------------------------------------------------------------

struct FaceRec {
	std::vector<int> v;      // 0-based global vertex indices
	std::vector<int> vt;     // 0-based global texcoord indices, -1 = none
	int material = -1;       // index into ParseState::materials, -1 = none
	std::string usemtl;      // the usemtl token in force, "" = none
	std::string group;       // the last `g` name in force, "" = none
};

struct ObjectRec {
	std::string name;
	std::vector<FaceRec> faces;
};

struct ParseState {
	std::vector<std::array<double, 3>> vertices;
	std::vector<std::array<double, 2>> texcoords;
	std::vector<ObjectRec> objects;
	std::vector<tinyobj::material_t> materials;
	std::string current_group;
	std::string current_usemtl;
	int current_material = -1;
	std::string default_object_name;
	std::array<double, 3> origin {0.0, 0.0, 0.0};
	std::string error; // first structural error; LoadObjWithCallback has no way to abort
};

ObjectRec &CurrentObject(ParseState &st) {
	if (st.objects.empty()) {
		st.objects.push_back(ObjectRec {st.default_object_name, {}});
	}
	return st.objects.back();
}

void VertexCb(void *user, tinyobj::real_t x, tinyobj::real_t y, tinyobj::real_t z, tinyobj::real_t /*w*/) {
	auto &st = *static_cast<ParseState *>(user);
	st.vertices.push_back({x + st.origin[0], y + st.origin[1], z + st.origin[2]});
}

void TexcoordCb(void *user, tinyobj::real_t u, tinyobj::real_t v, tinyobj::real_t /*w*/) {
	auto &st = *static_cast<ParseState *>(user);
	st.texcoords.push_back({u, v});
}

// tinyobjloader's callback API hands over the RAW face tokens: 1-based, negative =
// relative to the current end of the list, 0 = absent. Resolving them is ours.
int ResolveIndex(int raw, size_t count) {
	if (raw > 0) {
		return raw <= static_cast<int>(count) ? raw - 1 : -2;
	}
	if (raw < 0) {
		auto idx = static_cast<long long>(count) + raw;
		return idx >= 0 ? static_cast<int>(idx) : -2;
	}
	return -1; // absent
}

void IndexCb(void *user, tinyobj::index_t *indices, int num_indices) {
	auto &st = *static_cast<ParseState *>(user);
	if (!st.error.empty()) {
		return;
	}
	if (num_indices < 3) {
		st.error = "a face has fewer than three vertices";
		return;
	}
	FaceRec face;
	for (int i = 0; i < num_indices; i++) {
		int v = ResolveIndex(indices[i].vertex_index, st.vertices.size());
		if (v < 0) {
			st.error = "face references vertex " + std::to_string(indices[i].vertex_index) + " but " +
			           std::to_string(st.vertices.size()) + " vertices have been declared";
			return;
		}
		int vt = ResolveIndex(indices[i].texcoord_index, st.texcoords.size());
		if (vt == -2) {
			st.error = "face references texture coordinate " + std::to_string(indices[i].texcoord_index) +
			           " but " + std::to_string(st.texcoords.size()) + " have been declared";
			return;
		}
		face.v.push_back(v);
		face.vt.push_back(vt);
	}
	face.material = st.current_material;
	face.usemtl = st.current_usemtl;
	face.group = st.current_group;
	CurrentObject(st).faces.push_back(std::move(face));
}

void UsemtlCb(void *user, const char *name, int material_id) {
	auto &st = *static_cast<ParseState *>(user);
	st.current_usemtl = name != nullptr ? name : "";
	st.current_material = material_id;
}

void MtllibCb(void *user, const tinyobj::material_t *materials, int num_materials) {
	auto &st = *static_cast<ParseState *>(user);
	st.materials.assign(materials, materials + num_materials);
}

void GroupCb(void *user, const char **names, int num_names) {
	auto &st = *static_cast<ParseState *>(user);
	st.current_group = num_names > 0 ? names[num_names - 1] : "";
}

void ObjectCb(void *user, const char *name) {
	auto &st = *static_cast<ParseState *>(user);
	std::string n = name != nullptr ? name : "";
	// A repeated `o` resumes that object: cjio writes one `o <id>` per geometry, so a
	// multi-geometry object arrives as several blocks under one name.
	for (auto &obj : st.objects) {
		if (obj.name == n) {
			std::rotate(std::find_if(st.objects.begin(), st.objects.end(),
			                         [&](const ObjectRec &o) { return o.name == n; }),
			            st.objects.end() - 1, st.objects.end());
			// (rotating keeps `objects.back()` as the current one without copying faces)
			return;
		}
	}
	st.objects.push_back(ObjectRec {n, {}});
}

// `.mtl` files named by `mtllib`, resolved against the OBJ's directory and read through
// DuckDB's FileSystem -- tinyobjloader's own MaterialFileReader would std::ifstream them
// and lose every remote path.
class DuckDBMaterialReader : public tinyobj::MaterialReader {
public:
	DuckDBMaterialReader(ClientContext &context, std::string base_dir)
	    : context_(context), base_dir_(std::move(base_dir)) {
	}
	bool operator()(const std::string &mat_id, std::vector<tinyobj::material_t> *materials,
	                std::map<std::string, int> *mat_map, std::string *warn, std::string *err) override {
		std::string path = base_dir_.empty() ? mat_id : base_dir_ + "/" + mat_id;
		std::string content;
		try {
			content = json_utils::ReadFileContent(context_, path);
		} catch (const CityJSONError &e) {
			if (warn != nullptr) {
				*warn += "mtllib '" + mat_id + "' could not be read: " + e.what() + "\n";
			}
			return false;
		}
		std::istringstream in(content);
		tinyobj::LoadMtl(mat_map, materials, &in, warn, err);
		return true;
	}

private:
	ClientContext &context_;
	std::string base_dir_;
};

std::string FileStem(const std::string &path) {
	auto base = path.substr(OBJReader::DirectoryOf(path).empty() ? 0 : OBJReader::DirectoryOf(path).size() + 1);
	auto dot = base.rfind('.');
	return dot == std::string::npos ? base : base.substr(0, dot);
}

std::string UpperExtension(const std::string &path) {
	auto dot = path.rfind('.');
	if (dot == std::string::npos) {
		return "";
	}
	std::string ext = path.substr(dot + 1);
	std::transform(ext.begin(), ext.end(), ext.begin(), [](unsigned char c) { return std::toupper(c); });
	return ext == "JPEG" ? "JPG" : ext;
}

// ------------------------------------------------------------------
// From parse state to CityJSON records
// ------------------------------------------------------------------

//! Closed iff every undirected edge is used by exactly two faces.
bool IsClosed(const ObjectRec &obj) {
	if (obj.faces.size() < 4) {
		return false;
	}
	std::map<std::pair<int, int>, int> edges;
	for (const auto &face : obj.faces) {
		size_t n = face.v.size();
		for (size_t i = 0; i < n; i++) {
			int a = face.v[i];
			int b = face.v[(i + 1) % n];
			edges[{std::min(a, b), std::max(a, b)}]++;
		}
	}
	return std::all_of(edges.begin(), edges.end(), [](const auto &kv) { return kv.second == 2; });
}

//! Surface type a face carries: its usemtl name when that is a surface type, else its
//! group name when that is, else none.
std::optional<std::string> SurfaceOf(const FaceRec &face) {
	if (OBJReader::IsSemanticSurfaceType(face.usemtl)) {
		return face.usemtl;
	}
	if (OBJReader::IsSemanticSurfaceType(face.group)) {
		return face.group;
	}
	return std::nullopt;
}

} // namespace

// ------------------------------------------------------------------
// OBJReader
// ------------------------------------------------------------------

struct OBJReader::Parsed {
	CityJSON header;
	std::vector<CityJSONFeature> features;
};

OBJReader::OBJReader(ClientContext &context, std::string file_path, OBJReadOptions options)
    : context_(context), file_path_(std::move(file_path)), options_(std::move(options)) {
}

OBJReader::~OBJReader() = default;

std::string OBJReader::Name() const {
	return file_path_;
}

std::string OBJReader::DirectoryOf(const std::string &path) {
	auto slash = path.find_last_of("/\\");
	return slash == std::string::npos ? std::string() : path.substr(0, slash);
}

bool OBJReader::IsSemanticSurfaceType(const std::string &name) {
	static const std::set<std::string> kTypes = {
	    "RoofSurface",         "WallSurface",       "GroundSurface",       "ClosureSurface",
	    "OuterCeilingSurface", "OuterFloorSurface", "Window",              "Door",
	    "InteriorWallSurface", "CeilingSurface",    "FloorSurface",        "WaterSurface",
	    "WaterGroundSurface",  "TrafficArea",       "AuxiliaryTrafficArea", "TransportationMarking",
	    "TransportationHole"};
	if (name.empty()) {
		return false;
	}
	return name[0] == '+' || kTypes.count(name) > 0;
}

std::optional<std::array<double, 3>> OBJReader::ParseOriginComment(const std::string &content) {
	size_t pos = 0;
	while (pos < content.size()) {
		auto eol = content.find('\n', pos);
		auto line = content.substr(pos, eol == std::string::npos ? std::string::npos : eol - pos);
		pos = eol == std::string::npos ? content.size() : eol + 1;
		if (line.rfind("# origin ", 0) != 0) {
			if (!line.empty() && line[0] != '#') {
				return std::nullopt; // the header block is over
			}
			continue;
		}
		std::istringstream in(line.substr(9));
		std::array<double, 3> o {};
		if (in >> o[0] >> o[1] >> o[2]) {
			return o;
		}
		return std::nullopt;
	}
	return std::nullopt;
}

const OBJReader::Parsed &OBJReader::Load() const {
	if (parsed_) {
		return *parsed_;
	}
	std::string content = json_utils::ReadFileContent(context_, file_path_);

	// Backslash line continuation is legal OBJ that the parser library does not
	// implement; refuse rather than silently mis-parse the joined line.
	{
		size_t pos = 0;
		size_t line_no = 1;
		while (pos < content.size()) {
			auto eol = content.find('\n', pos);
			auto end = eol == std::string::npos ? content.size() : eol;
			auto last = end;
			while (last > pos && (content[last - 1] == '\r' || content[last - 1] == ' ' || content[last - 1] == '\t')) {
				last--;
			}
			if (last > pos && content[last - 1] == '\\') {
				throw CityJSONError::Parse("line " + std::to_string(line_no) +
				                               " ends with a backslash; line continuation is not supported",
				                           file_path_);
			}
			pos = eol == std::string::npos ? content.size() : eol + 1;
			line_no++;
		}
	}

	ParseState st;
	st.default_object_name = FileStem(file_path_);
	if (auto origin = ParseOriginComment(content)) {
		st.origin = origin.value();
	}

	tinyobj::callback_t cb;
	cb.vertex_cb = VertexCb;
	cb.texcoord_cb = TexcoordCb;
	cb.index_cb = IndexCb;
	cb.usemtl_cb = UsemtlCb;
	cb.mtllib_cb = MtllibCb;
	cb.group_cb = GroupCb;
	cb.object_cb = ObjectCb;

	DuckDBMaterialReader mat_reader(context_, DirectoryOf(file_path_));
	std::istringstream in(content);
	std::string warn;
	std::string err;
	bool ok = tinyobj::LoadObjWithCallback(in, cb, &st, &mat_reader, &warn, &err);
	if (!ok || !err.empty()) {
		throw CityJSONError::Parse(err.empty() ? "tinyobjloader failed" : err, file_path_);
	}
	if (!st.error.empty()) {
		throw CityJSONError::InvalidGeometry(st.error, file_path_);
	}

	auto parsed = std::make_shared<Parsed>();

	// --- Appearance definitions: the MTL materials, and a texture per textured material.
	Appearance appearance;
	std::vector<int> texture_of_material(st.materials.size(), -1);
	for (size_t i = 0; i < st.materials.size(); i++) {
		const auto &m = st.materials[i];
		Material mat;
		mat.name = m.name;
		mat.diffuse_color = std::vector<double> {m.diffuse[0], m.diffuse[1], m.diffuse[2]};
		mat.specular_color = std::vector<double> {m.specular[0], m.specular[1], m.specular[2]};
		mat.emissive_color = std::vector<double> {m.emission[0], m.emission[1], m.emission[2]};
		mat.ambient_intensity = (m.ambient[0] + m.ambient[1] + m.ambient[2]) / 3.0;
		mat.transparency = 1.0 - m.dissolve;
		mat.shininess = std::min(1.0, std::max(0.0, static_cast<double>(m.shininess) / 1000.0));
		if (!m.unknown_parameter.empty()) {
			mat.other = json::object();
			for (const auto &kv : m.unknown_parameter) {
				mat.other[kv.first] = kv.second;
			}
		}
		appearance.materials.push_back(std::move(mat));
		if (!m.diffuse_texname.empty()) {
			Texture tex;
			tex.image_uri = m.diffuse_texname;
			tex.image_type = UpperExtension(m.diffuse_texname);
			tex.wrap_mode = "wrap";
			tex.texture_type = "unknown";
			texture_of_material[i] = static_cast<int>(appearance.textures.size());
			appearance.textures.push_back(std::move(tex));
		}
	}
	appearance.vertices_texture = st.texcoords;
	if (!appearance.Empty()) {
		parsed->header.appearance = appearance;
	}

	// --- Extent over every vertex, for obj_metadata.
	if (!st.vertices.empty()) {
		GeographicalExtent extent(st.vertices[0][0], st.vertices[0][1], st.vertices[0][2], st.vertices[0][0],
		                          st.vertices[0][1], st.vertices[0][2]);
		for (const auto &v : st.vertices) {
			extent = extent.Union(GeographicalExtent(v[0], v[1], v[2], v[0], v[1], v[2]));
		}
		Metadata md;
		md.geographical_extent = extent;
		parsed->header.metadata = md;
	}
	if (options_.crs.has_value()) {
		if (!parsed->header.metadata.has_value()) {
			parsed->header.metadata = Metadata();
		}
		parsed->header.metadata->reference_system = options_.crs.value();
	}

	// --- One feature per object.
	const bool force_solid = options_.geometry_type == "Solid";
	const bool force_multi = options_.geometry_type == "MultiSurface";
	for (const auto &obj : st.objects) {
		if (obj.faces.empty()) {
			continue;
		}
		CityJSONFeature feature(obj.name);
		std::unordered_map<int, int> local_of_global;
		auto local_index = [&](int g) {
			auto it = local_of_global.find(g);
			if (it != local_of_global.end()) {
				return it->second;
			}
			int idx = static_cast<int>(feature.vertices.size());
			feature.vertices.push_back(st.vertices[g]);
			local_of_global.emplace(g, idx);
			return idx;
		};

		const bool solid = force_solid || (!force_multi && IsClosed(obj));

		json faces = json::array();      // face -> [ring]
		json sem_values = json::array(); // face -> surface index | null
		json mat_values = json::array(); // face -> material id | null
		json tex_values = json::array(); // face -> [ [texId, uv...] ]
		std::vector<std::string> surfaces;
		bool any_surface = false;
		bool any_material = false;
		bool any_texture = false;

		for (const auto &face : obj.faces) {
			json ring = json::array();
			for (int g : face.v) {
				ring.push_back(local_index(g));
			}
			faces.push_back(json::array({ring}));

			if (auto surface = SurfaceOf(face)) {
				auto it = std::find(surfaces.begin(), surfaces.end(), surface.value());
				if (it == surfaces.end()) {
					surfaces.push_back(surface.value());
					it = surfaces.end() - 1;
				}
				sem_values.push_back(static_cast<int>(it - surfaces.begin()));
				any_surface = true;
			} else {
				sem_values.push_back(nullptr);
			}

			if (face.material >= 0) {
				mat_values.push_back(face.material);
				any_material = true;
			} else {
				mat_values.push_back(nullptr);
			}

			int tex = face.material >= 0 ? texture_of_material[face.material] : -1;
			bool has_uv = std::all_of(face.vt.begin(), face.vt.end(), [](int i) { return i >= 0; });
			if (tex >= 0 && has_uv) {
				json tring = json::array({tex});
				for (int vt : face.vt) {
					tring.push_back(vt);
				}
				tex_values.push_back(json::array({tring}));
				any_texture = true;
			} else {
				tex_values.push_back(json::array({json::array({nullptr})}));
			}
		}

		Geometry geom;
		geom.lod = options_.lod;
		if (solid) {
			geom.type = "Solid";
			geom.boundaries = json::array({faces});
		} else {
			geom.type = "MultiSurface";
			geom.boundaries = faces;
		}
		auto nest = [&](json values) { return solid ? json::array({std::move(values)}) : values; };
		if (any_surface) {
			json surfs = json::array();
			for (const auto &s : surfaces) {
				surfs.push_back(json {{"type", s}});
			}
			geom.semantics = json {{"surfaces", surfs}, {"values", nest(sem_values)}};
		}
		if (any_material) {
			geom.material = json {{"visual", json {{"values", nest(mat_values)}}}};
		}
		if (any_texture) {
			geom.texture = json {{"visual", json {{"values", nest(tex_values)}}}};
		}

		CityObject city_object(options_.object_type);
		city_object.feature_id = obj.name;
		city_object.geometry.push_back(std::move(geom));
		feature.city_objects[obj.name] = std::move(city_object);
		parsed->features.push_back(std::move(feature));
	}

	parsed_ = parsed;
	return *parsed_;
}

CityJSON OBJReader::ReadMetadata() const {
	return Load().header;
}

CityJSONFeatureChunk OBJReader::ReadAllChunks() const {
	auto features = Load().features;
	return CityJSONFeatureChunk::CreateChunks(std::move(features), STANDARD_VECTOR_SIZE);
}

std::vector<CityJSONFeature> OBJReader::ReadNFeatures(size_t n) const {
	const auto &features = Load().features;
	return std::vector<CityJSONFeature>(features.begin(), features.begin() + std::min(n, features.size()));
}

size_t OBJReader::CountCityObjects() const {
	return Load().features.size();
}

size_t OBJReader::CountFeatures() const {
	return Load().features.size();
}

std::vector<Column> OBJReader::Columns() const {
	if (cached_columns_.has_value()) {
		return cached_columns_.value();
	}
	// Same order as LocalCityJSONReader::Columns: head, bbox + geometry group, trailing.
	// No attribute columns: an OBJ carries none.
	std::vector<Column> columns = GetDefinedColumns();
	const auto &features = Load().features;
	auto geom_columns = CityObjectUtils::InferGeometryColumns(features, features.size());
	columns.insert(columns.end(), geom_columns.begin(), geom_columns.end());
	auto trailing = LODTableUtils::GetTrailingColumns();
	columns.insert(columns.end(), trailing.begin(), trailing.end());
	cached_columns_ = columns;
	return columns;
}

} // namespace cityjson
} // namespace duckdb
```

Check `ObjectCb`: the rotate is subtle — replace it with the straightforward version if the reviewer prefers: keep a `current` index in `ParseState` (`size_t current_object`) and have `CurrentObject` return `st.objects[st.current_object]`. Do that instead; it is clearer:

```cpp
// in ParseState:  size_t current_object = 0;
ObjectRec &CurrentObject(ParseState &st) {
	if (st.objects.empty()) {
		st.objects.push_back(ObjectRec {st.default_object_name, {}});
		st.current_object = 0;
	}
	return st.objects[st.current_object];
}
void ObjectCb(void *user, const char *name) {
	auto &st = *static_cast<ParseState *>(user);
	std::string n = name != nullptr ? name : "";
	for (size_t i = 0; i < st.objects.size(); i++) {
		if (st.objects[i].name == n) {
			st.current_object = i; // a repeated `o` resumes that object (cjio writes one per geometry)
			return;
		}
	}
	st.objects.push_back(ObjectRec {n, {}});
	st.current_object = st.objects.size() - 1;
}
```

- [ ] **Step 5: The table function**

```cpp
// src/include/cityjson/obj_table_function.hpp
#pragma once

#include "cityjson/obj_reader.hpp"
#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {
namespace cityjson {

//! Parse read_obj's named parameters. `lod` is required and normalised; `object_type`
//! defaults to Building; `geometry_type` is one of auto | Solid | MultiSurface.
OBJReadOptions ParseOBJReadOptions(const TableFunctionBindInput &input, const std::string &function_name);

//! Registers read_obj(path, lod := …) and obj_metadata(path [, crs := …]).
void RegisterOBJTableFunctions(ExtensionLoader &loader);

} // namespace cityjson
} // namespace duckdb
```

```cpp
// src/cityjson/obj_table_function.cpp
#include "cityjson/obj_table_function.hpp"

#include "cityjson/error.hpp"
#include "cityjson/lod_table.hpp"
#include "cityjson/table_function.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {
namespace cityjson {

OBJReadOptions ParseOBJReadOptions(const TableFunctionBindInput &input, const std::string &function_name) {
	OBJReadOptions options;
	bool has_lod = false;
	for (auto &kv : input.named_parameters) {
		if (kv.first == "lod") {
			options.lod = LODTableUtils::NormalizeLOD(StringValue::Get(kv.second));
			has_lod = true;
		} else if (kv.first == "object_type") {
			options.object_type = StringValue::Get(kv.second);
			if (options.object_type.empty()) {
				throw BinderException(function_name + ": object_type must not be empty");
			}
		} else if (kv.first == "geometry_type") {
			options.geometry_type = StringValue::Get(kv.second);
			if (options.geometry_type != "auto" && options.geometry_type != "Solid" &&
			    options.geometry_type != "MultiSurface") {
				throw BinderException(function_name + ": geometry_type must be 'auto', 'Solid' or 'MultiSurface', got '" +
				                      options.geometry_type + "'");
			}
		}
	}
	if (!has_lod) {
		throw BinderException(function_name + ": lod is required -- an OBJ carries no level of detail, and the value "
		                                      "names the geometry columns (e.g. lod := '2.2')");
	}
	return options;
}

static unique_ptr<FunctionData> OBJBind(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.empty()) {
		throw BinderException("read_obj requires a file path");
	}
	std::string file_name = StringValue::Get(input.inputs[0]);
	auto options = ParseOBJReadOptions(input, "read_obj");
	std::unique_ptr<CityJSONReader> reader = std::make_unique<OBJReader>(context, file_name, options);
	// Non-streaming: the whole file is parsed at bind and the chunks live in the bind
	// data, so init_global never has to reopen a reader (ReaderKind is irrelevant here).
	return BindCityJSONRead(context, input, return_types, names, "read_obj", std::move(reader));
}

void RegisterOBJTableFunctions(ExtensionLoader &loader) {
	TableFunction read_obj("read_obj", {LogicalType::VARCHAR}, CityJSONScan, OBJBind);
	read_obj.named_parameters["lod"] = LogicalType::VARCHAR;
	read_obj.named_parameters["object_type"] = LogicalType::VARCHAR;
	read_obj.named_parameters["geometry_type"] = LogicalType::VARCHAR;
	read_obj.named_parameters["appearance"] = LogicalType::VARCHAR; // 'local' (default) or 'sidecar'
	read_obj.named_parameters["sample_lines"] = LogicalType::BIGINT;
	read_obj.init_global = CityJSONInitGlobal;
	read_obj.init_local = CityJSONInitLocal;
	read_obj.cardinality = CityJSONCardinality;
	read_obj.statistics = CityJSONStatistics;
	read_obj.table_scan_progress = CityJSONProgress;
	read_obj.projection_pushdown = true;
	read_obj.filter_pushdown = false;
	read_obj.pushdown_complex_filter = CityJSONPushdownComplexFilter;
	loader.RegisterFunction(read_obj);
}

} // namespace cityjson
} // namespace duckdb
```

Note: `BindCityJSONReadRaw` throws `BinderException("Failed to read data: …")` around `ReadAllChunks`, so a `CityJSONError` from `Load()` surfaces as a binder error with the reader's message — that is what the error tests in Task 6 assert on.

- [ ] **Step 6: Register and list sources**

`CMakeLists.txt`, in `EXTENSION_SOURCES` after `src/cityjson/reader_factory.cpp`:

```cmake
    src/cityjson/obj_reader.cpp
    src/cityjson/obj_table_function.cpp
```

`src/cityjson_extension.cpp`: add `#include "cityjson/obj_table_function.hpp"` and, after `cityjson::RegisterAppearanceTableFunctions(loader);`:

```cpp
	// Register Wavefront OBJ input (read_obj, obj_metadata)
	cityjson::RegisterOBJTableFunctions(loader);
```

- [ ] **Step 7: Run the test**

Run: `just rebuild && ./build/release/test/unittest "test/sql/read_obj.test"`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add src/include/cityjson/obj_reader.hpp src/cityjson/obj_reader.cpp \
        src/include/cityjson/obj_table_function.hpp src/cityjson/obj_table_function.cpp \
        CMakeLists.txt src/cityjson_extension.cpp test/sql/read_obj.test
git commit -m "feat(obj): read_obj -- Wavefront OBJ into the object-table schema"
```

---

### Task 4: Geometry, semantics and bbox

**Files:**
- Test: `test/sql/read_obj.test` (append)
- Modify: `src/cityjson/obj_reader.cpp` only if a test fails

- [ ] **Step 1: Append the tests**

```
# --- Geometry: closed → Solid with one shell; open → MultiSurface. Face order is file order.
query IIII
SELECT id, geometry_properties_lod2_2.type, geometry_properties_lod2_2.shells,
       cityjson_wkb_geometry_type(geometry_lod2_2)
FROM read_obj('test/data/obj/cube.obj', lod := '2.2');
----
cube	Solid	[[6]]	PolyhedralSurface Z
slab	MultiSurface	NULL	MultiPolygon Z

query II
SELECT id, cityjson_wkb_extent(geometry_lod2_2) FROM read_obj('test/data/obj/cube.obj', lod := '2.2');
----
cube	{'xmin': 0.0, 'ymin': 0.0, 'zmin': 0.0, 'xmax': 1.0, 'ymax': 1.0, 'zmax': 1.0}
slab	{'xmin': 10.0, 'ymin': 0.0, 'zmin': 0.0, 'xmax': 12.0, 'ymax': 2.0, 'zmax': 0.0}

query II
SELECT id, bbox FROM read_obj('test/data/obj/cube.obj', lod := '2.2');
----
cube	{'xmin': 0.0, 'ymin': 0.0, 'zmin': 0.0, 'xmax': 1.0, 'ymax': 1.0, 'zmax': 1.0}
slab	{'xmin': 10.0, 'ymin': 0.0, 'zmin': 0.0, 'xmax': 12.0, 'ymax': 2.0, 'zmax': 0.0}

# --- Semantics from usemtl / g names that are surface types, in first-use order.
#     The slab inherits RoofSurface: OBJ state persists across `o`.
query III
SELECT id, geometry_properties_lod2_2.surfaces, geometry_properties_lod2_2.face_semantics
FROM read_obj('test/data/obj/cube.obj', lod := '2.2');
----
cube	[{"type":"GroundSurface"},{"type":"WallSurface"},{"type":"RoofSurface"}]	[0, 1, 1, 1, 1, 2]
slab	[{"type":"RoofSurface"}]	[0]

# --- The negative-index roof face resolved to vertices 5..8: the cube's top ring is at z = 1.
query I
SELECT list_count(geometry_properties_lod2_2.face_semantics) = 6 FROM read_obj('test/data/obj/cube.obj', lod := '2.2') WHERE id = 'cube';
----
true

# --- No `o`, no materials: one object named after the file stem, open → MultiSurface, no semantics.
query IIII
SELECT id, geometry_properties_lod2_2.type, geometry_properties_lod2_2.surfaces, material_lod2_2
FROM read_obj('test/data/obj/open_roof.obj', lod := '2.2');
----
open_roof	MultiSurface	NULL	NULL

# --- geometry_type forces the family; forcing Solid on an open mesh is allowed.
query II
SELECT id, geometry_properties_lod2_2.type FROM read_obj('test/data/obj/cube.obj', lod := '2.2', geometry_type := 'MultiSurface');
----
cube	MultiSurface
slab	MultiSurface

query II
SELECT id, geometry_properties_lod2_2.shells FROM read_obj('test/data/obj/open_roof.obj', lod := '2.2', geometry_type := 'Solid');
----
open_roof	[[2]]

statement error
SELECT * FROM read_obj('test/data/obj/cube.obj', lod := '2.2', geometry_type := 'CompositeSolid');
----
geometry_type must be 'auto', 'Solid' or 'MultiSurface'
```

- [ ] **Step 2: Run**

Run: `just rebuild && ./build/release/test/unittest "test/sql/read_obj.test"`
Expected: PASS. If the `surfaces` JSON text differs only in spacing, match what `GetGeometryPropertiesStruct` emits (`dump()` without spaces) and fix the expected string — the STRUCT field is text produced by the existing code, not by this reader. If `bbox` prints with a different float format, copy the format `test/sql/cityjson_bbox.test` uses.

- [ ] **Step 3: Commit**

```bash
git add test/sql/read_obj.test src/cityjson/obj_reader.cpp
git commit -m "test(obj): geometry family, semantics and extent of read_obj"
```

---

### Task 5: Materials and textures on the object rows

**Files:**
- Test: `test/sql/read_obj.test` (append)

- [ ] **Step 1: Append the tests**

```
# --- material_lod*: one theme "visual", values per face in WKB face order; ids are MTL ordinals.
query II
SELECT id, material_lod2_2 FROM read_obj('test/data/obj/cube.obj', lod := '2.2');
----
cube	{"visual":{"values":[[0,1,1,1,1,2]]}}
slab	{"visual":{"values":[2]}}

# --- texture_lod* in local form: [texId, vtIdx…] per ring for the four textured walls, [null] elsewhere.
query I
SELECT texture_lod2_2 FROM read_obj('test/data/obj/cube.obj', lod := '2.2') WHERE id = 'cube';
----
{"visual":{"values":[[[[null]],[[0,0,1,2,3]],[[0,0,1,2,3]],[[0,0,1,2,3]],[[0,0,1,2,3]],[[null]]]]}}

query I
SELECT texture_lod2_2 FROM read_obj('test/data/obj/cube.obj', lod := '2.2') WHERE id = 'slab';
----
NULL

# --- appearance := 'sidecar' inlines the UVs; ids are unchanged (one file, one definition set).
query I
SELECT texture_lod2_2 FROM read_obj('test/data/obj/cube.obj', lod := '2.2', appearance := 'sidecar') WHERE id = 'cube';
----
{"visual":{"values":[[[[null]],[[0,[0.0,0.0],[1.0,0.0],[1.0,1.0],[0.0,1.0]]],[[0,[0.0,0.0],[1.0,0.0],[1.0,1.0],[0.0,1.0]]],[[0,[0.0,0.0],[1.0,0.0],[1.0,1.0],[0.0,1.0]]],[[0,[0.0,0.0],[1.0,0.0],[1.0,1.0],[0.0,1.0]]],[[null]]]]}}

query I
SELECT material_lod2_2 FROM read_obj('test/data/obj/cube.obj', lod := '2.2', appearance := 'sidecar') WHERE id = 'cube';
----
{"visual":{"values":[[0,1,1,1,1,2]]}}
```

- [ ] **Step 2: Run**

Run: `just rebuild && ./build/release/test/unittest "test/sql/read_obj.test"`
Expected: PASS. The sidecar-form text is produced by `NormaliseTextureMap`; if its float formatting differs (`0` vs `0.0`), take the format the existing `cityjson_appearance_sidecar.test` shows and adjust the expected string — the assertion is about structure and ids.

- [ ] **Step 3: Commit**

```bash
git add test/sql/read_obj.test
git commit -m "test(obj): material and texture cells of read_obj"
```

---

### Task 6: Errors and the origin comment

**Files:**
- Test: `test/sql/read_obj.test` (append)
- Create: `test/data/obj/with_origin.obj`, `test/data/obj/bad_index.obj`, `test/data/obj/missing_mtl.obj`

- [ ] **Step 1: Fixtures**

`test/data/obj/with_origin.obj` — what the OBJ writer emits, minus the mtl:

```
# Written by duckdb-cityjson
# crs EPSG:7415
# origin 84500 446300 0
o NL.1
v 0.5 0.25 0
v 1.5 0.25 0
v 1.5 1.25 0
v 0.5 1.25 0
f 1 2 3 4
```

`test/data/obj/bad_index.obj` — a face past the end of the vertex list:

```
v 0 0 0
v 1 0 0
f 1 2 9
```

`test/data/obj/missing_mtl.obj` — an `mtllib` that does not exist:

```
mtllib missing.mtl
v 0 0 0
v 1 0 0
v 1 1 0
usemtl x
f 1 2 3
```

- [ ] **Step 2: Append the tests**

```
# --- `# origin` written by our exporter is added back, so a round trip is exact.
query I
SELECT bbox FROM read_obj('test/data/obj/with_origin.obj', lod := '0');
----
{'xmin': 84500.5, 'ymin': 446300.25, 'zmin': 0.0, 'xmax': 84501.5, 'ymax': 446301.25, 'zmax': 0.0}

# --- Documented limitation: backslash continuation is refused, not mis-parsed.
statement error
SELECT * FROM read_obj('test/data/obj/continuation.obj', lod := '1');
----
line 3 ends with a backslash; line continuation is not supported

# --- A face pointing past the vertex list is an error naming the offending index.
statement error
SELECT * FROM read_obj('test/data/obj/bad_index.obj', lod := '1');
----
face references vertex 9 but 2 vertices have been declared

# --- A missing .mtl is a warning, not an error: the geometry still reads, with no materials.
query II
SELECT COUNT(*), COUNT(material_lod1_0) FROM read_obj('test/data/obj/missing_mtl.obj', lod := '1');
----
1	0
```

- [ ] **Step 3: Run**

Run: `just rebuild && ./build/release/test/unittest "test/sql/read_obj.test"`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add test/sql/read_obj.test test/data/obj/with_origin.obj test/data/obj/bad_index.obj test/data/obj/missing_mtl.obj
git commit -m "test(obj): origin comment, continuation refusal and index errors"
```

---

### Task 7: `obj_materials` / `obj_textures` through a reader-opener

**Files:**
- Modify: `src/include/cityjson/appearance_table_function.hpp`, `src/cityjson/appearance_table_function.cpp`
- Modify: `src/cityjson/obj_table_function.cpp` (registration)
- Test: `test/sql/obj_sidecars.test`

**Interfaces:**
- Produces:
  ```cpp
  using ReaderOpener = std::function<std::unique_ptr<CityJSONReader>(ClientContext &, const std::string &path)>;
  enum class SidecarKind { MATERIALS, TEXTURES, TEMPLATES };          // moved out of the anonymous namespace
  TableFunction CreateAppearanceTableFunction(const std::string &name, SidecarKind kind, ReaderOpener opener);
  ```
- Consumes: `OBJReader`, `ParseOBJReadOptions` is *not* used (sidecars take only a path).

- [ ] **Step 1: Failing test**

`test/sql/obj_sidecars.test`:

```
# name: test/sql/obj_sidecars.test
# description: obj_materials and obj_textures expose the .mtl as CityParquet sidecar rows
# group: [sql]

require cityjson

query IIIIII
SELECT id, name, diffuseColor, specularColor, transparency, shininess FROM obj_materials('test/data/obj/cube.obj');
----
0	GroundSurface	[0.3, 0.3, 0.3]	[0.0, 0.0, 0.0]	0.0	0.001
1	brick	[0.7, 0.3, 0.2]	[0.1, 0.1, 0.1]	0.1	0.25
2	RoofSurface	[0.9, 0.06, 0.09]	[0.0, 0.0, 0.0]	0.0	0.001

# Same columns, same order, as cityjson_materials -- a package writer must not care which produced them.
query I
SELECT list_aggregate(list(column_name ORDER BY column_index), 'string_agg', ',')
FROM (SELECT column_name, row_number() OVER () AS column_index FROM (DESCRIBE SELECT * FROM obj_materials('test/data/obj/cube.obj')));
----
id,name,ambientIntensity,diffuseColor,specularColor,emissiveColor,transparency,shininess,isSmooth,other

query IIIIII
SELECT id, image_uri, image_data, image_type, wrapMode, textureType FROM obj_textures('test/data/obj/cube.obj');
----
0	brick.png	NULL	PNG	wrap	unknown

query I
SELECT COUNT(*) FROM obj_materials('test/data/obj/open_roof.obj');
----
0
```

- [ ] **Step 2: Run it and watch it fail**

Run: `just rebuild && ./build/release/test/unittest "test/sql/obj_sidecars.test"`
Expected: FAIL — `Table Function with name obj_materials does not exist`.

- [ ] **Step 3: Refactor the appearance bind to a `TableFunctionInfo`**

In `src/include/cityjson/appearance_table_function.hpp`, add includes `<functional>`, `<memory>`, `"cityjson/reader.hpp"` and, before `RegisterAppearanceTableFunctions`:

```cpp
//! Which sidecar a registered appearance table function produces.
enum class SidecarKind { MATERIALS, TEXTURES, TEMPLATES };

//! Opens the reader whose appearance definitions a sidecar function exposes. The CityJSON
//! sidecars open with OpenAnyCityJSONFile; the OBJ ones with an OBJReader.
using ReaderOpener = std::function<std::unique_ptr<CityJSONReader>(ClientContext &, const std::string &path)>;

//! The bind/scan pair behind every appearance sidecar function, parameterised by the
//! function's TableFunctionInfo so one bind serves every input format.
TableFunction CreateAppearanceTableFunction(const std::string &name, SidecarKind kind, ReaderOpener opener);
```

In `src/cityjson/appearance_table_function.cpp`:

1. Delete the anonymous-namespace `enum class SidecarKind` (it now comes from the header).
2. Add, inside the anonymous namespace, above `AppearanceBind`:
   ```cpp
   struct AppearanceFunctionInfo : public TableFunctionInfo {
   	std::string function_name;
   	SidecarKind kind;
   	ReaderOpener opener;
   	AppearanceFunctionInfo(std::string function_name, SidecarKind kind, ReaderOpener opener)
   	    : function_name(std::move(function_name)), kind(kind), opener(std::move(opener)) {
   	}
   };
   ```
3. Change `AppearanceBind`'s signature to the standard bind signature and read kind/opener from `input.info`:
   ```cpp
   unique_ptr<FunctionData> AppearanceBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
   	auto &info = input.info->Cast<AppearanceFunctionInfo>();
   	const SidecarKind kind = info.kind;
   	const char *function_name = info.function_name.c_str();
   	auto result = make_uniq<AppearanceBindData>();
   	result->file_name = StringValue::Get(input.inputs[0]);
   	result->kind = kind;

   	std::unique_ptr<CityJSONReader> reader;
   	try {
   		reader = info.opener(context, result->file_name);
   		… (the rest of the body unchanged) …
   ```
4. Delete `MaterialsBind`, `TexturesBind`, `TemplatesBind`.
5. Replace `RegisterAppearanceTableFunctions` and add the factory (outside the anonymous namespace):
   ```cpp
   TableFunction CreateAppearanceTableFunction(const std::string &name, SidecarKind kind, ReaderOpener opener) {
   	TableFunction func(name, {LogicalType(LogicalTypeId::VARCHAR)}, AppearanceScan, AppearanceBind);
   	func.init_global = AppearanceInitGlobal;
   	func.function_info = make_shared_ptr<AppearanceFunctionInfo>(name, kind, std::move(opener));
   	return func;
   }

   void RegisterAppearanceTableFunctions(ExtensionLoader &loader) {
   	ReaderOpener any = [](ClientContext &context, const std::string &path) {
   		return OpenAnyCityJSONFile(context, path);
   	};
   	loader.RegisterFunction(CreateAppearanceTableFunction("cityjson_materials", SidecarKind::MATERIALS, any));
   	loader.RegisterFunction(CreateAppearanceTableFunction("cityjson_textures", SidecarKind::TEXTURES, any));
   	loader.RegisterFunction(CreateAppearanceTableFunction("cityjson_geometry_templates", SidecarKind::TEMPLATES, any));
   }
   ```
   `AppearanceBindData::Equals` already compares `file_name` and `kind`; different function names never share bind data, so no change there.

- [ ] **Step 4: Register the OBJ sidecars**

In `src/cityjson/obj_table_function.cpp`, include `"cityjson/appearance_table_function.hpp"` and append to `RegisterOBJTableFunctions`:

```cpp
	// Sidecars need no LoD: the materials are file-global. Any lod satisfies the reader.
	ReaderOpener obj_opener = [](ClientContext &context, const std::string &path) {
		OBJReadOptions options;
		options.lod = "0.0";
		return std::unique_ptr<CityJSONReader>(std::make_unique<OBJReader>(context, path, options));
	};
	loader.RegisterFunction(CreateAppearanceTableFunction("obj_materials", SidecarKind::MATERIALS, obj_opener));
	loader.RegisterFunction(CreateAppearanceTableFunction("obj_textures", SidecarKind::TEXTURES, obj_opener));
```

- [ ] **Step 5: Run the new test and the existing appearance suites**

Run:
```
just rebuild
./build/release/test/unittest "test/sql/obj_sidecars.test"
./build/release/test/unittest "test/sql/cityjson_appearance*.test"
```
Expected: all PASS — the refactor must not change `cityjson_materials` / `cityjson_textures` / `cityjson_geometry_templates`.

- [ ] **Step 6: Commit**

```bash
git add src/include/cityjson/appearance_table_function.hpp src/cityjson/appearance_table_function.cpp \
        src/cityjson/obj_table_function.cpp test/sql/obj_sidecars.test
git commit -m "feat(obj): obj_materials and obj_textures; appearance sidecars take a reader opener"
```

---

### Task 8: `obj_metadata(path [, crs := …])`

**Files:**
- Modify: `src/cityjson/obj_table_function.cpp`
- Test: `test/sql/obj_metadata.test`

**Interfaces:**
- Consumes: `MetadataTableUtils::GetMetadataTableTypes/Names/CreateMetadataChunk` (metadata_table.hpp), `EpsgCodeFromReferenceSystem` (crs_projjson.hpp).

- [ ] **Step 1: Failing test**

```
# name: test/sql/obj_metadata.test
# description: obj_metadata reports the vertex extent and the caller-supplied CRS in cityjson_metadata's shape
# group: [sql]

require cityjson

# Same columns as cityjson_metadata: what COPY's metadata_query and cityparquet_write consume.
query I
SELECT list_aggregate(list(column_name ORDER BY column_index), 'string_agg', ',')
FROM (SELECT column_name, row_number() OVER () AS column_index FROM (DESCRIBE SELECT * FROM obj_metadata('test/data/obj/cube.obj')));
----
id,version,identifier,title,reference_date,transform_scale,transform_translate,geographical_extent,reference_system,point_of_contact,city_objects_count,features_count

query III
SELECT geographical_extent, reference_system, city_objects_count FROM obj_metadata('test/data/obj/cube.obj');
----
{'min_x': 0.0, 'min_y': 0.0, 'min_z': 0.0, 'max_x': 12.0, 'max_y': 2.0, 'max_z': 1.0}	NULL	2

# Short, URN and OGC forms all resolve; the struct is the OGC decomposition.
query I
SELECT reference_system FROM obj_metadata('test/data/obj/cube.obj', crs := 'EPSG:7415');
----
{'base_url': https://www.opengis.net/def/crs/, 'authority': EPSG, 'version': 0, 'code': 7415}

query I
SELECT reference_system.code FROM obj_metadata('test/data/obj/cube.obj', crs := 'https://www.opengis.net/def/crs/EPSG/0/28992');
----
28992

statement error
SELECT * FROM obj_metadata('test/data/obj/cube.obj', crs := 'Amersfoort');
----
obj_metadata: crs 'Amersfoort' is not an EPSG reference

# The CRS reaches a writer the way any source's does: through metadata_query.
statement ok
COPY (SELECT * FROM read_obj('test/data/obj/cube.obj', lod := '2.2'))
TO '__TEST_DIR__/cube_from_obj.city.jsonl'
(FORMAT cityjsonseq, metadata_query 'SELECT reference_system AS crs FROM obj_metadata(''test/data/obj/cube.obj'', crs := ''EPSG:7415'')');

query I
SELECT reference_system.code FROM cityjsonseq_metadata('__TEST_DIR__/cube_from_obj.city.jsonl');
----
7415
```

Check the exact struct field names of `geographical_extent` against `test/sql/cityjson_metadata.test` before running; copy that file's spelling.

- [ ] **Step 2: Run it and watch it fail**

Run: `just rebuild && ./build/release/test/unittest "test/sql/obj_metadata.test"`
Expected: FAIL — `obj_metadata does not exist`.

- [ ] **Step 3: Implement**

Append to `src/cityjson/obj_table_function.cpp` (includes: `"cityjson/crs_projjson.hpp"`, `"cityjson/metadata_table.hpp"`):

```cpp
namespace {

struct OBJMetadataBindData : public TableFunctionData {
	std::string file_name;
	CityJSON metadata;
	idx_t count = 0;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<OBJMetadataBindData>();
		result->file_name = file_name;
		result->metadata = metadata;
		result->count = count;
		return std::move(result);
	}
	bool Equals(const FunctionData &other) const override {
		auto &o = other.Cast<OBJMetadataBindData>();
		return file_name == o.file_name && metadata.metadata.has_value() == o.metadata.metadata.has_value() &&
		       (!metadata.metadata.has_value() ||
		        metadata.metadata->reference_system == o.metadata.metadata->reference_system);
	}
};

struct OBJMetadataGlobalState : public GlobalTableFunctionState {
	bool done = false;
	idx_t MaxThreads() const override {
		return 1;
	}
};

unique_ptr<FunctionData> OBJMetadataBind(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.empty()) {
		throw BinderException("obj_metadata requires a file path");
	}
	auto result = make_uniq<OBJMetadataBindData>();
	result->file_name = StringValue::Get(input.inputs[0]);

	OBJReadOptions options;
	options.lod = "0.0"; // irrelevant to metadata; the reader needs one
	for (auto &kv : input.named_parameters) {
		if (kv.first == "crs") {
			auto text = StringValue::Get(kv.second);
			auto code = EpsgCodeFromReferenceSystem(text);
			if (!code.has_value()) {
				throw BinderException("obj_metadata: crs '" + text + "' is not an EPSG reference (use EPSG:7415, "
				                      "urn:ogc:def:crs:EPSG::7415 or https://www.opengis.net/def/crs/EPSG/0/7415)");
			}
			options.crs = "https://www.opengis.net/def/crs/EPSG/0/" + std::to_string(code.value());
		}
	}

	OBJReader reader(context, result->file_name, options);
	try {
		result->metadata = reader.ReadMetadata();
		result->count = reader.CountCityObjects();
	} catch (const CityJSONError &e) {
		throw BinderException("obj_metadata: failed to read '" + result->file_name + "': " + e.what());
	}
	return_types = MetadataTableUtils::GetMetadataTableTypes();
	names = MetadataTableUtils::GetMetadataTableNames();
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> OBJMetadataInitGlobal(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<OBJMetadataGlobalState>();
}

void OBJMetadataScan(ClientContext &, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<OBJMetadataBindData>();
	auto &state = data.global_state->Cast<OBJMetadataGlobalState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	auto chunk = MetadataTableUtils::CreateMetadataChunk(bind_data.metadata, optional_idx(bind_data.count),
	                                                     optional_idx(bind_data.count));
	output.SetCardinality(1);
	for (idx_t col = 0; col < chunk->ColumnCount(); col++) {
		output.data[col].Reference(chunk->data[col]);
	}
	state.done = true;
}

} // namespace
```

and in `RegisterOBJTableFunctions`:

```cpp
	TableFunction obj_metadata("obj_metadata", {LogicalType::VARCHAR}, OBJMetadataScan, OBJMetadataBind);
	obj_metadata.named_parameters["crs"] = LogicalType::VARCHAR;
	obj_metadata.init_global = OBJMetadataInitGlobal;
	loader.RegisterFunction(obj_metadata);
```

`CreateMetadataChunk` reads the CRS from `cityjson.crs` first, then `metadata.reference_system`; the reader fills the latter, which `ParseCRSUri` decomposes because it is the OGC URL form.

- [ ] **Step 4: Run**

Run: `just rebuild && ./build/release/test/unittest "test/sql/obj_metadata.test"`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/cityjson/obj_table_function.cpp test/sql/obj_metadata.test
git commit -m "feat(obj): obj_metadata -- vertex extent and caller-supplied CRS"
```

---

### Task 9: Full suite

The FileSystem route is structural (`json_utils::ReadFileContent` is the only way bytes
enter `OBJReader` and `DuckDBMaterialReader`); `test/sql/cityjson_remote.test` is the
pattern to copy if a network-gated `read_obj` test is ever wanted.

- [ ] **Step 1: Run the whole suite**

Run: `just rebuild && make test`
Expected: everything green, including the untouched appearance suites.

---

### Task 10: `read_obj` as a COPY source

The CityJSON writers inherit a discovered source's metadata and appearance definitions
(`LoadSourceAppearance`); without this, `COPY (SELECT * FROM read_obj(...)) TO 'x.city.jsonl'`
writes material refs into an `appearance` array that does not exist. The reader must be
nameable by `FindCopySourceRef`, and its definitions must be re-emitted as JSON.

**Files:**
- Modify: `src/include/cityjson/cityjson_types.hpp`, `src/cityjson/cityjson_types.cpp` (`Material::ToJson`, `Texture::ToJson`, `Appearance::ToJson`)
- Modify: `src/include/cityjson/copy_source_ref.hpp`, `src/cityjson/copy_source_ref.cpp` (`is_obj`)
- Modify: `src/cityjson/copy_function.cpp` (source inheritance, `LoadSourceAppearance`)
- Test: `test/sql/obj_metadata.test` (extend the final COPY), `test/sql/read_obj.test` (append)

**Interfaces:**
- Produces: `json Material::ToJson() const; json Texture::ToJson() const; json Appearance::ToJson() const;` (CityJSON key names: `name`, `ambientIntensity`, `diffuseColor`, `specularColor`, `emissiveColor`, `transparency`, `shininess`, `isSmooth`, `other` members merged back; textures `image`, `type`, `wrapMode`, `textureType`, `borderColor`; `vertices-texture`); `CopySourceRef::is_obj`.

- [ ] **Step 1: Failing tests**

Append to `test/sql/obj_metadata.test`, after the existing `reference_system.code` check of `cube_from_obj.city.jsonl`:

```
# The definitions travel too: refs in the written file resolve against a real appearance block.
query II
SELECT (SELECT COUNT(*) FROM cityjson_materials('__TEST_DIR__/cube_from_obj.city.jsonl')),
       (SELECT COUNT(*) FROM cityjson_textures('__TEST_DIR__/cube_from_obj.city.jsonl'));
----
3	1

query I
SELECT name FROM cityjson_materials('__TEST_DIR__/cube_from_obj.city.jsonl') ORDER BY id;
----
GroundSurface
brick
RoofSurface
```

Append to `test/sql/read_obj.test`:

```
# --- OBJ -> CityJSON keeps geometry, semantics and appearance refs intact on the way out.
statement ok
COPY (SELECT * FROM read_obj('test/data/obj/cube.obj', lod := '2.2')) TO '__TEST_DIR__/cube.city.json' (FORMAT cityjson);

query III
SELECT id, geometry_properties_lod2_2.type, material_lod2_2
FROM read_cityjson('__TEST_DIR__/cube.city.json', lod := '2.2') ORDER BY id;
----
cube	Solid	{"visual":{"values":[[0,1,1,1,1,2]]}}
slab	MultiSurface	{"visual":{"values":[2]}}
```

Run: `just rebuild && ./build/release/test/unittest "test/sql/obj_metadata.test"`
Expected: FAIL — 0 materials in the written file.

- [ ] **Step 2: Serialise appearance definitions**

`cityjson_types.hpp`: add `json ToJson() const;` to `Material`, `Texture` and `Appearance`. `cityjson_types.cpp`:

```cpp
json Material::ToJson() const {
	json out = other.is_object() ? other : json::object();
	if (name) { out["name"] = *name; }
	if (ambient_intensity) { out["ambientIntensity"] = *ambient_intensity; }
	if (diffuse_color) { out["diffuseColor"] = *diffuse_color; }
	if (specular_color) { out["specularColor"] = *specular_color; }
	if (emissive_color) { out["emissiveColor"] = *emissive_color; }
	if (transparency) { out["transparency"] = *transparency; }
	if (shininess) { out["shininess"] = *shininess; }
	if (is_smooth) { out["isSmooth"] = *is_smooth; }
	return out;
}

json Texture::ToJson() const {
	json out = other.is_object() ? other : json::object();
	if (image_uri) { out["image"] = *image_uri; }
	if (image_type) { out["type"] = *image_type; }
	if (wrap_mode) { out["wrapMode"] = *wrap_mode; }
	if (texture_type) { out["textureType"] = *texture_type; }
	if (border_color) { out["borderColor"] = *border_color; }
	return out;
}

json Appearance::ToJson() const {
	json out = json::object();
	if (!materials.empty()) {
		out["materials"] = json::array();
		for (const auto &m : materials) { out["materials"].push_back(m.ToJson()); }
	}
	if (!textures.empty()) {
		out["textures"] = json::array();
		for (const auto &t : textures) { out["textures"].push_back(t.ToJson()); }
	}
	if (!vertices_texture.empty()) {
		out["vertices-texture"] = json::array();
		for (const auto &uv : vertices_texture) { out["vertices-texture"].push_back(json::array({uv[0], uv[1]})); }
	}
	return out;
}
```

(Expand the one-line `if` bodies to the repository's brace style when pasting; clang-format will.)

- [ ] **Step 3: Name the reader in the source ref**

`copy_source_ref.hpp`: add `bool is_obj = false;` to `CopySourceRef` with the comment `//! read_obj: metadata and appearance come from an OBJReader, not a CityJSON file.`

`copy_source_ref.cpp`, in the function-name chain: `} else if (call.function_name == "read_obj") { found.is_obj = true; }`.

- [ ] **Step 4: Inherit through an OBJReader**

`copy_function.cpp`, include `"cityjson/obj_reader.hpp"`. In `CityJSONCopyToBind`'s inheritance block replace
`auto reader = OpenAnyCityJSONFile(context, source_ref.path, 1);` with

```cpp
			std::unique_ptr<CityJSONReader> reader;
			if (source_ref.is_obj) {
				OBJReadOptions obj_options;
				obj_options.lod = "0.0"; // the definitions are file-global; any LoD serves
				reader = std::make_unique<OBJReader>(context, source_ref.path, obj_options);
			} else {
				reader = OpenAnyCityJSONFile(context, source_ref.path, 1);
			}
```

In `LoadSourceAppearance`, at the top, before `auto content = …`:

```cpp
	if (source_ref.is_obj) {
		// No JSON to lift a block from: re-emit the reader's definitions as CityJSON.
		OBJReadOptions obj_options;
		obj_options.lod = "0.0";
		OBJReader reader(context, source_ref.path, obj_options);
		auto header = reader.ReadMetadata();
		if (header.appearance.has_value() && !header.appearance->Empty()) {
			bind_data.source_appearance_header = header.appearance->ToJson();
		}
		return;
	}
```

Also fix the `metadata_from` branch from Plan 2's Task 1 if it has already landed: `ref.is_obj = StringUtil::EndsWith(StringUtil::Lower(explicit_metadata_from), ".obj");`. If this plan runs first, add `is_obj` detection to the existing branch (`ref.is_obj = …` alongside the current `is_seq`/`is_fcb` assignments).

- [ ] **Step 5: Run**

Run: `just rebuild && ./build/release/test/unittest "test/sql/obj_metadata.test" && ./build/release/test/unittest "test/sql/read_obj.test"`
Expected: PASS. The written CityJSON's `appearance.vertices-texture` is the whole `vt` pool and the texture refs are the local indices the reader produced, so `cityjson_textures` finds exactly one texture.

- [ ] **Step 6: Commit**

```bash
git add src/include/cityjson/cityjson_types.hpp src/cityjson/cityjson_types.cpp \
        src/include/cityjson/copy_source_ref.hpp src/cityjson/copy_source_ref.cpp \
        src/cityjson/copy_function.cpp test/sql/obj_metadata.test test/sql/read_obj.test
git commit -m "feat(obj): read_obj is a discoverable COPY source -- its materials and textures travel"
```

---

### Task 11: Documentation

**Files:**
- Modify: `docs/FUNCTIONS.md` (Contents + a new `## Mesh interchange` section before `## Output schema`), `docs/DESIGN_DOC.md` (§4 Readers), `docs/TRAPS.md` (§Readers)

- [ ] **Step 1: FUNCTIONS.md**

Add to the Contents list: `- [Mesh interchange](#mesh-interchange) — \`read_obj\`, \`obj_materials\`, \`obj_textures\`, \`obj_metadata\``.

Add the section (every example must be run against the build and its real output pasted; the values below are the hand-derived ones and must match what you see):

````markdown
## Mesh interchange

### `read_obj(path, lod := …)`

Reads a Wavefront OBJ into the object-table schema. An OBJ carries no level of
detail, class, or CRS, so the caller supplies them: `lod` is required and names
the geometry columns; `object_type` (default `'Building'`) is every object's
class; the CRS lives on `obj_metadata` (below), because a row has nowhere to
carry it.

```sql
SELECT id, object_type, geometry_properties_lod2_2.type AS geom, geometry_properties_lod2_2.surfaces
FROM read_obj('test/data/obj/cube.obj', lod := '2.2');
-- cube | Building | Solid        | [{"type":"GroundSurface"},{"type":"WallSurface"},{"type":"RoofSurface"}]
-- slab | Building | MultiSurface | [{"type":"RoofSurface"}]
```

| Parameter | Type | Description |
| --- | --- | --- |
| `lod` | VARCHAR | **Required.** Normalised like every reader: `'2'` yields `geometry_lod2_0` |
| `object_type` | VARCHAR | CityGML class for every object (default `Building`) |
| `geometry_type` | VARCHAR | `'auto'` (default): `Solid` when every edge is shared by exactly two faces, else `MultiSurface`; or force `'Solid'` / `'MultiSurface'` |
| `appearance` | VARCHAR | `'local'` (default) or `'sidecar'`, as on the other readers |
| `sample_lines` | BIGINT | Accepted for uniformity; the whole file is parsed regardless |

Mapping:

- Each `o` is one object; its name is `id` and `feature_id`. A file without `o`
  is one object named after the file stem. A repeated `o` name resumes that
  object.
- All faces of an object form one geometry, one outer ring each, in file order
  and with the file's winding. OBJ cannot express holes.
- A `usemtl` name that is a CityJSON semantic surface type (`RoofSurface`,
  `WallSurface`, `GroundSurface`, …, or any `+`-prefixed extension name) is the
  face's surface; failing that, the `g` name under the same rule. `usemtl`/`g`
  state persists across `o`, as OBJ specifies.
- `.mtl` materials are the appearance definitions in `mtllib` order
  (`obj_materials`); a material with `map_Kd` is also a texture
  (`obj_textures`), and faces under it with `v/vt` indices carry
  `texture_lod*` rings that index the file's `vt` list (`'sidecar'` inlines
  them). Faces without UVs carry `[null]`.
- Positive and negative (relative) indices; `v` with a fourth component; `vn`
  ignored. A line ending in `\` (continuation) is refused. A missing `.mtl`
  is a warning and the geometry still reads.
- Coordinates are taken as written, Z-up, no axis swap — what cjio, 3dfier and
  geoflow write. A `# origin x y z` header comment, as the OBJ writer emits, is
  added back.

### `obj_materials(path)` / `obj_textures(path)`

The `.mtl` as `materials.parquet` / `textures.parquet` rows, same columns as
`cityjson_materials` / `cityjson_textures`. `Kd`→`diffuseColor`, `Ks`→`specularColor`,
`Ke`→`emissiveColor`, mean `Ka`→`ambientIntensity`, `1 − d`→`transparency`,
`Ns / 1000`→`shininess`; `map_Kd`→`image_uri` with `image_type` from the extension,
`wrapMode` `wrap`, `textureType` `unknown`, `image_data` NULL.

### `obj_metadata(path [, crs := …])`

One row in `cityjson_metadata`'s shape: `geographical_extent` from every vertex,
`reference_system` from `crs` (`EPSG:7415`, the URN or the OGC URL; anything else
is an error), everything else NULL. This is where the CRS of an OBJ enters the
stack:

```sql
COPY (SELECT * FROM read_obj('campus.obj', lod := '2.2')) TO 'campus.city.jsonl'
(FORMAT cityjsonseq,
 metadata_query 'SELECT reference_system AS crs FROM obj_metadata(''campus.obj'', crs := ''EPSG:7415'')');
```
````

- [ ] **Step 2: DESIGN_DOC.md §4 Readers** — add one paragraph:

```markdown
`OBJReader` is the fourth reader and the first for a non-CityJSON source. It
parses the file once into `CityJSONFeature` records — one per `o`, each with its
own vertex pool — and a `CityJSON` header holding the `.mtl` materials, the
textured ones as textures, and the `vt` list as the UV pool, so the generic bind
and scan (WKB, `geometry_properties`, `bbox`, sidecar normalisation) apply
unchanged. What an OBJ cannot say is asked of the caller (`lod`, `object_type`,
`geometry_type`) or resolved by convention (semantic surfaces from `usemtl` / `g`
names). The appearance sidecar functions take their reader from a
`TableFunctionInfo`, which is what lets one bind serve both input formats.
```

- [ ] **Step 3: TRAPS.md §Readers** — add:

```markdown
- **tinyobjloader's mainline `LoadObj` collapses `o` and `g` into one name.**
  `OBJReader` uses the callback API for that reason; the callbacks hand over raw
  face tokens (1-based, negative = relative, 0 = absent), so index resolution is
  ours. Its `mtllib` handling stops at the first file of a line that loads, and
  it does not implement backslash continuation — the reader refuses such lines
  rather than mis-parse them.
- **OBJ state persists across `o`.** A `usemtl` or `g` before an `o` still
  governs the faces after it. `cube.obj`'s slab pins this.
```

- [ ] **Step 4: Copy `CLAUDE.md` to `AGENTS.md` if either changed** (they did not in this plan; skip unless you touched them).

- [ ] **Step 5: Commit**

```bash
git add docs/FUNCTIONS.md docs/DESIGN_DOC.md docs/TRAPS.md
git commit -m "docs(obj): read_obj, obj_materials, obj_textures, obj_metadata"
```

---

## Self-review notes

- Spec coverage, Part 1: surface (T3, T7, T8), CRS placement (T8), objects/geometry/semantics (T4), materials/textures (T5, T7), indices/continuation/origin/missing-mtl (T6), FileSystem route (T3 impl), reader nameable by `FindCopySourceRef` with definitions carried (T10), docs (T11). Dependencies section: T1.
- Naming used consistently: `OBJReader`, `OBJReadOptions`, `ParseOBJReadOptions`, `RegisterOBJTableFunctions`, `CreateAppearanceTableFunction`, `ReaderOpener`, `SidecarKind`.
- The `ObjectCb` rotate variant in Task 3 Step 4 is superseded by the `current_object` variant given immediately after it; implement the latter.
