# Mesh Model and OBJ Writer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `COPY … TO 'x.obj' (FORMAT obj)` writes a CityParquet-shaped relation as Wavefront OBJ + MTL, with the shared render model, hole-aware triangulation and appearance resolution the glTF writer (next plan) reuses.

**Architecture:** The existing COPY bind/sink/combine already rebuild each row as a CityJSON object with coordinates, semantics and appearance refs; only `Finalize` changes. A new `MeshModel` is built from those objects once per COPY (`mesh_model.cpp`), appearance ids are resolved through an `AppearanceSource` that is either the discovered source's local blocks or the two `*_query` options (`appearance_source.cpp`), holed faces are triangulated with earcut (`face_triangulation.cpp`), and `obj_writer.cpp` serialises. The bind's two format booleans become a `CopyFormat` enum.

**Tech Stack:** C++20, nlohmann-json, `mapbox/earcut.hpp` (already on the include path from the OBJ-reader plan, Task 1), DuckDB `CopyFunction`, sqllogictest.

**Spec:** `docs/superpowers/specs/2026-09-02-obj-gltf-interchange-design.md` — Part 2 up to and including *OBJ output*, *Sidecar files and DuckDB's temp-rename*, *Testing*.

**Prerequisite:** the OBJ-reader plan (`2026-09-02-obj-reader.md`) is merged: its Task 1 adds the ports and include dirs, its fixtures are reused here, and its `read_obj` provides the round-trip oracle.

## Global Constraints

- Sidecar output files (`.mtl`, copied images) are named from the **final** `bind_data.file_path`, never from `gstate.temp_file_path`; `mtllib` references the final basename.
- Main-file writes go to `gstate.temp_file_path` (DuckDB renames it afterwards). Files are written with `std::ofstream`, as the existing writers are; **reads** (source appearance, images) go through `duckdb::FileSystem`.
- `materials_query` / `textures_query` present ⇒ sidecar form; absent ⇒ local form; a discovered source read with `appearance := 'sidecar'` and no query option is refused at bind (mesh formats only).
- Coordinates in the OBJ are Z-up, no axis swap; doubles formatted with `precision` significant digits (17 = shortest round trip).
- No changelog voice in docs; British English.
- Build/test cycle: `just rebuild` then `./build/release/test/unittest "test/sql/<name>.test"`.

---

## File structure

| File | Responsibility |
| --- | --- |
| `src/include/cityjson/mesh_model.hpp`, `src/cityjson/mesh_model.cpp` | `MeshModel` types; `BuildMeshModel` from the COPY's collected objects; default colours; LoD choice; origin |
| `src/include/cityjson/appearance_source.hpp`, `src/cityjson/appearance_source.cpp` | `AppearanceSource`: resolved material/texture definitions and UV lookup, from local blocks or from the two queries; image byte loading |
| `src/include/cityjson/face_triangulation.hpp`, `src/cityjson/face_triangulation.cpp` | `TriangulateFace`: Newell normal, projection, earcut, winding check |
| `src/include/cityjson/obj_writer.hpp`, `src/cityjson/obj_writer.cpp` | `WriteOBJ` + `.mtl`; `FormatDouble` |
| `src/include/cityjson/copy_function.hpp`, `src/cityjson/copy_function.cpp` | `CopyFormat` enum; mesh options; refusal rule; `Finalize` dispatch; `obj` registration |
| `src/include/cityjson/copy_source_ref.hpp`, `src/cityjson/copy_source_ref.cpp` | `sidecar_appearance` captured from the reader call's `appearance` parameter |
| `src/cityjson_extension.cpp` | registration |
| `test/data/holed_face.city.json` | hand-written CityJSON with a face that has an inner ring |
| `test/cpp/test_face_triangulation.cpp` | kernel test for the triangulator (opt-in, like the other `test/cpp`) |
| `test/sql/copy_obj.test`, `test/sql/copy_mesh_appearance.test` | behaviour pins |
| `docs/FUNCTIONS.md`, `docs/DESIGN_DOC.md`, `docs/TRAPS.md` | docs |

---

### Task 1: `CopyFormat` enum replaces the two booleans

**Files:**
- Modify: `src/include/cityjson/copy_function.hpp` (`CityJSONCopyBindData`: `is_seq`, `is_fcb`)
- Modify: `src/cityjson/copy_function.cpp` (`Copy()`, `CityJSONCopyToBind`, the `metadata_from` branch, `CityJSONCopyToFinalize`)

**Interfaces:**
- Produces: `enum class CopyFormat : uint8_t { CityJSON, CityJSONSeq, FlatCityBuf, Obj, Gltf, Glb };` and `CopyFormat format` on `CityJSONCopyBindData`; `static bool IsMeshFormat(CopyFormat)`.

- [ ] **Step 1: Run the existing COPY suites to have a baseline**

Run: `just rebuild && ./build/release/test/unittest "test/sql/cityjson_copy*.test"`
Expected: PASS (this task is a refactor; these must stay green).

- [ ] **Step 2: Replace the booleans**

In `copy_function.hpp`, before `struct CityJSONCopyBindData`:

```cpp
//! The output format a COPY TO was bound with. One enum, not a boolean per format:
//! every dispatch site names the format it handles, and a new format cannot fall
//! through an `else` into the CityJSON writer.
enum class CopyFormat : uint8_t { CityJSON, CityJSONSeq, FlatCityBuf, Obj, Gltf, Glb };

inline bool IsMeshFormat(CopyFormat format) {
	return format == CopyFormat::Obj || format == CopyFormat::Gltf || format == CopyFormat::Glb;
}
```

In the struct, replace
```cpp
	bool is_seq = false; // true for cityjsonseq format
	bool is_fcb = false; // true for flatcitybuf format
```
with
```cpp
	CopyFormat format = CopyFormat::CityJSON;
```

In `copy_function.cpp`:
- `Copy()`: replace the two field copies with `result->format = format;`.
- `CityJSONCopyToBind`, top: replace the two assignments with
  ```cpp
	const auto &fmt = input.info.format;
	bind_data->format = fmt == "cityjsonseq"  ? CopyFormat::CityJSONSeq
	                    : fmt == "flatcitybuf" ? CopyFormat::FlatCityBuf
	                    : fmt == "obj"         ? CopyFormat::Obj
	                    : fmt == "gltf"        ? CopyFormat::Gltf
	                    : fmt == "glb"         ? CopyFormat::Glb
	                                           : CopyFormat::CityJSON;
  ```
- the `metadata_from` branch: `ref.is_seq = bind_data->format == CopyFormat::CityJSONSeq; ref.is_fcb = bind_data->format == CopyFormat::FlatCityBuf;` — **and** note this is wrong for mesh formats (the source's format is not the output's). Fix it properly now: detect from the path instead:
  ```cpp
	ref.is_fcb = StringUtil::EndsWith(StringUtil::Lower(explicit_metadata_from), ".fcb");
	ref.is_seq = StringUtil::EndsWith(StringUtil::Lower(explicit_metadata_from), ".jsonl");
  ```
- `CityJSONCopyToFinalize`: replace the `if (bind_data.is_seq) … else if (bind_data.is_fcb) … else` chain with a `switch (bind_data.format)` over `CityJSONSeq`, `FlatCityBuf` (inside `#ifdef CITYJSON_HAS_FCB`), `CityJSON`; the three mesh cases `throw InternalException("mesh COPY format bound without a writer")` for now (Task 6 fills them).

- [ ] **Step 3: Rebuild and re-run**

Run: `just rebuild && ./build/release/test/unittest "test/sql/cityjson_copy*.test"`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add src/include/cityjson/copy_function.hpp src/cityjson/copy_function.cpp
git commit -m "refactor(copy): one CopyFormat enum instead of a boolean per format"
```

---

### Task 2: Hole-aware face triangulation

**Files:**
- Create: `src/include/cityjson/face_triangulation.hpp`, `src/cityjson/face_triangulation.cpp`
- Create: `test/cpp/test_face_triangulation.cpp`, `test/cpp/run_face_triangulation_tests.sh`
- Modify: `CMakeLists.txt` (source)

**Interfaces:**
- Produces:
  ```cpp
  using Vertex3 = std::array<double, 3>;
  //! Triangulate one planar face. `rings[0]` is the outer ring, the rest are holes; every
  //! ring is a list of indices into `vertices`, unclosed (first vertex not repeated).
  //! Returns triangles as index triples into `vertices`, wound like the outer ring.
  //! Degenerate input (fewer than 3 outer vertices, zero-area normal) returns empty.
  std::vector<uint32_t> TriangulateFace(const std::vector<Vertex3> &vertices,
                                        const std::vector<std::vector<uint32_t>> &rings);
  Vertex3 NewellNormal(const std::vector<Vertex3> &vertices, const std::vector<uint32_t> &ring);
  ```

- [ ] **Step 1: The failing kernel test**

`test/cpp/test_face_triangulation.cpp` (Catch2 is not vendored here; the existing `test/cpp` harness is a plain executable with `main`, see `test_fcb_selective.cpp` — follow that: asserts via a tiny `CHECK` macro and a non-zero exit on failure):

```cpp
#include "cityjson/face_triangulation.hpp"

#include <cmath>
#include <cstdio>
#include <cstdlib>

using duckdb::cityjson::TriangulateFace;
using duckdb::cityjson::Vertex3;

static int failures = 0;
#define CHECK(cond)                                                                                                    \
	do {                                                                                                               \
		if (!(cond)) {                                                                                                 \
			std::fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);                                        \
			failures++;                                                                                                \
		}                                                                                                              \
	} while (0)

static double TriArea2D(const std::vector<Vertex3> &v, uint32_t a, uint32_t b, uint32_t c) {
	return 0.5 * ((v[b][0] - v[a][0]) * (v[c][1] - v[a][1]) - (v[c][0] - v[a][0]) * (v[b][1] - v[a][1]));
}

int main() {
	// A 4x4 square with a 2x2 square hole, CCW outer, CW hole (CityJSON's inner-ring rule).
	std::vector<Vertex3> v = {{0, 0, 0}, {4, 0, 0}, {4, 4, 0}, {0, 4, 0},   // outer
	                          {1, 1, 0}, {1, 3, 0}, {3, 3, 0}, {3, 1, 0}};  // hole (CW)
	auto tris = TriangulateFace(v, {{0, 1, 2, 3}, {4, 5, 6, 7}});
	CHECK(tris.size() == 8 * 3);
	double area = 0;
	for (size_t i = 0; i + 2 < tris.size(); i += 3) {
		double a = TriArea2D(v, tris[i], tris[i + 1], tris[i + 2]);
		CHECK(a > 0); // every triangle wound like the outer ring (CCW in XY)
		area += a;
	}
	CHECK(std::fabs(area - 12.0) < 1e-9);

	// A vertical wall (normal along -Y): projection must not collapse it.
	std::vector<Vertex3> wall = {{0, 0, 0}, {1, 0, 0}, {1, 0, 1}, {0, 0, 1}};
	auto wt = TriangulateFace(wall, {{0, 1, 2, 3}});
	CHECK(wt.size() == 6);

	// RD-magnitude coordinates: a 1 mm face far from the origin still triangulates.
	std::vector<Vertex3> far = {{85000.000, 446000.000, 5}, {85000.001, 446000.000, 5},
	                            {85000.001, 446000.001, 5}, {85000.000, 446000.001, 5}};
	CHECK(TriangulateFace(far, {{0, 1, 2, 3}}).size() == 6);

	// Degenerate: two vertices.
	CHECK(TriangulateFace(v, {{0, 1}}).empty());

	if (failures == 0) {
		std::printf("face_triangulation: all checks passed\n");
	}
	return failures == 0 ? 0 : 1;
}
```

`test/cpp/run_face_triangulation_tests.sh`:

```bash
#!/usr/bin/env bash
# Opt-in kernel test for the face triangulator. Needs a configured tree
# (build/release/vcpkg_installed) for mapbox/earcut.hpp and nlohmann/json.hpp.
set -euo pipefail
cd "$(dirname "$0")/../.."
INC="$(find build/release/vcpkg_installed -maxdepth 2 -type d -name include | head -1)"
OUT="build/face_triangulation_test"
c++ -std=c++20 -O1 -Isrc/include -I"$INC" -Iduckdb/src/include \
    test/cpp/test_face_triangulation.cpp src/cityjson/face_triangulation.cpp -o "$OUT"
"$OUT"
```

- [ ] **Step 2: Run it and watch it fail**

Run: `chmod +x test/cpp/run_face_triangulation_tests.sh && test/cpp/run_face_triangulation_tests.sh`
Expected: compile error — header does not exist.

- [ ] **Step 3: Implement**

```cpp
// src/include/cityjson/face_triangulation.hpp
#pragma once

#include <array>
#include <cstdint>
#include <vector>

namespace duckdb {
namespace cityjson {

using Vertex3 = std::array<double, 3>;

//! Newell's method: the area-weighted normal of a (possibly non-convex) planar ring.
Vertex3 NewellNormal(const std::vector<Vertex3> &vertices, const std::vector<uint32_t> &ring);

//! Triangulate one planar face. `rings[0]` is the outer ring, the rest are holes; every
//! ring is a list of indices into `vertices`, unclosed. Returns index triples into
//! `vertices`, wound like the outer ring. Degenerate input returns empty.
std::vector<uint32_t> TriangulateFace(const std::vector<Vertex3> &vertices,
                                      const std::vector<std::vector<uint32_t>> &rings);

} // namespace cityjson
} // namespace duckdb
```

```cpp
// src/cityjson/face_triangulation.cpp
#include "cityjson/face_triangulation.hpp"

#include <mapbox/earcut.hpp>

#include <cmath>

namespace duckdb {
namespace cityjson {

Vertex3 NewellNormal(const std::vector<Vertex3> &vertices, const std::vector<uint32_t> &ring) {
	Vertex3 n {0.0, 0.0, 0.0};
	const size_t count = ring.size();
	for (size_t i = 0; i < count; i++) {
		const auto &a = vertices[ring[i]];
		const auto &b = vertices[ring[(i + 1) % count]];
		n[0] += (a[1] - b[1]) * (a[2] + b[2]);
		n[1] += (a[2] - b[2]) * (a[0] + b[0]);
		n[2] += (a[0] - b[0]) * (a[1] + b[1]);
	}
	return n;
}

std::vector<uint32_t> TriangulateFace(const std::vector<Vertex3> &vertices,
                                      const std::vector<std::vector<uint32_t>> &rings) {
	std::vector<uint32_t> out;
	if (rings.empty() || rings[0].size() < 3) {
		return out;
	}
	const Vertex3 n = NewellNormal(vertices, rings[0]);
	const double ax = std::fabs(n[0]);
	const double ay = std::fabs(n[1]);
	const double az = std::fabs(n[2]);
	if (ax == 0.0 && ay == 0.0 && az == 0.0) {
		return out;
	}
	// Drop the dominant axis; keep a right-handed (u, v) pair so the projected outer
	// ring's orientation matches its 3D orientation as seen along the normal.
	int u;
	int v;
	if (az >= ax && az >= ay) {
		u = 0;
		v = 1;
	} else if (ax >= ay) {
		u = 1;
		v = 2;
	} else {
		u = 2;
		v = 0;
	}

	// Shift to the first vertex before projecting: earcut's tests are signed areas, and
	// on absolute projected coordinates (|x| ~ 1e5) those drown for small faces.
	const Vertex3 &o = vertices[rings[0][0]];
	using Point = std::array<double, 2>;
	std::vector<std::vector<Point>> polygon;
	std::vector<uint32_t> flat; // earcut index -> vertex index
	polygon.reserve(rings.size());
	for (const auto &ring : rings) {
		std::vector<Point> pts;
		pts.reserve(ring.size());
		for (uint32_t idx : ring) {
			const auto &p = vertices[idx];
			pts.push_back({p[u] - o[u], p[v] - o[v]});
			flat.push_back(idx);
		}
		polygon.push_back(std::move(pts));
	}

	// Sign of the projected outer ring: the winding every triangle must share.
	double outer_area = 0.0;
	for (size_t i = 0; i < polygon[0].size(); i++) {
		const auto &p = polygon[0][i];
		const auto &q = polygon[0][(i + 1) % polygon[0].size()];
		outer_area += p[0] * q[1] - q[0] * p[1];
	}

	std::vector<uint32_t> tri = mapbox::earcut<uint32_t>(polygon);
	out.reserve(tri.size());
	for (size_t i = 0; i + 2 < tri.size(); i += 3) {
		uint32_t a = tri[i];
		uint32_t b = tri[i + 1];
		uint32_t c = tri[i + 2];
		const auto &pa = polygon[0].size() > a ? polygon[0][a] : Point {0, 0}; // placeholder, replaced below
		(void)pa;
		// Signed area of the triangle in projected space, from the flat point list.
		auto point = [&](uint32_t k) -> Point {
			size_t r = 0;
			while (k >= polygon[r].size()) {
				k -= static_cast<uint32_t>(polygon[r].size());
				r++;
			}
			return polygon[r][k];
		};
		Point A = point(a);
		Point B = point(b);
		Point C = point(c);
		double area = (B[0] - A[0]) * (C[1] - A[1]) - (C[0] - A[0]) * (B[1] - A[1]);
		if ((area < 0) != (outer_area < 0)) {
			std::swap(b, c);
		}
		out.push_back(flat[a]);
		out.push_back(flat[b]);
		out.push_back(flat[c]);
	}
	return out;
}

} // namespace cityjson
} // namespace duckdb
```

Remove the two placeholder lines (`const auto &pa …; (void)pa;`) — they are noise left from drafting; the `point` lambda is the real lookup.

Add `src/cityjson/face_triangulation.cpp` to `EXTENSION_SOURCES` under a new comment `# Mesh interchange (OBJ / glTF writers)`.

- [ ] **Step 4: Run**

Run: `test/cpp/run_face_triangulation_tests.sh`
Expected: `face_triangulation: all checks passed`. If the hole test yields 8 triangles but a negative area, the (u, v) choice for that axis is mirrored — swap `u`/`v` in that branch; the assertion "wound like the outer ring" is the contract.

Also `just rebuild` to prove it compiles inside the extension.

- [ ] **Step 5: Commit**

```bash
git add src/include/cityjson/face_triangulation.hpp src/cityjson/face_triangulation.cpp \
        test/cpp/test_face_triangulation.cpp test/cpp/run_face_triangulation_tests.sh CMakeLists.txt
git commit -m "feat(mesh): hole-aware face triangulation on earcut"
```

---

### Task 3: Appearance source — local blocks or sidecar queries

**Files:**
- Create: `src/include/cityjson/appearance_source.hpp`, `src/cityjson/appearance_source.cpp`
- Modify: `CMakeLists.txt`

**Interfaces:**
- Produces:
  ```cpp
  struct MeshMaterial { std::string name; std::array<double,3> diffuse{0.7,0.7,0.7};
                        std::optional<std::array<double,3>> specular, emissive; double transparency = 0.0;
                        std::optional<double> shininess; };
  struct MeshTexture  { std::string image_uri; std::string image_type; std::string wrap_mode = "wrap";
                        std::vector<uint8_t> image_data; };   // bytes possibly empty until LoadImage
  class AppearanceSource {
  public:
    static AppearanceSource FromLocal(const std::optional<json> &header, const std::map<std::string, json> &by_feature);
    static AppearanceSource FromQueries(ClientContext &, const std::optional<std::string> &materials_query,
                                        const std::optional<std::string> &textures_query);
    bool IsSidecar() const;
    std::optional<int64_t> ResolveMaterial(const std::string &feature_id, int64_t ref) const;  // -> global id
    std::optional<int64_t> ResolveTexture (const std::string &feature_id, int64_t ref) const;
    std::optional<std::array<double,2>> UV(const std::string &feature_id, const json &uv_ref) const; // int (local) or [u,v]
    const std::map<int64_t, MeshMaterial> &Materials() const;
    std::map<int64_t, MeshTexture> &Textures();
    //! Fill image_data from image_uri (relative to `base_dir`) when empty; false + warning text on failure.
    bool LoadImage(ClientContext &, int64_t texture_id, const std::string &base_dir, std::string &warning);
  };
  ```
- Consumes: `AppearanceIndex` (appearance_normalise.hpp), `Appearance::FromJson`, `Material`, `Texture`, `json_utils::ReadFileContent`, `Connection`.

- [ ] **Step 1: Write the header**

```cpp
// src/include/cityjson/appearance_source.hpp
#pragma once

#include "cityjson/appearance_normalise.hpp"
#include "cityjson/json_utils.hpp"
#include "duckdb.hpp"

#include <array>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace duckdb {
namespace cityjson {

//! A material as a mesh writer needs it: colour and transparency, nothing CityJSON-shaped.
struct MeshMaterial {
	std::string name;
	std::array<double, 3> diffuse {0.7, 0.7, 0.7};
	std::optional<std::array<double, 3>> specular;
	std::optional<std::array<double, 3>> emissive;
	double transparency = 0.0;
	std::optional<double> shininess;
};

//! A texture as a mesh writer needs it. `image_data` is empty until LoadImage fills it
//! from `image_uri` (or the sidecar's own bytes).
struct MeshTexture {
	std::string image_uri;
	std::string image_type; // "PNG", "JPG"
	std::string wrap_mode = "wrap";
	std::vector<uint8_t> image_data;
};

/**
 * Where a COPY's appearance references resolve.
 *
 * `material_lod*` cells look identical in local form (feature-local indices into the
 * source's own blocks) and sidecar form (dataset-global ids into materials.parquet), so
 * the COPY is told which it has: materials_query / textures_query present means sidecar,
 * absent means local. This class hides the difference from the mesh writers.
 */
class AppearanceSource {
public:
	//! Local form: the discovered source's raw `appearance` header block and, for
	//! CityJSONSeq, each feature's own block (its refs are local to that block).
	static AppearanceSource FromLocal(const std::optional<json> &header,
	                                  const std::map<std::string, json> &by_feature);
	//! Sidecar form: rows shaped like cityjson_materials / cityjson_textures.
	static AppearanceSource FromQueries(ClientContext &context, const std::optional<std::string> &materials_query,
	                                    const std::optional<std::string> &textures_query);

	bool IsSidecar() const {
		return sidecar_;
	}
	std::optional<int64_t> ResolveMaterial(const std::string &feature_id, int64_t ref) const;
	std::optional<int64_t> ResolveTexture(const std::string &feature_id, int64_t ref) const;
	//! A ring's UV element: an integer index into the feature's (or header's) pool in
	//! local form, an inline [u, v] pair in sidecar form. nullopt when unresolvable; an
	//! integer in sidecar form throws InvalidInputException (the declared form is wrong).
	std::optional<std::array<double, 2>> UV(const std::string &feature_id, const json &uv_ref) const;

	const std::map<int64_t, MeshMaterial> &Materials() const {
		return materials_;
	}
	std::map<int64_t, MeshTexture> &Textures() {
		return textures_;
	}
	bool LoadImage(ClientContext &context, int64_t texture_id, const std::string &base_dir, std::string &warning);

private:
	bool sidecar_ = false;
	AppearanceIndex index_; // local form only
	std::vector<std::array<double, 2>> header_uv_pool_;
	std::map<std::string, std::vector<std::array<double, 2>>> uv_pool_by_feature_;
	std::map<int64_t, MeshMaterial> materials_;
	std::map<int64_t, MeshTexture> textures_;
};

} // namespace cityjson
} // namespace duckdb
```

- [ ] **Step 2: Implement**

```cpp
// src/cityjson/appearance_source.cpp
#include "cityjson/appearance_source.hpp"

#include "cityjson/cityjson_types.hpp"
#include "cityjson/error.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/client_context.hpp"

#include <algorithm>
#include <cctype>

namespace duckdb {
namespace cityjson {

namespace {

std::optional<std::array<double, 3>> Triple(const std::optional<std::vector<double>> &v) {
	if (!v.has_value() || v->size() < 3) {
		return std::nullopt;
	}
	return std::array<double, 3> {(*v)[0], (*v)[1], (*v)[2]};
}

MeshMaterial FromMaterial(const Material &m, int64_t id) {
	MeshMaterial out;
	out.name = m.name.value_or("material_" + std::to_string(id));
	if (auto d = Triple(m.diffuse_color)) {
		out.diffuse = *d;
	}
	out.specular = Triple(m.specular_color);
	out.emissive = Triple(m.emissive_color);
	out.transparency = m.transparency.value_or(0.0);
	out.shininess = m.shininess;
	return out;
}

MeshTexture FromTexture(const Texture &t) {
	MeshTexture out;
	out.image_uri = t.image_uri.value_or("");
	out.image_type = t.image_type.value_or("");
	out.wrap_mode = t.wrap_mode.value_or("wrap");
	return out;
}

std::optional<std::array<double, 3>> TripleFromList(const Value &v) {
	if (v.IsNull() || v.type().id() != LogicalTypeId::LIST) {
		return std::nullopt;
	}
	auto &children = ListValue::GetChildren(v);
	if (children.size() < 3) {
		return std::nullopt;
	}
	return std::array<double, 3> {children[0].GetValue<double>(), children[1].GetValue<double>(),
	                              children[2].GetValue<double>()};
}

} // namespace

AppearanceSource AppearanceSource::FromLocal(const std::optional<json> &header,
                                             const std::map<std::string, json> &by_feature) {
	AppearanceSource src;
	src.sidecar_ = false;
	CityJSON doc;
	if (header.has_value()) {
		doc.appearance = Appearance::FromJson(header.value());
		src.header_uv_pool_ = doc.appearance->vertices_texture;
	}
	std::vector<CityJSONFeature> features;
	for (const auto &kv : by_feature) {
		CityJSONFeature f(kv.first);
		f.appearance = Appearance::FromJson(kv.second);
		src.uv_pool_by_feature_[kv.first] = f.appearance->vertices_texture;
		features.push_back(std::move(f));
	}
	src.index_ = AppearanceIndex::Build(doc, features);
	for (size_t i = 0; i < src.index_.materials.size(); i++) {
		src.materials_[static_cast<int64_t>(i)] = FromMaterial(src.index_.materials[i], static_cast<int64_t>(i));
	}
	for (size_t i = 0; i < src.index_.textures.size(); i++) {
		src.textures_[static_cast<int64_t>(i)] = FromTexture(src.index_.textures[i]);
	}
	return src;
}

AppearanceSource AppearanceSource::FromQueries(ClientContext &context, const std::optional<std::string> &materials_query,
                                               const std::optional<std::string> &textures_query) {
	AppearanceSource src;
	src.sidecar_ = true;
	// A separate connection: the COPY bind holds the current one (see ParseMetadataFromQuery).
	Connection conn(*context.db);
	auto column = [](const vector<string> &names, const char *name) -> idx_t {
		for (idx_t i = 0; i < names.size(); i++) {
			if (StringUtil::Lower(names[i]) == StringUtil::Lower(name)) {
				return i;
			}
		}
		return DConstants::INVALID_INDEX;
	};
	auto get = [](DataChunk &chunk, idx_t col, idx_t row) -> Value {
		return col == DConstants::INVALID_INDEX ? Value() : chunk.data[col].GetValue(row);
	};

	if (materials_query.has_value()) {
		auto result = conn.Query(materials_query.value());
		if (result->HasError()) {
			throw BinderException("materials_query failed: " + result->GetError());
		}
		auto c_id = column(result->names, "id");
		if (c_id == DConstants::INVALID_INDEX) {
			throw BinderException("materials_query must return an `id` column (materials.parquet shape)");
		}
		auto c_name = column(result->names, "name");
		auto c_diff = column(result->names, "diffuseColor");
		auto c_spec = column(result->names, "specularColor");
		auto c_emis = column(result->names, "emissiveColor");
		auto c_tran = column(result->names, "transparency");
		auto c_shin = column(result->names, "shininess");
		while (auto chunk = result->Fetch()) {
			for (idx_t row = 0; row < chunk->size(); row++) {
				auto id_val = get(*chunk, c_id, row);
				if (id_val.IsNull()) {
					continue;
				}
				int64_t id = id_val.GetValue<int64_t>();
				MeshMaterial m;
				auto name = get(*chunk, c_name, row);
				m.name = name.IsNull() ? "material_" + std::to_string(id) : name.ToString();
				if (auto d = TripleFromList(get(*chunk, c_diff, row))) {
					m.diffuse = *d;
				}
				m.specular = TripleFromList(get(*chunk, c_spec, row));
				m.emissive = TripleFromList(get(*chunk, c_emis, row));
				auto t = get(*chunk, c_tran, row);
				m.transparency = t.IsNull() ? 0.0 : t.GetValue<double>();
				auto s = get(*chunk, c_shin, row);
				if (!s.IsNull()) {
					m.shininess = s.GetValue<double>();
				}
				src.materials_[id] = std::move(m);
			}
		}
	}
	if (textures_query.has_value()) {
		auto result = conn.Query(textures_query.value());
		if (result->HasError()) {
			throw BinderException("textures_query failed: " + result->GetError());
		}
		auto c_id = column(result->names, "id");
		if (c_id == DConstants::INVALID_INDEX) {
			throw BinderException("textures_query must return an `id` column (textures.parquet shape)");
		}
		auto c_uri = column(result->names, "image_uri");
		auto c_data = column(result->names, "image_data");
		auto c_type = column(result->names, "image_type");
		auto c_wrap = column(result->names, "wrapMode");
		while (auto chunk = result->Fetch()) {
			for (idx_t row = 0; row < chunk->size(); row++) {
				auto id_val = get(*chunk, c_id, row);
				if (id_val.IsNull()) {
					continue;
				}
				int64_t id = id_val.GetValue<int64_t>();
				MeshTexture t;
				auto uri = get(*chunk, c_uri, row);
				t.image_uri = uri.IsNull() ? "" : uri.ToString();
				auto type = get(*chunk, c_type, row);
				t.image_type = type.IsNull() ? "" : type.ToString();
				auto wrap = get(*chunk, c_wrap, row);
				t.wrap_mode = wrap.IsNull() ? "wrap" : wrap.ToString();
				auto data = get(*chunk, c_data, row);
				if (!data.IsNull()) {
					auto &blob = StringValue::Get(data);
					t.image_data.assign(blob.begin(), blob.end());
				}
				src.textures_[id] = std::move(t);
			}
		}
	}
	return src;
}

std::optional<int64_t> AppearanceSource::ResolveMaterial(const std::string &feature_id, int64_t ref) const {
	int64_t id = sidecar_ ? ref : index_.ResolveMaterial(feature_id, ref);
	if (id < 0 || materials_.count(id) == 0) {
		return std::nullopt;
	}
	return id;
}

std::optional<int64_t> AppearanceSource::ResolveTexture(const std::string &feature_id, int64_t ref) const {
	int64_t id = sidecar_ ? ref : index_.ResolveTexture(feature_id, ref);
	if (id < 0 || textures_.count(id) == 0) {
		return std::nullopt;
	}
	return id;
}

std::optional<std::array<double, 2>> AppearanceSource::UV(const std::string &feature_id, const json &uv_ref) const {
	if (uv_ref.is_array() && uv_ref.size() >= 2 && uv_ref[0].is_number() && uv_ref[1].is_number()) {
		return std::array<double, 2> {uv_ref[0].get<double>(), uv_ref[1].get<double>()};
	}
	if (!uv_ref.is_number_integer()) {
		return std::nullopt;
	}
	if (sidecar_) {
		// Sidecar form inlines every UV pair (spec, appearance sidecars). A bare index here
		// means the cells are not in the form the *_query options declared -- refuse
		// rather than write a texture-less face and call it success.
		throw InvalidInputException("texture cell of feature '%s' carries a UV index (%s) but materials_query / "
		                            "textures_query declare sidecar form, whose UVs are inlined [u, v] pairs; "
		                            "read the source with appearance := 'sidecar', or drop the *_query options",
		                            feature_id, uv_ref.dump());
	}
	auto idx = uv_ref.get<int64_t>();
	const std::vector<std::array<double, 2>> *pool = &header_uv_pool_;
	auto it = uv_pool_by_feature_.find(feature_id);
	if (it != uv_pool_by_feature_.end() && !it->second.empty()) {
		pool = &it->second;
	}
	if (idx < 0 || static_cast<size_t>(idx) >= pool->size()) {
		return std::nullopt;
	}
	return (*pool)[static_cast<size_t>(idx)];
}

bool AppearanceSource::LoadImage(ClientContext &context, int64_t texture_id, const std::string &base_dir,
                                 std::string &warning) {
	auto it = textures_.find(texture_id);
	if (it == textures_.end()) {
		warning = "texture " + std::to_string(texture_id) + " is not defined";
		return false;
	}
	auto &tex = it->second;
	if (!tex.image_data.empty()) {
		return true;
	}
	if (tex.image_uri.empty()) {
		warning = "texture " + std::to_string(texture_id) + " has neither image_data nor image_uri";
		return false;
	}
	const bool absolute = tex.image_uri.find("://") != std::string::npos || tex.image_uri.rfind('/', 0) == 0;
	std::string path = absolute || base_dir.empty() ? tex.image_uri : base_dir + "/" + tex.image_uri;
	try {
		auto content = json_utils::ReadFileContent(context, path);
		tex.image_data.assign(content.begin(), content.end());
	} catch (const CityJSONError &e) {
		warning = "texture " + std::to_string(texture_id) + ": could not read '" + path + "': " + e.what();
		return false;
	}
	if (tex.image_type.empty()) {
		auto dot = tex.image_uri.rfind('.');
		std::string ext = dot == std::string::npos ? "" : tex.image_uri.substr(dot + 1);
		std::transform(ext.begin(), ext.end(), ext.begin(), [](unsigned char c) { return std::toupper(c); });
		tex.image_type = ext == "JPEG" ? "JPG" : ext;
	}
	return true;
}

} // namespace cityjson
} // namespace duckdb
```

Add `src/cityjson/appearance_source.cpp` to `EXTENSION_SOURCES` under the mesh comment. `StringUtil` needs `#include "duckdb/common/string_util.hpp"`.

- [ ] **Step 3: Build**

Run: `just rebuild`
Expected: compiles. (Behaviour is pinned through the SQL tests in Tasks 6–7; this class has no SQL surface of its own.)

- [ ] **Step 4: Commit**

```bash
git add src/include/cityjson/appearance_source.hpp src/cityjson/appearance_source.cpp CMakeLists.txt
git commit -m "feat(mesh): AppearanceSource -- local blocks or sidecar queries behind one resolver"
```

---

### Task 4: The mesh model

**Files:**
- Create: `src/include/cityjson/mesh_model.hpp`, `src/cityjson/mesh_model.cpp`
- Modify: `CMakeLists.txt`

**Interfaces:**
- Produces:
  ```cpp
  struct MeshFace { std::vector<std::vector<uint32_t>> rings; std::vector<std::vector<std::array<double,2>>> uvs;
                    int32_t surface = -1; int64_t material = -1; int64_t texture = -1; };
  struct MeshObject { std::string id, object_type, lod; json attributes;
                      std::vector<Vertex3> vertices; std::vector<std::string> surfaces; std::vector<MeshFace> faces; };
  struct MeshModel { std::vector<MeshObject> objects; std::optional<GeographicalExtent> extent;
                     Vertex3 origin{0,0,0}; std::optional<std::string> crs; std::vector<std::string> warnings; };
  struct MeshBuildOptions { std::optional<std::string> lod; std::string origin = "auto"; };
  //! objects: feature_id -> [(id, CityObject json)] as the COPY sink collects them, feature_order for stability.
  MeshModel BuildMeshModel(const std::map<std::string, std::vector<std::pair<std::string, json>>> &objects,
                           const std::vector<std::string> &feature_order, const AppearanceSource &appearance,
                           const MeshBuildOptions &options, const std::optional<std::string> &crs);
  //! Default colour for a face without a material: by surface type, else by object type, else grey.
  std::array<double,3> DefaultColour(const std::string &surface_type, const std::string &object_type);
  //! The material/surface/class name an untextured face is grouped under in OBJ and glTF.
  std::string FaceGroupName(const MeshObject &object, const MeshFace &face, const AppearanceSource &appearance);
  ```

- [ ] **Step 1: Header**

```cpp
// src/include/cityjson/mesh_model.hpp
#pragma once

#include "cityjson/appearance_source.hpp"
#include "cityjson/cityjson_types.hpp"
#include "cityjson/face_triangulation.hpp"
#include "cityjson/json_utils.hpp"

#include <array>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace duckdb {
namespace cityjson {

//! One polygon of an object: rings (outer first) as indices into the object's vertex
//! pool, optional per-ring UVs (same lengths as the rings), and what it is dressed in.
struct MeshFace {
	std::vector<std::vector<uint32_t>> rings;
	std::vector<std::vector<std::array<double, 2>>> uvs; // empty, or one list per ring
	int32_t surface = -1;                                // index into MeshObject::surfaces
	int64_t material = -1;                               // AppearanceSource material id
	int64_t texture = -1;                                // AppearanceSource texture id
};

struct MeshObject {
	std::string id;
	std::string object_type;
	std::string lod;
	json attributes; // object or null
	std::vector<Vertex3> vertices;
	std::vector<std::string> surfaces; // semantic surface types, in first-use order
	std::vector<MeshFace> faces;
};

struct MeshModel {
	std::vector<MeshObject> objects;
	std::optional<GeographicalExtent> extent; // over every vertex, world coordinates
	Vertex3 origin {0.0, 0.0, 0.0};           // subtracted by the writers
	std::optional<std::string> crs;
	std::vector<std::string> warnings;
};

struct MeshBuildOptions {
	std::optional<std::string> lod; // normalised; nullopt = highest per object
	std::string origin = "auto";    // "auto" | "none" | "x,y,z"
};

MeshModel BuildMeshModel(const std::map<std::string, std::vector<std::pair<std::string, json>>> &objects,
                         const std::vector<std::string> &feature_order, const AppearanceSource &appearance,
                         const MeshBuildOptions &options, const std::optional<std::string> &crs);

std::array<double, 3> DefaultColour(const std::string &surface_type, const std::string &object_type);

std::string FaceGroupName(const MeshObject &object, const MeshFace &face, const AppearanceSource &appearance);

//! Parse "x,y,z"; nullopt for anything else.
std::optional<Vertex3> ParseOriginOption(const std::string &text);

} // namespace cityjson
} // namespace duckdb
```

- [ ] **Step 2: Implementation**

```cpp
// src/cityjson/mesh_model.cpp
#include "cityjson/mesh_model.hpp"

#include <algorithm>
#include <cmath>
#include <functional>
#include <sstream>
#include <unordered_map>

namespace duckdb {
namespace cityjson {

namespace {

// How many array levels sit above a face for each CityJSON geometry type.
int FaceDepth(const std::string &type) {
	if (type == "MultiSurface" || type == "CompositeSurface") {
		return 0;
	}
	if (type == "Solid") {
		return 1;
	}
	if (type == "MultiSolid" || type == "CompositeSolid") {
		return 2;
	}
	return -1; // points and lines are not renderable surfaces
}

// Walk `boundaries` down to face level, with the parallel `values` arrays (semantics /
// material at face level, texture one level deeper) kept in step. `values` may be null
// or shorter than the boundaries; a missing entry reads as null.
void ForEachFace(const json &boundaries, const json &sem, const json &mat, const json &tex, int depth,
                 const std::function<void(const json &face, const json &sem_v, const json &mat_v, const json &tex_v)> &fn) {
	auto at = [](const json &arr, size_t i) -> const json & {
		static const json null_value;
		return arr.is_array() && i < arr.size() ? arr[i] : null_value;
	};
	if (depth == 0) {
		if (!boundaries.is_array()) {
			return;
		}
		for (size_t i = 0; i < boundaries.size(); i++) {
			fn(boundaries[i], at(sem, i), at(mat, i), at(tex, i));
		}
		return;
	}
	if (!boundaries.is_array()) {
		return;
	}
	for (size_t i = 0; i < boundaries.size(); i++) {
		ForEachFace(boundaries[i], at(sem, i), at(mat, i), at(tex, i), depth - 1, fn);
	}
}

// The first theme's `values` (or a `value` broadcast is handled by the caller).
const json &FirstThemeValues(const json &themed, json &broadcast_holder) {
	static const json null_value;
	if (!themed.is_object() || themed.empty()) {
		return null_value;
	}
	const json &theme = themed.begin().value();
	if (theme.is_object()) {
		auto v = theme.find("values");
		if (v != theme.end()) {
			return *v;
		}
		auto single = theme.find("value");
		if (single != theme.end()) {
			broadcast_holder = *single; // one id for the whole geometry
			return broadcast_holder;
		}
	}
	return null_value;
}

double LodNumber(const std::string &lod) {
	try {
		return std::stod(lod);
	} catch (...) {
		return -1.0;
	}
}

} // namespace

std::optional<Vertex3> ParseOriginOption(const std::string &text) {
	std::string s = text;
	std::replace(s.begin(), s.end(), ',', ' ');
	std::istringstream in(s);
	Vertex3 o {};
	if (in >> o[0] >> o[1] >> o[2]) {
		return o;
	}
	return std::nullopt;
}

std::array<double, 3> DefaultColour(const std::string &surface_type, const std::string &object_type) {
	// Up3date's semantic palette, then cjio's per-class palette.
	static const std::map<std::string, std::array<double, 3>> by_surface = {
	    {"RoofSurface", {0.9, 0.06, 0.09}},        {"WallSurface", {0.8, 0.8, 0.8}},
	    {"GroundSurface", {0.35, 0.35, 0.35}},    {"ClosureSurface", {0.6, 0.6, 0.7}},
	    {"OuterCeilingSurface", {0.7, 0.7, 0.75}}, {"OuterFloorSurface", {0.5, 0.5, 0.5}},
	    {"Window", {0.4, 0.55, 0.75}},            {"Door", {0.45, 0.3, 0.2}},
	    {"InteriorWallSurface", {0.9, 0.9, 0.85}}, {"CeilingSurface", {0.95, 0.95, 0.95}},
	    {"FloorSurface", {0.6, 0.55, 0.5}},        {"WaterSurface", {0.3, 0.55, 0.85}},
	    {"WaterGroundSurface", {0.25, 0.35, 0.5}}, {"TrafficArea", {0.4, 0.4, 0.4}},
	    {"AuxiliaryTrafficArea", {0.55, 0.55, 0.5}}};
	static const std::map<std::string, std::array<double, 3>> by_class = {
	    {"Building", {0.72, 0.32, 0.22}},        {"BuildingPart", {0.72, 0.32, 0.22}},
	    {"BuildingInstallation", {0.72, 0.32, 0.22}}, {"Road", {0.5, 0.5, 0.5}},
	    {"Railway", {0.4, 0.4, 0.4}},            {"TransportSquare", {0.55, 0.55, 0.55}},
	    {"WaterBody", {0.3, 0.7, 0.9}},          {"PlantCover", {0.35, 0.65, 0.3}},
	    {"SolitaryVegetationObject", {0.3, 0.6, 0.25}}, {"LandUse", {0.85, 0.8, 0.4}},
	    {"Bridge", {0.6, 0.4, 0.7}},             {"BridgePart", {0.6, 0.4, 0.7}},
	    {"Tunnel", {0.15, 0.15, 0.15}},          {"TunnelPart", {0.15, 0.15, 0.15}},
	    {"TINRelief", {0.55, 0.4, 0.25}},        {"GenericCityObject", {0.9, 0.5, 0.7}}};
	auto s = by_surface.find(surface_type);
	if (s != by_surface.end()) {
		return s->second;
	}
	auto c = by_class.find(object_type);
	if (c != by_class.end()) {
		return c->second;
	}
	return {0.6, 0.6, 0.6};
}

std::string FaceGroupName(const MeshObject &object, const MeshFace &face, const AppearanceSource &appearance) {
	if (face.material >= 0) {
		auto it = appearance.Materials().find(face.material);
		if (it != appearance.Materials().end()) {
			return it->second.name;
		}
	}
	if (face.surface >= 0 && static_cast<size_t>(face.surface) < object.surfaces.size()) {
		return object.surfaces[face.surface];
	}
	return object.object_type;
}

MeshModel BuildMeshModel(const std::map<std::string, std::vector<std::pair<std::string, json>>> &objects,
                         const std::vector<std::string> &feature_order, const AppearanceSource &appearance,
                         const MeshBuildOptions &options, const std::optional<std::string> &crs) {
	MeshModel model;
	model.crs = crs;

	for (const auto &feature_id : feature_order) {
		auto fit = objects.find(feature_id);
		if (fit == objects.end()) {
			continue;
		}
		for (const auto &entry : fit->second) {
			const std::string &id = entry.first;
			const json &city_obj = entry.second;
			auto geoms = city_obj.find("geometry");
			if (geoms == city_obj.end() || !geoms->is_array() || geoms->empty()) {
				continue;
			}

			// Choose the geometry: the requested LoD, else the highest one that is a surface.
			const json *chosen = nullptr;
			double best = -1.0;
			for (const auto &g : *geoms) {
				std::string lod = g.value("lod", "");
				std::string type = g.value("type", "");
				if (FaceDepth(type) < 0) {
					continue;
				}
				if (options.lod.has_value()) {
					if (lod == options.lod.value()) {
						chosen = &g;
						break;
					}
					continue;
				}
				double n = LodNumber(lod);
				if (n > best) {
					best = n;
					chosen = &g;
				}
			}
			if (chosen == nullptr) {
				continue;
			}
			const json &geom = *chosen;

			MeshObject object;
			object.id = id;
			object.object_type = city_obj.value("type", "");
			object.lod = geom.value("lod", "");
			auto attrs = city_obj.find("attributes");
			object.attributes = attrs != city_obj.end() ? *attrs : json(nullptr);

			// Semantic surfaces: the type of each entry, in the source's order.
			json sem_values;
			if (geom.contains("semantics") && geom["semantics"].is_object()) {
				const json &sem = geom["semantics"];
				if (sem.contains("surfaces") && sem["surfaces"].is_array()) {
					for (const auto &s : sem["surfaces"]) {
						object.surfaces.push_back(s.is_object() ? s.value("type", "") : "");
					}
				}
				if (sem.contains("values")) {
					sem_values = sem["values"];
				}
			}
			json mat_broadcast;
			json tex_broadcast;
			const json &mat_values = geom.contains("material") ? FirstThemeValues(geom["material"], mat_broadcast) : json();
			const json &tex_values = geom.contains("texture") ? FirstThemeValues(geom["texture"], tex_broadcast) : json();
			const bool mat_is_broadcast = !mat_broadcast.is_null();

			std::map<std::array<double, 3>, uint32_t> vertex_index;
			auto add_vertex = [&](const json &p) -> std::optional<uint32_t> {
				if (!p.is_array() || p.size() < 3) {
					return std::nullopt;
				}
				Vertex3 v {p[0].get<double>(), p[1].get<double>(), p[2].get<double>()};
				auto it = vertex_index.find(v);
				if (it != vertex_index.end()) {
					return it->second;
				}
				auto idx = static_cast<uint32_t>(object.vertices.size());
				object.vertices.push_back(v);
				vertex_index.emplace(v, idx);
				return idx;
			};

			auto bit = geom.find("boundaries");
			if (bit == geom.end()) {
				continue;
			}
			ForEachFace(*bit, sem_values, mat_is_broadcast ? json() : mat_values, tex_values,
			            FaceDepth(geom.value("type", "")),
			            [&](const json &face_json, const json &sem_v, const json &mat_v, const json &tex_v) {
				            MeshFace face;
				            if (!face_json.is_array()) {
					            return;
				            }
				            for (size_t r = 0; r < face_json.size(); r++) {
					            std::vector<uint32_t> ring;
					            for (const auto &p : face_json[r]) {
						            if (auto idx = add_vertex(p)) {
							            ring.push_back(*idx);
						            }
					            }
					            if (ring.size() >= 3) {
						            face.rings.push_back(std::move(ring));
					            } else if (r == 0) {
						            return; // an outer ring with under three vertices is not a face
					            }
				            }
				            if (sem_v.is_number_integer()) {
					            face.surface = sem_v.get<int32_t>();
				            }
				            const json &m = mat_is_broadcast ? mat_broadcast : mat_v;
				            if (m.is_number_integer()) {
					            if (auto id = appearance.ResolveMaterial(feature_id, m.get<int64_t>())) {
						            face.material = *id;
					            }
				            }
				            // Texture: per ring [texId, uv, uv, ...]; the id is the first ring's.
				            if (tex_v.is_array() && !tex_v.empty() && tex_v[0].is_array() && !tex_v[0].empty() &&
				                tex_v[0][0].is_number_integer()) {
					            auto tid = appearance.ResolveTexture(feature_id, tex_v[0][0].get<int64_t>());
					            if (tid.has_value()) {
						            std::vector<std::vector<std::array<double, 2>>> uvs;
						            bool complete = true;
						            for (size_t r = 0; r < face.rings.size() && complete; r++) {
							            const json &tring = r < tex_v.size() ? tex_v[r] : json();
							            std::vector<std::array<double, 2>> ring_uv;
							            for (size_t k = 1; tring.is_array() && k < tring.size(); k++) {
								            if (auto uv = appearance.UV(feature_id, tring[k])) {
									            ring_uv.push_back(*uv);
								            }
							            }
							            complete = ring_uv.size() == face.rings[r].size();
							            uvs.push_back(std::move(ring_uv));
						            }
						            if (complete) {
							            face.texture = *tid;
							            face.uvs = std::move(uvs);
						            }
					            }
				            }
				            object.faces.push_back(std::move(face));
			            });

			if (object.faces.empty()) {
				continue;
			}
			for (const auto &v : object.vertices) {
				GeographicalExtent e(v[0], v[1], v[2], v[0], v[1], v[2]);
				model.extent = model.extent.has_value() ? model.extent->Union(e) : e;
			}
			model.objects.push_back(std::move(object));
		}
	}

	if (options.origin == "none") {
		model.origin = {0.0, 0.0, 0.0};
	} else if (options.origin == "auto") {
		if (model.extent.has_value()) {
			model.origin = {model.extent->min_x, model.extent->min_y, model.extent->min_z};
		}
	} else if (auto o = ParseOriginOption(options.origin)) {
		model.origin = *o;
	}
	return model;
}

} // namespace cityjson
} // namespace duckdb
```

Note on `std::map<std::array<double,3>, …>`: `std::array` has `operator<`, so this compiles. Exact-match dedup is deliberate — WKB gave us exact doubles.

Add `src/cityjson/mesh_model.cpp` to `EXTENSION_SOURCES`.

- [ ] **Step 3: Build**

Run: `just rebuild`
Expected: compiles.

- [ ] **Step 4: Commit**

```bash
git add src/include/cityjson/mesh_model.hpp src/cityjson/mesh_model.cpp CMakeLists.txt
git commit -m "feat(mesh): MeshModel built from the COPY's collected CityJSON objects"
```

---

### Task 5: The OBJ writer

**Files:**
- Create: `src/include/cityjson/obj_writer.hpp`, `src/cityjson/obj_writer.cpp`
- Modify: `CMakeLists.txt`

**Interfaces:**
- Produces:
  ```cpp
  struct OBJWriteOptions { bool triangulate = false; int precision = 17; };
  //! Format with `precision` significant digits; 17 means shortest round-trip (15 then 17).
  std::string FormatDouble(double v, int precision);
  //! Write `obj_path` (the temp path) referencing `mtl_basename`, and the .mtl at `mtl_path`
  //! (the final sidecar path). `image_writer(texture_id, basename)` copies a texture next to the
  //! OBJ and returns false when it could not (the material then gets no map_Kd).
  void WriteOBJ(const MeshModel &model, AppearanceSource &appearance, const std::string &obj_path,
                const std::string &mtl_path, const std::string &mtl_basename, const OBJWriteOptions &options,
                const std::function<bool(int64_t texture_id, std::string &basename)> &image_writer);
  ```

- [ ] **Step 1: Header**

```cpp
// src/include/cityjson/obj_writer.hpp
#pragma once

#include "cityjson/appearance_source.hpp"
#include "cityjson/mesh_model.hpp"

#include <functional>
#include <string>

namespace duckdb {
namespace cityjson {

struct OBJWriteOptions {
	bool triangulate = false; // hole-free faces stay n-gons unless true
	int precision = 17;       // significant digits; 17 = shortest round-trip
};

std::string FormatDouble(double v, int precision);

void WriteOBJ(const MeshModel &model, AppearanceSource &appearance, const std::string &obj_path,
              const std::string &mtl_path, const std::string &mtl_basename, const OBJWriteOptions &options,
              const std::function<bool(int64_t texture_id, std::string &basename)> &image_writer);

} // namespace cityjson
} // namespace duckdb
```

- [ ] **Step 2: Implementation**

```cpp
// src/cityjson/obj_writer.cpp
#include "cityjson/obj_writer.hpp"

#include "cityjson/error.hpp"
#include "cityjson/face_triangulation.hpp"

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <map>
#include <set>

namespace duckdb {
namespace cityjson {

std::string FormatDouble(double v, int precision) {
	char buf[64];
	if (precision >= 17) {
		// Shortest text that round-trips: 15 digits when that reads back exactly, 17 otherwise.
		std::snprintf(buf, sizeof(buf), "%.15g", v);
		if (std::strtod(buf, nullptr) != v) {
			std::snprintf(buf, sizeof(buf), "%.17g", v);
		}
	} else {
		std::snprintf(buf, sizeof(buf), "%.*g", precision < 1 ? 1 : precision, v);
	}
	return buf;
}

namespace {

// Everything an .mtl entry needs, keyed by the usemtl name the OBJ uses.
struct MtlEntry {
	std::array<double, 3> kd;
	std::optional<std::array<double, 3>> ks;
	std::optional<std::array<double, 3>> ke;
	double d = 1.0;
	std::optional<double> ns;
	std::string map_kd; // empty = none
};

std::string Sanitise(const std::string &name) {
	// OBJ tokens are whitespace-delimited; keep names to one token.
	std::string out = name;
	for (auto &c : out) {
		if (c == ' ' || c == '\t' || c == '\r' || c == '\n') {
			c = '_';
		}
	}
	return out.empty() ? "unnamed" : out;
}

} // namespace

void WriteOBJ(const MeshModel &model, AppearanceSource &appearance, const std::string &obj_path,
              const std::string &mtl_path, const std::string &mtl_basename, const OBJWriteOptions &options,
              const std::function<bool(int64_t texture_id, std::string &basename)> &image_writer) {
	std::ofstream out(obj_path);
	if (!out.is_open()) {
		throw CityJSONError::FileWrite("Failed to open output file: " + obj_path);
	}

	std::map<std::string, MtlEntry> mtl;               // in first-use order is not needed; name-keyed
	std::vector<std::string> mtl_order;                // deterministic .mtl order
	std::map<int64_t, std::string> image_basename;     // texture id -> copied file, "" = failed
	auto texture_file = [&](int64_t tex) -> std::string {
		auto it = image_basename.find(tex);
		if (it != image_basename.end()) {
			return it->second;
		}
		std::string basename;
		bool ok = image_writer(tex, basename);
		image_basename[tex] = ok ? basename : "";
		return image_basename[tex];
	};
	// The usemtl name of a face, registering its .mtl entry on first use. A textured
	// face gets its own entry (OBJ ties map_Kd to the material; CityJSON does not).
	auto material_name = [&](const MeshObject &object, const MeshFace &face) -> std::string {
		std::string base = Sanitise(FaceGroupName(object, face, appearance));
		std::string map;
		if (face.texture >= 0) {
			map = texture_file(face.texture);
		}
		std::string name = map.empty() ? base : base + "__tex" + std::to_string(face.texture);
		if (mtl.count(name) == 0) {
			MtlEntry e;
			auto mit = face.material >= 0 ? appearance.Materials().find(face.material) : appearance.Materials().end();
			if (mit != appearance.Materials().end()) {
				e.kd = mit->second.diffuse;
				e.ks = mit->second.specular;
				e.ke = mit->second.emissive;
				e.d = 1.0 - mit->second.transparency;
				if (mit->second.shininess.has_value()) {
					e.ns = mit->second.shininess.value() * 1000.0;
				}
			} else {
				std::string surface = face.surface >= 0 && static_cast<size_t>(face.surface) < object.surfaces.size()
				                          ? object.surfaces[face.surface]
				                          : "";
				e.kd = DefaultColour(surface, object.object_type);
			}
			e.map_kd = map;
			mtl[name] = e;
			mtl_order.push_back(name);
		}
		return name;
	};

	out << "# Written by duckdb-cityjson\n";
	if (model.crs.has_value()) {
		out << "# crs " << model.crs.value() << "\n";
	}
	const auto &o = model.origin;
	if (o[0] != 0.0 || o[1] != 0.0 || o[2] != 0.0) {
		out << "# origin " << FormatDouble(o[0], 17) << " " << FormatDouble(o[1], 17) << " " << FormatDouble(o[2], 17)
		    << "\n";
	}
	out << "mtllib " << mtl_basename << "\n";

	size_t v_base = 0;  // vertices written so far (OBJ indices are global and 1-based)
	size_t vt_base = 0; // texture coordinates written so far
	for (const auto &object : model.objects) {
		out << "o " << Sanitise(object.id) << "\n";
		for (const auto &v : object.vertices) {
			out << "v " << FormatDouble(v[0] - o[0], options.precision) << " "
			    << FormatDouble(v[1] - o[1], options.precision) << " " << FormatDouble(v[2] - o[2], options.precision)
			    << "\n";
		}
		// Per-object UV pool, deduplicated exactly.
		std::map<std::array<double, 2>, size_t> uv_index;
		std::vector<std::array<double, 2>> uv_list;
		auto uv_of = [&](const std::array<double, 2> &uv) {
			auto it = uv_index.find(uv);
			if (it != uv_index.end()) {
				return it->second;
			}
			uv_list.push_back(uv);
			uv_index.emplace(uv, uv_list.size());
			return uv_list.size(); // 1-based within this object
		};
		// Face lines are buffered so `vt` lines can precede them.
		std::string faces;
		std::string current_group;
		std::string current_material;
		for (const auto &face : object.faces) {
			std::string group = face.surface >= 0 && static_cast<size_t>(face.surface) < object.surfaces.size()
			                        ? Sanitise(object.surfaces[face.surface])
			                        : "";
			if (group != current_group) {
				faces += "g " + (group.empty() ? std::string("default") : group) + "\n";
				current_group = group;
			}
			std::string mat = material_name(object, face);
			if (mat != current_material) {
				faces += "usemtl " + mat + "\n";
				current_material = mat;
			}
			const bool textured = !face.uvs.empty() && face.uvs.size() == face.rings.size();
			auto emit_polygon = [&](const std::vector<uint32_t> &ring, const std::vector<std::array<double, 2>> *uvs) {
				faces += "f";
				for (size_t k = 0; k < ring.size(); k++) {
					faces += " " + std::to_string(v_base + ring[k] + 1);
					if (uvs != nullptr) {
						faces += "/" + std::to_string(vt_base + uv_of((*uvs)[k]));
					}
				}
				faces += "\n";
			};
			if (face.rings.size() > 1 || options.triangulate) {
				auto tris = TriangulateFace(object.vertices, face.rings);
				// UV per vertex index (a vertex has one UV within a face).
				std::map<uint32_t, std::array<double, 2>> uv_at;
				if (textured) {
					for (size_t r = 0; r < face.rings.size(); r++) {
						for (size_t k = 0; k < face.rings[r].size(); k++) {
							uv_at[face.rings[r][k]] = face.uvs[r][k];
						}
					}
				}
				for (size_t t = 0; t + 2 < tris.size(); t += 3) {
					std::vector<uint32_t> tri {tris[t], tris[t + 1], tris[t + 2]};
					if (textured) {
						std::vector<std::array<double, 2>> tuv {uv_at[tri[0]], uv_at[tri[1]], uv_at[tri[2]]};
						emit_polygon(tri, &tuv);
					} else {
						emit_polygon(tri, nullptr);
					}
				}
			} else {
				emit_polygon(face.rings[0], textured ? &face.uvs[0] : nullptr);
			}
		}
		for (const auto &uv : uv_list) {
			out << "vt " << FormatDouble(uv[0], options.precision) << " " << FormatDouble(uv[1], options.precision) << "\n";
		}
		out << faces;
		v_base += object.vertices.size();
		vt_base += uv_list.size();
	}
	out.close();
	if (!out) {
		throw CityJSONError::FileWrite("Failed writing output file: " + obj_path);
	}

	std::ofstream mtl_out(mtl_path);
	if (!mtl_out.is_open()) {
		throw CityJSONError::FileWrite("Failed to open output file: " + mtl_path);
	}
	mtl_out << "# Written by duckdb-cityjson\n";
	for (const auto &name : mtl_order) {
		const auto &e = mtl[name];
		mtl_out << "newmtl " << name << "\n";
		mtl_out << "Kd " << FormatDouble(e.kd[0], 6) << " " << FormatDouble(e.kd[1], 6) << " " << FormatDouble(e.kd[2], 6)
		        << "\n";
		if (e.ks.has_value()) {
			mtl_out << "Ks " << FormatDouble((*e.ks)[0], 6) << " " << FormatDouble((*e.ks)[1], 6) << " "
			        << FormatDouble((*e.ks)[2], 6) << "\n";
		}
		if (e.ke.has_value()) {
			mtl_out << "Ke " << FormatDouble((*e.ke)[0], 6) << " " << FormatDouble((*e.ke)[1], 6) << " "
			        << FormatDouble((*e.ke)[2], 6) << "\n";
		}
		mtl_out << "d " << FormatDouble(e.d, 6) << "\n";
		if (e.ns.has_value()) {
			mtl_out << "Ns " << FormatDouble(*e.ns, 6) << "\n";
		}
		if (!e.map_kd.empty()) {
			mtl_out << "map_Kd " << e.map_kd << "\n";
		}
		mtl_out << "\n";
	}
	mtl_out.close();
	if (!mtl_out) {
		throw CityJSONError::FileWrite("Failed writing output file: " + mtl_path);
	}
}

} // namespace cityjson
} // namespace duckdb
```

Add `src/cityjson/obj_writer.cpp` to `EXTENSION_SOURCES`. Include `<cstring>` if `strtod` needs it (`<cstdlib>` suffices).

- [ ] **Step 3: Build**

Run: `just rebuild`
Expected: compiles.

- [ ] **Step 4: Commit**

```bash
git add src/include/cityjson/obj_writer.hpp src/cityjson/obj_writer.cpp CMakeLists.txt
git commit -m "feat(mesh): OBJ + MTL writer over the mesh model"
```

---

### Task 6: `COPY … (FORMAT obj)` — options, source-ref rule, finalize

**Files:**
- Modify: `src/include/cityjson/copy_source_ref.hpp`, `src/cityjson/copy_source_ref.cpp`
- Modify: `src/include/cityjson/copy_function.hpp`, `src/cityjson/copy_function.cpp`
- Modify: `src/cityjson_extension.cpp`
- Create: `test/data/holed_face.city.json`
- Test: `test/sql/copy_obj.test`

**Interfaces:**
- Produces: `CopySourceRef::sidecar_appearance`; on `CityJSONCopyBindData`: `std::optional<std::string> mesh_lod; std::string mesh_origin = "auto"; bool obj_triangulate = false; int obj_precision = 17; std::optional<std::string> materials_query, textures_query; bool gltf_attributes = false;`; `void RegisterMeshCopyFunctions(ExtensionLoader &)` (registers `obj` now; the glTF plan adds `gltf`/`glb` to the same function).

- [ ] **Step 1: Fixture with a holed face**

`test/data/holed_face.city.json` — a MultiSurface with one face carrying an inner ring, plus a plain face, at LoD 2.2 (CityJSON inner rings are wound opposite to the outer):

```json
{
  "type": "CityJSON",
  "version": "2.0",
  "transform": {"scale": [1.0, 1.0, 1.0], "translate": [0.0, 0.0, 0.0]},
  "metadata": {"referenceSystem": "https://www.opengis.net/def/crs/EPSG/0/7415"},
  "CityObjects": {
    "courtyard": {
      "type": "Building",
      "attributes": {"name": "ring"},
      "geometry": [{
        "type": "MultiSurface",
        "lod": "2.2",
        "boundaries": [
          [[0, 1, 2, 3], [4, 5, 6, 7]],
          [[8, 9, 10, 11]]
        ],
        "semantics": {
          "surfaces": [{"type": "RoofSurface"}, {"type": "WallSurface"}],
          "values": [0, 1]
        }
      }]
    }
  },
  "vertices": [
    [84500.0, 446300.0, 10.0], [84504.0, 446300.0, 10.0], [84504.0, 446304.0, 10.0], [84500.0, 446304.0, 10.0],
    [84501.0, 446301.0, 10.0], [84501.0, 446303.0, 10.0], [84503.0, 446303.0, 10.0], [84503.0, 446301.0, 10.0],
    [84500.0, 446300.0, 0.0], [84504.0, 446300.0, 0.0], [84504.0, 446300.0, 10.0], [84500.0, 446300.0, 10.0]
  ]
}
```

Hand-derived: 12 distinct vertices; the holed roof becomes 8 triangles; the wall stays one 4-gon unless `triangulate true` (then 2 triangles). Total faces in the OBJ: 9 by default, 10 with `triangulate true`. `origin 'auto'` = `(84500, 446300, 0)`.

- [ ] **Step 2: Failing test**

`test/sql/copy_obj.test`:

```
# name: test/sql/copy_obj.test
# description: COPY TO obj writes one o per object, g per surface, usemtl per material, and an .mtl beside it
# group: [sql]

require cityjson

# --- Structure: o per object, g per semantic surface, faces as n-gons, origin subtracted and recorded.
statement ok
COPY (SELECT * FROM read_cityjson('test/data/holed_face.city.json', lod := '2.2'))
TO '__TEST_DIR__/holed.obj' (FORMAT obj);

query I
SELECT content FROM read_text('__TEST_DIR__/holed.obj');
----
# Written by duckdb-cityjson
# crs https://www.opengis.net/def/crs/EPSG/0/7415
# origin 84500 446300 0
mtllib holed.mtl
o courtyard
v 0 0 10
v 4 0 10
v 4 4 10
v 0 4 10
v 1 1 10
v 1 3 10
v 3 3 10
v 3 1 10
v 0 0 0
v 4 0 0
g RoofSurface
usemtl RoofSurface
<eight `f a b c` lines: the roof's triangulation -- paste the real ones after the first run>
g WallSurface
usemtl WallSurface
f 9 10 2 1

# (Which eight triangles earcut picks is an implementation detail; the count checks below pin
#  the invariant -- eight 3-vertex faces -- independently. Keep every other line verbatim.)

query I
SELECT COUNT(*) FROM (SELECT UNNEST(string_split(content, chr(10))) AS line FROM read_text('__TEST_DIR__/holed.obj')) WHERE line LIKE 'f %';
----
9

query I
SELECT COUNT(*) FROM (SELECT UNNEST(string_split(content, chr(10))) AS line FROM read_text('__TEST_DIR__/holed.obj'))
WHERE line LIKE 'f %' AND len(string_split(line, ' ')) = 4;
----
8

# --- The .mtl sits beside the final file, named after it, with default colours per surface type.
query I
SELECT content FROM read_text('__TEST_DIR__/holed.mtl');
----
# Written by duckdb-cityjson
newmtl RoofSurface
Kd 0.9 0.06 0.09
d 1

newmtl WallSurface
Kd 0.8 0.8 0.8
d 1


# --- triangulate true: every face a triangle (8 + 2).
statement ok
COPY (SELECT * FROM read_cityjson('test/data/holed_face.city.json', lod := '2.2'))
TO '__TEST_DIR__/holed_tri.obj' (FORMAT obj, triangulate true);

query II
SELECT COUNT(*), COUNT(*) FILTER (len(string_split(line, ' ')) = 4)
FROM (SELECT UNNEST(string_split(content, chr(10))) AS line FROM read_text('__TEST_DIR__/holed_tri.obj')) WHERE line LIKE 'f %';
----
10	10

# --- origin 'none' keeps world coordinates at full precision; precision 3 rounds them.
statement ok
COPY (SELECT * FROM read_cityjson('test/data/holed_face.city.json', lod := '2.2'))
TO '__TEST_DIR__/holed_world.obj' (FORMAT obj, origin 'none');

query I
SELECT line FROM (SELECT UNNEST(string_split(content, chr(10))) AS line FROM read_text('__TEST_DIR__/holed_world.obj')) WHERE line LIKE 'v %' LIMIT 1;
----
v 84500 446300 10

query I
SELECT COUNT(*) FROM (SELECT UNNEST(string_split(content, chr(10))) AS line FROM read_text('__TEST_DIR__/holed_world.obj')) WHERE line LIKE '# origin%';
----
0

statement ok
COPY (SELECT * FROM read_cityjson('test/data/holed_face.city.json', lod := '2.2'))
TO '__TEST_DIR__/holed_p3.obj' (FORMAT obj, origin 'none', precision 3);

query I
SELECT line FROM (SELECT UNNEST(string_split(content, chr(10))) AS line FROM read_text('__TEST_DIR__/holed_p3.obj')) WHERE line LIKE 'v %' LIMIT 1;
----
v 8.45e+04 4.46e+05 10

# --- lod picks the column; an object with nothing at that LoD is skipped; default = highest present.
statement ok
COPY (SELECT * FROM read_cityjsonseq('test/data/delft_subset.city.jsonl'))
TO '__TEST_DIR__/delft_lod12.obj' (FORMAT obj, lod '1.2');

query I
SELECT (SELECT COUNT(*) FROM (SELECT UNNEST(string_split(content, chr(10))) AS line FROM read_text('__TEST_DIR__/delft_lod12.obj')) WHERE line LIKE 'o %')
     = (SELECT COUNT(*) FROM read_cityjsonseq('test/data/delft_subset.city.jsonl') WHERE geometry_lod1_2 IS NOT NULL);
----
true

# --- Round trip through read_obj: ids, surface types and face counts agree.
#     (Circular by construction -- the fixture assertions above are what prove the conventions;
#      this proves the two sides agree on them.)
statement ok
COPY (SELECT * FROM read_cityjson('test/data/holed_face.city.json', lod := '2.2'))
TO '__TEST_DIR__/rt.obj' (FORMAT obj);

query III
SELECT id, geometry_properties_lod2_2.surfaces, len(geometry_properties_lod2_2.face_semantics)
FROM read_obj('__TEST_DIR__/rt.obj', lod := '2.2');
----
courtyard	[{"type":"RoofSurface"},{"type":"WallSurface"}]	9

query I
SELECT bbox FROM read_obj('__TEST_DIR__/rt.obj', lod := '2.2');
----
{'xmin': 84500.0, 'ymin': 446300.0, 'zmin': 0.0, 'xmax': 84504.0, 'ymax': 446304.0, 'zmax': 10.0}

# --- Required columns are the CityJSON writer's.
statement error
COPY (SELECT 1 AS x) TO '__TEST_DIR__/bad.obj' (FORMAT obj);
----
requires an 'id' column
```

Finalise the first `read_text` expectation after the first real run: replace the placeholder line with the eight roof `f` lines earcut produced, **only after** checking they are eight 3-vertex faces (the count queries pin that independently). Do not loosen the header, vertex, group or wall lines.

- [ ] **Step 3: Run it and watch it fail**

Run: `just rebuild && ./build/release/test/unittest "test/sql/copy_obj.test"`
Expected: FAIL — `Copy Function with name obj does not exist`.

- [ ] **Step 4: Capture the reader's `appearance` parameter on the source ref**

`copy_source_ref.hpp` — add to `CopySourceRef`:

```cpp
	//! The reader call was `appearance := 'sidecar'`: its material/texture cells hold
	//! dataset-global ids, not indices into the file's own blocks.
	bool sidecar_appearance = false;
```

`copy_source_ref.cpp`, in `CollectFromTableRef` after `found.path = …`:

```cpp
		// Named parameters are children with an alias. Only `appearance` matters here.
		for (auto &child : call.children) {
			if (child->alias != "appearance" || child->GetExpressionType() != ExpressionType::VALUE_CONSTANT) {
				continue;
			}
			auto &c = child->Cast<ConstantExpression>();
			if (!c.value.IsNull() && c.value.type().id() == LogicalTypeId::VARCHAR) {
				found.sidecar_appearance = StringUtil::Lower(StringValue::Get(c.value)) == "sidecar";
			}
		}
```

(add `#include "duckdb/common/string_util.hpp"`.)

- [ ] **Step 5: Bind data fields and option parsing**

`copy_function.hpp`, on `CityJSONCopyBindData` after the FCB options:

```cpp
	// Mesh write options (COPY TO ... FORMAT obj | gltf | glb).
	std::optional<std::string> mesh_lod;   // normalised; nullopt = highest per object
	std::string mesh_origin = "auto";      // 'auto' | 'none' | 'x,y,z'
	bool obj_triangulate = false;
	int obj_precision = 17;
	bool gltf_attributes = false;
	// Sidecar-form appearance definitions; their presence declares the form (§appearance).
	std::optional<std::string> materials_query;
	std::optional<std::string> textures_query;
	// Resolved at bind for mesh formats (from the queries, or the source's local blocks),
	// so a bad query fails before a single row is sunk -- as metadata_query does.
	std::optional<AppearanceSource> appearance_source;
```

(`#include "cityjson/appearance_source.hpp"` in the header.) Copy them all in `Copy()`.

`copy_function.cpp`, in the option loop of `CityJSONCopyToBind`, add branches (include `"cityjson/lod_table.hpp"` and `"cityjson/mesh_model.hpp"`):

```cpp
		} else if (loption == "lod") {
			bind_data->mesh_lod = LODTableUtils::NormalizeLOD(val.ToString());
		} else if (loption == "origin") {
			auto text = val.ToString();
			if (text != "auto" && text != "none" && !ParseOriginOption(text).has_value()) {
				throw BinderException("origin must be 'auto', 'none' or 'x,y,z', got '" + text + "'");
			}
			bind_data->mesh_origin = text;
		} else if (loption == "triangulate") {
			bind_data->obj_triangulate = val.GetValue<bool>();
		} else if (loption == "precision") {
			auto p = val.GetValue<int64_t>();
			if (p < 1 || p > 17) {
				throw BinderException("precision must be between 1 and 17 significant digits");
			}
			bind_data->obj_precision = static_cast<int>(p);
		} else if (loption == "attributes") {
			bind_data->gltf_attributes = val.GetValue<bool>();
		} else if (loption == "materials_query") {
			bind_data->materials_query = val.ToString();
		} else if (loption == "textures_query") {
			bind_data->textures_query = val.ToString();
		}
```

After the source-ref resolution block (after the `else if (!has_explicit_crs && !has_metadata_query) { … warning … }`), add the refusal rule:

```cpp
	// Mesh formats must be told which appearance form the cells are in: material cells
	// look identical in both, and resolving sidecar ids against the source's local
	// blocks would silently recolour every face.
	if (IsMeshFormat(bind_data->format) && bind_data->source_ref.has_value() &&
	    bind_data->source_ref->sidecar_appearance && !bind_data->materials_query.has_value() &&
	    !bind_data->textures_query.has_value()) {
		throw BinderException("COPY TO " + input.info.format +
		                      ": the source was read with appearance := 'sidecar', so its material/texture cells hold "
		                      "sidecar ids; pass materials_query / textures_query (e.g. materials_query 'SELECT * FROM "
		                      "cityjson_materials(''" +
		                      bind_data->source_ref->path + "'')') so they can be resolved");
	}
	if (IsMeshFormat(bind_data->format)) {
		bind_data->appearance_source = BuildAppearanceSource(context, *bind_data);
	}
```

`BuildAppearanceSource` (Step 6) must therefore be declared above `CityJSONCopyToBind` — put the *Mesh formats: shared preparation* block before the bind, or forward-declare it.

- [ ] **Step 6: Finalize for `Obj`**

In `copy_function.cpp` add includes `"cityjson/appearance_source.hpp"`, `"cityjson/obj_writer.hpp"`, `"duckdb/common/file_system.hpp"`, and a helper above `CityJSONCopyToFinalize`:

```cpp
// ============================================================
// Mesh formats: shared preparation
// ============================================================

struct MeshTargets {
	std::string final_dir;   // directory of the final output path ("" = cwd)
	std::string final_stem;  // "campus" for "campus.obj"
	std::string source_dir;  // where relative image URIs resolve ("" = unknown)
};

static MeshTargets ResolveMeshTargets(const CityJSONCopyBindData &bind_data) {
	MeshTargets t;
	const auto &p = bind_data.file_path;
	auto slash = p.find_last_of("/\\");
	t.final_dir = slash == std::string::npos ? "" : p.substr(0, slash);
	std::string base = slash == std::string::npos ? p : p.substr(slash + 1);
	auto dot = base.rfind('.');
	t.final_stem = dot == std::string::npos ? base : base.substr(0, dot);
	if (bind_data.source_ref.has_value()) {
		const auto &s = bind_data.source_ref->path;
		auto ss = s.find_last_of("/\\");
		t.source_dir = ss == std::string::npos ? "" : s.substr(0, ss);
	}
	return t;
}

static std::string JoinDir(const std::string &dir, const std::string &name) {
	return dir.empty() ? name : dir + "/" + name;
}

static AppearanceSource BuildAppearanceSource(ClientContext &context, const CityJSONCopyBindData &bind_data) {
	if (bind_data.materials_query.has_value() || bind_data.textures_query.has_value()) {
		return AppearanceSource::FromQueries(context, bind_data.materials_query, bind_data.textures_query);
	}
	return AppearanceSource::FromLocal(bind_data.source_appearance_header, bind_data.source_appearance_by_feature);
}

// Copies texture `id`'s bytes beside the final output under the image's own basename;
// returns false (and logs) when the bytes cannot be had.
static bool CopyTextureImage(ClientContext &context, AppearanceSource &appearance, int64_t texture_id,
                             const MeshTargets &targets, std::string &basename) {
	std::string warning;
	if (!appearance.LoadImage(context, texture_id, targets.source_dir, warning)) {
		DUCKDB_LOG_WARNING(context, "cityjson: " + warning + "; faces fall back to the material colour");
		return false;
	}
	auto &tex = appearance.Textures().at(texture_id);
	auto slash = tex.image_uri.find_last_of("/\\");
	basename = slash == std::string::npos ? tex.image_uri : tex.image_uri.substr(slash + 1);
	if (basename.empty()) {
		basename = "texture_" + std::to_string(texture_id) + "." + StringUtil::Lower(tex.image_type);
	}
	std::ofstream img(JoinDir(targets.final_dir, basename), std::ios::binary);
	if (!img.is_open()) {
		DUCKDB_LOG_WARNING(context, "cityjson: could not write texture image '" + basename + "'");
		return false;
	}
	img.write(reinterpret_cast<const char *>(tex.image_data.data()), static_cast<std::streamsize>(tex.image_data.size()));
	return static_cast<bool>(img);
}

static void FinalizeObj(ClientContext &context, CityJSONCopyBindData &bind_data, CityJSONCopyGlobalState &gstate) {
	auto targets = ResolveMeshTargets(bind_data);
	// A copy: LoadImage fills texture bytes, and the bind data must stay as bound.
	AppearanceSource appearance = bind_data.appearance_source.value();
	MeshBuildOptions build;
	build.lod = bind_data.mesh_lod;
	build.origin = bind_data.mesh_origin;
	auto model = BuildMeshModel(gstate.feature_objects, gstate.feature_order, appearance, build, bind_data.crs);
	for (const auto &w : model.warnings) {
		DUCKDB_LOG_WARNING(context, "cityjson: " + w);
	}
	OBJWriteOptions options;
	options.triangulate = bind_data.obj_triangulate;
	options.precision = bind_data.obj_precision;
	const std::string mtl_basename = targets.final_stem + ".mtl";
	WriteOBJ(model, appearance, gstate.temp_file_path, JoinDir(targets.final_dir, mtl_basename), mtl_basename, options,
	         [&](int64_t texture_id, std::string &basename) {
		         return CopyTextureImage(context, appearance, texture_id, targets, basename);
	         });
}
```

In `CityJSONCopyToFinalize`'s switch, `case CopyFormat::Obj: FinalizeObj(context, bind_data, gstate); break;` (leave `Gltf`/`Glb` throwing until the next plan). Add `#include <fstream>`.

- [ ] **Step 7: Register**

In `copy_function.hpp` declare `void RegisterMeshCopyFunctions(ExtensionLoader &loader);`. In `copy_function.cpp`:

```cpp
void RegisterMeshCopyFunctions(ExtensionLoader &loader) {
	CopyFunction obj("obj");
	obj.extension = "obj";
	obj.copy_to_bind = CityJSONCopyToBind;
	obj.copy_to_initialize_global = CityJSONCopyToInitGlobal;
	obj.copy_to_initialize_local = CityJSONCopyToInitLocal;
	obj.copy_to_sink = CityJSONCopyToSink;
	obj.copy_to_combine = CityJSONCopyToCombine;
	obj.copy_to_finalize = CityJSONCopyToFinalize;
	loader.RegisterFunction(obj);
}
```

In `cityjson_extension.cpp` after the two CityJSON COPY registrations:

```cpp
	// Register mesh interchange COPY TO functions (obj; gltf/glb)
	cityjson::RegisterMeshCopyFunctions(loader);
```

- [ ] **Step 8: Run**

Run: `just rebuild && ./build/release/test/unittest "test/sql/copy_obj.test"`
Expected: PASS after the one adjustment noted in Step 2. If `# crs` is absent, the discovered source's `metadata.referenceSystem` did not reach `bind_data.crs` — check that `read_cityjson('test/data/holed_face.city.json', …)` is the only reader in the SELECT (it is) and that `holed_face.city.json` carries `metadata.referenceSystem` (it does).

- [ ] **Step 9: Run the COPY suites**

Run: `./build/release/test/unittest "test/sql/cityjson_copy*.test"`
Expected: PASS.

- [ ] **Step 10: Commit**

```bash
git add src/include/cityjson/copy_source_ref.hpp src/cityjson/copy_source_ref.cpp \
        src/include/cityjson/copy_function.hpp src/cityjson/copy_function.cpp src/cityjson_extension.cpp \
        test/data/holed_face.city.json test/sql/copy_obj.test
git commit -m "feat(copy): COPY TO obj -- Wavefront OBJ + MTL from any CityParquet-shaped relation"
```

---

### Task 7: Appearance on the way out — local, sidecar, refusal, textures

**Files:**
- Test: `test/sql/copy_mesh_appearance.test`

- [ ] **Step 1: Write the test**

```
# name: test/sql/copy_mesh_appearance.test
# description: mesh COPY resolves local-form refs against the source and sidecar-form ids against the query options
# group: [sql]

require cityjson

# --- Local form: the discovered source's own definitions colour the MTL.
statement ok
COPY (SELECT * FROM read_cityjsonseq('test/data/railway_appearance.city.jsonl'))
TO '__TEST_DIR__/railway.obj' (FORMAT obj, lod '3');

# Materials named after the CityJSON material names (UUID_…), with their diffuse colours.
query I
SELECT COUNT(*) FROM (SELECT UNNEST(string_split(content, chr(10))) AS line FROM read_text('__TEST_DIR__/railway.mtl'))
WHERE line LIKE 'newmtl UUID_%';
----
2

query I
SELECT COUNT(*) > 0 FROM (SELECT UNNEST(string_split(content, chr(10))) AS line FROM read_text('__TEST_DIR__/railway.mtl'))
WHERE line = 'Kd 0.496094 0.429688 0.296875';
----
true

# Textured faces: the images are not on disk beside the fixture, so no map_Kd is written and
# the faces fall back to colour -- the COPY still succeeds (a warning is logged).
query I
SELECT COUNT(*) FROM (SELECT UNNEST(string_split(content, chr(10))) AS line FROM read_text('__TEST_DIR__/railway.mtl'))
WHERE line LIKE 'map_Kd%';
----
0

# --- Sidecar form from a package: *_query resolve global ids; the source's blocks are ignored.
statement ok
CREATE TABLE railway_rows AS SELECT * FROM read_cityjsonseq('test/data/railway_appearance.city.jsonl', appearance := 'sidecar');

statement ok
CREATE TABLE railway_materials AS SELECT * FROM cityjson_materials('test/data/railway_appearance.city.jsonl');

statement ok
COPY (SELECT * FROM railway_rows) TO '__TEST_DIR__/railway_pkg.obj'
(FORMAT obj, lod '3', materials_query 'SELECT * FROM railway_materials');

query I
SELECT COUNT(*) FROM (SELECT UNNEST(string_split(content, chr(10))) AS line FROM read_text('__TEST_DIR__/railway_pkg.mtl'))
WHERE line LIKE 'newmtl UUID_%';
----
2

# --- The refusal: a discovered source read in sidecar mode with no *_query is an error, not a guess.
statement error
COPY (SELECT * FROM read_cityjsonseq('test/data/railway_appearance.city.jsonl', appearance := 'sidecar'))
TO '__TEST_DIR__/refused.obj' (FORMAT obj, lod '3');
----
read with appearance := 'sidecar'

# …and the same query with the option is accepted.
statement ok
COPY (SELECT * FROM read_cityjsonseq('test/data/railway_appearance.city.jsonl', appearance := 'sidecar'))
TO '__TEST_DIR__/accepted.obj'
(FORMAT obj, lod '3', materials_query 'SELECT * FROM cityjson_materials(''test/data/railway_appearance.city.jsonl'')');

# --- Textures with bytes: an image_data blob is copied beside the OBJ and referenced by map_Kd.
statement ok
CREATE TABLE tex_rows AS
SELECT * FROM read_obj('test/data/obj/cube.obj', lod := '2.2', appearance := 'sidecar');

statement ok
CREATE TABLE tex_defs AS
SELECT id, image_uri, '\x89PNG\x0D\x0A\x1A\x0A'::BLOB AS image_data, image_type, wrapMode, textureType, borderColor, other
FROM obj_textures('test/data/obj/cube.obj');

statement ok
COPY (SELECT * FROM tex_rows) TO '__TEST_DIR__/textured.obj'
(FORMAT obj, materials_query 'SELECT * FROM obj_materials(''test/data/obj/cube.obj'')', textures_query 'SELECT * FROM tex_defs');

query I
SELECT line FROM (SELECT UNNEST(string_split(content, chr(10))) AS line FROM read_text('__TEST_DIR__/textured.mtl')) WHERE line LIKE 'map_Kd%';
----
map_Kd brick.png

query I
SELECT COUNT(*) FROM (SELECT UNNEST(string_split(content, chr(10))) AS line FROM read_text('__TEST_DIR__/textured.obj'))
WHERE line LIKE 'f %/%';
----
4

query I
SELECT length(content) FROM read_blob('__TEST_DIR__/brick.png');
----
8

query I
SELECT COUNT(*) FROM (SELECT UNNEST(string_split(content, chr(10))) AS line FROM read_text('__TEST_DIR__/textured.mtl'))
WHERE line = 'newmtl brick__tex0';
----
1
```

- [ ] **Step 2: Run**

Run: `just rebuild && ./build/release/test/unittest "test/sql/copy_mesh_appearance.test"`
Expected: PASS. Likely adjustments: the `Kd` formatting of `0.49609375` at 6 significant digits is `0.496094`; if `FormatDouble` prints differently, fix the *test* only if the printed value is the correct 6-digit rounding. If the refusal does not fire, check that `FindCopySourceRef` sees `appearance` as `child->alias` (DuckDB stores named parameters that way; print `call.children[i]->alias` in a debugger if in doubt).

- [ ] **Step 3: Commit**

```bash
git add test/sql/copy_mesh_appearance.test
git commit -m "test(copy): mesh appearance resolution -- local, sidecar, refusal, textures"
```

---

### Task 8: Documentation

**Files:**
- Modify: `docs/FUNCTIONS.md` (Contents; the `Mesh interchange` section from the reader plan gains the writer), `docs/DESIGN_DOC.md` (§9 Writing), `docs/TRAPS.md` (§COPY)

- [ ] **Step 1: FUNCTIONS.md** — under `## Mesh interchange`, after `obj_metadata`, add (run every example, paste real output):

````markdown
### `COPY … TO 'x.obj' (FORMAT obj)`

Writes any relation the CityJSON writers accept (`id`, `feature_id`,
`object_type`, `geometry_lod*` and companions) as Wavefront OBJ, following what
cjio, 3dfier and geoflow write: one `o` per object, `g` per semantic surface,
`usemtl` per material, Z-up, no axis swap.

```sql
COPY (SELECT * FROM read_cityjsonseq('https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl'))
TO 'delft.obj' (FORMAT obj, lod '2.2');
```

| Option | Default | Meaning |
| --- | --- | --- |
| `lod` | highest per object | Which `geometry_lodX_Y` to export; objects with nothing there are skipped |
| `origin` | `'auto'` | Subtract the extent's minimum corner (`'auto'`), nothing (`'none'`), or `'x,y,z'`. Recorded as `# origin x y z`, which `read_obj` adds back |
| `triangulate` | `false` | Hole-free faces stay n-gons; faces with holes are always triangulated (OBJ cannot express holes) |
| `precision` | `17` | Significant digits; 17 is the shortest text that round-trips |
| `materials_query`, `textures_query` | none | SQL returning `materials.parquet` / `textures.parquet`-shaped rows. **Their presence declares the cells to be sidecar-form.** Absent, refs are local-form and resolve against the discovered source or `metadata_from` |
| `metadata_from` | discovered | As for the CityJSON writers |

A `.mtl` named after the OBJ is written beside it: `Kd`/`Ks`/`Ke`/`d`/`Ns` from the
CityJSON material, `map_Kd` when the face carries a texture whose bytes could be
found (`image_data`, else `image_uri` relative to the source), copied beside the OBJ
under its own basename. A textured face gets its own entry (`brick__tex0`) because
OBJ ties images to materials. Faces without a material are named after their
semantic surface type, else their class, with a fixed palette.

Which form a cell is in cannot be told from the cell (`{"visual":{"values":[2,…]}}`
either way), so a source that was read with `appearance := 'sidecar'` and no
`*_query` is refused rather than guessed:

```sql
COPY (SELECT * FROM loaded.building) TO 'out.obj'
(FORMAT obj, materials_query 'SELECT * FROM loaded.materials', textures_query 'SELECT * FROM loaded.textures');
```
````

- [ ] **Step 2: DESIGN_DOC.md §9 Writing** — add:

```markdown
The mesh writers share the CityJSON writers' bind, sink and combine: the sink
already rebuilds each row as a CityJSON object with coordinates, semantics and
appearance refs, and only `Finalize` differs. `BuildMeshModel` flattens those
objects into per-object vertex pools and faces (rings, surface, material,
texture, UVs) once per COPY; `AppearanceSource` resolves refs either through the
source's own blocks (local form) or through the two query options (sidecar
form), and the writer never sees the difference. Faces with holes are
triangulated with earcut after projection onto their Newell normal, shifted to
the ring's first vertex so the signed-area tests do not drown at projected
magnitudes.
```

- [ ] **Step 3: TRAPS.md §COPY** — add:

```markdown
- **Local and sidecar material cells are indistinguishable.** Only texture cells
  differ (`[id, 5, 6]` vs `[id, [u,v], …]`). Mesh COPY is therefore *told* the
  form: `materials_query`/`textures_query` present means sidecar. A source read
  with `appearance := 'sidecar'` is refused without them; `FindCopySourceRef`
  carries that flag.
- **Sidecar files and the temp-rename.** `Finalize` writes the main file to
  DuckDB's temp path, which is renamed afterwards. The `.mtl`, `.bin` and copied
  images are not covered: they are written directly under the *final* stem, and
  `mtllib` / `uri` reference final basenames. Naming them from the temp path
  produces a file that references a name that never exists.
```

- [ ] **Step 4: Commit**

```bash
git add docs/FUNCTIONS.md docs/DESIGN_DOC.md docs/TRAPS.md
git commit -m "docs(copy): COPY TO obj and the appearance-form rule"
```

---

## Self-review notes

- Spec coverage, Part 2 (OBJ half): surface + options (T6), `CopyFormat` (T1), mesh model (T4), triangulation (T2), appearance resolution incl. rule 1–4 (T3, T6, T7), OBJ output format (T5, T6 test), sidecar/temp-rename (T6 helper), tests (T2, T6, T7), docs (T8). Deferred to the glTF plan: everything under *glTF / GLB output*, `attributes` semantics (the option is parsed here so bind data is complete), and the `just` opt-in recipes.
- Names used consistently across tasks: `CopyFormat`, `IsMeshFormat`, `AppearanceSource` (`FromLocal`, `FromQueries`, `ResolveMaterial`, `ResolveTexture`, `UV`, `Materials`, `Textures`, `LoadImage`), `MeshMaterial`, `MeshTexture`, `MeshFace`, `MeshObject`, `MeshModel`, `MeshBuildOptions`, `BuildMeshModel`, `DefaultColour`, `FaceGroupName`, `ParseOriginOption`, `TriangulateFace`, `NewellNormal`, `Vertex3`, `FormatDouble`, `OBJWriteOptions`, `WriteOBJ`, `RegisterMeshCopyFunctions`, `ResolveMeshTargets`, `BuildAppearanceSource` (called at bind; result on `appearance_source`), `CopyTextureImage`, `FinalizeObj`.
- The first `read_text` expectation in T6 is explicitly to be finalised against real earcut output, with independent count assertions pinning the invariant; that is not a placeholder, it is the one spot where the exact fan is an implementation detail.
