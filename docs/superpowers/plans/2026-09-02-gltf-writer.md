# glTF / GLB Writer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `COPY … TO 'x.glb' (FORMAT glb)` and `FORMAT gltf` write a CityParquet-shaped relation as glTF 2.0 — one node and mesh per object, materials and textures from the CityJSON appearance, Z-up handled by a root-node matrix, origin and CRS recorded.

**Architecture:** The mesh model, appearance source and triangulator from the OBJ-writer plan are reused unchanged; `gltf_writer.cpp` turns a `MeshModel` into a `tinygltf::Model` (one buffer, float32 positions relative to the origin, u16/u32 indices, one primitive per material-or-surface group) and tinygltf serialises it. `Finalize` gains the two cases. Sidecar files (`.bin`, external images for `.gltf`) are named from the final stem.

**Tech Stack:** tinygltf 2.9.7 (header-only, `TINYGLTF_NO_STB_IMAGE*`, images as raw bytes), nlohmann-json, sqllogictest with the `json` extension.

**Spec:** `docs/superpowers/specs/2026-09-02-obj-gltf-interchange-design.md` — *glTF / GLB output*, *Sidecar files and DuckDB's temp-rename*, *Testing* (opt-in validator).

**Prerequisites:** the OBJ-reader plan (ports, include dirs, `third_party_impl.cpp`) and the mesh-model-and-OBJ-writer plan (`MeshModel`, `AppearanceSource`, `TriangulateFace`, `CopyFormat`, `RegisterMeshCopyFunctions`, `ResolveMeshTargets`, `BuildAppearanceSource`, `FinalizeObj`, the `attributes` option) are merged.

## Global Constraints

- Positions are float32 relative to `model.origin`; `accessor.min`/`max` are mandatory and computed from the written floats.
- Root node matrix (column-major) `[1,0,0,0, 0,0,-1,0, 0,1,0,0, 0,0,0,1]`; vertex data stays Z-up.
- One `buffers[0]`; every bufferView offset a multiple of 4; GLB embeds images as bufferViews; `.gltf` writes `<stem>.bin` and images beside the final path.
- `TINYGLTF_NO_STB_IMAGE`, `TINYGLTF_NO_STB_IMAGE_WRITE`, `TINYGLTF_NO_EXTERNAL_IMAGE` are compile definitions on both extension targets (every TU that includes `tiny_gltf.h` must see the same set, or the `TinyGLTF` constructor references an undefined symbol).
- UV `v` is flipped (`1 - v`): glTF's texture origin is top-left, CityJSON's bottom-left.
- Build/test cycle: `just rebuild` then `./build/release/test/unittest "test/sql/<name>.test"`.

---

## File structure

| File | Responsibility |
| --- | --- |
| `src/cityjson/third_party_impl.cpp` | adds `TINYGLTF_IMPLEMENTATION` |
| `CMakeLists.txt` | compile definitions; new source |
| `src/include/cityjson/gltf_writer.hpp`, `src/cityjson/gltf_writer.cpp` | `WriteGltf`: MeshModel → tinygltf::Model → file |
| `src/cityjson/copy_function.cpp` | `FinalizeGltf`; `gltf` and `glb` registration |
| `test/sql/copy_gltf.test` | behaviour pins |
| `justfile` | `test-gltf-validate`, `test-obj-cjio` opt-in recipes |
| `docs/FUNCTIONS.md`, `docs/DESIGN_DOC.md`, `docs/TRAPS.md` | docs |

---

### Task 1: tinygltf compiles into the extension

**Files:**
- Modify: `src/cityjson/third_party_impl.cpp`, `CMakeLists.txt`

- [ ] **Step 1: Implementation TU**

Append to `src/cityjson/third_party_impl.cpp`:

```cpp
// tinygltf: JSON via the nlohmann-json already linked (the vcpkg port rewrote its
// include); images never decoded -- they travel as raw bytes in bufferViews or files.
#define TINYGLTF_IMPLEMENTATION
#include <tiny_gltf.h>
```

- [ ] **Step 2: Compile definitions**

In `CMakeLists.txt`, after the two `target_compile_features(... cxx_std_20)` lines:

```cmake
# tinygltf configuration, on every TU that includes the header: without stb, the
# TinyGLTF constructor must not reference the stb-backed default image callbacks, and
# that decision is made per-TU at include time.
foreach(tgt ${EXTENSION_NAME} ${LOADABLE_EXTENSION_NAME})
  target_compile_definitions(${tgt} PRIVATE TINYGLTF_NO_STB_IMAGE TINYGLTF_NO_STB_IMAGE_WRITE
                                            TINYGLTF_NO_EXTERNAL_IMAGE)
endforeach()
```

- [ ] **Step 3: Build**

Run: `just rebuild`
Expected: compiles and links. If the link reports `tinygltf::LoadImageData` undefined, a TU is missing the definitions — check both targets received them.

- [ ] **Step 4: Commit**

```bash
git add src/cityjson/third_party_impl.cpp CMakeLists.txt
git commit -m "build: compile tinygltf into the extension without stb"
```

---

### Task 2: The glTF writer

**Files:**
- Create: `src/include/cityjson/gltf_writer.hpp`, `src/cityjson/gltf_writer.cpp`
- Modify: `CMakeLists.txt`

**Interfaces:**
- Produces:
  ```cpp
  struct GltfWriteOptions { bool binary = false; bool attributes = false;
                            std::string bin_basename;          // ".gltf" only: "<stem>.bin"
                            std::string image_dir;             // ".gltf" only: final directory for images
                          };
  //! Writes the model to `out_path` (the temp path). For .gltf, the buffer is written to
  //! dirname(out_path)/bin_basename and images beside it under their own basenames.
  //! `load_image(texture_id)` returns false when the bytes cannot be had (the material is
  //! then written without a texture).
  void WriteGltf(const MeshModel &model, AppearanceSource &appearance, const std::string &out_path,
                 const GltfWriteOptions &options, const std::function<bool(int64_t texture_id)> &load_image);
  ```
- Consumes: `MeshModel`, `MeshObject`, `MeshFace`, `AppearanceSource`, `DefaultColour`, `FaceGroupName`, `TriangulateFace`, `Vertex3`.

- [ ] **Step 1: Header**

```cpp
// src/include/cityjson/gltf_writer.hpp
#pragma once

#include "cityjson/appearance_source.hpp"
#include "cityjson/mesh_model.hpp"

#include <functional>
#include <string>

namespace duckdb {
namespace cityjson {

struct GltfWriteOptions {
	bool binary = false;      // .glb (one file) vs .gltf (+ .bin + images)
	bool attributes = false;  // per-node extras with object_type, lod and the attributes
	std::string bin_basename; // .gltf: the buffer file name, final stem + ".bin"
	std::string image_dir;    // .gltf: directory the images are written into
};

void WriteGltf(const MeshModel &model, AppearanceSource &appearance, const std::string &out_path,
               const GltfWriteOptions &options, const std::function<bool(int64_t texture_id)> &load_image);

} // namespace cityjson
} // namespace duckdb
```

- [ ] **Step 2: Implementation**

```cpp
// src/cityjson/gltf_writer.cpp
#include "cityjson/gltf_writer.hpp"

#include "cityjson/error.hpp"
#include "cityjson/face_triangulation.hpp"

#include <tiny_gltf.h>

#include <cstring>
#include <fstream>
#include <limits>
#include <map>

namespace duckdb {
namespace cityjson {

namespace {

// ---- nlohmann json -> tinygltf::Value, for extras -------------------------------
tinygltf::Value ToValue(const json &j) {
	switch (j.type()) {
	case json::value_t::boolean:
		return tinygltf::Value(j.get<bool>());
	case json::value_t::number_integer:
	case json::value_t::number_unsigned:
		return tinygltf::Value(static_cast<int>(j.get<int64_t>()));
	case json::value_t::number_float:
		return tinygltf::Value(j.get<double>());
	case json::value_t::string:
		return tinygltf::Value(j.get<std::string>());
	case json::value_t::array: {
		tinygltf::Value::Array arr;
		for (const auto &e : j) {
			arr.push_back(ToValue(e));
		}
		return tinygltf::Value(arr);
	}
	case json::value_t::object: {
		tinygltf::Value::Object obj;
		for (auto it = j.begin(); it != j.end(); ++it) {
			obj[it.key()] = ToValue(it.value());
		}
		return tinygltf::Value(obj);
	}
	default:
		return tinygltf::Value();
	}
}

// ---- one growing buffer with 4-byte aligned views ----------------------------------
struct BufferBuilder {
	std::vector<unsigned char> data;

	void Align() {
		while (data.size() % 4 != 0) {
			data.push_back(0);
		}
	}
	//! Appends `bytes`, returns the bufferView index (target 0 = none).
	int AddView(tinygltf::Model &model, const void *bytes, size_t size, int target) {
		Align();
		tinygltf::BufferView view;
		view.buffer = 0;
		view.byteOffset = data.size();
		view.byteLength = size;
		if (target != 0) {
			view.target = target;
		}
		const auto *p = static_cast<const unsigned char *>(bytes);
		data.insert(data.end(), p, p + size);
		model.bufferViews.push_back(view);
		return static_cast<int>(model.bufferViews.size() - 1);
	}
};

// A primitive under construction: one per (material, texture, surface) group of an object.
struct PrimitiveBuild {
	std::string key;
	int material = -1;
	std::vector<float> positions;                  // xyz relative to origin
	std::vector<float> uvs;                        // uv, only when textured
	std::vector<uint32_t> indices;
	std::map<uint32_t, uint32_t> local_of_vertex;  // object vertex -> primitive vertex (untextured)
	bool textured = false;
};

int WrapMode(const std::string &wrap) {
	if (wrap == "clamp" || wrap == "border") {
		return TINYGLTF_TEXTURE_WRAP_CLAMP_TO_EDGE;
	}
	if (wrap == "mirror") {
		return TINYGLTF_TEXTURE_WRAP_MIRRORED_REPEAT;
	}
	return TINYGLTF_TEXTURE_WRAP_REPEAT;
}

std::string MimeType(const std::string &image_type) {
	if (image_type == "PNG") {
		return "image/png";
	}
	if (image_type == "JPG" || image_type == "JPEG") {
		return "image/jpeg";
	}
	return "application/octet-stream";
}

std::string BaseName(const std::string &path) {
	auto slash = path.find_last_of("/\\");
	return slash == std::string::npos ? path : path.substr(slash + 1);
}

// Image writer for .gltf output: raw bytes, no re-encoding.
bool WriteRawImage(const std::string *basepath, const std::string *filename, const tinygltf::Image *image,
                   bool /*embed*/, const tinygltf::FsCallbacks *, const tinygltf::URICallbacks *, std::string *out_uri,
                   void *) {
	if (image->image.empty() || filename == nullptr || filename->empty()) {
		*out_uri = filename != nullptr ? *filename : "";
		return true;
	}
	std::string path = basepath != nullptr && !basepath->empty() ? *basepath + "/" + *filename : *filename;
	std::ofstream out(path, std::ios::binary);
	if (!out.is_open()) {
		return false;
	}
	out.write(reinterpret_cast<const char *>(image->image.data()), static_cast<std::streamsize>(image->image.size()));
	*out_uri = *filename;
	return static_cast<bool>(out);
}

} // namespace

void WriteGltf(const MeshModel &model, AppearanceSource &appearance, const std::string &out_path,
               const GltfWriteOptions &options, const std::function<bool(int64_t texture_id)> &load_image) {
	tinygltf::Model gltf;
	gltf.asset.version = "2.0";
	gltf.asset.generator = "duckdb-cityjson";
	{
		json extras;
		extras["crs"] = model.crs.has_value() ? json(model.crs.value()) : json(nullptr);
		extras["origin"] = json::array({model.origin[0], model.origin[1], model.origin[2]});
		extras["up"] = "z";
		extras["units"] = "m";
		gltf.asset.extras = ToValue(extras);
	}

	BufferBuilder buffer;

	// ---- materials: one per distinct (name, texture) actually used --------------------
	std::map<std::string, int> material_index;
	std::map<int64_t, int> texture_index; // AppearanceSource texture id -> gltf texture
	auto gltf_texture = [&](int64_t tex_id) -> int {
		auto it = texture_index.find(tex_id);
		if (it != texture_index.end()) {
			return it->second;
		}
		if (!load_image(tex_id)) {
			texture_index[tex_id] = -1;
			return -1;
		}
		auto &tex = appearance.Textures().at(tex_id);
		tinygltf::Image image;
		image.mimeType = MimeType(tex.image_type);
		image.as_is = true;
		if (options.binary) {
			image.bufferView = buffer.AddView(gltf, tex.image_data.data(), tex.image_data.size(), 0);
		} else {
			image.image.assign(tex.image_data.begin(), tex.image_data.end());
			image.uri = BaseName(tex.image_uri);
			if (image.uri.empty()) {
				image.uri = "texture_" + std::to_string(tex_id) + (tex.image_type == "PNG" ? ".png" : ".jpg");
			}
		}
		gltf.images.push_back(image);
		tinygltf::Sampler sampler;
		sampler.wrapS = WrapMode(tex.wrap_mode);
		sampler.wrapT = WrapMode(tex.wrap_mode);
		gltf.samplers.push_back(sampler);
		tinygltf::Texture texture;
		texture.source = static_cast<int>(gltf.images.size() - 1);
		texture.sampler = static_cast<int>(gltf.samplers.size() - 1);
		gltf.textures.push_back(texture);
		int idx = static_cast<int>(gltf.textures.size() - 1);
		texture_index[tex_id] = idx;
		return idx;
	};
	auto gltf_material = [&](const MeshObject &object, const MeshFace &face) -> int {
		int tex = face.texture >= 0 && !face.uvs.empty() ? gltf_texture(face.texture) : -1;
		std::string name = FaceGroupName(object, face, appearance);
		std::string key = name + (tex >= 0 ? "#tex" + std::to_string(tex) : "");
		auto it = material_index.find(key);
		if (it != material_index.end()) {
			return it->second;
		}
		tinygltf::Material m;
		m.name = name;
		std::array<double, 3> colour;
		double transparency = 0.0;
		auto mit = face.material >= 0 ? appearance.Materials().find(face.material) : appearance.Materials().end();
		if (mit != appearance.Materials().end()) {
			colour = mit->second.diffuse;
			transparency = mit->second.transparency;
			if (mit->second.emissive.has_value()) {
				m.emissiveFactor = {(*mit->second.emissive)[0], (*mit->second.emissive)[1], (*mit->second.emissive)[2]};
			}
		} else {
			std::string surface = face.surface >= 0 && static_cast<size_t>(face.surface) < object.surfaces.size()
			                          ? object.surfaces[face.surface]
			                          : "";
			colour = DefaultColour(surface, object.object_type);
		}
		m.pbrMetallicRoughness.baseColorFactor = {colour[0], colour[1], colour[2], 1.0 - transparency};
		m.pbrMetallicRoughness.metallicFactor = 0.0;
		m.pbrMetallicRoughness.roughnessFactor = 1.0;
		m.doubleSided = true;
		m.alphaMode = transparency > 0.0 ? "BLEND" : "OPAQUE";
		if (tex >= 0) {
			m.pbrMetallicRoughness.baseColorTexture.index = tex;
			m.pbrMetallicRoughness.baseColorTexture.texCoord = 0;
		}
		gltf.materials.push_back(m);
		int idx = static_cast<int>(gltf.materials.size() - 1);
		material_index[key] = idx;
		return idx;
	};

	// ---- root node: Z-up -> Y-up ---------------------------------------------------------
	tinygltf::Node root;
	root.name = "root";
	root.matrix = {1, 0, 0, 0, 0, 0, -1, 0, 0, 1, 0, 0, 0, 0, 0, 1};
	gltf.nodes.push_back(root);
	const int root_index = 0;

	const auto &o = model.origin;
	for (const auto &object : model.objects) {
		// Group faces into primitives.
		std::vector<PrimitiveBuild> prims;
		std::map<std::string, size_t> prim_of_key;
		for (const auto &face : object.faces) {
			int material = gltf_material(object, face);
			const bool textured = face.texture >= 0 && !face.uvs.empty() && texture_index[face.texture] >= 0;
			std::string key = std::to_string(material) + (textured ? "t" : "p");
			auto pit = prim_of_key.find(key);
			if (pit == prim_of_key.end()) {
				PrimitiveBuild pb;
				pb.key = key;
				pb.material = material;
				pb.textured = textured;
				prims.push_back(std::move(pb));
				pit = prim_of_key.emplace(key, prims.size() - 1).first;
			}
			auto &pb = prims[pit->second];

			auto tris = TriangulateFace(object.vertices, face.rings);
			if (tris.empty()) {
				continue;
			}
			// Untextured: share primitive vertices across faces by object vertex index.
			// Textured: a vertex's UV differs per face, so each face gets its own vertices.
			std::map<uint32_t, std::array<double, 2>> uv_at;
			std::map<uint32_t, uint32_t> face_local;
			if (textured) {
				for (size_t r = 0; r < face.rings.size(); r++) {
					for (size_t k = 0; k < face.rings[r].size(); k++) {
						uv_at[face.rings[r][k]] = face.uvs[r][k];
					}
				}
			}
			auto emit = [&](uint32_t v) -> uint32_t {
				auto &table = textured ? face_local : pb.local_of_vertex;
				auto it = table.find(v);
				if (it != table.end()) {
					return it->second;
				}
				const auto &p = object.vertices[v];
				pb.positions.push_back(static_cast<float>(p[0] - o[0]));
				pb.positions.push_back(static_cast<float>(p[1] - o[1]));
				pb.positions.push_back(static_cast<float>(p[2] - o[2]));
				if (textured) {
					pb.uvs.push_back(static_cast<float>(uv_at[v][0]));
					pb.uvs.push_back(static_cast<float>(1.0 - uv_at[v][1]));
				}
				auto idx = static_cast<uint32_t>(pb.positions.size() / 3 - 1);
				table.emplace(v, idx);
				return idx;
			};
			for (uint32_t t : tris) {
				pb.indices.push_back(emit(t));
			}
		}

		tinygltf::Mesh mesh;
		mesh.name = object.id;
		for (auto &pb : prims) {
			if (pb.indices.empty()) {
				continue;
			}
			const size_t vertex_count = pb.positions.size() / 3;

			tinygltf::Accessor pos;
			pos.bufferView = buffer.AddView(gltf, pb.positions.data(), pb.positions.size() * sizeof(float),
			                                TINYGLTF_TARGET_ARRAY_BUFFER);
			pos.componentType = TINYGLTF_COMPONENT_TYPE_FLOAT;
			pos.count = vertex_count;
			pos.type = TINYGLTF_TYPE_VEC3;
			pos.minValues = {std::numeric_limits<double>::max(), std::numeric_limits<double>::max(),
			                 std::numeric_limits<double>::max()};
			pos.maxValues = {std::numeric_limits<double>::lowest(), std::numeric_limits<double>::lowest(),
			                 std::numeric_limits<double>::lowest()};
			for (size_t i = 0; i < pb.positions.size(); i++) {
				pos.minValues[i % 3] = std::min(pos.minValues[i % 3], static_cast<double>(pb.positions[i]));
				pos.maxValues[i % 3] = std::max(pos.maxValues[i % 3], static_cast<double>(pb.positions[i]));
			}
			gltf.accessors.push_back(pos);
			const int pos_accessor = static_cast<int>(gltf.accessors.size() - 1);

			int uv_accessor = -1;
			if (pb.textured) {
				tinygltf::Accessor uv;
				uv.bufferView =
				    buffer.AddView(gltf, pb.uvs.data(), pb.uvs.size() * sizeof(float), TINYGLTF_TARGET_ARRAY_BUFFER);
				uv.componentType = TINYGLTF_COMPONENT_TYPE_FLOAT;
				uv.count = vertex_count;
				uv.type = TINYGLTF_TYPE_VEC2;
				gltf.accessors.push_back(uv);
				uv_accessor = static_cast<int>(gltf.accessors.size() - 1);
			}

			tinygltf::Accessor idx;
			if (vertex_count < 65535) {
				std::vector<uint16_t> narrow(pb.indices.begin(), pb.indices.end());
				idx.bufferView = buffer.AddView(gltf, narrow.data(), narrow.size() * sizeof(uint16_t),
				                                TINYGLTF_TARGET_ELEMENT_ARRAY_BUFFER);
				idx.componentType = TINYGLTF_COMPONENT_TYPE_UNSIGNED_SHORT;
			} else {
				idx.bufferView = buffer.AddView(gltf, pb.indices.data(), pb.indices.size() * sizeof(uint32_t),
				                                TINYGLTF_TARGET_ELEMENT_ARRAY_BUFFER);
				idx.componentType = TINYGLTF_COMPONENT_TYPE_UNSIGNED_INT;
			}
			idx.count = pb.indices.size();
			idx.type = TINYGLTF_TYPE_SCALAR;
			gltf.accessors.push_back(idx);

			tinygltf::Primitive prim;
			prim.mode = TINYGLTF_MODE_TRIANGLES;
			prim.attributes["POSITION"] = pos_accessor;
			if (uv_accessor >= 0) {
				prim.attributes["TEXCOORD_0"] = uv_accessor;
			}
			prim.indices = static_cast<int>(gltf.accessors.size() - 1);
			prim.material = pb.material;
			mesh.primitives.push_back(prim);
		}
		if (mesh.primitives.empty()) {
			continue;
		}
		gltf.meshes.push_back(mesh);

		tinygltf::Node node;
		node.name = object.id;
		node.mesh = static_cast<int>(gltf.meshes.size() - 1);
		if (options.attributes) {
			json extras = object.attributes.is_object() ? object.attributes : json::object();
			extras["object_type"] = object.object_type;
			extras["lod"] = object.lod;
			node.extras = ToValue(extras);
		}
		gltf.nodes.push_back(node);
		gltf.nodes[root_index].children.push_back(static_cast<int>(gltf.nodes.size() - 1));
	}

	tinygltf::Buffer buf;
	buffer.Align();
	buf.data = std::move(buffer.data);
	if (!options.binary) {
		buf.uri = options.bin_basename;
	}
	gltf.buffers.push_back(std::move(buf));

	tinygltf::Scene scene;
	scene.nodes.push_back(root_index);
	gltf.scenes.push_back(scene);
	gltf.defaultScene = 0;

	tinygltf::TinyGLTF writer;
	writer.SetImageWriter(WriteRawImage, nullptr);
	// .glb: images and the buffer embedded. .gltf: buffer to bin_basename and images to
	// files, both beside out_path -- which is the temp path, in the final directory, so
	// the sidecars land next to the final file under their final names.
	bool ok = writer.WriteGltfSceneToFile(&gltf, out_path, /*embedImages=*/options.binary,
	                                      /*embedBuffers=*/options.binary, /*prettyPrint=*/!options.binary,
	                                      /*writeBinary=*/options.binary);
	if (!ok) {
		throw CityJSONError::FileWrite("Failed writing glTF output: " + out_path);
	}
}

} // namespace cityjson
} // namespace duckdb
```

Add `src/cityjson/gltf_writer.cpp` to `EXTENSION_SOURCES`. Note `#include <algorithm>` for `std::min/max`.

One subtlety pinned in the test below: for `.gltf`, tinygltf resolves `buffer.uri` and image URIs against the *directory of `out_path`*. DuckDB's temp path lives in the same directory as the final file, so the sidecars land beside the final file; the names come from `bin_basename` / the image basename, never from the temp name.

- [ ] **Step 3: Build**

Run: `just rebuild`
Expected: compiles.

- [ ] **Step 4: Commit**

```bash
git add src/include/cityjson/gltf_writer.hpp src/cityjson/gltf_writer.cpp CMakeLists.txt
git commit -m "feat(mesh): glTF / GLB writer over the mesh model on tinygltf"
```

---

### Task 3: `COPY … (FORMAT gltf | glb)`

**Files:**
- Modify: `src/cityjson/copy_function.cpp`
- Test: `test/sql/copy_gltf.test`

- [ ] **Step 1: Failing test**

`test/sql/copy_gltf.test`:

```
# name: test/sql/copy_gltf.test
# description: COPY TO gltf/glb writes one node and mesh per object, Y-up via the root matrix, origin and CRS in asset.extras
# group: [sql]

require cityjson

require json

# --- .gltf: JSON we can inspect, .bin beside it under the final stem.
statement ok
COPY (SELECT * FROM read_cityjson('test/data/holed_face.city.json', lod := '2.2'))
TO '__TEST_DIR__/holed.gltf' (FORMAT gltf);

statement ok
CREATE VIEW holed AS SELECT content::JSON AS j FROM read_text('__TEST_DIR__/holed.gltf');

query IIII
SELECT j->>'$.asset.version', j->>'$.asset.generator', j->'$.asset.extras.origin', j->>'$.asset.extras.crs' FROM holed;
----
2.0	duckdb-cityjson	[84500.0,446300.0,0.0]	https://www.opengis.net/def/crs/EPSG/0/7415

# Root node carries the Z-up -> Y-up matrix; one child per object, named after its id.
query III
SELECT json_array_length(j->'$.nodes'), j->'$.nodes[0].matrix', j->>'$.nodes[1].name' FROM holed;
----
2	[1.0,0.0,0.0,0.0,0.0,0.0,-1.0,0.0,0.0,1.0,0.0,0.0,0.0,0.0,0.0,1.0]	courtyard

query I
SELECT j->'$.scenes[0].nodes' FROM holed;
----
[0]

# One primitive per surface group (RoofSurface, WallSurface), materials named after them.
query II
SELECT json_array_length(j->'$.meshes[0].primitives'), (SELECT list(m->>'$.name') FROM (SELECT UNNEST(json_extract(j, '$.materials[*]')) AS m FROM holed)) FROM holed;
----
2	[RoofSurface, WallSurface]

query I
SELECT j->'$.materials[0].pbrMetallicRoughness.baseColorFactor' FROM holed;
----
[0.9,0.06,0.09,1.0]

query I
SELECT j->>'$.materials[0].doubleSided' FROM holed;
----
true

# Positions are float32 relative to the origin: min/max are mandatory and computed.
query II
SELECT j->'$.accessors[0].min', j->'$.accessors[0].max' FROM holed;
----
[0.0,0.0,10.0]	[4.0,4.0,10.0]

# 8 roof triangles + 2 wall triangles = 30 indices, unsigned short.
query II
SELECT (SELECT SUM((a->>'$.count')::INT) FROM (SELECT UNNEST(json_extract(j, '$.accessors[*]')) AS a FROM holed) WHERE a->>'$.type' = 'SCALAR'),
       (SELECT DISTINCT a->>'$.componentType' FROM (SELECT UNNEST(json_extract(j, '$.accessors[*]')) AS a FROM holed) WHERE a->>'$.type' = 'SCALAR');
----
30	5123

# The buffer sits beside the final file, named after it, with exactly the declared length.
query I
SELECT j->>'$.buffers[0].uri' FROM holed;
----
holed.bin

query I
SELECT (SELECT octet_length(content) FROM read_blob('__TEST_DIR__/holed.bin')) = (SELECT (j->>'$.buffers[0].byteLength')::BIGINT FROM holed);
----
true

# --- attributes true: the row's attributes plus object_type and lod on the node.
statement ok
COPY (SELECT * FROM read_cityjson('test/data/holed_face.city.json', lod := '2.2'))
TO '__TEST_DIR__/holed_attr.gltf' (FORMAT gltf, attributes true);

query III
SELECT j->>'$.nodes[1].extras.name', j->>'$.nodes[1].extras.object_type', j->>'$.nodes[1].extras.lod'
FROM (SELECT content::JSON AS j FROM read_text('__TEST_DIR__/holed_attr.gltf'));
----
ring	Building	2.2

query I
SELECT j->'$.nodes[1].extras' IS NULL FROM holed;
----
true

# --- .glb: one file. Magic, version, total length; the JSON chunk starts at byte 21.
#     A stem of its own, so the "no .bin" probe below cannot be satisfied by the .gltf export above.
statement ok
COPY (SELECT * FROM read_cityjson('test/data/holed_face.city.json', lod := '2.2'))
TO '__TEST_DIR__/holed_glb.glb' (FORMAT glb);

query III
SELECT content[1:4] = 'glTF'::BLOB, content[5:8] = '\x02\x00\x00\x00'::BLOB, content[21:21] = '{'::BLOB
FROM read_blob('__TEST_DIR__/holed_glb.glb');
----
true	true	true

query I
SELECT octet_length(content) % 4 FROM read_blob('__TEST_DIR__/holed_glb.glb');
----
0

# No .bin beside a .glb.
statement error
SELECT * FROM read_blob('__TEST_DIR__/holed_glb.bin');
----
No files found

# --- Textures: bytes from a sidecar go into a bufferView (glb) or a file (gltf).
statement ok
CREATE TABLE tex_rows AS SELECT * FROM read_obj('test/data/obj/cube.obj', lod := '2.2', appearance := 'sidecar');

statement ok
CREATE TABLE tex_defs AS
SELECT id, image_uri, '\x89PNG\x0D\x0A\x1A\x0A'::BLOB AS image_data, image_type, wrapMode, textureType, borderColor, other
FROM obj_textures('test/data/obj/cube.obj');

statement ok
COPY (SELECT * FROM tex_rows) TO '__TEST_DIR__/cube.glb'
(FORMAT glb, materials_query 'SELECT * FROM obj_materials(''test/data/obj/cube.obj'')', textures_query 'SELECT * FROM tex_defs');

statement ok
COPY (SELECT * FROM tex_rows) TO '__TEST_DIR__/cube.gltf'
(FORMAT gltf, materials_query 'SELECT * FROM obj_materials(''test/data/obj/cube.obj'')', textures_query 'SELECT * FROM tex_defs');

query IIII
SELECT json_array_length(j->'$.images'), j->>'$.images[0].mimeType', j->>'$.images[0].uri',
       (SELECT COUNT(*) FROM (SELECT UNNEST(json_extract(j, '$.meshes[0].primitives[*]')) AS p FROM (SELECT content::JSON AS j FROM read_text('__TEST_DIR__/cube.gltf'))) WHERE p->'$.attributes.TEXCOORD_0' IS NOT NULL)
FROM (SELECT content::JSON AS j FROM read_text('__TEST_DIR__/cube.gltf'));
----
1	image/png	brick.png	1

query I
SELECT octet_length(content) FROM read_blob('__TEST_DIR__/brick.png');
----
8

# The textured material references the texture; the cube's three usemtl groups plus the textured split make the primitives.
query I
SELECT (SELECT m->'$.pbrMetallicRoughness.baseColorTexture.index' FROM (SELECT UNNEST(json_extract(j, '$.materials[*]')) AS m FROM (SELECT content::JSON AS j FROM read_text('__TEST_DIR__/cube.gltf'))) WHERE m->>'$.name' = 'brick');
----
0

# --- lod selection and skipping, as for obj.
statement ok
COPY (SELECT * FROM read_cityjsonseq('test/data/delft_subset.city.jsonl'))
TO '__TEST_DIR__/delft.glb' (FORMAT glb, lod '2.2');

statement ok
COPY (SELECT * FROM read_cityjsonseq('test/data/delft_subset.city.jsonl'))
TO '__TEST_DIR__/delft.gltf' (FORMAT gltf, lod '2.2');

query I
SELECT (SELECT json_array_length(j->'$.nodes') - 1 FROM (SELECT content::JSON AS j FROM read_text('__TEST_DIR__/delft.gltf')))
     = (SELECT COUNT(*) FROM read_cityjsonseq('test/data/delft_subset.city.jsonl') WHERE geometry_lod2_2 IS NOT NULL);
----
true

# --- The appearance-form refusal applies to every mesh format.
statement error
COPY (SELECT * FROM read_cityjsonseq('test/data/railway_appearance.city.jsonl', appearance := 'sidecar'))
TO '__TEST_DIR__/refused.glb' (FORMAT glb);
----
read with appearance := 'sidecar'
```

Notes for the executor: DuckDB's `->>` on JSON returns VARCHAR, `->` returns JSON text; if the JSON extension prints numbers as `84500.0` vs `84500`, take what it prints — the assertion is the value. BLOB slicing `content[1:4]` is 1-based inclusive. If `read_blob` on a missing file errors with different text, match its actual message.

- [ ] **Step 2: Run and watch it fail**

Run: `just rebuild && ./build/release/test/unittest "test/sql/copy_gltf.test"`
Expected: FAIL — `Copy Function with name gltf does not exist`.

- [ ] **Step 3: Finalize and registration**

In `copy_function.cpp` (include `"cityjson/gltf_writer.hpp"`), next to `FinalizeObj`:

```cpp
static void FinalizeGltf(ClientContext &context, CityJSONCopyBindData &bind_data, CityJSONCopyGlobalState &gstate) {
	auto targets = ResolveMeshTargets(bind_data);
	AppearanceSource appearance = bind_data.appearance_source.value(); // a copy: LoadImage mutates
	MeshBuildOptions build;
	build.lod = bind_data.mesh_lod;
	build.origin = bind_data.mesh_origin;
	auto model = BuildMeshModel(gstate.feature_objects, gstate.feature_order, appearance, build, bind_data.crs);
	for (const auto &w : model.warnings) {
		DUCKDB_LOG_WARNING(context, "cityjson: " + w);
	}
	GltfWriteOptions options;
	options.binary = bind_data.format == CopyFormat::Glb;
	options.attributes = bind_data.gltf_attributes;
	options.bin_basename = targets.final_stem + ".bin";
	options.image_dir = targets.final_dir;
	WriteGltf(model, appearance, gstate.temp_file_path, options, [&](int64_t texture_id) {
		std::string warning;
		if (!appearance.LoadImage(context, texture_id, targets.source_dir, warning)) {
			DUCKDB_LOG_WARNING(context, "cityjson: " + warning + "; faces fall back to the material colour");
			return false;
		}
		return true;
	});
}
```

In the `Finalize` switch: `case CopyFormat::Gltf: case CopyFormat::Glb: FinalizeGltf(context, bind_data, gstate); break;`.

In `RegisterMeshCopyFunctions`, after `obj`:

```cpp
	for (const char *name : {"gltf", "glb"}) {
		CopyFunction fn(name);
		fn.extension = name;
		fn.copy_to_bind = CityJSONCopyToBind;
		fn.copy_to_initialize_global = CityJSONCopyToInitGlobal;
		fn.copy_to_initialize_local = CityJSONCopyToInitLocal;
		fn.copy_to_sink = CityJSONCopyToSink;
		fn.copy_to_combine = CityJSONCopyToCombine;
		fn.copy_to_finalize = CityJSONCopyToFinalize;
		loader.RegisterFunction(fn);
	}
```

- [ ] **Step 4: Run**

Run: `just rebuild && ./build/release/test/unittest "test/sql/copy_gltf.test"`
Expected: PASS. If the `.bin` lands under the temp name, tinygltf derived the buffer file from the output path — confirm `buf.uri` is set before `WriteGltfSceneToFile`; if images are written with a directory prefix, `BaseName` did not strip the fixture's `appearances/` path.

- [ ] **Step 5: Whole suite**

Run: `make test`
Expected: green.

- [ ] **Step 6: Commit**

```bash
git add src/cityjson/copy_function.cpp test/sql/copy_gltf.test
git commit -m "feat(copy): COPY TO gltf / glb"
```

---

### Task 4: Opt-in validation recipes

**Files:**
- Modify: `justfile`

- [ ] **Step 1: Recipes**

Append to `justfile` (after the other `test-*` recipes):

```make
# Validate a GLB export of the Delft tile with the Khronos glTF validator (needs node/npx
# and network for the tile). Zero errors is the bar; warnings are printed for reading.
test-gltf-validate:
    #!/usr/bin/env bash
    set -euo pipefail
    OUT="build/gltf_validate"
    mkdir -p "$OUT"
    ./build/release/duckdb -c "INSTALL httpfs; LOAD httpfs; LOAD 'build/release/extension/cityjson/cityjson.duckdb_extension'; \
      COPY (SELECT * FROM read_cityjsonseq('https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl')) \
      TO '$OUT/delft.glb' (FORMAT glb, lod '2.2', attributes true);"
    npx --yes gltf-validator -o "$OUT/report.json" "$OUT/delft.glb" || true
    python3 - "$OUT/report.json" <<'EOF'
    import json, sys
    r = json.load(open(sys.argv[1]))
    issues = r.get("issues", {})
    print("errors:", issues.get("numErrors"), "warnings:", issues.get("numWarnings"))
    for m in issues.get("messages", [])[:20]:
        print(" ", m.get("severity"), m.get("code"), m.get("pointer"))
    sys.exit(0 if issues.get("numErrors", 1) == 0 else 1)
    EOF

# Compare an OBJ export of the Delft tile with cjio's (pip install cjio). cjio triangulates
# and writes world coordinates, so only the vertex sets are comparable: both must contain
# the same distinct (x, y, z) triples once our origin is added back.
test-obj-cjio:
    #!/usr/bin/env bash
    set -euo pipefail
    OUT="build/obj_cjio"
    mkdir -p "$OUT"
    curl -sSL -o "$OUT/delft.city.json" https://cityjson.open3d.city/cityjson/delft.city.json
    cjio "$OUT/delft.city.json" export obj "$OUT/cjio.obj"
    ./build/release/duckdb -c "LOAD 'build/release/extension/cityjson/cityjson.duckdb_extension'; \
      COPY (SELECT * FROM read_cityjson('$OUT/delft.city.json')) TO '$OUT/ours.obj' (FORMAT obj, origin 'none', precision 3);"
    python3 - "$OUT/cjio.obj" "$OUT/ours.obj" <<'EOF'
    import sys
    def verts(p):
        s = set()
        for line in open(p):
            if line.startswith("v "):
                x, y, z = (round(float(t), 3) for t in line.split()[1:4])
                s.add((x, y, z))
        return s
    a, b = verts(sys.argv[1]), verts(sys.argv[2])
    print("cjio", len(a), "ours", len(b), "only-cjio", len(a - b), "only-ours", len(b - a))
    sys.exit(0 if a == b else 1)
    EOF
```

- [ ] **Step 2: Run them once** (network + node + cjio needed; skip silently if unavailable but record the outcome in the commit message)

Run: `just test-gltf-validate` and `just test-obj-cjio`.
Expected: `errors: 0` and equal vertex sets. If the validator flags `ACCESSOR_MIN_MISMATCH` or alignment codes, that is a writer bug — fix `gltf_writer.cpp`, do not relax the recipe.

- [ ] **Step 3: Commit**

```bash
git add justfile
git commit -m "test(mesh): opt-in glTF validator and cjio parity recipes"
```

---

### Task 5: Documentation

**Files:**
- Modify: `docs/FUNCTIONS.md` (Contents entry gains `COPY … gltf/glb`; `## Mesh interchange` section), `docs/DESIGN_DOC.md` (§9), `docs/TRAPS.md` (§COPY, §DuckDB-Wasm)

- [ ] **Step 1: FUNCTIONS.md** — under `## Mesh interchange`, after the OBJ writer (run every example, paste real output):

````markdown
### `COPY … TO 'x.glb' (FORMAT glb)` / `'x.gltf' (FORMAT gltf)`

glTF 2.0: one node and one mesh per object, the node named after the id, one
primitive per material or semantic-surface group, float32 positions relative
to `origin`, a root node whose matrix turns the Z-up data into glTF's Y-up
without touching the vertices (what cjio and py3dtiles do, and what the 3D
Tiles specification advises for Z-up sources).

```sql
COPY (SELECT * FROM read_cityjsonseq('https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl'))
TO 'delft.glb' (FORMAT glb, lod '2.2', attributes true);
```

Options `lod`, `origin`, `materials_query`, `textures_query`, `metadata_from` as
for `obj`, plus `attributes` (default `false`): every attribute column of the
row, with `object_type` and `lod`, into the node's `extras`.

`asset.extras` records `{"crs", "origin", "up": "z", "units": "m"}` so a 3D
Tiles packager can put the origin into a tile `transform` in double precision.
Materials map `diffuseColor` to `baseColorFactor` (alpha `1 − transparency`,
`BLEND` when transparent), `metallicFactor 0`, `roughnessFactor 1`,
`doubleSided true`; textures use `TEXCOORD_0` with the V axis flipped to glTF's
top-left origin. GLB embeds images as bufferViews from their raw bytes; `.gltf`
writes `<stem>.bin` and the images beside the file. No normals are written —
viewers compute flat ones, which is what a city model wants.

Under duckdb-wasm write GLB: a `.gltf` needs sidecar files the browser cannot
place beside it.

`EXT_mesh_features` / `EXT_structural_metadata` are not written; node names and
`extras` carry the identity and attributes.
````

- [ ] **Step 2: DESIGN_DOC.md §9** — append one paragraph:

```markdown
The glTF writer builds a `tinygltf::Model` from the same mesh model: one
buffer, 4-byte-aligned views, positions as float32 relative to the origin
(float32 at projected magnitudes would lose centimetres), indices narrowed to
u16 when a primitive has fewer than 65 535 vertices, and images as raw bytes
in bufferViews — tinygltf is built without stb, so nothing is ever decoded.
tinygltf was chosen over cgltf and fastgltf because it serialises `extras` and
arbitrary extension JSON verbatim, which keeps the 3D Tiles metadata
extensions a data change rather than a library change.
```

- [ ] **Step 3: TRAPS.md** — §COPY: add

```markdown
- **tinygltf without stb.** Every translation unit that includes
  `tiny_gltf.h` must see `TINYGLTF_NO_STB_IMAGE` and
  `TINYGLTF_NO_STB_IMAGE_WRITE` (compile definitions on both targets); one TU
  without them makes the `TinyGLTF` constructor reference the stb-backed
  default callbacks and the link fails. With them, the default image writer
  is absent, so the `.gltf` path installs its own (`WriteRawImage`).
- **A `.gltf`'s sidecars.** tinygltf writes `buffers[0].uri` and image files
  relative to the *output path's directory*; that is DuckDB's temp path, which
  shares the final file's directory, so the names come from the final stem and
  the temp name never leaks.
```

§DuckDB-Wasm: add `- **Mesh output**: prefer GLB; `.gltf` and OBJ write sidecar files, which MEMFS holds but the browser cannot hand to the user as a set.`

- [ ] **Step 4: Commit**

```bash
git add docs/FUNCTIONS.md docs/DESIGN_DOC.md docs/TRAPS.md
git commit -m "docs(copy): COPY TO gltf / glb"
```

---

## Self-review notes

- Spec coverage, *glTF / GLB output*: scene graph (T2), buffers incl. min/max, u16/u32, TEXCOORD flip, no normals, doubleSided (T2 + T3 test), materials (T2), images GLB/gltf (T2, T3 test), metadata `asset.extras` + `attributes` (T2, T3 test), precision rationale (docs). *Sidecar/temp-rename*: T2 note, T3 finalize, TRAPS. *Testing*: `copy_gltf.test` (T3), validator recipe (T4). Out of scope stays out (no EXT_* written; stated in docs).
- Names consistent with the previous plan: `MeshModel`, `AppearanceSource::LoadImage/Textures/Materials`, `FaceGroupName`, `DefaultColour`, `TriangulateFace`, `CopyFormat::Gltf/Glb`, `ResolveMeshTargets`, `appearance_source` (bind-time), `RegisterMeshCopyFunctions`; new here: `GltfWriteOptions`, `WriteGltf`, `FinalizeGltf`, `WriteRawImage`, `BufferBuilder`, `PrimitiveBuild`, `ToValue`.
