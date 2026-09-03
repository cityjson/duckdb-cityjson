#include "cityjson/gltf_writer.hpp"

#include "cityjson/error.hpp"
#include "cityjson/face_triangulation.hpp"
#include "cityjson/mesh_copy.hpp"

#include <tiny_gltf.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <limits>
#include <map>
#include <string>
#include <vector>

namespace duckdb {
namespace cityjson {

namespace {

// ---- nlohmann json -> tinygltf::Value, for extras ---------------------------------
//
// `Value` is deliberately spelled out at every use: appearance_source.hpp pulls in
// duckdb.hpp, so an unqualified `Value` inside namespace duckdb is duckdb::Value.
tinygltf::Value ToValue(const json &j) {
	switch (j.type()) {
	case json::value_t::boolean:
		return tinygltf::Value(j.get<bool>());
	case json::value_t::number_unsigned: {
		// tinygltf's integer constructor takes `int`. Anything wider travels as a double,
		// which is exact up to 2^53 and rounds beyond it -- still nearer the value than
		// the truncation an `int` would give.
		const uint64_t v = j.get<uint64_t>();
		if (v <= static_cast<uint64_t>(std::numeric_limits<int>::max())) {
			return tinygltf::Value(static_cast<int>(v));
		}
		return tinygltf::Value(static_cast<double>(v));
	}
	case json::value_t::number_integer: {
		const int64_t v = j.get<int64_t>();
		if (v >= static_cast<int64_t>(std::numeric_limits<int>::min()) &&
		    v <= static_cast<int64_t>(std::numeric_limits<int>::max())) {
			return tinygltf::Value(static_cast<int>(v));
		}
		return tinygltf::Value(static_cast<double>(v));
	}
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
		// Null and the parser's sentinels have no glTF spelling; tinygltf drops a
		// null-typed member from its object, which is what "absent" should mean.
		return {};
	}
}

// ---- one growing buffer with 4-byte aligned views ---------------------------------
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

//! What a face resolved to: the glTF material, and the glTF texture that material carries
//! (-1 when the face is untextured or its image could not be had).
struct FaceMaterial {
	int material = -1;
	int texture = -1;
};

// A primitive under construction: one per glTF material within an object.
struct PrimitiveBuild {
	int material = -1;
	bool textured = false;
	std::vector<float> positions; // xyz relative to the model origin
	std::vector<float> uvs;       // uv, only when textured
	std::vector<uint32_t> indices;
	std::map<uint32_t, uint32_t> local_of_vertex; // object vertex -> primitive vertex (untextured)
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

//! The image's media type, from the declared type and then from the magic bytes. Empty
//! when neither says: a glTF image with no mimeType and no URI extension is invalid, so
//! the texture is dropped rather than written unreadable.
std::string MimeType(const std::string &image_type, const std::vector<uint8_t> &bytes) {
	std::string declared;
	for (char c : image_type) {
		declared += static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
	}
	if (declared == "PNG") {
		return "image/png";
	}
	if (declared == "JPG" || declared == "JPEG") {
		return "image/jpeg";
	}
	if (bytes.size() >= 4 && bytes[0] == 0x89 && bytes[1] == 'P' && bytes[2] == 'N' && bytes[3] == 'G') {
		return "image/png";
	}
	if (bytes.size() >= 2 && bytes[0] == 0xFF && bytes[1] == 0xD8) {
		return "image/jpeg";
	}
	return "";
}

// Image writer for .gltf output: raw bytes, no re-encoding. tinygltf has no default one
// here -- the extension compiles it with TINYGLTF_NO_STB_IMAGE_WRITE -- so this is what
// SetImageWriter must install for a sidecar image to reach disk at all.
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
               const GltfWriteOptions &options, const std::function<bool(int64_t texture_id)> &load_image,
               std::vector<std::string> &warnings) {
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

	// ---- textures: the image once per AppearanceSource texture id ---------------------
	std::map<int64_t, int> texture_index;          // texture id -> glTF texture, -1 = unusable
	std::map<std::string, int64_t> basename_owner; // .gltf: which texture claimed each file name
	auto gltf_texture = [&](int64_t tex_id) -> int {
		auto known = texture_index.find(tex_id);
		if (known != texture_index.end()) {
			return known->second;
		}
		// Every failure caches -1, so a texture is loaded, sniffed and warned about once.
		if (!load_image(tex_id)) {
			texture_index[tex_id] = -1; // the caller's load_image reports why
			return -1;
		}
		auto tit = appearance.Textures().find(tex_id);
		if (tit == appearance.Textures().end() || tit->second.image_data.empty()) {
			warnings.push_back("texture " + std::to_string(tex_id) + ": no image data");
			texture_index[tex_id] = -1;
			return -1;
		}
		auto &tex = tit->second;
		std::string mime = MimeType(tex.image_type, tex.image_data);
		if (mime.empty()) {
			warnings.push_back("texture " + std::to_string(tex_id) + ": unknown image type");
			texture_index[tex_id] = -1;
			return -1;
		}

		tinygltf::Image image;
		image.mimeType = mime;
		if (options.binary) {
			image.bufferView = buffer.AddView(gltf, tex.image_data.data(), tex.image_data.size(), 0);
		} else {
			image.image.assign(tex.image_data.begin(), tex.image_data.end());
			// tinygltf writes an image's `uri` *or* its `mimeType`, never both, so a
			// name with no extension is one no viewer can type: the fallback comes from
			// the media type, which is known here and never empty.
			image.uri = TextureBasename(tex, tex_id, mime == "image/png" ? ".png" : ".jpg", basename_owner);
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

	// ---- materials: one per distinct identity actually used ---------------------------
	std::map<MaterialIdentity, int> material_index;
	auto face_material = [&](const MeshObject &object, const MeshFace &face) -> FaceMaterial {
		const bool has_uvs = !face.uvs.empty() && face.uvs.size() == face.rings.size();
		const int tex = face.texture >= 0 && has_uvs ? gltf_texture(face.texture) : -1;

		auto mit = face.material >= 0 ? appearance.Materials().find(face.material) : appearance.Materials().end();
		const bool has_material = mit != appearance.Materials().end();

		// The identity carries the *AppearanceSource* texture id, not the glTF index, so
		// it means the same thing here as in the .mtl writer; the two are in bijection. A
		// texture whose bytes could not be had leaves the face with its colour alone --
		// which is what an untextured face gets -- so it must not key an entry of its own.
		const MaterialIdentity key = IdentityOf(object, face, appearance, tex >= 0 ? face.texture : -1);
		// The default colour is the surface type's when the identity is a surface's, and
		// the object class's otherwise -- which is what `label` already holds.
		const std::string surface = key.kind == MaterialKind::DefaultSurface ? key.label : "";
		auto it = material_index.find(key);
		if (it != material_index.end()) {
			return {it->second, tex};
		}

		tinygltf::Material m;
		// glTF material names need not be unique, so the rendered name can repeat where
		// the identity above does not.
		m.name = FaceGroupName(object, face, appearance);
		std::array<double, 3> colour {};
		double transparency = 0.0;
		if (has_material) {
			colour = mit->second.diffuse;
			transparency = mit->second.transparency;
			if (mit->second.emissive.has_value()) {
				m.emissiveFactor = {(*mit->second.emissive)[0], (*mit->second.emissive)[1], (*mit->second.emissive)[2]};
			}
		} else {
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
		return {idx, tex};
	};

	// ---- root node: Z-up -> Y-up ------------------------------------------------------
	tinygltf::Node root;
	root.name = "root";
	root.matrix = {1, 0, 0, 0, 0, 0, -1, 0, 0, 1, 0, 0, 0, 0, 0, 1};
	gltf.nodes.push_back(root);
	const int root_index = 0;

	const auto &o = model.origin;
	for (const auto &object : model.objects) {
		// Group the object's faces into one primitive per glTF material. The material
		// identity already carries the texture, so every face of a primitive agrees on
		// whether it is textured.
		std::vector<PrimitiveBuild> prims;
		std::map<int, size_t> prim_of_material;
		for (size_t face_ordinal = 0; face_ordinal < object.faces.size(); face_ordinal++) {
			const auto &face = object.faces[face_ordinal];
			const FaceMaterial fm = face_material(object, face);
			const bool textured = fm.texture >= 0;
			auto pit = prim_of_material.find(fm.material);
			if (pit == prim_of_material.end()) {
				PrimitiveBuild pb;
				pb.material = fm.material;
				pb.textured = textured;
				prims.push_back(std::move(pb));
				pit = prim_of_material.emplace(fm.material, prims.size() - 1).first;
			}
			auto &pb = prims[pit->second];

			// Corner indices into the flattened ring order, not vertex indices: one vertex
			// can appear at two ring positions with different UVs, and only the corner
			// distinguishes them.
			auto corners = TriangulateFaceCorners(object.vertices, face.rings);
			if (corners.empty()) {
				// A face whose outer ring has no plane normal (every vertex collinear, or
				// coincident) cannot be triangulated, and glTF has no way to say so.
				warnings.push_back("object " + object.id + ": face " + std::to_string(face_ordinal) +
				                   " has no plane normal and could not be triangulated, skipped");
				continue;
			}
			std::vector<uint32_t> corner_vertex;
			std::vector<std::array<double, 2>> corner_uv;
			for (size_t r = 0; r < face.rings.size(); r++) {
				D_ASSERT(!textured || face.uvs[r].size() == face.rings[r].size());
				for (size_t k = 0; k < face.rings[r].size(); k++) {
					corner_vertex.push_back(face.rings[r][k]);
					if (textured) {
						corner_uv.push_back(face.uvs[r][k]);
					}
				}
			}

			// Untextured: share primitive vertices across the object's faces by vertex
			// index. Textured: one primitive vertex per face corner, since the UV is a
			// per-corner attribute.
			std::map<uint32_t, uint32_t> face_local;
			auto emit = [&](uint32_t corner) -> uint32_t {
				const uint32_t vertex = corner_vertex[corner];
				auto &table = textured ? face_local : pb.local_of_vertex;
				const uint32_t table_key = textured ? corner : vertex;
				auto it = table.find(table_key);
				if (it != table.end()) {
					return it->second;
				}
				const auto &p = object.vertices[vertex];
				pb.positions.push_back(static_cast<float>(p[0] - o[0]));
				pb.positions.push_back(static_cast<float>(p[1] - o[1]));
				pb.positions.push_back(static_cast<float>(p[2] - o[2]));
				if (textured) {
					pb.uvs.push_back(static_cast<float>(corner_uv[corner][0]));
					pb.uvs.push_back(static_cast<float>(1.0 - corner_uv[corner][1]));
				}
				auto idx = static_cast<uint32_t>(pb.positions.size() / 3 - 1);
				table.emplace(table_key, idx);
				return idx;
			};
			for (uint32_t corner : corners) {
				pb.indices.push_back(emit(corner));
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
			// min/max are required on POSITION by the spec, not optional decoration.
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
				std::vector<uint16_t> narrow;
				narrow.reserve(pb.indices.size());
				for (uint32_t i : pb.indices) {
					narrow.push_back(static_cast<uint16_t>(i));
				}
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

	// A model with nothing in it -- no rows, or every face dropped -- gets no buffer at
	// all. glTF allows zero buffers alongside zero bufferViews, but a buffer's byteLength
	// has a minimum of 1, so an empty one would be invalid (and would leave an empty .bin
	// beside the output).
	if (!buffer.data.empty()) {
		tinygltf::Buffer buf;
		buffer.Align();
		buf.data = std::move(buffer.data);
		if (!options.binary) {
			// Non-empty and not a data URI: tinygltf writes the bytes to this name under
			// dirname(out_path) and records the name in the JSON. Left empty for .glb, which
			// is the condition under which tinygltf emits the BIN chunk instead.
			buf.uri = options.bin_basename;
		}
		gltf.buffers.push_back(std::move(buf));
	}

	tinygltf::Scene scene;
	scene.nodes.push_back(root_index);
	gltf.scenes.push_back(scene);
	gltf.defaultScene = 0;

	if (options.binary) {
		// A GLB's chunk headers are uint32_t, and tinygltf's WriteBinaryGltfStream casts
		// the JSON and BIN sizes into them without checking -- so a model past 4 GiB is
		// written with wrapped lengths and no error at all. This is a cheap early
		// refusal, not the authoritative one: the JSON is not serialised yet, so a fixed
		// 64 MiB allowance stands in for it, and it is a floor, not a bound -- measured
		// JSON for city geometry runs about 1.9x the buffer size, so this only catches
		// the case where the buffer alone is already near the limit. The exact check is
		// the post-write one below, against the file's actual size.
		constexpr uint64_t GLB_MAX = (uint64_t(1) << 32) - 28; // 12-byte header + two 8-byte chunk headers
		constexpr uint64_t JSON_ALLOWANCE = uint64_t(64) << 20;
		const uint64_t bin_size = gltf.buffers.empty() ? 0 : gltf.buffers.front().data.size();
		if (bin_size + JSON_ALLOWANCE >= GLB_MAX) {
			throw CityJSONError::FileWrite(
			    "GLB output would exceed the 4 GiB limit a GLB's 32-bit chunk headers can address (" +
			    std::to_string(bin_size) +
			    " bytes of geometry and images alone): write FORMAT gltf, whose buffer is a "
			    "separate file, or split the export with a lower `lod` or a WHERE clause");
		}
	}

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

	if (options.binary) {
		// The pre-write guard above is a floor, not the real limit: the JSON tinygltf
		// serialises here can run far past the 64 MiB allowance it assumed. This is the
		// exact check, against the bytes actually written -- it is what makes the ~1.5
		// GiB - ~3.9 GiB range (past the pre-write floor, past what a 1.9x JSON estimate
		// alone would refuse) safe rather than silently corrupted. `out_path` is still
		// DuckDB's temp path at this point, sitting in the final directory under a temp
		// name; removing it here means no file -- final or temp -- is left behind, and
		// throwing keeps COPY's rename to the final path from ever running.
		std::error_code ec;
		const auto written = std::filesystem::file_size(out_path, ec);
		if (ec) {
			throw CityJSONError::FileWrite("Could not stat GLB output to check its size: " + out_path + ": " +
			                               ec.message());
		}
		constexpr uint64_t GLB_HARD_MAX = uint64_t(1) << 32;
		if (written >= GLB_HARD_MAX) {
			std::filesystem::remove(out_path, ec); // best-effort: the throw below is what matters
			throw CityJSONError::FileWrite(
			    "GLB output exceeds the 4 GiB limit a GLB's 32-bit chunk headers can address (" +
			    std::to_string(written) +
			    " bytes written): write FORMAT gltf, whose buffer is a separate file, or split the export "
			    "with a lower `lod` or a WHERE clause");
		}
	}
}

} // namespace cityjson
} // namespace duckdb
