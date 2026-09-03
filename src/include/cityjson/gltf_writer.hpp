#pragma once

#include "cityjson/appearance_source.hpp"
#include "cityjson/mesh_model.hpp"

#include <functional>
#include <string>
#include <vector>

namespace duckdb {
namespace cityjson {

struct GltfWriteOptions {
	bool binary = false;      // .glb (one file) vs .gltf (+ .bin + images)
	bool attributes = false;  // per-node extras with object_type, lod and the attributes
	std::string bin_basename; // .gltf: the buffer file name, final stem + ".bin"
};

//! Write `model` as glTF at `out_path`. For `.gltf` the buffer is written beside it as
//! `options.bin_basename` and each texture image under its own basename, both resolved
//! against `dirname(out_path)`; for `.glb` everything is embedded in the one file.
//! `load_image(texture_id)` fills the texture's bytes and returns false when they cannot
//! be had -- the faces then keep their material colour and no texture is written. Faces
//! that cannot be triangulated are skipped, one line appended to `warnings` each, for the
//! caller to log.
void WriteGltf(const MeshModel &model, AppearanceSource &appearance, const std::string &out_path,
               const GltfWriteOptions &options, const std::function<bool(int64_t texture_id)> &load_image,
               std::vector<std::string> &warnings);

} // namespace cityjson
} // namespace duckdb
