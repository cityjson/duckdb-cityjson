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
