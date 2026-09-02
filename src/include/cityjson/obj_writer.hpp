#pragma once

#include "cityjson/appearance_source.hpp"
#include "cityjson/mesh_model.hpp"

#include <functional>
#include <string>
#include <vector>

namespace duckdb {
namespace cityjson {

struct OBJWriteOptions {
	bool triangulate = false; // hole-free faces stay n-gons unless true
	int precision = 17;       // significant digits; 17 = shortest round-trip
};

std::string FormatDouble(double v, int precision);

//! Write `model` as `obj_path` plus the `.mtl` at `mtl_path`, which the OBJ names by
//! `mtl_basename` (the final file's, never the temp path's). `image_writer` is asked
//! for a texture's copied image on first use and reports the basename to put in
//! `map_Kd`; returning false leaves the face with its material colour. Faces that
//! cannot be written -- a face with no plane normal to triangulate against -- are
//! skipped, one line appended to `warnings` each, for the caller to log.
void WriteOBJ(const MeshModel &model, AppearanceSource &appearance, const std::string &obj_path,
              const std::string &mtl_path, const std::string &mtl_basename, const OBJWriteOptions &options,
              const std::function<bool(int64_t texture_id, std::string &basename)> &image_writer,
              std::vector<std::string> &warnings);

} // namespace cityjson
} // namespace duckdb
