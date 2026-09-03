#pragma once

#include "cityjson/obj_reader.hpp"
#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {
namespace cityjson {

//! Parse read_obj's named parameters. `lod` is required and normalised; `object_type`
//! defaults to Building; `geometry_type` is one of auto | Solid | MultiSurface.
OBJReadOptions ParseOBJReadOptions(const TableFunctionBindInput &input, const std::string &function_name);

//! Registers read_obj(path, lod := …), obj_materials(path), obj_textures(path) and
//! obj_metadata(path [, crs := …]).
void RegisterOBJTableFunctions(ExtensionLoader &loader);

} // namespace cityjson
} // namespace duckdb
