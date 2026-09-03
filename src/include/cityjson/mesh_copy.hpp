#pragma once

#include "cityjson/appearance_source.hpp"
#include "cityjson/copy_function.hpp"

#include <map>
#include <string>

namespace duckdb {

class ExtensionLoader;

namespace cityjson {

//! Where a mesh COPY writes, and where what it reads resolves against.
struct MeshTargets {
	std::string final_dir;  // directory of the final output path ("" = cwd)
	std::string final_stem; // "campus" for "campus.obj"
	std::string source_dir; // where relative image URIs resolve ("" = unknown)
};

MeshTargets ResolveMeshTargets(const CityJSONCopyBindData &bind_data);

std::string JoinDir(const std::string &dir, const std::string &name);

//! Where a mesh COPY's material/texture references resolve: the sidecar queries when
//! either is given (their presence is what declares the sidecar form), else the
//! discovered source's own appearance blocks.
AppearanceSource BuildAppearanceSource(ClientContext &context, const CityJSONCopyBindData &bind_data);

//! Copies texture `id`'s bytes beside the final output under the image's own basename;
//! returns false (and logs) when the bytes cannot be had. `basename_owner` records which
//! texture claimed each name, so a second texture whose URI shares a basename is written
//! under a name qualified by its id rather than over the first one's bytes.
bool CopyTextureImage(ClientContext &context, AppearanceSource &appearance, int64_t texture_id,
                      const MeshTargets &targets, std::map<std::string, int64_t> &basename_owner,
                      std::string &basename);

//! The `obj` branch of the COPY finalize: build the mesh model the sink accumulated and
//! write it, with the `.mtl` and any images beside the *final* path.
void FinalizeObj(ClientContext &context, CityJSONCopyBindData &bind_data, CityJSONCopyGlobalState &gstate);

//! The `gltf`/`glb` branch of the COPY finalize: build the mesh model the sink
//! accumulated and write it, with the `.bin` and any images beside the *final* path
//! for `.gltf`, and everything in the one file for `.glb`.
void FinalizeGltf(ClientContext &context, CityJSONCopyBindData &bind_data, CityJSONCopyGlobalState &gstate);

//! COPY TO for the mesh interchange formats. One function per format name, all bound
//! by the same bind/sink/finalize: `obj` today, `gltf`/`glb` alongside it.
void RegisterMeshCopyFunctions(ExtensionLoader &loader);

} // namespace cityjson
} // namespace duckdb
