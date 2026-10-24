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

//! The file name texture `id`'s image is written under, beside the output: the image's
//! own basename when its URI has one, else `texture_<id>` followed by `fallback_ext`
//! (which the caller derives -- the declared `image_type` for `obj`, the media type
//! glTF has to state anyway for `gltf` -- and which is empty when nothing says).
//!
//! Images land flat in one directory, so two URIs differing only in their directory
//! ("a/x.png", "b/x.png") would arrive under one name and the second would overwrite the
//! first, silently re-texturing its faces. `basename_owner` records which texture claimed
//! each name; a name already claimed is qualified with `texture_<id>_`. A texture is
//! resolved once per write, so an existing claim is always some other texture's.
std::string TextureBasename(const MeshTexture &tex, int64_t id, const std::string &fallback_ext,
                            std::map<std::string, int64_t> &basename_owner);

//! Copies texture `id`'s bytes beside the final output under `TextureBasename`'s name;
//! returns false (and logs) when the bytes cannot be had.
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
