#include "cityjson/mesh_copy.hpp"

#include "cityjson/gltf_writer.hpp"
#include "cityjson/mesh_model.hpp"
#include "cityjson/obj_writer.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <fstream>
#include <map>

namespace duckdb {
namespace cityjson {

MeshTargets ResolveMeshTargets(const CityJSONCopyBindData &bind_data) {
	MeshTargets t;
	const auto &p = bind_data.file_path;
	auto slash = p.find_last_of("/\\");
	t.final_dir = slash == std::string::npos ? "" : p.substr(0, slash);
	std::string base = slash == std::string::npos ? p : p.substr(slash + 1);
	auto dot = base.rfind('.');
	t.final_stem = dot == std::string::npos ? base : base.substr(0, dot);
	if (bind_data.source_ref.has_value()) {
		// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
		const auto &source_path = bind_data.source_ref->path;
		auto ss = source_path.find_last_of("/\\");
		t.source_dir = ss == std::string::npos ? "" : source_path.substr(0, ss);
	}
	return t;
}

std::string JoinDir(const std::string &dir, const std::string &name) {
	return dir.empty() ? name : dir + "/" + name;
}

AppearanceSource BuildAppearanceSource(ClientContext &context, const CityJSONCopyBindData &bind_data) {
	if (bind_data.materials_query.has_value() || bind_data.textures_query.has_value()) {
		return AppearanceSource::FromQueries(context, bind_data.materials_query, bind_data.textures_query);
	}
	return AppearanceSource::FromLocal(bind_data.source_appearance_header, bind_data.source_appearance_by_feature);
}

bool CopyTextureImage(ClientContext &context, AppearanceSource &appearance, int64_t texture_id,
                      const MeshTargets &targets, std::map<std::string, int64_t> &basename_owner,
                      std::string &basename) {
	std::string warning;
	if (!appearance.LoadImage(context, texture_id, targets.source_dir, warning)) {
		DUCKDB_LOG_WARNING(context, "cityjson: " + warning + "; faces fall back to the material colour");
		return false;
	}
	auto &tex = appearance.Textures().at(texture_id);
	auto slash = tex.image_uri.find_last_of("/\\");
	basename = slash == std::string::npos ? tex.image_uri : tex.image_uri.substr(slash + 1);
	if (basename.empty()) {
		// No URI to name the file after. The extension is what a viewer reads the format
		// from, so it is appended only when the row declares one -- never a trailing dot.
		basename = "texture_" + std::to_string(texture_id);
		if (!tex.image_type.empty()) {
			basename += "." + StringUtil::Lower(tex.image_type);
		}
	}
	// Images are copied flat beside the output, so two URIs differing only in their
	// directory ("a/x.png", "b/x.png") arrive under one name and the second would
	// overwrite the first, silently re-texturing its faces. Qualify the later one.
	auto owner = basename_owner.find(basename);
	if (owner != basename_owner.end() && owner->second != texture_id) {
		basename = "texture_" + std::to_string(texture_id) + "_" + basename;
	}
	basename_owner[basename] = texture_id;
	std::ofstream img(JoinDir(targets.final_dir, basename), std::ios::binary);
	if (!img.is_open()) {
		DUCKDB_LOG_WARNING(context, "cityjson: could not write texture image '" + basename + "'");
		return false;
	}
	img.write(reinterpret_cast<const char *>(tex.image_data.data()),
	          static_cast<std::streamsize>(tex.image_data.size()));
	return static_cast<bool>(img);
}

//! What both mesh finalizes need before they can write: where the output goes, a private
//! copy of the appearance (the writers' LoadImage fills texture bytes into it, and the
//! bind data must stay as bound), and the model the sink's rows build, with the build's
//! own warnings already logged.
struct MeshCopyInputs {
	MeshTargets targets;
	AppearanceSource appearance;
	MeshModel model;
};

static MeshCopyInputs PrepareMeshCopy(ClientContext &context, CityJSONCopyBindData &bind_data,
                                      CityJSONCopyGlobalState &gstate, const char *format_label) {
	MeshCopyInputs in;
	in.targets = ResolveMeshTargets(bind_data);
	if (!bind_data.appearance_source.has_value()) {
		// The bind resolves this for every mesh format; reaching Finalize without it
		// means the bind and the finalize disagree about what a mesh format is.
		throw InternalException(std::string(format_label) + " COPY finalised without an appearance source");
	}
	// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
	in.appearance = bind_data.appearance_source.value();
	MeshBuildOptions build;
	build.lod = bind_data.mesh_lod;
	build.origin = bind_data.mesh_origin;
	in.model = BuildMeshModel(gstate.feature_objects, gstate.feature_order, in.appearance, build, bind_data.crs);
	for (const auto &w : in.model.warnings) {
		DUCKDB_LOG_WARNING(context, "cityjson: " + w);
	}
	return in;
}

void FinalizeObj(ClientContext &context, CityJSONCopyBindData &bind_data, CityJSONCopyGlobalState &gstate) {
	auto in = PrepareMeshCopy(context, bind_data, gstate, "obj");
	auto &targets = in.targets;
	auto &appearance = in.appearance;
	OBJWriteOptions options;
	options.triangulate = bind_data.obj_triangulate;
	options.precision = bind_data.obj_precision;
	const std::string mtl_basename = targets.final_stem + ".mtl";
	std::vector<std::string> write_warnings;
	std::map<std::string, int64_t> basename_owner; // copied image name -> the texture that claimed it
	WriteOBJ(
	    in.model, appearance, gstate.temp_file_path, JoinDir(targets.final_dir, mtl_basename), mtl_basename, options,
	    [&](int64_t texture_id, std::string &basename) {
		    return CopyTextureImage(context, appearance, texture_id, targets, basename_owner, basename);
	    },
	    write_warnings);
	for (const auto &w : write_warnings) {
		DUCKDB_LOG_WARNING(context, "cityjson: " + w);
	}
}

void FinalizeGltf(ClientContext &context, CityJSONCopyBindData &bind_data, CityJSONCopyGlobalState &gstate) {
	auto in = PrepareMeshCopy(context, bind_data, gstate, "glTF");
	auto &targets = in.targets;
	auto &appearance = in.appearance;
	GltfWriteOptions options;
	options.binary = bind_data.format == CopyFormat::Glb;
	options.attributes = bind_data.gltf_attributes;
	// The writer is handed the temp path, which sits in the final directory, so the
	// buffer and the images it writes beside it land next to the final file -- under
	// the final stem, which the temp name does not carry.
	options.bin_basename = targets.final_stem + ".bin";
	std::vector<std::string> write_warnings;
	WriteGltf(
	    in.model, appearance, gstate.temp_file_path, options,
	    [&](int64_t texture_id) {
		    std::string warning;
		    if (!appearance.LoadImage(context, texture_id, targets.source_dir, warning)) {
			    DUCKDB_LOG_WARNING(context, "cityjson: " + warning + "; faces fall back to the material colour");
			    return false;
		    }
		    return true;
	    },
	    write_warnings);
	for (const auto &w : write_warnings) {
		DUCKDB_LOG_WARNING(context, "cityjson: " + w);
	}
}

void RegisterMeshCopyFunctions(ExtensionLoader &loader) {
	// One function per format name, all bound by the same bind/sink/finalize: the
	// format is what the bind reads off the name, and the finalize dispatches on it.
	for (const char *name : {"obj", "gltf", "glb"}) {
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
}

} // namespace cityjson
} // namespace duckdb
