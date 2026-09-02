#include "cityjson/mesh_copy.hpp"

#include "cityjson/mesh_model.hpp"
#include "cityjson/obj_writer.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <fstream>

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
	img.write(reinterpret_cast<const char *>(tex.image_data.data()),
	          static_cast<std::streamsize>(tex.image_data.size()));
	return static_cast<bool>(img);
}

void FinalizeObj(ClientContext &context, CityJSONCopyBindData &bind_data, CityJSONCopyGlobalState &gstate) {
	auto targets = ResolveMeshTargets(bind_data);
	if (!bind_data.appearance_source.has_value()) {
		// The bind resolves this for every mesh format; reaching Finalize without it
		// means the bind and the finalize disagree about what a mesh format is.
		throw InternalException("obj COPY finalised without an appearance source");
	}
	// A copy: LoadImage fills texture bytes, and the bind data must stay as bound.
	// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
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
	std::vector<std::string> write_warnings;
	WriteOBJ(
	    model, appearance, gstate.temp_file_path, JoinDir(targets.final_dir, mtl_basename), mtl_basename, options,
	    [&](int64_t texture_id, std::string &basename) {
		    return CopyTextureImage(context, appearance, texture_id, targets, basename);
	    },
	    write_warnings);
	for (const auto &w : write_warnings) {
		DUCKDB_LOG_WARNING(context, "cityjson: " + w);
	}
}

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

} // namespace cityjson
} // namespace duckdb
