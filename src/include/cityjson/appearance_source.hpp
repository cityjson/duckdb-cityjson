#pragma once

#include "cityjson/appearance_normalise.hpp"
#include "cityjson/json_utils.hpp"
#include "duckdb.hpp"

#include <array>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace duckdb {
namespace cityjson {

//! A material as a mesh writer needs it: colour and transparency, nothing CityJSON-shaped.
struct MeshMaterial {
	std::string name;
	std::array<double, 3> diffuse {0.7, 0.7, 0.7};
	std::optional<std::array<double, 3>> specular;
	std::optional<std::array<double, 3>> emissive;
	double transparency = 0.0;
	std::optional<double> shininess;
};

//! A texture as a mesh writer needs it. `image_data` is empty until LoadImage fills it
//! from `image_uri` (or the sidecar's own bytes).
struct MeshTexture {
	std::string image_uri;
	std::string image_type; // "PNG", "JPG"
	std::string wrap_mode = "wrap";
	std::vector<uint8_t> image_data;
};

/**
 * Where a COPY's appearance references resolve.
 *
 * `material_lod*` cells look identical in local form (feature-local indices into the
 * source's own blocks) and sidecar form (dataset-global ids into materials.parquet), so
 * the COPY is told which it has: materials_query / textures_query present means sidecar,
 * absent means local. This class hides the difference from the mesh writers.
 */
class AppearanceSource {
public:
	//! Local form: the discovered source's raw `appearance` header block and, for
	//! CityJSONSeq, each feature's own block (its refs are local to that block).
	static AppearanceSource FromLocal(const std::optional<json> &header, const std::map<std::string, json> &by_feature);
	//! Sidecar form: rows shaped like cityjson_materials / cityjson_textures.
	static AppearanceSource FromQueries(ClientContext &context, const std::optional<std::string> &materials_query,
	                                    const std::optional<std::string> &textures_query);

	bool IsSidecar() const {
		return sidecar_;
	}
	std::optional<int64_t> ResolveMaterial(const std::string &feature_id, int64_t ref) const;
	std::optional<int64_t> ResolveTexture(const std::string &feature_id, int64_t ref) const;
	//! A ring's UV element: an inline [u, v] pair, in both local and sidecar form --
	//! only the material/texture *ids* differ between the two modes, never how a UV is
	//! spelled. nullopt when the element is neither a pair nor a number (e.g. absent);
	//! a bare integer index throws InvalidInputException, since neither form uses one.
	std::optional<std::array<double, 2>> UV(const std::string &feature_id, const json &uv_ref) const;

	const std::map<int64_t, MeshMaterial> &Materials() const {
		return materials_;
	}
	std::map<int64_t, MeshTexture> &Textures() {
		return textures_;
	}
	bool LoadImage(ClientContext &context, int64_t texture_id, const std::string &base_dir, std::string &warning);

private:
	bool sidecar_ = false;
	AppearanceIndex index_; // local form only
	std::map<int64_t, MeshMaterial> materials_;
	std::map<int64_t, MeshTexture> textures_;
};

} // namespace cityjson
} // namespace duckdb
