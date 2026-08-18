#pragma once

#include "cityjson/cityjson_types.hpp"

#include <map>
#include <string>
#include <vector>

namespace duckdb {
namespace cityjson {

/**
 * The dataset-global appearance sets, and the per-feature index maps that reach them.
 *
 * CityJSONSeq does **not** put all appearance definitions in one place. The header line
 * carries the union, and each feature repeats the subset it actually uses — with its own
 * local indices. So a feature's material index `0` names *that feature's*
 * `appearance.materials[0]`, which is not in general the header's entry `0`.
 * (`test/data/railway_appearance.city.jsonl` is exactly this shape: the header declares
 * two materials and two textures, one feature repeats the textures, the other repeats
 * the materials.)
 *
 * CityParquet requires dataset-global ids, so the definitions are **interned**: each
 * distinct definition gets one id, and every feature's local indices are mapped onto it.
 * Header entries are interned first so their ids remain their ordinal positions, which
 * is what a plain CityJSON document (which has only a header appearance) yields too.
 *
 * Two definitions are the same when their content is the same. Identity is not
 * available — CityJSON gives a material no id of its own — so structural equality is
 * the only thing to key on.
 */
struct AppearanceIndex {
	//! Deduplicated, in id order: index i is the definition with id i.
	std::vector<Material> materials;
	std::vector<Texture> textures;

	//! feature id -> (local index -> global id). A feature absent from the map has no
	//! definitions of its own and indexes the header set directly.
	std::map<std::string, std::vector<int64_t>> material_map;
	std::map<std::string, std::vector<int64_t>> texture_map;

	//! Resolve a local index for one feature to its global id. Falls back to the
	//! identity mapping when the feature declared no definitions of its own, which is
	//! the plain-CityJSON case. Returns -1 when the index cannot be resolved.
	int64_t ResolveMaterial(const std::string &feature_id, int64_t local_index) const;
	int64_t ResolveTexture(const std::string &feature_id, int64_t local_index) const;

	static AppearanceIndex Build(const CityJSON &header, const std::vector<CityJSONFeature> &features);
};

/**
 * Rewrite one geometry's material map so every index is a dataset-global sidecar id.
 */
json NormaliseMaterialMap(const json &material_map, const AppearanceIndex &index, const std::string &feature_id);

/**
 * Rewrite one geometry's texture map: every texture index becomes a global sidecar id,
 * and every UV *index* is replaced by the `[u, v]` pair it names in `uv_pool`.
 *
 * The UV pool is per-feature, so a stored index would be meaningless once features are
 * merged into one table — which is exactly what CityParquet does. Inlining the
 * coordinates is what makes the object table self-contained.
 *
 * A ring is recognised as the innermost array (the one whose own elements are scalars)
 * rather than assumed at a fixed depth: the nesting above it varies with geometry type,
 * a Solid sitting one level deeper than a MultiSurface.
 */
json NormaliseTextureMap(const json &texture_map, const AppearanceIndex &index, const std::string &feature_id,
                         const std::vector<std::array<double, 2>> &uv_pool);

//! Stable content keys, used to intern definitions by structural equality.
std::string MaterialKey(const Material &material);
std::string TextureKey(const Texture &texture);

} // namespace cityjson
} // namespace duckdb
