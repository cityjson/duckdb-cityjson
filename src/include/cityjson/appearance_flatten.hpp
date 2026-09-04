#pragma once

#include "cityjson/cityjson_types.hpp"

#include <array>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace duckdb {
namespace cityjson {

/**
 * The flattening kernel behind the typed `material_lod*` / `texture_lod*` columns.
 *
 * The specification (04-appearance-templates.mdx, "material / texture columns") makes
 * appearance positional: entry `i` of a theme is WKB face `i`, exactly as
 * `face_semantics` is. So there is one face walk in this extension -- `FlattenPerFace`
 * below -- and `geometry_properties` uses it too. A consumer never has to know a
 * geometry's nesting to reach a face's material.
 */

//! One theme's material ids, one per WKB face; nullopt where the source assigns none.
struct MaterialCell {
	std::vector<std::pair<std::string, std::vector<std::optional<int64_t>>>> themes;
};

//! One ring's texture: the sidecar (or feature-local) id and one `[u, v]` pair per
//! ring vertex. A ring with no texture is bare -- `id` unset and `uv` empty together.
struct TextureRing {
	std::optional<int64_t> id;
	std::vector<std::array<double, 2>> uv;
};

//! One theme's textures: per WKB face, per ring of that face.
struct TextureCell {
	std::vector<std::pair<std::string, std::vector<std::vector<TextureRing>>>> themes;
};

//! One entry per WKB face in wkb_encoder order: the `values` leaf for that face
//! (a material id, or a texture face's ring array), or null. Expands CityJSON's
//! null shorthand at every level: a missing, non-array or null level yields nulls
//! sized by `boundaries`, a short level is padded and a long one truncated.
json FlattenPerFace(const Geometry &geometry, const json &values);

//! The ring count of every face, in the same order.
std::vector<size_t> RingCountsPerFace(const Geometry &geometry);

//! The vertex count of every ring of every face (source indices; the WKB point count
//! minus one, since a WKB ring repeats its first point to close and that repeat
//! carries no UV pair).
std::vector<std::vector<size_t>> RingVertexCountsPerFace(const Geometry &geometry);

//! Maps a source appearance index onto the id space the column carries. Sidecar mode
//! resolves through the interned index; local mode is the identity. Returns -1 when
//! the index cannot be resolved, which makes the entry null.
using IdResolver = std::function<int64_t(int64_t)>;

/**
 * Flatten one geometry's CityJSON material map into per-theme, per-face ids.
 *
 * A theme's `values` is walked with `FlattenPerFace`; a whole-geometry `value` is
 * expanded to one id per face, because the column has no broadcast shorthand. A theme
 * carrying neither key contributes nothing: the reader is lenient on source data, and
 * such a theme holds no appearance to carry.
 */
MaterialCell FlattenMaterialMap(const Geometry &geometry, const json &material_map, const IdResolver &resolve);

/**
 * Flatten one geometry's CityJSON texture map into per-theme, per-face, per-ring
 * structs, with the UV indices resolved against `uv_pool` and inlined.
 *
 * Every face of the geometry gets an entry and every ring of that face gets a struct,
 * so the cell satisfies the length invariants whatever the source omits. A ring the
 * source leaves out, writes as `[null]`, points at an unresolvable texture, or indexes
 * outside the pool is bare; surplus indices beyond the ring's vertex count are dropped.
 */
TextureCell FlattenTextureMap(const Geometry &geometry, const json &texture_map, const IdResolver &resolve,
                              const std::vector<std::array<double, 2>> &uv_pool);

} // namespace cityjson
} // namespace duckdb
