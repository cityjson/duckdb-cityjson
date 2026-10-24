#include "cityjson/appearance_flatten.hpp"

namespace duckdb {
namespace cityjson {

namespace {

// JSON null sentinel, used as a stable reference when a source `values` entry is
// missing -- CityJSON's whole-shell / whole-solid null shorthand.
const json kJsonNull = json();

using FaceVisitor = std::function<void(const json &surface, const json &value)>;

//! Walk the geometry's WKB faces in encoder order, handing the visitor each face's
//! surface (its ring array) together with the `values` leaf aligned to it.
//!
//! The WKB encoder emits one face per CityJSON surface in document order, shells
//! flattened outer-first, dropping nothing (wkb_encoder.cpp), so this order is the
//! emitted face order. Every level tolerates a missing or non-array `values`: that is
//! CityJSON's null shorthand, and it expands to nulls sized by `boundaries`.
void ForEachFace(const Geometry &geometry, const json &values, const FaceVisitor &visit) {
	const json &boundaries = geometry.boundaries;
	if (!boundaries.is_array()) {
		return;
	}
	const json &top = values.is_array() ? values : kJsonNull;

	auto walk_shell = [&visit](const json &shell, const json &shell_values) {
		if (!shell.is_array()) {
			return;
		}
		for (size_t i = 0; i < shell.size(); ++i) {
			const bool has = shell_values.is_array() && i < shell_values.size() && !shell_values[i].is_null();
			visit(shell[i], has ? shell_values[i] : kJsonNull);
		}
	};

	const std::string &type = geometry.type;
	if (type == "Solid") {
		// boundaries = [shell, ...]; values = [[per-surface], ...]
		for (size_t si = 0; si < boundaries.size(); ++si) {
			const json &shell_values = (top.is_array() && si < top.size()) ? top[si] : kJsonNull;
			walk_shell(boundaries[si], shell_values);
		}
		return;
	}
	if (type == "MultiSolid" || type == "CompositeSolid") {
		// boundaries = [solid, ...]; solid = [shell, ...]; values = [[[..], ..], ..]
		for (size_t soi = 0; soi < boundaries.size(); ++soi) {
			const json &solid = boundaries[soi];
			if (!solid.is_array()) {
				continue;
			}
			const json &solid_values = (top.is_array() && soi < top.size()) ? top[soi] : kJsonNull;
			for (size_t si = 0; si < solid.size(); ++si) {
				const json &shell_values =
				    (solid_values.is_array() && si < solid_values.size()) ? solid_values[si] : kJsonNull;
				walk_shell(solid[si], shell_values);
			}
		}
		return;
	}
	// MultiSurface / CompositeSurface: boundaries = [surface, ...]; values = [leaf, ...]
	walk_shell(boundaries, top);
}

//! Resolve one `values` leaf to an id, or nothing. A non-integer leaf (the null
//! shorthand included) and an index the resolver rejects both yield nothing.
std::optional<int64_t> ResolveLeaf(const json &leaf, const IdResolver &resolve) {
	if (!leaf.is_number_integer()) {
		return std::nullopt;
	}
	const auto resolved = resolve(leaf.get<int64_t>());
	if (resolved < 0) {
		return std::nullopt;
	}
	return resolved;
}

//! Build one ring's texture from its source entry `[texId, uvIdx, ...]`. Anything the
//! ring cannot state completely -- an unresolvable texture, a non-integer or
//! out-of-pool UV index, fewer indices than the ring has vertices -- makes the ring
//! bare rather than shortening it: the invariant is that `uv` either holds one pair
//! per ring vertex or is absent altogether.
TextureRing BuildRing(const json &ring_values, size_t vertex_count, const IdResolver &resolve,
                      const std::vector<std::array<double, 2>> &uv_pool) {
	if (!ring_values.is_array() || ring_values.empty()) {
		return TextureRing();
	}
	const auto id = ResolveLeaf(ring_values[0], resolve);
	if (!id.has_value()) {
		return TextureRing();
	}

	std::vector<std::array<double, 2>> uv;
	uv.reserve(vertex_count);
	for (size_t v = 0; v < vertex_count; ++v) {
		const size_t at = v + 1;
		if (at >= ring_values.size() || !ring_values[at].is_number_integer()) {
			return TextureRing();
		}
		const auto uv_index = ring_values[at].get<int64_t>();
		if (uv_index < 0 || uv_index >= static_cast<int64_t>(uv_pool.size())) {
			return TextureRing();
		}
		uv.push_back(uv_pool[static_cast<size_t>(uv_index)]);
	}

	TextureRing ring;
	ring.id = id;
	ring.uv = std::move(uv);
	return ring;
}

} // namespace

json FlattenPerFace(const Geometry &geometry, const json &values) {
	json out = json::array();
	ForEachFace(geometry, values, [&out](const json &, const json &value) {
		if (value.is_null()) {
			out.push_back(nullptr);
		} else {
			out.push_back(value);
		}
	});
	return out;
}

std::vector<size_t> RingCountsPerFace(const Geometry &geometry) {
	std::vector<size_t> out;
	ForEachFace(geometry, kJsonNull,
	            [&out](const json &surface, const json &) { out.push_back(surface.is_array() ? surface.size() : 0); });
	return out;
}

std::vector<std::vector<size_t>> RingVertexCountsPerFace(const Geometry &geometry) {
	std::vector<std::vector<size_t>> out;
	ForEachFace(geometry, kJsonNull, [&out](const json &surface, const json &) {
		std::vector<size_t> rings;
		if (surface.is_array()) {
			rings.reserve(surface.size());
			for (const auto &ring : surface) {
				rings.push_back(ring.is_array() ? ring.size() : 0);
			}
		}
		out.push_back(std::move(rings));
	});
	return out;
}

MaterialCell FlattenMaterialMap(const Geometry &geometry, const json &material_map, const IdResolver &resolve) {
	MaterialCell cell;
	if (!material_map.is_object()) {
		return cell;
	}
	const auto n_faces = RingCountsPerFace(geometry).size();

	for (const auto &entry : material_map.items()) {
		const json &theme = entry.value();
		if (!theme.is_object()) {
			continue;
		}
		std::vector<std::optional<int64_t>> ids;
		const auto values = theme.find("values");
		const auto value = theme.find("value");
		if (values != theme.end()) {
			const json flat = FlattenPerFace(geometry, *values);
			ids.reserve(flat.size());
			for (const auto &leaf : flat) {
				ids.push_back(ResolveLeaf(leaf, resolve));
			}
		} else if (value != theme.end()) {
			// The column carries no whole-geometry shorthand, so a `value` broadcasts.
			ids.assign(n_faces, ResolveLeaf(*value, resolve));
		} else {
			continue;
		}
		cell.themes.emplace_back(entry.key(), std::move(ids));
	}
	return cell;
}

TextureCell FlattenTextureMap(const Geometry &geometry, const json &texture_map, const IdResolver &resolve,
                              const std::vector<std::array<double, 2>> &uv_pool) {
	TextureCell cell;
	if (!texture_map.is_object()) {
		return cell;
	}
	const auto ring_vertices = RingVertexCountsPerFace(geometry);

	for (const auto &entry : texture_map.items()) {
		const json &theme = entry.value();
		if (!theme.is_object()) {
			continue;
		}
		// CityJSON gives textures no `value` broadcast: a texture is per ring.
		const auto values = theme.find("values");
		if (values == theme.end()) {
			continue;
		}
		const json flat = FlattenPerFace(geometry, *values);

		std::vector<std::vector<TextureRing>> faces;
		faces.reserve(ring_vertices.size());
		for (size_t f = 0; f < ring_vertices.size(); ++f) {
			const json &face_values = f < flat.size() ? flat[f] : kJsonNull;
			std::vector<TextureRing> rings;
			rings.reserve(ring_vertices[f].size());
			for (size_t r = 0; r < ring_vertices[f].size(); ++r) {
				const json &ring_values =
				    (face_values.is_array() && r < face_values.size()) ? face_values[r] : kJsonNull;
				rings.push_back(BuildRing(ring_values, ring_vertices[f][r], resolve, uv_pool));
			}
			faces.push_back(std::move(rings));
		}
		cell.themes.emplace_back(entry.key(), std::move(faces));
	}
	return cell;
}

} // namespace cityjson
} // namespace duckdb
