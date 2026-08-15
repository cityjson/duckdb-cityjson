#include "cityjson/arrow_native_encoder.hpp"

#include "cityjson/error.hpp"

#include <unordered_map>

namespace duckdb {
namespace cityjson {

namespace {

/**
 * Builds the row-local vertex pool.
 *
 * Compaction is by distinct SOURCE INDEX, in first-seen order. Two different
 * source indices are two different pool entries even when their coordinates are
 * numerically identical -- CityJSON permits that and the vertices are
 * topologically distinct, so merging them would need a float-equality policy this
 * design deliberately does not have (design doc, round-2 review item 1).
 *
 * The real saving is unaffected: adjacent faces of a closed polyhedron share an
 * edge by referencing the SAME source index, which is exactly what this collapses.
 */
class Compactor {
public:
	Compactor(const std::vector<std::array<double, 3>> &vertices, const std::optional<Transform> &transform)
	    : vertices_(vertices), transform_(transform) {
	}

	uint32_t LocalIndex(const json &index_json) {
		if (!index_json.is_number_integer()) {
			throw CityJSONError::InvalidGeometry("Vertex index is not an integer: " + index_json.dump());
		}
		auto raw = index_json.get<int64_t>();
		if (raw < 0 || static_cast<uint64_t>(raw) >= vertices_.size()) {
			throw CityJSONError::InvalidGeometry("Vertex index out of bounds: " + std::to_string(raw) +
			                                     " not in [0, " + std::to_string(vertices_.size()) + ")");
		}
		auto source = static_cast<uint32_t>(raw);

		auto it = seen_.find(source);
		if (it != seen_.end()) {
			return it->second;
		}
		auto local = static_cast<uint32_t>(compacted_.size());
		const auto &vertex = vertices_[source];
		compacted_.push_back(transform_.has_value() ? transform_->Apply(vertex) : vertex);
		seen_.emplace(source, local);
		return local;
	}

	//! One ring. Closure is implicit: the first index is not repeated at the end
	//! (design doc, "Ring-closure convention").
	//!
	//! Real files do contain pre-closed rings, so a repeated endpoint is dropped
	//! rather than passed through -- keeping it would put a duplicate vertex and a
	//! zero-length final edge in a representation that declares the opposite. This
	//! mirrors the cleaning the WKB path already does from the other direction,
	//! where it appends a closing point only when front != back.
	std::vector<uint32_t> Ring(const json &ring_json) {
		if (!ring_json.is_array()) {
			throw CityJSONError::InvalidGeometry("Ring is not an array: " + ring_json.dump());
		}
		std::vector<uint32_t> ring;
		ring.reserve(ring_json.size());
		for (const auto &index_json : ring_json) {
			ring.push_back(LocalIndex(index_json));
		}
		// Guarded on size so a degenerate one- or two-index ring is left as it is
		// rather than trimmed away to nothing.
		if (ring.size() > 2 && ring.front() == ring.back()) {
			ring.pop_back();
		}
		return ring;
	}

	//! One face: exterior ring first, then any interior rings. Winding is CityJSON's
	//! own -- nothing is reversed, exactly as the WKB path also preserves source order.
	CompactedFace Face(const json &face_json) {
		if (!face_json.is_array()) {
			throw CityJSONError::InvalidGeometry("Face is not an array: " + face_json.dump());
		}
		CompactedFace face;
		face.rings.reserve(face_json.size());
		for (const auto &ring_json : face_json) {
			face.rings.push_back(Ring(ring_json));
		}
		return face;
	}

	CompactedShell Shell(const json &shell_json) {
		if (!shell_json.is_array()) {
			throw CityJSONError::InvalidGeometry("Shell is not an array: " + shell_json.dump());
		}
		CompactedShell shell;
		shell.faces.reserve(shell_json.size());
		for (const auto &face_json : shell_json) {
			shell.faces.push_back(Face(face_json));
		}
		return shell;
	}

	CompactedSolid Solid(const json &solid_json) {
		if (!solid_json.is_array()) {
			throw CityJSONError::InvalidGeometry("Solid is not an array: " + solid_json.dump());
		}
		CompactedSolid solid;
		solid.shells.reserve(solid_json.size());
		for (const auto &shell_json : solid_json) {
			solid.shells.push_back(Shell(shell_json));
		}
		return solid;
	}

	std::vector<std::array<double, 3>> Take() {
		return std::move(compacted_);
	}

private:
	const std::vector<std::array<double, 3>> &vertices_;
	const std::optional<Transform> &transform_;
	std::unordered_map<uint32_t, uint32_t> seen_;
	std::vector<std::array<double, 3>> compacted_;
};

} // namespace

CompactedGeometry ArrowNativeEncoder::Encode(const Geometry &geometry,
                                             const std::vector<std::array<double, 3>> &vertices,
                                             const std::optional<Transform> &transform) {
	if (!geometry.boundaries.is_array()) {
		throw CityJSONError::InvalidGeometry("Geometry boundaries are not an array for type " + geometry.type);
	}

	Compactor compactor(vertices, transform);
	CompactedGeometry result;

	if (geometry.type == "MultiSurface" || geometry.type == "CompositeSurface") {
		// boundaries is a list of faces. The solid and shell dimensions are padding:
		// length 1, no meaning, present only so this row has the same physical shape
		// as a Solid row in the same column.
		CompactedSolid solid;
		solid.shells.push_back(compactor.Shell(geometry.boundaries));
		result.solids.push_back(std::move(solid));
	} else if (geometry.type == "Solid") {
		// boundaries is a list of shells: index 0 exterior, the rest cavities. Only
		// the solid dimension is padding here -- the shell dimension is real.
		result.solids.push_back(compactor.Solid(geometry.boundaries));
	} else if (geometry.type == "MultiSolid" || geometry.type == "CompositeSolid") {
		// boundaries is a list of solids. No padding at all.
		result.solids.reserve(geometry.boundaries.size());
		for (const auto &solid_json : geometry.boundaries) {
			result.solids.push_back(compactor.Solid(solid_json));
		}
	} else {
		throw CityJSONError::InvalidGeometry(
		    "Geometry type '" + geometry.type +
		    "' is not supported by the arrow-native encoding in phase 1 (MultiSurface, CompositeSurface, Solid, "
		    "MultiSolid and CompositeSolid are)");
	}

	result.vertices = compactor.Take();
	return result;
}

} // namespace cityjson
} // namespace duckdb
