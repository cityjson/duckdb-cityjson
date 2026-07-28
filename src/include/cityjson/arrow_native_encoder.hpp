#pragma once

#include "cityjson/cityjson_types.hpp"

#include <array>
#include <cstdint>
#include <optional>
#include <vector>

namespace duckdb {
namespace cityjson {

/**
 * One face of a shell: an exterior ring followed by any interior rings (holes),
 * each a list of indices into the owning CompactedGeometry's vertex pool.
 *
 * Rings do NOT repeat the closing vertex -- first index != last index, closure is
 * implicit. This departs from WKB, which requires literal closure, and follows
 * CityJSON's own convention (design doc, "Ring-closure convention").
 */
struct CompactedFace {
	std::vector<std::vector<uint32_t>> rings;
};

//! One shell: for a Solid family, index 0 is the exterior shell and the rest are
//! cavities. For a surface family this is a padding dimension holding every face.
struct CompactedShell {
	std::vector<CompactedFace> faces;
};

//! One solid. For a surface family this is a padding dimension of length 1.
struct CompactedSolid {
	std::vector<CompactedShell> shells;
};

/**
 * A single geometry in the arrow-native encoding: this object's own vertex pool
 * plus the nested index structure that references it.
 *
 * `solids` is always populated, including for the surface families, whose two
 * outer dimensions are padding of length 1 and carry no solid/shell meaning --
 * they exist so every row of a geometry column has one physical Arrow shape.
 * Nothing may infer the CityJSON type from this nesting; that is what
 * geometry_properties_lod*.type is for (design doc, "Critical invariant").
 */
struct CompactedGeometry {
	std::vector<std::array<double, 3>> vertices;
	std::vector<CompactedSolid> solids;
};

/**
 * Encodes a CityJSON Geometry into the arrow-native form, mirroring WKBEncoder's
 * static shape and taking the same three arguments.
 */
class ArrowNativeEncoder {
public:
	/**
	 * Build the compacted geometry for one CityObject's geometry.
	 *
	 * The vertex pool is built by compacting the DISTINCT SOURCE INDICES the
	 * geometry references into a dense local range, in first-seen order -- never by
	 * comparing coordinate values. CityJSON permits two distinct indices to carry
	 * identical coordinates and they stay two entries; merging them would need a
	 * float-equality policy defined nowhere and would collapse topologically
	 * distinct vertices (design doc, round-2 review item 1).
	 *
	 * This matters because CityJSON's vertex list is scoped to a whole feature
	 * (parent plus children) while a Parquet row is one CityObject, so the row
	 * carries only the slice of that list it actually references.
	 *
	 * @param geometry Geometry to encode; must be one of the phase-1 types
	 * @param vertices The feature's vertex pool, which boundaries index into
	 * @param transform Optional transform applied to each dereferenced vertex
	 * @return The compacted geometry
	 * @throws CityJSONError if the type is out of phase-1 scope, the boundaries are
	 *         malformed, or an index falls outside the vertex pool
	 */
	static CompactedGeometry Encode(const Geometry &geometry, const std::vector<std::array<double, 3>> &vertices,
	                                const std::optional<Transform> &transform = std::nullopt);
};

} // namespace cityjson
} // namespace duckdb
