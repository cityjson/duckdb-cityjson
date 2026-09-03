#pragma once

#include <array>
#include <cstdint>
#include <vector>

namespace duckdb {
namespace cityjson {

using Vertex3 = std::array<double, 3>;

//! Newell's method: the area-weighted normal of a (possibly non-convex) planar ring.
Vertex3 NewellNormal(const std::vector<Vertex3> &vertices, const std::vector<uint32_t> &ring);

//! Triangulate one planar face into triples of *corner* indices: positions in the
//! flattened ring order, ring 0's first, then ring 1's, and so on. That is what a writer
//! carrying a per-corner attribute stream (a UV per ring position, as CityJSON's texture
//! rings are) indexes, since one vertex can hold different attributes in different rings.
//! `rings[0]` is the outer ring, the rest are holes; every ring is a list of indices into
//! `vertices`, unclosed. Triples are wound like the outer ring. Degenerate input (fewer
//! than 3 outer vertices, zero-area normal) returns empty.
//!
//! Two preconditions are assumed, not checked: every ring index is in range for
//! `vertices`, and a hole lies inside the outer ring -- one that does not is silently
//! dropped from the triangulation.
std::vector<uint32_t> TriangulateFaceCorners(const std::vector<Vertex3> &vertices,
                                             const std::vector<std::vector<uint32_t>> &rings);

//! The same triangulation as index triples into `vertices` -- each corner mapped through
//! the flattened ring order it came from.
std::vector<uint32_t> TriangulateFace(const std::vector<Vertex3> &vertices,
                                      const std::vector<std::vector<uint32_t>> &rings);

} // namespace cityjson
} // namespace duckdb
