#pragma once

#include <array>
#include <cstdint>
#include <vector>

namespace duckdb {
namespace cityjson {

using Vertex3 = std::array<double, 3>;

//! Newell's method: the area-weighted normal of a (possibly non-convex) planar ring.
Vertex3 NewellNormal(const std::vector<Vertex3> &vertices, const std::vector<uint32_t> &ring);

//! Triangulate one planar face. `rings[0]` is the outer ring, the rest are holes; every
//! ring is a list of indices into `vertices`, unclosed. Returns index triples into
//! `vertices`, wound like the outer ring. Degenerate input (fewer than 3 outer vertices,
//! zero-area normal) returns empty.
//!
//! Two preconditions are assumed, not checked: every ring index is in range for
//! `vertices`, and a hole lies inside the outer ring -- one that does not is silently
//! dropped from the triangulation.
std::vector<uint32_t> TriangulateFace(const std::vector<Vertex3> &vertices,
                                      const std::vector<std::vector<uint32_t>> &rings);

} // namespace cityjson
} // namespace duckdb
