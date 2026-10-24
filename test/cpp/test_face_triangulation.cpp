#include "cityjson/face_triangulation.hpp"

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdio>
#include <cstdlib>

using duckdb::cityjson::TriangulateFace;
using duckdb::cityjson::TriangulateFaceCorners;
using duckdb::cityjson::Vertex3;

static int failures = 0;
#define CHECK(cond)                                                                                                    \
	do {                                                                                                               \
		if (!(cond)) {                                                                                                 \
			std::fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);                                       \
			failures++;                                                                                                \
		}                                                                                                              \
	} while (0)

static double TriArea2D(const std::vector<Vertex3> &v, uint32_t a, uint32_t b, uint32_t c) {
	return 0.5 * ((v[b][0] - v[a][0]) * (v[c][1] - v[a][1]) - (v[c][0] - v[a][0]) * (v[b][1] - v[a][1]));
}

// Same shoelace, but on the (y, z) pair -- for faces whose dominant axis is x, where the
// natural 2D reading of the ring is (y, z), not (x, y).
static double TriAreaYZ(const std::vector<Vertex3> &v, uint32_t a, uint32_t b, uint32_t c) {
	return 0.5 * ((v[b][1] - v[a][1]) * (v[c][2] - v[a][2]) - (v[c][1] - v[a][1]) * (v[b][2] - v[a][2]));
}

int main() {
	// A 4x4 square with a 2x2 square hole, CCW outer, CW hole (CityJSON's inner-ring rule).
	std::vector<Vertex3> v = {{0, 0, 0}, {4, 0, 0}, {4, 4, 0}, {0, 4, 0},  // outer
	                          {1, 1, 0}, {1, 3, 0}, {3, 3, 0}, {3, 1, 0}}; // hole (CW)
	auto tris = TriangulateFace(v, {{0, 1, 2, 3}, {4, 5, 6, 7}});
	CHECK(tris.size() == 8 * 3);
	double area = 0;
	for (size_t i = 0; i + 2 < tris.size(); i += 3) {
		double a = TriArea2D(v, tris[i], tris[i + 1], tris[i + 2]);
		CHECK(a > 0); // every triangle wound like the outer ring (CCW in XY)
		area += a;
	}
	CHECK(std::fabs(area - 12.0) < 1e-9);

	// Same square and hole, outer ring reversed (CW): every triangle must wind CW too --
	// the re-wind tracks whatever the outer ring's winding is, not a fixed CCW default.
	auto tris_rev = TriangulateFace(v, {{3, 2, 1, 0}, {4, 5, 6, 7}});
	CHECK(tris_rev.size() == 8 * 3);
	double area_rev = 0;
	for (size_t i = 0; i + 2 < tris_rev.size(); i += 3) {
		double a = TriArea2D(v, tris_rev[i], tris_rev[i + 1], tris_rev[i + 2]);
		CHECK(a < 0); // every triangle wound like the (now CW) outer ring
		area_rev += a;
	}
	CHECK(std::fabs(area_rev + 12.0) < 1e-9);

	// The corner form: triples of positions in the flattened ring order (ring 0's
	// vertices, then ring 1's), which is what a writer with a per-corner attribute
	// stream indexes -- distinct from TriangulateFace's triples of vertex ids. A ring
	// naming vertices 0..7 (as every other case in this file does) cannot tell the two
	// apart: corner positions and vertex ids happen to occupy the same range, so a
	// TriangulateFaceCorners that mistakenly returned vertex ids would pass unnoticed.
	// Here the ring references only vertices 8..15 -- the same 4x4-square-with-a-2x2-hole
	// geometry as the very first triangulation above, offset by 8 -- so corner positions
	// (0..7) and vertex ids (8..15) occupy disjoint ranges and a mix-up is visible.
	std::vector<Vertex3> v16(16, Vertex3 {0, 0, 0});
	v16[8] = {0, 0, 0};
	v16[9] = {4, 0, 0};
	v16[10] = {4, 4, 0};
	v16[11] = {0, 4, 0};
	v16[12] = {1, 1, 0};
	v16[13] = {1, 3, 0};
	v16[14] = {3, 3, 0};
	v16[15] = {3, 1, 0};
	std::vector<std::vector<uint32_t>> holed_offset = {{8, 9, 10, 11}, {12, 13, 14, 15}};
	auto corners = TriangulateFaceCorners(v16, holed_offset);
	auto by_vertex = TriangulateFace(v16, holed_offset);
	std::vector<uint32_t> flat;
	for (const auto &ring : holed_offset) {
		for (uint32_t idx : ring) {
			flat.push_back(idx);
		}
	}
	CHECK(flat.size() == 8);
	CHECK(corners.size() == by_vertex.size());
	bool in_range = true;
	for (uint32_t c : corners) {
		if (c >= flat.size()) { // a ring position, never a raw vertex id (8..15)
			in_range = false;
		}
	}
	CHECK(in_range);

	// Mapping every corner through the flattened ring order must reproduce the same set
	// of triangles TriangulateFace names by vertex id -- as triangles, not as an ordered
	// sequence, since nothing about the corner form promises the same per-triangle order.
	auto sorted_triples = [](const std::vector<uint32_t> &tri) {
		std::vector<std::array<uint32_t, 3>> triples;
		for (size_t i = 0; i + 2 < tri.size(); i += 3) {
			std::array<uint32_t, 3> t {tri[i], tri[i + 1], tri[i + 2]};
			std::sort(t.begin(), t.end());
			triples.push_back(t);
		}
		std::sort(triples.begin(), triples.end());
		return triples;
	};
	std::vector<uint32_t> mapped;
	mapped.reserve(corners.size());
	for (uint32_t c : corners) {
		mapped.push_back(c < flat.size() ? flat[c] : 0);
	}
	CHECK(sorted_triples(mapped) == sorted_triples(by_vertex));

	// A vertical wall (normal along -Y): projection must not collapse it.
	std::vector<Vertex3> wall = {{0, 0, 0}, {1, 0, 0}, {1, 0, 1}, {0, 0, 1}};
	auto wt = TriangulateFace(wall, {{0, 1, 2, 3}});
	CHECK(wt.size() == 6);

	// A wall in the x = 0 plane (normal along +-x): exercises the ax-dominant projection
	// branch (u = 1, v = 2), with a winding check in the (y, z) plane the ring already lies in.
	std::vector<Vertex3> xwall = {{0, 0, 0}, {0, 1, 0}, {0, 1, 1}, {0, 0, 1}}; // CCW in (y, z)
	auto xt = TriangulateFace(xwall, {{0, 1, 2, 3}});
	CHECK(xt.size() == 6);
	for (size_t i = 0; i + 2 < xt.size(); i += 3) {
		CHECK(TriAreaYZ(xwall, xt[i], xt[i + 1], xt[i + 2]) > 0);
	}

	// RD-magnitude coordinates: a 1 mm face far from the origin still triangulates, and the
	// coordinate shift before projection keeps its winding correct. On raw (unshifted)
	// projected coordinates (|x| ~ 1e5), the outer ring's shoelace sum is products of ~1e10
	// terms that should cancel to ~1e-6 -- exactly the regime where double-precision
	// cancellation error can flip or zero the sign. The CCW ordering below happens to survive
	// that unshifted computation (it degrades to a zero outer-area, which this kernel's
	// tie-break then reads as "target CCW" -- coincidentally correct here); the CW ordering
	// does not: the same broken tie-break still reads "target CCW" and wins every triangle
	// the wrong way. Both orderings pin the shift, since only one fails without it.
	std::vector<Vertex3> far = {
	    {85000.000, 446000.000, 5}, {85000.001, 446000.000, 5}, {85000.001, 446000.001, 5}, {85000.000, 446000.001, 5}};
	auto far_tris = TriangulateFace(far, {{0, 1, 2, 3}});
	CHECK(far_tris.size() == 6);
	CHECK(TriArea2D(far, far_tris[0], far_tris[1], far_tris[2]) > 0);
	CHECK(TriArea2D(far, far_tris[3], far_tris[4], far_tris[5]) > 0);

	auto far_tris_rev = TriangulateFace(far, {{3, 2, 1, 0}});
	CHECK(far_tris_rev.size() == 6);
	CHECK(TriArea2D(far, far_tris_rev[0], far_tris_rev[1], far_tris_rev[2]) < 0);
	CHECK(TriArea2D(far, far_tris_rev[3], far_tris_rev[4], far_tris_rev[5]) < 0);

	// Degenerate: two vertices.
	CHECK(TriangulateFace(v, {{0, 1}}).empty());

	if (failures == 0) {
		std::printf("face_triangulation: all checks passed\n");
	}
	return failures == 0 ? 0 : 1;
}
