#include "cityjson/face_triangulation.hpp"

#include <cmath>
#include <cstdio>
#include <cstdlib>

using duckdb::cityjson::TriangulateFace;
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

	// A vertical wall (normal along -Y): projection must not collapse it.
	std::vector<Vertex3> wall = {{0, 0, 0}, {1, 0, 0}, {1, 0, 1}, {0, 0, 1}};
	auto wt = TriangulateFace(wall, {{0, 1, 2, 3}});
	CHECK(wt.size() == 6);

	// RD-magnitude coordinates: a 1 mm face far from the origin still triangulates.
	std::vector<Vertex3> far = {
	    {85000.000, 446000.000, 5}, {85000.001, 446000.000, 5}, {85000.001, 446000.001, 5}, {85000.000, 446000.001, 5}};
	CHECK(TriangulateFace(far, {{0, 1, 2, 3}}).size() == 6);

	// Degenerate: two vertices.
	CHECK(TriangulateFace(v, {{0, 1}}).empty());

	if (failures == 0) {
		std::printf("face_triangulation: all checks passed\n");
	}
	return failures == 0 ? 0 : 1;
}
