#include "cityjson/face_triangulation.hpp"

#include <mapbox/earcut.hpp>

#include <cmath>
#include <utility>

namespace duckdb {
namespace cityjson {

Vertex3 NewellNormal(const std::vector<Vertex3> &vertices, const std::vector<uint32_t> &ring) {
	Vertex3 n {0.0, 0.0, 0.0};
	const size_t count = ring.size();
	for (size_t i = 0; i < count; i++) {
		const auto &a = vertices[ring[i]];
		const auto &b = vertices[ring[(i + 1) % count]];
		n[0] += (a[1] - b[1]) * (a[2] + b[2]);
		n[1] += (a[2] - b[2]) * (a[0] + b[0]);
		n[2] += (a[0] - b[0]) * (a[1] + b[1]);
	}
	return n;
}

std::vector<uint32_t> TriangulateFace(const std::vector<Vertex3> &vertices,
                                      const std::vector<std::vector<uint32_t>> &rings) {
	std::vector<uint32_t> out;
	if (rings.empty() || rings[0].size() < 3) {
		return out;
	}
	const Vertex3 n = NewellNormal(vertices, rings[0]);
	const double ax = std::fabs(n[0]);
	const double ay = std::fabs(n[1]);
	const double az = std::fabs(n[2]);
	if (ax == 0.0 && ay == 0.0 && az == 0.0) {
		return out;
	}
	// Drop the dominant axis. |n[axis]| / 2 is the area of the ring's projection onto the
	// plane perpendicular to that axis, so dropping the largest component keeps that area
	// at its maximum, |A_uv| = max(ax, ay, az) / 2 > 0 -- guaranteeing a non-degenerate 2D
	// polygon for earcut. It says nothing about which way (u, v) winds relative to the 3D
	// ring; the per-triangle re-wind below, not this choice, is what enforces the "wound
	// like the outer ring" contract.
	int u;
	int v;
	if (az >= ax && az >= ay) {
		u = 0;
		v = 1;
	} else if (ax >= ay) {
		u = 1;
		v = 2;
	} else {
		u = 2;
		v = 0;
	}

	// Shift to the first vertex before projecting: earcut's tests are signed areas, and
	// on absolute projected coordinates (|x| ~ 1e5) those drown for small faces.
	const Vertex3 &o = vertices[rings[0][0]];
	using Point = std::array<double, 2>;
	std::vector<std::vector<Point>> polygon;
	std::vector<uint32_t> flat; // earcut index -> vertex index
	polygon.reserve(rings.size());
	for (const auto &ring : rings) {
		std::vector<Point> pts;
		pts.reserve(ring.size());
		for (uint32_t idx : ring) {
			const auto &p = vertices[idx];
			pts.push_back({p[u] - o[u], p[v] - o[v]});
			flat.push_back(idx);
		}
		polygon.push_back(std::move(pts));
	}

	// Sign of the projected outer ring: the winding every triangle must share.
	double outer_area = 0.0;
	for (size_t i = 0; i < polygon[0].size(); i++) {
		const auto &p = polygon[0][i];
		const auto &q = polygon[0][(i + 1) % polygon[0].size()];
		outer_area += p[0] * q[1] - q[0] * p[1];
	}

	std::vector<uint32_t> tri = mapbox::earcut<uint32_t>(polygon);
	out.reserve(tri.size());
	for (size_t i = 0; i + 2 < tri.size(); i += 3) {
		uint32_t a = tri[i];
		uint32_t b = tri[i + 1];
		uint32_t c = tri[i + 2];
		// Signed area of the triangle in projected space, from the flat point list.
		auto point = [&](uint32_t k) -> Point {
			size_t r = 0;
			while (k >= polygon[r].size()) {
				k -= static_cast<uint32_t>(polygon[r].size());
				r++;
			}
			return polygon[r][k];
		};
		Point A = point(a);
		Point B = point(b);
		Point C = point(c);
		double area = (B[0] - A[0]) * (C[1] - A[1]) - (C[0] - A[0]) * (B[1] - A[1]);
		if ((area < 0) != (outer_area < 0)) {
			std::swap(b, c);
		}
		out.push_back(flat[a]);
		out.push_back(flat[b]);
		out.push_back(flat[c]);
	}
	return out;
}

} // namespace cityjson
} // namespace duckdb
