// Just enough of Transform for the encoder harness to link.
//
// cityjson_types.cpp defines these, but linking it drags in json_utils, lod_table
// and column_types -- the last of which needs libduckdb, which this harness
// deliberately does not link. These three definitions are copied verbatim from
// src/cityjson/cityjson_types.cpp:15-25; if that file changes, so must this.
#include "cityjson/cityjson_types.hpp"

namespace duckdb {
namespace cityjson {

Transform::Transform() : scale({1.0, 1.0, 1.0}), translate({0.0, 0.0, 0.0}) {
}

Transform::Transform(std::array<double, 3> scale, std::array<double, 3> translate)
    : scale(scale), translate(translate) {
}

std::array<double, 3> Transform::Apply(const std::array<double, 3> &vertex) const {
	return {vertex[0] * scale[0] + translate[0], vertex[1] * scale[1] + translate[1],
	        vertex[2] * scale[2] + translate[2]};
}

} // namespace cityjson
} // namespace duckdb
