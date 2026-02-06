#include "cityjson/wkb_encoder.hpp"
#include "cityjson/error.hpp"
#include <algorithm>
#include <cstring>
#include <unordered_map>

namespace duckdb {
namespace cityjson {

// =============================================================================
// Type mappings
// =============================================================================

static const std::unordered_map<std::string, WKBGeometryType> CITYJSON_TO_OGC_TYPE = {
    {"MultiPoint", WKBGeometryType::MultiPointZ},
    {"MultiLineString", WKBGeometryType::MultiLineStringZ},
    {"MultiSurface", WKBGeometryType::MultiPolygonZ},
    {"CompositeSurface", WKBGeometryType::MultiPolygonZ},
    {"Solid", WKBGeometryType::PolyhedralSurfaceZ},
    {"MultiSolid", WKBGeometryType::GeometryCollectionZ},
    {"CompositeSolid", WKBGeometryType::GeometryCollectionZ}};

static const std::unordered_map<std::string, CityJSONGeometryTypeCode> CITYJSON_TYPE_CODES = {
    {"Point", CityJSONGeometryTypeCode::Point},
    {"MultiPoint", CityJSONGeometryTypeCode::MultiPoint},
    {"LineString", CityJSONGeometryTypeCode::LineString},
    {"MultiLineString", CityJSONGeometryTypeCode::MultiLineString},
    {"Surface", CityJSONGeometryTypeCode::Surface},
    {"CompositeSurface", CityJSONGeometryTypeCode::CompositeSurface},
    {"TIN", CityJSONGeometryTypeCode::TIN},
    {"MultiSurface", CityJSONGeometryTypeCode::MultiSurface},
    {"Solid", CityJSONGeometryTypeCode::Solid},
    {"CompositeSolid", CityJSONGeometryTypeCode::CompositeSolid},
    {"MultiSolid", CityJSONGeometryTypeCode::MultiSolid}};

// =============================================================================
// Public methods
// =============================================================================

std::vector<uint8_t> WKBEncoder::Encode(const Geometry &geometry, const std::vector<std::array<double, 3>> &vertices,
                                        const std::optional<Transform> &transform) {
	auto ogc_type = GetOGCType(geometry.type);
	return EncodeAsType(geometry, ogc_type, vertices, transform);
}

std::vector<uint8_t> WKBEncoder::EncodeAsType(const Geometry &geometry, WKBGeometryType target_type,
                                              const std::vector<std::array<double, 3>> &vertices,
                                              const std::optional<Transform> &transform) {
	std::vector<uint8_t> out;
	out.reserve(1024); // Pre-allocate reasonable size

	const auto &boundaries = geometry.boundaries;

	switch (target_type) {
	case WKBGeometryType::MultiPointZ:
		EncodeMultiPoint(out, boundaries, vertices, transform);
		break;

	case WKBGeometryType::MultiLineStringZ:
		EncodeMultiLineString(out, boundaries, vertices, transform);
		break;

	case WKBGeometryType::MultiPolygonZ:
		if (geometry.type == "CompositeSurface") {
			EncodeCompositeSurface(out, boundaries, vertices, transform);
		} else {
			EncodeMultiSurface(out, boundaries, vertices, transform);
		}
		break;

	case WKBGeometryType::PolyhedralSurfaceZ:
		EncodeSolid(out, boundaries, vertices, transform);
		break;

	case WKBGeometryType::GeometryCollectionZ:
		EncodeMultiSolid(out, boundaries, vertices, transform);
		break;

	default:
		throw CityJSONError::InvalidGeometry("Unsupported WKB geometry type: " +
		                                     std::to_string(static_cast<int>(target_type)));
	}

	return out;
}

WKBGeometryType WKBEncoder::GetOGCType(const std::string &cityjson_type) {
	auto it = CITYJSON_TO_OGC_TYPE.find(cityjson_type);
	if (it != CITYJSON_TO_OGC_TYPE.end()) {
		return it->second;
	}
	throw CityJSONError::InvalidGeometry("Unknown CityJSON geometry type: " + cityjson_type);
}

CityJSONGeometryTypeCode WKBEncoder::GetTypeCode(const std::string &cityjson_type) {
	auto it = CITYJSON_TYPE_CODES.find(cityjson_type);
	if (it != CITYJSON_TYPE_CODES.end()) {
		return it->second;
	}
	// Return a default or throw
	throw CityJSONError::InvalidGeometry("Unknown CityJSON geometry type for type code: " + cityjson_type);
}

bool WKBEncoder::IsSupported(const std::string &cityjson_type) {
	return CITYJSON_TO_OGC_TYPE.find(cityjson_type) != CITYJSON_TO_OGC_TYPE.end();
}

// =============================================================================
// Helper methods
// =============================================================================

std::array<double, 3> WKBEncoder::ApplyTransform(const std::array<double, 3> &vertex, const Transform &transform) {
	return transform.Apply(vertex);
}

std::array<double, 3> WKBEncoder::GetVertex(const std::vector<std::array<double, 3>> &vertices, uint32_t index,
                                            const std::optional<Transform> &transform) {
	if (index >= vertices.size()) {
		throw CityJSONError::InvalidGeometry("Vertex index out of bounds: " + std::to_string(index) +
		                                     " >= " + std::to_string(vertices.size()));
	}

	const auto &vertex = vertices[index];

	if (transform.has_value()) {
		return ApplyTransform(vertex, transform.value());
	}

	return vertex;
}

void WKBEncoder::ReverseRing(std::vector<uint32_t> &ring) {
	std::reverse(ring.begin(), ring.end());
}

// =============================================================================
// WKB primitive writers
// =============================================================================

void WKBEncoder::WriteByte(std::vector<uint8_t> &out, uint8_t value) {
	out.push_back(value);
}

void WKBEncoder::WriteUInt32(std::vector<uint8_t> &out, uint32_t value) {
	// Little-endian byte order (NDR)
	out.push_back(static_cast<uint8_t>(value & 0xFF));
	out.push_back(static_cast<uint8_t>((value >> 8) & 0xFF));
	out.push_back(static_cast<uint8_t>((value >> 16) & 0xFF));
	out.push_back(static_cast<uint8_t>((value >> 24) & 0xFF));
}

void WKBEncoder::WriteDouble(std::vector<uint8_t> &out, double value) {
	// Little-endian byte order (NDR)
	uint8_t bytes[8];
	std::memcpy(bytes, &value, sizeof(double));
	for (int i = 0; i < 8; ++i) {
		out.push_back(bytes[i]);
	}
}

void WKBEncoder::WriteHeader(std::vector<uint8_t> &out, WKBGeometryType type) {
	WriteByte(out, WKB_NDR); // Little-endian
	WriteUInt32(out, static_cast<uint32_t>(type));
}

void WKBEncoder::WritePoint3D(std::vector<uint8_t> &out, const std::array<double, 3> &point) {
	WriteDouble(out, point[0]); // x
	WriteDouble(out, point[1]); // y
	WriteDouble(out, point[2]); // z
}

// =============================================================================
// Geometry type encoders
// =============================================================================

void WKBEncoder::EncodeMultiPoint(std::vector<uint8_t> &out, const json &boundaries,
                                  const std::vector<std::array<double, 3>> &vertices,
                                  const std::optional<Transform> &transform) {
	// CityJSON MultiPoint: boundaries = [idx1, idx2, ...]
	// WKB MULTIPOINT Z: header + numPoints + [POINT Z ...]

	if (!boundaries.is_array()) {
		throw CityJSONError::InvalidGeometry("MultiPoint boundaries must be an array");
	}

	WriteHeader(out, WKBGeometryType::MultiPointZ);
	WriteUInt32(out, static_cast<uint32_t>(boundaries.size()));

	for (const auto &idx_json : boundaries) {
		uint32_t idx = idx_json.get<uint32_t>();
		auto vertex = GetVertex(vertices, idx, transform);

		// Each point is a complete POINT Z geometry
		WriteHeader(out, WKBGeometryType::PointZ);
		WritePoint3D(out, vertex);
	}
}

void WKBEncoder::EncodeLineString(std::vector<uint8_t> &out, const json &indices,
                                  const std::vector<std::array<double, 3>> &vertices,
                                  const std::optional<Transform> &transform) {
	// Single LineString: indices = [idx1, idx2, ...]
	// WKB LINESTRING Z: header + numPoints + [x, y, z ...]

	if (!indices.is_array()) {
		throw CityJSONError::InvalidGeometry("LineString indices must be an array");
	}

	WriteHeader(out, WKBGeometryType::LineStringZ);
	WriteUInt32(out, static_cast<uint32_t>(indices.size()));

	for (const auto &idx_json : indices) {
		uint32_t idx = idx_json.get<uint32_t>();
		auto vertex = GetVertex(vertices, idx, transform);
		WritePoint3D(out, vertex);
	}
}

void WKBEncoder::EncodeMultiLineString(std::vector<uint8_t> &out, const json &boundaries,
                                       const std::vector<std::array<double, 3>> &vertices,
                                       const std::optional<Transform> &transform) {
	// CityJSON MultiLineString: boundaries = [[idx1, idx2, ...], [idx3, idx4, ...], ...]
	// WKB MULTILINESTRING Z: header + numLineStrings + [LINESTRING Z ...]

	if (!boundaries.is_array()) {
		throw CityJSONError::InvalidGeometry("MultiLineString boundaries must be an array");
	}

	WriteHeader(out, WKBGeometryType::MultiLineStringZ);
	WriteUInt32(out, static_cast<uint32_t>(boundaries.size()));

	for (const auto &line : boundaries) {
		EncodeLineString(out, line, vertices, transform);
	}
}

void WKBEncoder::EncodePolygon(std::vector<uint8_t> &out, const json &surface,
                               const std::vector<std::array<double, 3>> &vertices,
                               const std::optional<Transform> &transform) {
	// CityJSON Surface: surface = [[exterior], [hole1], [hole2], ...]
	// Each ring is an array of vertex indices
	// WKB POLYGON Z: header + numRings + [numPoints + [x, y, z ...] ...]
	//
	// NOTE: Ring orientation conversion:
	// CityJSON/CityGML: exterior rings are clockwise, holes are counterclockwise
	// OGC/PostGIS: exterior rings are counterclockwise, holes are clockwise
	// We need to reverse ring orientation for OGC compatibility

	if (!surface.is_array()) {
		throw CityJSONError::InvalidGeometry("Polygon surface must be an array of rings");
	}

	WriteHeader(out, WKBGeometryType::PolygonZ);
	WriteUInt32(out, static_cast<uint32_t>(surface.size()));

	bool is_exterior_ring = true;
	for (const auto &ring_json : surface) {
		if (!ring_json.is_array()) {
			throw CityJSONError::InvalidGeometry("Polygon ring must be an array");
		}

		// Convert to vector of indices for reversal
		std::vector<uint32_t> ring;
		ring.reserve(ring_json.size());
		for (const auto &idx_json : ring_json) {
			ring.push_back(idx_json.get<uint32_t>());
		}

		// Reverse ring orientation for OGC compatibility
		ReverseRing(ring);

		// For a closed ring, OGC requires first == last point
		// CityJSON does NOT repeat the first vertex, so we need to close it
		bool needs_closing = !ring.empty() && ring.front() != ring.back();
		uint32_t num_points = static_cast<uint32_t>(ring.size()) + (needs_closing ? 1 : 0);

		WriteUInt32(out, num_points);

		for (uint32_t idx : ring) {
			auto vertex = GetVertex(vertices, idx, transform);
			WritePoint3D(out, vertex);
		}

		// Close the ring if needed
		if (needs_closing && !ring.empty()) {
			auto vertex = GetVertex(vertices, ring.front(), transform);
			WritePoint3D(out, vertex);
		}

		is_exterior_ring = false;
	}
}

void WKBEncoder::EncodeMultiSurface(std::vector<uint8_t> &out, const json &boundaries,
                                    const std::vector<std::array<double, 3>> &vertices,
                                    const std::optional<Transform> &transform) {
	// CityJSON MultiSurface: boundaries = [[[exterior], [hole1], ...], [[exterior2], ...], ...]
	// Each element is a surface (polygon with rings)
	// WKB MULTIPOLYGON Z: header + numPolygons + [POLYGON Z ...]

	if (!boundaries.is_array()) {
		throw CityJSONError::InvalidGeometry("MultiSurface boundaries must be an array");
	}

	WriteHeader(out, WKBGeometryType::MultiPolygonZ);
	WriteUInt32(out, static_cast<uint32_t>(boundaries.size()));

	for (const auto &surface : boundaries) {
		EncodePolygon(out, surface, vertices, transform);
	}
}

void WKBEncoder::EncodeCompositeSurface(std::vector<uint8_t> &out, const json &boundaries,
                                        const std::vector<std::array<double, 3>> &vertices,
                                        const std::optional<Transform> &transform) {
	// CompositeSurface has the same structure as MultiSurface
	// Both are encoded as MULTIPOLYGON Z
	EncodeMultiSurface(out, boundaries, vertices, transform);
}

void WKBEncoder::EncodeSolid(std::vector<uint8_t> &out, const json &boundaries,
                             const std::vector<std::array<double, 3>> &vertices,
                             const std::optional<Transform> &transform) {
	// CityJSON Solid: boundaries = [
	//   [ [[ext_ring1]], [[ext_ring2]], ... ],  // exterior shell
	//   [ [[int_ring1]], [[int_ring2]], ... ],  // interior shell 1
	//   ...
	// ]
	// Each shell is an array of surfaces (polygons)
	//
	// WKB POLYHEDRALSURFACE Z: header + numPolygons + [POLYGON Z ...]
	// We flatten all shells into a single collection of polygons

	if (!boundaries.is_array()) {
		throw CityJSONError::InvalidGeometry("Solid boundaries must be an array of shells");
	}

	// First, count total polygons across all shells
	uint32_t total_polygons = 0;
	for (const auto &shell : boundaries) {
		if (shell.is_array()) {
			total_polygons += static_cast<uint32_t>(shell.size());
		}
	}

	WriteHeader(out, WKBGeometryType::PolyhedralSurfaceZ);
	WriteUInt32(out, total_polygons);

	// Encode all polygons from all shells
	for (const auto &shell : boundaries) {
		if (!shell.is_array()) {
			throw CityJSONError::InvalidGeometry("Solid shell must be an array of surfaces");
		}

		for (const auto &surface : shell) {
			EncodePolygon(out, surface, vertices, transform);
		}
	}
}

void WKBEncoder::EncodeMultiSolid(std::vector<uint8_t> &out, const json &boundaries,
                                  const std::vector<std::array<double, 3>> &vertices,
                                  const std::optional<Transform> &transform) {
	// CityJSON MultiSolid/CompositeSolid: boundaries = [solid1, solid2, ...]
	// Each solid has the structure described in EncodeSolid
	//
	// WKB GEOMETRYCOLLECTION Z: header + numGeometries + [POLYHEDRALSURFACE Z ...]

	if (!boundaries.is_array()) {
		throw CityJSONError::InvalidGeometry("MultiSolid boundaries must be an array of solids");
	}

	WriteHeader(out, WKBGeometryType::GeometryCollectionZ);
	WriteUInt32(out, static_cast<uint32_t>(boundaries.size()));

	for (const auto &solid : boundaries) {
		// Create a temporary Geometry object for the solid
		// and encode it as a PolyhedralSurface
		Geometry solid_geom;
		solid_geom.type = "Solid";
		solid_geom.boundaries = solid;

		EncodeSolid(out, solid, vertices, transform);
	}
}

} // namespace cityjson
} // namespace duckdb
