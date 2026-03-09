#include "cityjson/wkb_decoder.hpp"
#include "cityjson/wkb_encoder.hpp" // for WKBGeometryType, WKB_NDR, WKB_XDR
#include "cityjson/error.hpp"
#include <algorithm>
#include <cstring>

namespace duckdb {
namespace cityjson {

// =============================================================================
// Read primitives
// =============================================================================

uint8_t WKBDecoder::ReadByte(const uint8_t *data, size_t &offset, size_t size) {
	if (offset >= size) {
		throw CityJSONError::InvalidGeometry("WKB: unexpected end of data reading byte");
	}
	return data[offset++];
}

uint32_t WKBDecoder::ReadUInt32(const uint8_t *data, size_t &offset, size_t size, bool swap) {
	if (offset + 4 > size) {
		throw CityJSONError::InvalidGeometry("WKB: unexpected end of data reading uint32");
	}
	uint32_t value;
	std::memcpy(&value, data + offset, 4);
	offset += 4;
	if (swap) {
		value = ((value & 0xFF) << 24) | ((value & 0xFF00) << 8) | ((value >> 8) & 0xFF00) | ((value >> 24) & 0xFF);
	}
	return value;
}

double WKBDecoder::ReadDouble(const uint8_t *data, size_t &offset, size_t size, bool swap) {
	if (offset + 8 > size) {
		throw CityJSONError::InvalidGeometry("WKB: unexpected end of data reading double");
	}
	double value;
	if (swap) {
		uint8_t bytes[8];
		for (int i = 0; i < 8; i++) {
			bytes[7 - i] = data[offset + i];
		}
		std::memcpy(&value, bytes, 8);
	} else {
		std::memcpy(&value, data + offset, 8);
	}
	offset += 8;
	return value;
}

std::array<double, 3> WKBDecoder::ReadPoint3D(const uint8_t *data, size_t &offset, size_t size, bool swap) {
	double x = ReadDouble(data, offset, size, swap);
	double y = ReadDouble(data, offset, size, swap);
	double z = ReadDouble(data, offset, size, swap);
	return {x, y, z};
}

// =============================================================================
// Decode polygon rings (shared by MultiPolygon and PolyhedralSurface)
// =============================================================================

json WKBDecoder::DecodePolygonRings(const uint8_t *data, size_t &offset, size_t size, bool swap) {
	// Read polygon header
	uint8_t byte_order = ReadByte(data, offset, size);
	bool poly_swap = (byte_order == WKB_XDR); // XDR = big-endian, needs swap on little-endian

	uint32_t poly_type = ReadUInt32(data, offset, size, poly_swap);
	(void)poly_type; // should be PolygonZ

	uint32_t num_rings = ReadUInt32(data, offset, size, poly_swap);

	// CityJSON surface: [[exterior_ring], [hole1], [hole2], ...]
	json surface = json::array();

	for (uint32_t r = 0; r < num_rings; r++) {
		uint32_t num_points = ReadUInt32(data, offset, size, poly_swap);

		std::vector<std::array<double, 3>> points;
		points.reserve(num_points);
		for (uint32_t p = 0; p < num_points; p++) {
			points.push_back(ReadPoint3D(data, offset, size, poly_swap));
		}

		// Remove closing vertex if first == last
		if (points.size() >= 2) {
			auto &first = points.front();
			auto &last = points.back();
			if (first[0] == last[0] && first[1] == last[1] && first[2] == last[2]) {
				points.pop_back();
			}
		}

		// Reverse ring orientation (OGC → CityJSON)
		std::reverse(points.begin(), points.end());

		// Convert to JSON array of [x,y,z]
		json ring = json::array();
		for (auto &pt : points) {
			ring.push_back(json::array({pt[0], pt[1], pt[2]}));
		}
		surface.push_back(ring);
	}

	return surface;
}

// =============================================================================
// Decode specific geometry types
// =============================================================================

json WKBDecoder::DecodeMultiPoint(const uint8_t *data, size_t &offset, size_t size, bool swap) {
	uint32_t num_points = ReadUInt32(data, offset, size, swap);

	// CityJSON MultiPoint: boundaries = [[x,y,z], [x,y,z], ...]
	json boundaries = json::array();
	for (uint32_t i = 0; i < num_points; i++) {
		// Each point has its own header
		uint8_t byte_order = ReadByte(data, offset, size);
		bool pt_swap = (byte_order == WKB_XDR);
		uint32_t pt_type = ReadUInt32(data, offset, size, pt_swap);
		(void)pt_type; // should be PointZ

		auto pt = ReadPoint3D(data, offset, size, pt_swap);
		boundaries.push_back(json::array({pt[0], pt[1], pt[2]}));
	}

	return boundaries;
}

json WKBDecoder::DecodeMultiLineString(const uint8_t *data, size_t &offset, size_t size, bool swap) {
	uint32_t num_lines = ReadUInt32(data, offset, size, swap);

	// CityJSON MultiLineString: boundaries = [[[x,y,z], ...], [[x,y,z], ...], ...]
	json boundaries = json::array();
	for (uint32_t i = 0; i < num_lines; i++) {
		// Each linestring has its own header
		uint8_t byte_order = ReadByte(data, offset, size);
		bool ls_swap = (byte_order == WKB_XDR);
		uint32_t ls_type = ReadUInt32(data, offset, size, ls_swap);
		(void)ls_type; // should be LineStringZ

		uint32_t num_points = ReadUInt32(data, offset, size, ls_swap);
		json line = json::array();
		for (uint32_t p = 0; p < num_points; p++) {
			auto pt = ReadPoint3D(data, offset, size, ls_swap);
			line.push_back(json::array({pt[0], pt[1], pt[2]}));
		}
		boundaries.push_back(line);
	}

	return boundaries;
}

json WKBDecoder::DecodeMultiPolygon(const uint8_t *data, size_t &offset, size_t size, bool swap) {
	uint32_t num_polygons = ReadUInt32(data, offset, size, swap);

	// CityJSON MultiSurface: boundaries = [surface1, surface2, ...]
	// Each surface = [[exterior_ring], [hole1], ...]
	json boundaries = json::array();
	for (uint32_t i = 0; i < num_polygons; i++) {
		boundaries.push_back(DecodePolygonRings(data, offset, size, swap));
	}

	return boundaries;
}

json WKBDecoder::DecodePolyhedralSurface(const uint8_t *data, size_t &offset, size_t size, bool swap) {
	uint32_t num_polygons = ReadUInt32(data, offset, size, swap);

	// CityJSON Solid: boundaries = [shell]
	// The encoder flattens all shells into polygons, so we reconstruct as a single exterior shell
	// shell = [surface1, surface2, ...]
	json shell = json::array();
	for (uint32_t i = 0; i < num_polygons; i++) {
		shell.push_back(DecodePolygonRings(data, offset, size, swap));
	}

	// Solid boundaries = [shell1, shell2, ...] — wrap in outer array
	json boundaries = json::array();
	boundaries.push_back(shell);
	return boundaries;
}

json WKBDecoder::DecodeGeometryCollection(const uint8_t *data, size_t &offset, size_t size, bool swap) {
	uint32_t num_geoms = ReadUInt32(data, offset, size, swap);

	// CityJSON MultiSolid: boundaries = [solid1_boundaries, solid2_boundaries, ...]
	// Each sub-geometry should be a PolyhedralSurface
	json boundaries = json::array();
	for (uint32_t i = 0; i < num_geoms; i++) {
		// Read sub-geometry header
		uint8_t byte_order = ReadByte(data, offset, size);
		bool sub_swap = (byte_order == WKB_XDR);
		uint32_t sub_type = ReadUInt32(data, offset, size, sub_swap);
		(void)sub_type; // should be PolyhedralSurfaceZ

		auto solid_boundaries = DecodePolyhedralSurface(data, offset, size, sub_swap);
		boundaries.push_back(solid_boundaries);
	}

	return boundaries;
}

// =============================================================================
// Public decode
// =============================================================================

WKBDecodeResult WKBDecoder::Decode(const uint8_t *data, size_t size) {
	if (size < 5) {
		throw CityJSONError::InvalidGeometry("WKB: data too short");
	}

	size_t offset = 0;

	// Read byte order
	uint8_t byte_order = ReadByte(data, offset, size);
	bool swap = (byte_order == WKB_XDR); // XDR = big-endian, needs swap on little-endian

	// Read geometry type
	uint32_t wkb_type = ReadUInt32(data, offset, size, swap);

	WKBDecodeResult result;

	switch (static_cast<WKBGeometryType>(wkb_type)) {
	case WKBGeometryType::MultiPointZ:
		result.cityjson_type = "MultiPoint";
		result.boundaries = DecodeMultiPoint(data, offset, size, swap);
		break;
	case WKBGeometryType::MultiLineStringZ:
		result.cityjson_type = "MultiLineString";
		result.boundaries = DecodeMultiLineString(data, offset, size, swap);
		break;
	case WKBGeometryType::MultiPolygonZ:
		result.cityjson_type = "MultiSurface";
		result.boundaries = DecodeMultiPolygon(data, offset, size, swap);
		break;
	case WKBGeometryType::PolyhedralSurfaceZ:
		result.cityjson_type = "Solid";
		result.boundaries = DecodePolyhedralSurface(data, offset, size, swap);
		break;
	case WKBGeometryType::GeometryCollectionZ:
		result.cityjson_type = "MultiSolid";
		result.boundaries = DecodeGeometryCollection(data, offset, size, swap);
		break;
	default:
		throw CityJSONError::InvalidGeometry("WKB: unsupported geometry type " + std::to_string(wkb_type));
	}

	return result;
}

} // namespace cityjson
} // namespace duckdb
