#pragma once

#include "cityjson/json_utils.hpp"
#include <array>
#include <cstdint>
#include <string>
#include <vector>

namespace duckdb {
namespace cityjson {

/**
 * Result of decoding a WKB geometry back to CityJSON representation
 */
struct WKBDecodeResult {
	std::string cityjson_type; // e.g. "Solid", "MultiSurface"
	json boundaries;           // nested arrays with [x,y,z] coordinate arrays
};

/**
 * WKB Decoder for converting OGC Well-Known Binary back to CityJSON geometry
 *
 * This decoder reverses the WKBEncoder transformations:
 * - Parses WKB binary → extracts 3D coordinates
 * - Maps OGC types back to CityJSON types
 * - Reverses ring orientation (OGC CCW → CityJSON CW)
 * - Removes ring closure (OGC repeats first vertex; CityJSON does not)
 * - Outputs boundaries with [x,y,z] coordinate arrays
 */
class WKBDecoder {
public:
	/**
	 * Decode WKB binary to CityJSON geometry representation
	 *
	 * @param data Pointer to WKB binary data
	 * @param size Size of the WKB data in bytes
	 * @return WKBDecodeResult with CityJSON type and boundaries
	 */
	static WKBDecodeResult Decode(const uint8_t *data, size_t size);

private:
	// Read primitives from WKB buffer
	static uint8_t ReadByte(const uint8_t *data, size_t &offset, size_t size);
	static uint32_t ReadUInt32(const uint8_t *data, size_t &offset, size_t size, bool swap);
	static double ReadDouble(const uint8_t *data, size_t &offset, size_t size, bool swap);
	static std::array<double, 3> ReadPoint3D(const uint8_t *data, size_t &offset, size_t size, bool swap);

	// Decode specific geometry types
	static json DecodeMultiPoint(const uint8_t *data, size_t &offset, size_t size, bool swap);
	static json DecodeMultiLineString(const uint8_t *data, size_t &offset, size_t size, bool swap);
	static json DecodeMultiPolygon(const uint8_t *data, size_t &offset, size_t size, bool swap);
	static json DecodePolyhedralSurface(const uint8_t *data, size_t &offset, size_t size, bool swap);
	static json DecodeGeometryCollection(const uint8_t *data, size_t &offset, size_t size, bool swap);

	// Helper: decode a single polygon's rings (used by MultiPolygon and PolyhedralSurface)
	static json DecodePolygonRings(const uint8_t *data, size_t &offset, size_t size, bool swap);
};

} // namespace cityjson
} // namespace duckdb
