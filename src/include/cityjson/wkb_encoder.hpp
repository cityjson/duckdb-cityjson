#pragma once

#include "cityjson/cityjson_types.hpp"
#include <array>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace duckdb {
namespace cityjson {

/**
 * OGC WKB geometry type codes
 * ISO 13249-3 SQL/MM Part 3: Spatial
 *
 * For 3D (Z dimension), add 1000 to the type code
 * For 4D (ZM), add 3000 to the type code
 */
enum class WKBGeometryType : uint32_t {
	// 2D types
	Point = 1,
	LineString = 2,
	Polygon = 3,
	MultiPoint = 4,
	MultiLineString = 5,
	MultiPolygon = 6,
	GeometryCollection = 7,

	// Extended types (ISO 13249-3)
	PolyhedralSurface = 15,
	TIN = 16,
	Triangle = 17,

	// 3D types (Z dimension) - add 1000 to base type
	PointZ = 1001,
	LineStringZ = 1002,
	PolygonZ = 1003,
	MultiPointZ = 1004,
	MultiLineStringZ = 1005,
	MultiPolygonZ = 1006,
	GeometryCollectionZ = 1007,
	PolyhedralSurfaceZ = 1015,
	TINZ = 1016,
	TriangleZ = 1017
};

/**
 * Byte order markers for WKB encoding
 */
constexpr uint8_t WKB_XDR = 0; // Big Endian (Network byte order)
constexpr uint8_t WKB_NDR = 1; // Little Endian (Intel byte order)

/**
 * CityJSON geometry type codes for geometry_properties JSON
 * Based on 3DCityDB geometry module specification
 */
enum class CityJSONGeometryTypeCode : int {
	Point = 1,
	MultiPoint = 2,
	LineString = 3,
	MultiLineString = 4,
	Surface = 5,
	CompositeSurface = 6,
	TIN = 7,
	MultiSurface = 8,
	Solid = 9,
	CompositeSolid = 10,
	MultiSolid = 11
};

/**
 * WKB Encoder for converting CityJSON geometry to OGC Well-Known Binary format
 *
 * This encoder:
 * - Dereferences CityJSON vertex indices to actual 3D coordinates
 * - Applies transform (scale/translate) if present
 * - Reverses ring orientation for OGC compatibility
 *   (CityJSON: exterior CW, interior CCW; OGC: exterior CCW, interior CW)
 * - Outputs WKB in little-endian format (NDR)
 *
 * Supported CityJSON to OGC mappings:
 * - MultiPoint      -> MULTIPOINT Z
 * - MultiLineString -> MULTILINESTRING Z
 * - MultiSurface    -> MULTIPOLYGON Z
 * - CompositeSurface -> MULTIPOLYGON Z
 * - Solid           -> POLYHEDRALSURFACE Z
 * - MultiSolid      -> GEOMETRYCOLLECTION Z
 * - CompositeSolid  -> GEOMETRYCOLLECTION Z
 */
class WKBEncoder {
public:
	/**
	 * Encode CityJSON Geometry to WKB
	 *
	 * @param geometry The CityJSON geometry object
	 * @param vertices Shared vertex array from CityJSON
	 * @param transform Optional transform metadata (scale/translate)
	 * @return WKB bytes as vector
	 */
	static std::vector<uint8_t> Encode(const Geometry &geometry, const std::vector<std::array<double, 3>> &vertices,
	                                   const std::optional<Transform> &transform = std::nullopt);

	/**
	 * Encode with explicit geometry type override
	 * Useful for forcing a specific OGC type regardless of CityJSON type
	 *
	 * @param geometry The CityJSON geometry object
	 * @param target_type Target WKB geometry type
	 * @param vertices Shared vertex array from CityJSON
	 * @param transform Optional transform metadata
	 * @return WKB bytes as vector
	 */
	static std::vector<uint8_t> EncodeAsType(const Geometry &geometry, WKBGeometryType target_type,
	                                         const std::vector<std::array<double, 3>> &vertices,
	                                         const std::optional<Transform> &transform = std::nullopt);

	/**
	 * Get target OGC geometry type for a CityJSON geometry type
	 *
	 * @param cityjson_type CityJSON geometry type name (e.g., "Solid", "MultiSurface")
	 * @return Corresponding WKB geometry type (with Z dimension)
	 */
	static WKBGeometryType GetOGCType(const std::string &cityjson_type);

	/**
	 * Get CityJSON geometry type code for geometry_properties JSON
	 *
	 * @param cityjson_type CityJSON geometry type name
	 * @return Type code for geometry_properties JSON
	 */
	static CityJSONGeometryTypeCode GetTypeCode(const std::string &cityjson_type);

	/**
	 * Check if a CityJSON geometry type is supported for WKB encoding
	 *
	 * @param cityjson_type CityJSON geometry type name
	 * @return true if the type can be encoded to WKB
	 */
	static bool IsSupported(const std::string &cityjson_type);

private:
	// Helper: Apply transform to a vertex
	static std::array<double, 3> ApplyTransform(const std::array<double, 3> &vertex, const Transform &transform);

	// Helper: Get vertex from index with optional transform
	static std::array<double, 3> GetVertex(const std::vector<std::array<double, 3>> &vertices, uint32_t index,
	                                       const std::optional<Transform> &transform);

	// Helper: Reverse ring orientation (CityJSON to OGC)
	static void ReverseRing(std::vector<uint32_t> &ring);

	// Write primitives to WKB buffer
	static void WriteByte(std::vector<uint8_t> &out, uint8_t value);
	static void WriteUInt32(std::vector<uint8_t> &out, uint32_t value);
	static void WriteDouble(std::vector<uint8_t> &out, double value);

	// Write geometry type header
	static void WriteHeader(std::vector<uint8_t> &out, WKBGeometryType type);

	// Write a 3D point (x, y, z)
	static void WritePoint3D(std::vector<uint8_t> &out, const std::array<double, 3> &point);

	// Encode specific geometry types
	// Each method appends WKB bytes to 'out'

	// MultiPoint: boundaries = [idx1, idx2, ...]
	static void EncodeMultiPoint(std::vector<uint8_t> &out, const json &boundaries,
	                             const std::vector<std::array<double, 3>> &vertices,
	                             const std::optional<Transform> &transform);

	// MultiLineString: boundaries = [[idx1, idx2, ...], [idx3, idx4, ...], ...]
	static void EncodeMultiLineString(std::vector<uint8_t> &out, const json &boundaries,
	                                  const std::vector<std::array<double, 3>> &vertices,
	                                  const std::optional<Transform> &transform);

	// Single LineString (helper for MultiLineString)
	static void EncodeLineString(std::vector<uint8_t> &out, const json &indices,
	                             const std::vector<std::array<double, 3>> &vertices,
	                             const std::optional<Transform> &transform);

	// MultiSurface: boundaries = [[[exterior], [hole1], ...], ...]
	// Each surface is a polygon with exterior ring and optional interior rings
	static void EncodeMultiSurface(std::vector<uint8_t> &out, const json &boundaries,
	                               const std::vector<std::array<double, 3>> &vertices,
	                               const std::optional<Transform> &transform);

	// CompositeSurface: Same structure as MultiSurface
	// Encoded as MULTIPOLYGON Z
	static void EncodeCompositeSurface(std::vector<uint8_t> &out, const json &boundaries,
	                                   const std::vector<std::array<double, 3>> &vertices,
	                                   const std::optional<Transform> &transform);

	// Single Polygon (helper for MultiSurface)
	// surface = [[exterior], [hole1], [hole2], ...]
	static void EncodePolygon(std::vector<uint8_t> &out, const json &surface,
	                          const std::vector<std::array<double, 3>> &vertices,
	                          const std::optional<Transform> &transform);

	// Solid: boundaries = [[[[shell1.surface1]], [[shell1.surface2]]], ...]
	// Exterior shell followed by interior shells
	// Encoded as POLYHEDRALSURFACE Z (all shell polygons flattened)
	static void EncodeSolid(std::vector<uint8_t> &out, const json &boundaries,
	                        const std::vector<std::array<double, 3>> &vertices,
	                        const std::optional<Transform> &transform);

	// MultiSolid / CompositeSolid: Array of solids
	// Encoded as GEOMETRYCOLLECTION Z of POLYHEDRALSURFACE Z
	static void EncodeMultiSolid(std::vector<uint8_t> &out, const json &boundaries,
	                             const std::vector<std::array<double, 3>> &vertices,
	                             const std::optional<Transform> &transform);
};

} // namespace cityjson
} // namespace duckdb
