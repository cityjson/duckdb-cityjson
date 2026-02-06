#pragma once

#include "cityjson/cityjson_types.hpp"
#include "cityjson/wkb_encoder.hpp"
#include <optional>
#include <string>
#include <vector>

namespace duckdb {
namespace cityjson {

/**
 * Represents a child element in the geometry hierarchy
 * Used to preserve CityJSON structure in geometry_properties
 */
struct GeometryPropertyChild {
	int type;                             // Geometry type code
	std::string cityjson_type;            // Original CityJSON type
	std::optional<std::string> object_id; // Optional geometry identifier
	std::optional<size_t> parent;         // Index in parent array
	std::optional<size_t> geometry_index; // Index in WKB geometry

	GeometryPropertyChild() = default;
};

/**
 * Represents a semantic surface from CityJSON
 */
struct SemanticSurface {
	std::string type; // Surface type (e.g., "WallSurface", "RoofSurface")
	json attributes;  // Additional surface attributes

	SemanticSurface() = default;
	SemanticSurface(std::string type) : type(std::move(type)) {
	}
};

/**
 * Container for CityJSON semantics data
 */
struct GeometrySemantics {
	std::vector<SemanticSurface> surfaces; // Semantic surface definitions
	json values;                           // Maps boundary indices to surface indices

	GeometrySemantics() = default;
};

/**
 * Complete geometry properties structure for preservation in database
 * This JSON structure preserves CityJSON metadata not representable in WKB
 */
struct GeometryProperties {
	int type;                                    // Geometry type code (1-11)
	std::string cityjson_type;                   // Original CityJSON type name
	std::string lod;                             // Level of Detail
	std::optional<std::string> object_id;        // Optional geometry identifier
	std::vector<GeometryPropertyChild> children; // Child geometry elements
	std::optional<GeometrySemantics> semantics;  // CityJSON surface semantics
	// Note: materials and textures are stored in separate tables

	GeometryProperties() = default;
};

/**
 * Serializer for converting CityJSON Geometry to geometry_properties JSON
 *
 * The geometry_properties column stores JSON metadata that:
 * 1. Preserves CityJSON/CityGML semantics lost in WKB conversion
 * 2. Enables round-trip conversion (WKB → CityJSON)
 * 3. Stores semantic surface information
 *
 * Based on 3DCityDB geometry module specification
 */
class GeometryPropertiesSerializer {
public:
	/**
	 * Serialize CityJSON Geometry to properties JSON
	 *
	 * @param geometry The CityJSON geometry object
	 * @return JSON object containing geometry properties
	 */
	static json Serialize(const Geometry &geometry);

	/**
	 * Get geometry type code for CityJSON type
	 *
	 * @param cityjson_type CityJSON geometry type name
	 * @return Integer type code (1-11)
	 */
	static int GetTypeCode(const std::string &cityjson_type);

	/**
	 * Get CityJSON type name from type code
	 *
	 * @param type_code Integer type code (1-11)
	 * @return CityJSON geometry type name
	 */
	static std::string GetTypeName(int type_code);

	/**
	 * Serialize semantics from CityJSON geometry
	 *
	 * @param semantics_json JSON semantics object from CityJSON geometry
	 * @return JSON object for geometry_properties semantics field
	 */
	static json SerializeSemantics(const json &semantics_json);

	/**
	 * Build geometry properties from Geometry object
	 *
	 * @param geometry The CityJSON geometry object
	 * @return GeometryProperties structure
	 */
	static GeometryProperties Build(const Geometry &geometry);

private:
	/**
	 * Build hierarchical children structure for complex geometries
	 * (Solid, CompositeSolid, MultiSolid)
	 *
	 * @param geometry The CityJSON geometry object
	 * @return Vector of child property entries
	 */
	static std::vector<GeometryPropertyChild> BuildChildren(const Geometry &geometry);

	/**
	 * Count total polygons in a Solid's boundaries
	 *
	 * @param boundaries Solid boundaries JSON
	 * @return Total polygon count
	 */
	static size_t CountPolygonsInSolid(const json &boundaries);
};

} // namespace cityjson
} // namespace duckdb
