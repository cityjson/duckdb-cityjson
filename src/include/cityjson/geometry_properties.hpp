#pragma once

#include "cityjson/cityjson_types.hpp"

namespace duckdb {
namespace cityjson {

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
	 * Serialize CityJSON Geometry to the spec §8 geometry_properties payload:
	 * {type, surfaces?, face_semantics?, shells?}. Carries no `lod` key -- the
	 * level of detail lives in the column name.
	 *
	 * The result is the intermediate form; VectorWriter turns it into the
	 * STRUCT(type, surfaces, face_semantics, shells) the column actually holds.
	 *
	 * @param geometry The CityJSON geometry object
	 * @return JSON object containing geometry properties
	 */
	static json Serialize(const Geometry &geometry);
};

} // namespace cityjson
} // namespace duckdb
