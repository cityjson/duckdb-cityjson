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

/**
 * `geometry_properties.type` is the CityGML CM geometry type (spec
 * 03-geometry-semantics.mdx). The names coincide with CityJSON's for every type but
 * one: CityJSON's `MultiLineString` is the CM's `MultiCurve`. These map between the two,
 * passing every other name through.
 */
std::string CityGMLGeometryType(const std::string &cityjson_type);
std::string CityJSONGeometryType(const std::string &citygml_type);

} // namespace cityjson
} // namespace duckdb
