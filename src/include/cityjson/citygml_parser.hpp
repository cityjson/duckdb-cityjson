#pragma once

#include "cityjson/cityjson_types.hpp"
#include "cityjson/json_utils.hpp"
#include <pugixml.hpp>
#include <string>
#include <vector>
#include <map>
#include <array>

namespace duckdb {
namespace cityjson {

/**
 * Simple append-only vertex pool for building CityJSON-style indexed geometry
 * from CityGML inline coordinates. No deduplication.
 */
class VertexPool {
public:
	/**
	 * Add a vertex and return its index
	 */
	size_t AddVertex(double x, double y, double z);

	/**
	 * Get all vertices
	 */
	const std::vector<std::array<double, 3>> &GetVertices() const;

	/**
	 * Current vertex count
	 */
	size_t Size() const;

private:
	std::vector<std::array<double, 3>> vertices_;
};

/**
 * Collects semantic surface information during boundary surface parsing
 */
struct SemanticEntry {
	std::string type; // e.g., "WallSurface", "RoofSurface"
};

/**
 * Parser that converts CityGML XML (2.0 and 3.0) directly into the internal
 * C++ types (CityObject, Geometry, CityJSONFeature, CityJSON) used by the
 * DuckDB scan infrastructure.
 */
class CityGMLParser {
public:
	struct ParseResult {
		CityJSON metadata;
		std::vector<CityJSONFeature> features;
	};

	/**
	 * Parse a CityGML XML string into internal types
	 */
	static ParseResult Parse(const std::string &xml_content);

	/**
	 * Detect CityGML version (2.0 or 3.0) from namespace declarations
	 */
	static std::string DetectVersion(const pugi::xml_document &doc);

	/**
	 * Check if XML content looks like CityGML
	 */
	static bool IsCityGML(const std::string &content);

private:
	// ---- Metadata extraction ----
	static CityJSON ParseMetadata(const pugi::xml_node &root, const std::string &version);
	static std::optional<CRS> ParseCRS(const pugi::xml_node &envelope);
	static std::optional<GeographicalExtent> ParseEnvelopeExtent(const pugi::xml_node &envelope);

	// ---- CityObject parsing ----
	static std::string MapCityObjectType(const std::string &element_name);
	static void ParseCityObjectMember(const pugi::xml_node &member_node,
	                                  std::map<std::string, CityObject> &city_objects,
	                                  VertexPool &pool, const std::string &parent_id = "");

	static CityObject ParseSingleCityObject(const pugi::xml_node &node, const std::string &type_name,
	                                        VertexPool &pool);

	// ---- Attribute extraction ----
	static std::map<std::string, json> ParseAttributes(const pugi::xml_node &node);
	static json ParseAttributeValue(const std::string &text);

	// ---- Generic attributes (CityGML 2.0) ----
	static void ParseGenericAttributes(const pugi::xml_node &node, std::map<std::string, json> &attrs);

	// ---- Geometry parsing ----
	struct BoundarySurface {
		std::string semantic_type;     // "WallSurface", "RoofSurface", etc.
		json surface_boundaries;       // [[ring_indices], ...] per polygon
	};

	static std::string ExtractLOD(const std::string &element_name);
	static void CollectLODGeometries(const pugi::xml_node &city_object_node,
	                                 std::vector<Geometry> &geometries,
	                                 VertexPool &pool);

	static Geometry BuildGeometryFromBoundarySurfaces(const std::string &lod,
	                                                  const std::string &geom_type,
	                                                  const std::vector<BoundarySurface> &surfaces);

	// ---- GML geometry primitives ----
	static std::vector<size_t> ParseLinearRing(const pugi::xml_node &ring_node, VertexPool &pool);
	static json ParsePolygon(const pugi::xml_node &polygon_node, VertexPool &pool);
	static json ParseMultiSurface(const pugi::xml_node &node, VertexPool &pool);
	static json ParseCompositeSurface(const pugi::xml_node &node, VertexPool &pool);
	static json ParseSolid(const pugi::xml_node &node, VertexPool &pool);
	static json ParseTriangulatedSurface(const pugi::xml_node &node, VertexPool &pool);

	// ---- Coordinate parsing ----
	static std::vector<std::array<double, 3>> ParsePosList(const std::string &text);
	static std::array<double, 3> ParsePos(const std::string &text);

	// ---- Helpers ----
	static std::string StripPrefix(const std::string &name);
	static pugi::xml_node FindChildByLocalName(const pugi::xml_node &parent, const std::string &local_name);
	static std::vector<pugi::xml_node> FindChildrenByLocalName(const pugi::xml_node &parent,
	                                                            const std::string &local_name);

	// Known CityGML attribute element names (namespace-stripped) to extract
	static const std::vector<std::string> &KnownAttributes();

	// Known boundary surface type names
	static bool IsBoundarySurface(const std::string &local_name);
	static std::string BoundarySurfaceSemanticType(const std::string &local_name);
};

} // namespace cityjson
} // namespace duckdb
