#include "cityjson/citygml_parser.hpp"
#include "cityjson/error.hpp"
#include <sstream>
#include <algorithm>
#include <cstring>
#include <regex>
#include <set>

namespace duckdb {
namespace cityjson {

// ============================================================
// VertexPool
// ============================================================

size_t VertexPool::AddVertex(double x, double y, double z) {
	size_t idx = vertices_.size();
	vertices_.push_back({x, y, z});
	return idx;
}

const std::vector<std::array<double, 3>> &VertexPool::GetVertices() const {
	return vertices_;
}

size_t VertexPool::Size() const {
	return vertices_.size();
}

// ============================================================
// Helpers
// ============================================================

std::string CityGMLParser::StripPrefix(const std::string &name) {
	auto pos = name.find(':');
	if (pos != std::string::npos) {
		return name.substr(pos + 1);
	}
	return name;
}

pugi::xml_node CityGMLParser::FindChildByLocalName(const pugi::xml_node &parent, const std::string &local_name) {
	for (auto child = parent.first_child(); child; child = child.next_sibling()) {
		if (StripPrefix(child.name()) == local_name) {
			return child;
		}
	}
	return pugi::xml_node();
}

std::vector<pugi::xml_node> CityGMLParser::FindChildrenByLocalName(const pugi::xml_node &parent,
                                                                    const std::string &local_name) {
	std::vector<pugi::xml_node> result;
	for (auto child = parent.first_child(); child; child = child.next_sibling()) {
		if (StripPrefix(child.name()) == local_name) {
			result.push_back(child);
		}
	}
	return result;
}

const std::vector<std::string> &CityGMLParser::KnownAttributes() {
	static const std::vector<std::string> attrs = {
	    "yearOfConstruction",
	    "yearOfDemolition",
	    "measuredHeight",
	    "storeysAboveGround",
	    "storeysBelowGround",
	    "storeyHeightsAboveGround",
	    "storeyHeightsBelowGround",
	    "roofType",
	    "function",
	    "usage",
	    "class",
	    "creationDate",
	    "terminationDate",
	    "description",
	    "name",
	    // Transportation
	    "surfaceMaterial",
	    "trafficDirection",
	    // WaterBody
	    "waterLevel",
	    // Vegetation
	    "species",
	    "height",
	    "trunkDiameter",
	    "crownDiameter",
	    // LandUse
	    "area",
	};
	return attrs;
}

bool CityGMLParser::IsBoundarySurface(const std::string &local_name) {
	static const std::set<std::string> boundary_surfaces = {
	    "WallSurface",
	    "RoofSurface",
	    "GroundSurface",
	    "ClosureSurface",
	    "OuterCeilingSurface",
	    "OuterFloorSurface",
	    "InteriorWallSurface",
	    "CeilingSurface",
	    "FloorSurface",
	    "Window",
	    "Door",
	    "WaterSurface",
	    "WaterGroundSurface",
	    "WaterClosureSurface",
	    "TrafficArea",
	    "AuxiliaryTrafficArea",
	    "TransportationMarking",
	    "TransportationHole",
	};
	return boundary_surfaces.count(local_name) > 0;
}

std::string CityGMLParser::BoundarySurfaceSemanticType(const std::string &local_name) {
	// The semantic type is the same as the local name for all boundary surfaces
	return local_name;
}

// ============================================================
// Version Detection
// ============================================================

std::string CityGMLParser::DetectVersion(const pugi::xml_document &doc) {
	auto root = doc.first_child();
	if (!root) {
		return "2.0";
	}

	// Check all namespace declarations on the root element
	for (auto attr = root.first_attribute(); attr; attr = attr.next_attribute()) {
		std::string attr_name = attr.name();
		std::string attr_value = attr.value();

		// Look for citygml namespace URIs
		if (attr_value.find("citygml/3.0") != std::string::npos) {
			return "3.0";
		}
		if (attr_value.find("citygml/2.0") != std::string::npos) {
			return "2.0";
		}
		if (attr_value.find("citygml/1.0") != std::string::npos) {
			return "1.0";
		}
	}

	// Default to 2.0
	return "2.0";
}

bool CityGMLParser::IsCityGML(const std::string &content) {
	// Quick heuristic: look for CityModel element in first 1024 bytes
	auto sample = content.substr(0, std::min(content.size(), size_t(1024)));
	return sample.find("CityModel") != std::string::npos;
}

// ============================================================
// Coordinate Parsing
// ============================================================

std::vector<std::array<double, 3>> CityGMLParser::ParsePosList(const std::string &text) {
	std::vector<std::array<double, 3>> points;
	std::istringstream iss(text);
	double x, y, z;
	while (iss >> x >> y >> z) {
		points.push_back({x, y, z});
	}
	return points;
}

std::array<double, 3> CityGMLParser::ParsePos(const std::string &text) {
	std::istringstream iss(text);
	double x = 0, y = 0, z = 0;
	iss >> x >> y >> z;
	return {x, y, z};
}

// ============================================================
// GML Geometry Primitives
// ============================================================

std::vector<size_t> CityGMLParser::ParseLinearRing(const pugi::xml_node &ring_node, VertexPool &pool) {
	std::vector<size_t> indices;

	// Try gml:posList first
	auto pos_list = FindChildByLocalName(ring_node, "posList");
	if (pos_list) {
		auto points = ParsePosList(pos_list.child_value());
		for (const auto &pt : points) {
			indices.push_back(pool.AddVertex(pt[0], pt[1], pt[2]));
		}
	} else {
		// Try gml:pos elements
		auto pos_nodes = FindChildrenByLocalName(ring_node, "pos");
		for (const auto &pos_node : pos_nodes) {
			auto pt = ParsePos(pos_node.child_value());
			indices.push_back(pool.AddVertex(pt[0], pt[1], pt[2]));
		}
	}

	// CityJSON boundaries don't repeat the closing vertex (GML does)
	// Remove the last vertex if it equals the first
	if (indices.size() >= 2) {
		const auto &verts = pool.GetVertices();
		const auto &first = verts[indices.front()];
		const auto &last = verts[indices.back()];
		if (first[0] == last[0] && first[1] == last[1] && first[2] == last[2]) {
			indices.pop_back();
		}
	}

	// CityJSON uses CW exterior rings, GML uses CCW.
	// Reverse the ring to convert from GML to CityJSON convention.
	std::reverse(indices.begin(), indices.end());

	return indices;
}

json CityGMLParser::ParsePolygon(const pugi::xml_node &polygon_node, VertexPool &pool) {
	// A polygon in CityJSON: [[exterior_ring], [hole1], [hole2], ...]
	json polygon = json::array();

	// Exterior ring
	auto exterior = FindChildByLocalName(polygon_node, "exterior");
	if (exterior) {
		auto linear_ring = FindChildByLocalName(exterior, "LinearRing");
		if (linear_ring) {
			auto ring_indices = ParseLinearRing(linear_ring, pool);
			json ring_json = json::array();
			for (auto idx : ring_indices) {
				ring_json.push_back(static_cast<int>(idx));
			}
			polygon.push_back(ring_json);
		}
	}

	// Interior rings (holes)
	auto interiors = FindChildrenByLocalName(polygon_node, "interior");
	for (const auto &interior : interiors) {
		auto linear_ring = FindChildByLocalName(interior, "LinearRing");
		if (linear_ring) {
			auto ring_indices = ParseLinearRing(linear_ring, pool);
			json ring_json = json::array();
			for (auto idx : ring_indices) {
				ring_json.push_back(static_cast<int>(idx));
			}
			polygon.push_back(ring_json);
		}
	}

	return polygon;
}

json CityGMLParser::ParseMultiSurface(const pugi::xml_node &node, VertexPool &pool) {
	// MultiSurface in CityJSON: [surface1, surface2, ...] where each surface is [[ext], [hole], ...]
	json boundaries = json::array();

	auto surface_members = FindChildrenByLocalName(node, "surfaceMember");
	for (const auto &sm : surface_members) {
		// surfaceMember can contain a Polygon directly
		auto polygon = FindChildByLocalName(sm, "Polygon");
		if (polygon) {
			boundaries.push_back(ParsePolygon(polygon, pool));
			continue;
		}
		// Or a CompositeSurface
		auto comp = FindChildByLocalName(sm, "CompositeSurface");
		if (comp) {
			auto inner = ParseCompositeSurface(comp, pool);
			for (const auto &s : inner) {
				boundaries.push_back(s);
			}
		}
	}

	// Also check for surfaceMembers (plural, GML 3.2)
	auto surface_members_plural = FindChildByLocalName(node, "surfaceMembers");
	if (surface_members_plural) {
		for (auto child = surface_members_plural.first_child(); child; child = child.next_sibling()) {
			auto local = StripPrefix(child.name());
			if (local == "Polygon") {
				boundaries.push_back(ParsePolygon(child, pool));
			}
		}
	}

	return boundaries;
}

json CityGMLParser::ParseCompositeSurface(const pugi::xml_node &node, VertexPool &pool) {
	// CompositeSurface is structurally the same as MultiSurface in CityJSON
	return ParseMultiSurface(node, pool);
}

json CityGMLParser::ParseSolid(const pugi::xml_node &node, VertexPool &pool) {
	// Solid in CityJSON: [shell1, shell2, ...] where each shell is [surface1, surface2, ...]
	json boundaries = json::array();

	// Exterior shell
	auto exterior = FindChildByLocalName(node, "exterior");
	if (exterior) {
		auto comp_surface = FindChildByLocalName(exterior, "CompositeSurface");
		if (comp_surface) {
			auto shell = ParseCompositeSurface(comp_surface, pool);
			boundaries.push_back(shell);
		} else {
			// Try Shell element (GML 3.2)
			auto shell_node = FindChildByLocalName(exterior, "Shell");
			if (shell_node) {
				auto shell = ParseMultiSurface(shell_node, pool);
				boundaries.push_back(shell);
			}
		}
	}

	// Interior shells
	auto interiors = FindChildrenByLocalName(node, "interior");
	for (const auto &interior : interiors) {
		auto comp_surface = FindChildByLocalName(interior, "CompositeSurface");
		if (comp_surface) {
			auto shell = ParseCompositeSurface(comp_surface, pool);
			boundaries.push_back(shell);
		} else {
			auto shell_node = FindChildByLocalName(interior, "Shell");
			if (shell_node) {
				auto shell = ParseMultiSurface(shell_node, pool);
				boundaries.push_back(shell);
			}
		}
	}

	return boundaries;
}

json CityGMLParser::ParseTriangulatedSurface(const pugi::xml_node &node, VertexPool &pool) {
	// TriangulatedSurface → CompositeSurface (each triangle is a polygon)
	json boundaries = json::array();

	auto patches = FindChildByLocalName(node, "trianglePatches");
	if (!patches) {
		patches = FindChildByLocalName(node, "patches");
	}
	if (patches) {
		auto triangles = FindChildrenByLocalName(patches, "Triangle");
		for (const auto &tri : triangles) {
			auto exterior = FindChildByLocalName(tri, "exterior");
			if (exterior) {
				auto linear_ring = FindChildByLocalName(exterior, "LinearRing");
				if (linear_ring) {
					auto ring_indices = ParseLinearRing(linear_ring, pool);
					json ring_json = json::array();
					for (auto idx : ring_indices) {
						ring_json.push_back(static_cast<int>(idx));
					}
					// Each triangle as a surface with one ring (no holes)
					json surface = json::array();
					surface.push_back(ring_json);
					boundaries.push_back(surface);
				}
			}
		}
	}

	return boundaries;
}

// ============================================================
// LOD Extraction
// ============================================================

std::string CityGMLParser::ExtractLOD(const std::string &element_name) {
	// Extract LOD from element names like "lod2Solid", "lod2MultiSurface", "lod1Solid", etc.
	std::string local = StripPrefix(element_name);

	// Match "lod" followed by a digit
	if (local.size() >= 4 && local.substr(0, 3) == "lod" && std::isdigit(local[3])) {
		return std::string(1, local[3]);
	}

	return "";
}

// ============================================================
// Geometry Collection from CityObject
// ============================================================

void CityGMLParser::CollectLODGeometries(const pugi::xml_node &city_object_node,
                                         std::vector<Geometry> &geometries, VertexPool &pool) {
	// First, collect boundary surfaces and their geometry per LOD
	// Map: lod → vector of BoundarySurface
	std::map<std::string, std::vector<BoundarySurface>> lod_boundary_surfaces;

	// Find all boundedBy children
	auto bounded_by_nodes = FindChildrenByLocalName(city_object_node, "boundedBy");
	for (const auto &bounded_by : bounded_by_nodes) {
		// The child of boundedBy is the boundary surface element (WallSurface, RoofSurface, etc.)
		for (auto bs_node = bounded_by.first_child(); bs_node; bs_node = bs_node.next_sibling()) {
			std::string bs_local_name = StripPrefix(bs_node.name());
			if (!IsBoundarySurface(bs_local_name)) {
				continue;
			}

			std::string semantic_type = BoundarySurfaceSemanticType(bs_local_name);

			// Find LOD geometry elements within this boundary surface
			for (auto child = bs_node.first_child(); child; child = child.next_sibling()) {
				std::string child_name = child.name();
				std::string lod = ExtractLOD(child_name);
				if (lod.empty()) {
					continue;
				}

				// Parse the GML geometry inside
				auto multi_surface = FindChildByLocalName(child, "MultiSurface");
				if (multi_surface) {
					json surf_boundaries = ParseMultiSurface(multi_surface, pool);
					for (const auto &surf : surf_boundaries) {
						BoundarySurface bs;
						bs.semantic_type = semantic_type;
						bs.surface_boundaries = surf;
						lod_boundary_surfaces[lod].push_back(bs);
					}
				}
			}
		}
	}

	// Build geometries from boundary surfaces (with semantics)
	for (auto &[lod, surfaces] : lod_boundary_surfaces) {
		// Determine geometry type: if there's a lod*Solid, wrap as Solid; otherwise MultiSurface
		std::string geom_type = "MultiSurface";

		// Check if the city object has an explicit solid geometry at this LOD
		for (auto child = city_object_node.first_child(); child; child = child.next_sibling()) {
			std::string child_lod = ExtractLOD(child.name());
			std::string child_local = StripPrefix(child.name());
			if (child_lod == lod && child_local.find("Solid") != std::string::npos) {
				geom_type = "Solid";
				break;
			}
		}

		auto geom = BuildGeometryFromBoundarySurfaces(lod, geom_type, surfaces);
		geometries.push_back(geom);
	}

	// Also handle LOD geometry elements that don't come from boundary surfaces
	// (e.g., objects without boundedBy, or lod0 footprints)
	std::set<std::string> handled_lods;
	for (const auto &[lod, _] : lod_boundary_surfaces) {
		handled_lods.insert(lod);
	}

	for (auto child = city_object_node.first_child(); child; child = child.next_sibling()) {
		std::string child_name = child.name();
		std::string lod = ExtractLOD(child_name);
		if (lod.empty() || handled_lods.count(lod) > 0) {
			continue;
		}

		std::string child_local = StripPrefix(child_name);

		// Determine what geometry type this is
		Geometry geom;
		geom.lod = lod;

		if (child_local.find("Solid") != std::string::npos) {
			auto solid = FindChildByLocalName(child, "Solid");
			if (solid) {
				geom.type = "Solid";
				geom.boundaries = ParseSolid(solid, pool);
				geometries.push_back(geom);
				handled_lods.insert(lod);
			}
		} else if (child_local.find("MultiSurface") != std::string::npos) {
			auto ms = FindChildByLocalName(child, "MultiSurface");
			if (ms) {
				geom.type = "MultiSurface";
				geom.boundaries = ParseMultiSurface(ms, pool);
				geometries.push_back(geom);
				handled_lods.insert(lod);
			}
		} else if (child_local.find("MultiCurve") != std::string::npos) {
			// MultiCurve → MultiLineString (skip for now, not commonly needed)
		} else if (child_local.find("FootPrint") != std::string::npos ||
		           child_local.find("RoofEdge") != std::string::npos) {
			auto ms = FindChildByLocalName(child, "MultiSurface");
			if (ms) {
				geom.type = "MultiSurface";
				geom.boundaries = ParseMultiSurface(ms, pool);
				geometries.push_back(geom);
				handled_lods.insert(lod);
			}
		}
	}
}

Geometry CityGMLParser::BuildGeometryFromBoundarySurfaces(const std::string &lod, const std::string &geom_type,
                                                          const std::vector<BoundarySurface> &surfaces) {
	Geometry geom;
	geom.lod = lod;
	geom.type = geom_type;

	// Build boundaries
	json all_surfaces = json::array();
	for (const auto &bs : surfaces) {
		all_surfaces.push_back(bs.surface_boundaries);
	}

	if (geom_type == "Solid") {
		// Solid boundaries: [shell] where shell = [surface, surface, ...]
		json shell = json::array();
		shell.push_back(all_surfaces);
		geom.boundaries = shell;
	} else {
		// MultiSurface boundaries: [surface, surface, ...]
		geom.boundaries = all_surfaces;
	}

	// Build semantics
	json sem_surfaces = json::array();
	json sem_values = json::array();
	for (size_t i = 0; i < surfaces.size(); i++) {
		json sem_obj;
		sem_obj["type"] = surfaces[i].semantic_type;
		sem_surfaces.push_back(sem_obj);
		sem_values.push_back(static_cast<int>(i));
	}

	json semantics;
	semantics["surfaces"] = sem_surfaces;

	if (geom_type == "Solid") {
		// For Solid, values is [[v1, v2, ...]] (one array per shell)
		json shell_values = json::array();
		shell_values.push_back(sem_values);
		semantics["values"] = shell_values;
	} else {
		semantics["values"] = sem_values;
	}

	geom.semantics = semantics;

	return geom;
}

// ============================================================
// Attribute Parsing
// ============================================================

json CityGMLParser::ParseAttributeValue(const std::string &text) {
	// Try integer
	try {
		size_t pos;
		long long val = std::stoll(text, &pos);
		if (pos == text.size()) {
			return json(val);
		}
	} catch (...) {
	}

	// Try double
	try {
		size_t pos;
		double val = std::stod(text, &pos);
		if (pos == text.size()) {
			return json(val);
		}
	} catch (...) {
	}

	// Fall back to string
	return json(text);
}

std::map<std::string, json> CityGMLParser::ParseAttributes(const pugi::xml_node &node) {
	std::map<std::string, json> attrs;

	for (auto child = node.first_child(); child; child = child.next_sibling()) {
		std::string local_name = StripPrefix(child.name());

		// Skip geometry-related elements, boundedBy, consistsOfBuildingPart, address, etc.
		if (local_name.find("lod") == 0 || local_name == "boundedBy" ||
		    local_name == "consistsOfBuildingPart" || local_name == "address" ||
		    local_name == "interiorRoom" || local_name == "consistsOfBridgePart" ||
		    local_name == "consistsOfTunnelPart" || local_name == "outerBuildingInstallation" ||
		    local_name == "interiorBuildingInstallation" || local_name == "opening" ||
		    local_name == "reliefComponent" || local_name == "groupMember" ||
		    local_name == "parent" || local_name == "trafficArea" ||
		    local_name == "auxiliaryTrafficArea" || local_name == "section" ||
		    local_name == "intersection" || local_name == "tin" ||
		    local_name == "boundedBy" || local_name.empty()) {
			continue;
		}

		// Check if this is a leaf element with text content
		std::string text = child.child_value();
		if (!text.empty() && !child.first_child().first_child()) {
			attrs[local_name] = ParseAttributeValue(text);
		}
	}

	// Parse generic attributes (CityGML 2.0 style)
	ParseGenericAttributes(node, attrs);

	return attrs;
}

void CityGMLParser::ParseGenericAttributes(const pugi::xml_node &node, std::map<std::string, json> &attrs) {
	// CityGML 2.0 generic attributes: gen:stringAttribute, gen:intAttribute, etc.
	for (auto child = node.first_child(); child; child = child.next_sibling()) {
		std::string local_name = StripPrefix(child.name());

		if (local_name == "stringAttribute" || local_name == "intAttribute" ||
		    local_name == "doubleAttribute" || local_name == "dateAttribute" ||
		    local_name == "uriAttribute" || local_name == "measureAttribute") {

			std::string attr_name = child.attribute("name").as_string();
			if (attr_name.empty()) {
				continue;
			}

			auto value_node = FindChildByLocalName(child, "value");
			if (value_node) {
				std::string text = value_node.child_value();
				if (local_name == "intAttribute") {
					try {
						attrs[attr_name] = json(std::stoll(text));
					} catch (...) {
						attrs[attr_name] = json(text);
					}
				} else if (local_name == "doubleAttribute" || local_name == "measureAttribute") {
					try {
						attrs[attr_name] = json(std::stod(text));
					} catch (...) {
						attrs[attr_name] = json(text);
					}
				} else {
					attrs[attr_name] = json(text);
				}
			}
		}
	}
}

// ============================================================
// CityObject Type Mapping
// ============================================================

std::string CityGMLParser::MapCityObjectType(const std::string &element_name) {
	std::string local = StripPrefix(element_name);

	// Direct mapping for most types
	static const std::map<std::string, std::string> type_map = {
	    {"Building", "Building"},
	    {"BuildingPart", "BuildingPart"},
	    {"BuildingInstallation", "BuildingInstallation"},
	    {"BuildingRoom", "BuildingRoom"},
	    {"Room", "BuildingRoom"},
	    {"BuildingFurniture", "BuildingFurniture"},
	    {"BuildingStorey", "BuildingStorey"},
	    {"BuildingUnit", "BuildingUnit"},
	    {"Bridge", "Bridge"},
	    {"BridgePart", "BridgePart"},
	    {"BridgeInstallation", "BridgeInstallation"},
	    {"BridgeConstructionElement", "BridgeConstructionElement"},
	    {"Tunnel", "Tunnel"},
	    {"TunnelPart", "TunnelPart"},
	    {"TunnelInstallation", "TunnelInstallation"},
	    {"Road", "Road"},
	    {"Railway", "Railway"},
	    {"TransportSquare", "TransportSquare"},
	    {"Track", "Road"},
	    {"WaterBody", "WaterBody"},
	    {"PlantCover", "PlantCover"},
	    {"SolitaryVegetationObject", "SolitaryVegetationObject"},
	    {"LandUse", "LandUse"},
	    {"CityFurniture", "CityFurniture"},
	    {"ReliefFeature", "TINRelief"},
	    {"TINRelief", "TINRelief"},
	    {"GenericCityObject", "GenericCityObject"},
	    {"CityObjectGroup", "CityObjectGroup"},
	    {"OtherConstruction", "OtherConstruction"},
	};

	auto it = type_map.find(local);
	if (it != type_map.end()) {
		return it->second;
	}

	// Unknown type — use as-is
	return local;
}

// ============================================================
// CityObject Parsing
// ============================================================

CityObject CityGMLParser::ParseSingleCityObject(const pugi::xml_node &node, const std::string &type_name,
                                                 VertexPool &pool) {
	CityObject obj;
	obj.type = type_name;
	obj.attributes = ParseAttributes(node);

	// Collect geometry at all LODs
	CollectLODGeometries(node, obj.geometry, pool);

	return obj;
}

void CityGMLParser::ParseCityObjectMember(const pugi::xml_node &member_node,
                                          std::map<std::string, CityObject> &city_objects, VertexPool &pool,
                                          const std::string &parent_id) {
	for (auto child = member_node.first_child(); child; child = child.next_sibling()) {
		std::string element_name = child.name();
		std::string type_name = MapCityObjectType(element_name);
		std::string local_name = StripPrefix(element_name);

		// Get the gml:id
		std::string obj_id = child.attribute("gml:id").as_string();
		if (obj_id.empty()) {
			obj_id = child.attribute("id").as_string();
		}
		if (obj_id.empty()) {
			// Generate an ID
			obj_id = type_name + "_" + std::to_string(city_objects.size());
		}

		// Special handling for ReliefFeature → dig into reliefComponent for TINRelief
		if (local_name == "ReliefFeature") {
			auto relief_components = FindChildrenByLocalName(child, "reliefComponent");
			for (const auto &rc : relief_components) {
				auto tin_relief = FindChildByLocalName(rc, "TINRelief");
				if (tin_relief) {
					std::string tin_id = tin_relief.attribute("gml:id").as_string();
					if (tin_id.empty()) {
						tin_id = obj_id + "_tin";
					}

					CityObject tin_obj;
					tin_obj.type = "TINRelief";
					tin_obj.attributes = ParseAttributes(tin_relief);

					// Find the tin geometry
					auto tin_node = FindChildByLocalName(tin_relief, "tin");
					if (tin_node) {
						auto tri_surface = FindChildByLocalName(tin_node, "TriangulatedSurface");
						if (tri_surface) {
							// Get LOD from the reliefComponent or TINRelief
							std::string lod = "0";
							auto lod_node = FindChildByLocalName(tin_relief, "lod");
							if (lod_node) {
								lod = lod_node.child_value();
							} else {
								auto lod_node2 = FindChildByLocalName(child, "lod");
								if (lod_node2) {
									lod = lod_node2.child_value();
								}
							}

							Geometry geom;
							geom.type = "CompositeSurface";
							geom.lod = lod;
							geom.boundaries = ParseTriangulatedSurface(tri_surface, pool);
							tin_obj.geometry.push_back(geom);
						}
					}

					if (!parent_id.empty()) {
						tin_obj.parents.push_back(parent_id);
					}

					city_objects[tin_id] = std::move(tin_obj);
				}
			}
			// Don't add the ReliefFeature itself as a CityObject (CityJSON flattens it)
			continue;
		}

		auto city_obj = ParseSingleCityObject(child, type_name, pool);

		if (!parent_id.empty()) {
			city_obj.parents.push_back(parent_id);
		}

		// Handle sub-objects (BuildingPart, etc.)
		// consistsOfBuildingPart → children
		auto building_parts = FindChildrenByLocalName(child, "consistsOfBuildingPart");
		for (const auto &bp_wrapper : building_parts) {
			auto bp_node = FindChildByLocalName(bp_wrapper, "BuildingPart");
			if (bp_node) {
				std::string bp_id = bp_node.attribute("gml:id").as_string();
				if (bp_id.empty()) {
					bp_id = obj_id + "_part_" + std::to_string(city_obj.children.size());
				}
				city_obj.children.push_back(bp_id);

				// Recursively parse the building part
				auto bp_obj = ParseSingleCityObject(bp_node, "BuildingPart", pool);
				bp_obj.parents.push_back(obj_id);

				// Handle nested sub-parts within building part
				city_objects[bp_id] = std::move(bp_obj);
			}
		}

		// outerBuildingInstallation → children
		auto installations = FindChildrenByLocalName(child, "outerBuildingInstallation");
		for (const auto &inst_wrapper : installations) {
			auto inst_node = FindChildByLocalName(inst_wrapper, "BuildingInstallation");
			if (inst_node) {
				std::string inst_id = inst_node.attribute("gml:id").as_string();
				if (inst_id.empty()) {
					inst_id = obj_id + "_inst_" + std::to_string(city_obj.children.size());
				}
				city_obj.children.push_back(inst_id);

				auto inst_obj = ParseSingleCityObject(inst_node, "BuildingInstallation", pool);
				inst_obj.parents.push_back(obj_id);
				city_objects[inst_id] = std::move(inst_obj);
			}
		}

		city_objects[obj_id] = std::move(city_obj);
	}
}

// ============================================================
// Metadata Parsing
// ============================================================

std::optional<CRS> CityGMLParser::ParseCRS(const pugi::xml_node &envelope) {
	std::string srs_name = envelope.attribute("srsName").as_string();
	if (srs_name.empty()) {
		return std::nullopt;
	}

	CRS crs;
	crs.name = srs_name;

	// Try to extract EPSG code
	// Patterns: "urn:ogc:def:crs,crs:EPSG::25832,crs:EPSG::5783"
	//           "urn:ogc:def:crs:EPSG::4326"
	//           "EPSG:4326"
	auto epsg_pos = srs_name.find("EPSG");
	if (epsg_pos != std::string::npos) {
		crs.authority = "EPSG";
		// Find the code after "EPSG::" or "EPSG:"
		auto code_start = srs_name.find_first_of("0123456789", epsg_pos);
		if (code_start != std::string::npos) {
			auto code_end = srs_name.find_first_not_of("0123456789", code_start);
			crs.code = srs_name.substr(code_start, code_end - code_start);
		}
	}

	return crs;
}

std::optional<GeographicalExtent> CityGMLParser::ParseEnvelopeExtent(const pugi::xml_node &envelope) {
	auto lower = FindChildByLocalName(envelope, "lowerCorner");
	auto upper = FindChildByLocalName(envelope, "upperCorner");

	if (!lower || !upper) {
		return std::nullopt;
	}

	auto lower_pt = ParsePos(lower.child_value());
	auto upper_pt = ParsePos(upper.child_value());

	return GeographicalExtent(lower_pt[0], lower_pt[1], lower_pt[2], upper_pt[0], upper_pt[1], upper_pt[2]);
}

CityJSON CityGMLParser::ParseMetadata(const pugi::xml_node &root, const std::string &version) {
	CityJSON metadata;
	// CityJSON version is always "2.0" (it's the internal schema version)
	metadata.version = "2.0";

	// Parse CRS and extent from gml:boundedBy/gml:Envelope
	auto bounded_by = FindChildByLocalName(root, "boundedBy");
	if (bounded_by) {
		auto envelope = FindChildByLocalName(bounded_by, "Envelope");
		if (envelope) {
			auto crs = ParseCRS(envelope);
			if (crs.has_value()) {
				metadata.crs = crs;
			}

			auto extent = ParseEnvelopeExtent(envelope);
			if (extent.has_value()) {
				Metadata md;
				md.geographic_extent = extent;
				metadata.metadata = md;
			}
		}
	}

	// No transform for CityGML (real-world coordinates)
	// metadata.transform is left as nullopt

	return metadata;
}

// ============================================================
// Main Parse Function
// ============================================================

CityGMLParser::ParseResult CityGMLParser::Parse(const std::string &xml_content) {
	ParseResult result;

	pugi::xml_document doc;
	auto parse_result = doc.load_string(xml_content.c_str());
	if (!parse_result) {
		throw CityJSONError::Parse("Failed to parse CityGML XML: " + std::string(parse_result.description()));
	}

	// Find root CityModel element (may have namespace prefix)
	pugi::xml_node root;
	for (auto child = doc.first_child(); child; child = child.next_sibling()) {
		std::string local = StripPrefix(child.name());
		if (local == "CityModel") {
			root = child;
			break;
		}
	}

	if (!root) {
		throw CityJSONError::Parse("No CityModel root element found in CityGML file");
	}

	// Detect version
	std::string gml_version = DetectVersion(doc);

	// Parse metadata
	result.metadata = ParseMetadata(root, gml_version);

	// Parse CityObjects from cityObjectMember elements
	VertexPool pool;
	std::map<std::string, CityObject> all_city_objects;

	auto members = FindChildrenByLocalName(root, "cityObjectMember");
	for (const auto &member : members) {
		ParseCityObjectMember(member, all_city_objects, pool);
	}

	// Create a single feature containing all city objects (like LocalCityJSONReader)
	if (!all_city_objects.empty()) {
		CityJSONFeature feature;
		feature.id = "citygml_feature";
		feature.type = "CityJSONFeature";
		feature.city_objects = std::move(all_city_objects);
		feature.vertices = pool.GetVertices();
		result.features.push_back(std::move(feature));
	}

	return result;
}

} // namespace cityjson
} // namespace duckdb
