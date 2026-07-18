#include "cityjson/geometry_properties.hpp"
#include "cityjson/error.hpp"
#include <unordered_map>

namespace duckdb {
namespace cityjson {

// =============================================================================
// Type code mappings
// =============================================================================

static const std::unordered_map<std::string, int> CITYJSON_TYPE_CODES = {
    {"Point", 1},   {"MultiPoint", 2},       {"LineString", 3}, {"MultiLineString", 4},
    {"Surface", 5}, {"CompositeSurface", 6}, {"TIN", 7},        {"MultiSurface", 8},
    {"Solid", 9},   {"CompositeSolid", 10},  {"MultiSolid", 11}};

static const std::unordered_map<int, std::string> TYPE_CODE_NAMES = {
    {1, "Point"},   {2, "MultiPoint"},       {3, "LineString"}, {4, "MultiLineString"},
    {5, "Surface"}, {6, "CompositeSurface"}, {7, "TIN"},        {8, "MultiSurface"},
    {9, "Solid"},   {10, "CompositeSolid"},  {11, "MultiSolid"}};

// =============================================================================
// Public methods
// =============================================================================

int GeometryPropertiesSerializer::GetTypeCode(const std::string &cityjson_type) {
	auto it = CITYJSON_TYPE_CODES.find(cityjson_type);
	if (it != CITYJSON_TYPE_CODES.end()) {
		return it->second;
	}
	// Default to -1 for unknown types
	return -1;
}

std::string GeometryPropertiesSerializer::GetTypeName(int type_code) {
	auto it = TYPE_CODE_NAMES.find(type_code);
	if (it != TYPE_CODE_NAMES.end()) {
		return it->second;
	}
	return "Unknown";
}

// JSON null sentinel, used as a stable reference when a source `values` entry
// is missing (the whole-shell / whole-solid null shorthand of CityJSON §semantics).
static const json kJsonNull = json();

// Flatten one shell's `values` sub-array against a shell of `n_faces` surfaces,
// appending one entry per emitted WKB face (spec §8). A non-array `shell_values`
// is the CityJSON whole-shell null shorthand and expands to one null per face;
// a too-short array is padded with null, a too-long one truncated.
static void FlattenShellValues(json &out, const json &shell_values, size_t n_faces) {
	if (!shell_values.is_array()) {
		for (size_t i = 0; i < n_faces; ++i) {
			out.push_back(nullptr);
		}
		return;
	}
	for (size_t i = 0; i < n_faces; ++i) {
		if (i < shell_values.size() && !shell_values[i].is_null()) {
			out.push_back(shell_values[i]);
		} else {
			out.push_back(nullptr);
		}
	}
}

// Build the flat, WKB-face-aligned `face_semantics` array (spec §8). Because the
// WKB encoder emits one face per CityJSON surface in document order (shells
// flattened outer-first, no degenerate dropping — see wkb_encoder.cpp), the flat
// order here matches the emitted WKB faces one-to-one.
static json FlattenFaceSemantics(const Geometry &geometry, const json &values) {
	json out = json::array();
	const std::string &t = geometry.type;
	const json &b = geometry.boundaries;
	if (!b.is_array()) {
		return out;
	}

	if (t == "Solid") {
		// boundaries = [shell,...]; values = [[per-surface],...]
		for (size_t si = 0; si < b.size(); ++si) {
			size_t n_faces = b[si].is_array() ? b[si].size() : 0;
			const json &sv = (si < values.size()) ? values[si] : kJsonNull;
			FlattenShellValues(out, sv, n_faces);
		}
	} else if (t == "MultiSolid" || t == "CompositeSolid") {
		// boundaries = [solid,...]; solid = [shell,...]; values = [[[..],..],..]
		for (size_t soi = 0; soi < b.size(); ++soi) {
			const json &solid = b[soi];
			const json &solid_values = (soi < values.size()) ? values[soi] : kJsonNull;
			if (!solid.is_array()) {
				continue;
			}
			for (size_t si = 0; si < solid.size(); ++si) {
				size_t n_faces = solid[si].is_array() ? solid[si].size() : 0;
				const json &sv =
				    (solid_values.is_array() && si < solid_values.size()) ? solid_values[si] : kJsonNull;
				FlattenShellValues(out, sv, n_faces);
			}
		}
	} else {
		// MultiSurface / CompositeSurface: boundaries = [surface,...]; values = [int|null,...]
		for (size_t i = 0; i < b.size(); ++i) {
			if (i < values.size() && !values[i].is_null()) {
				out.push_back(values[i]);
			} else {
				out.push_back(nullptr);
			}
		}
	}
	return out;
}

json GeometryPropertiesSerializer::Serialize(const Geometry &geometry, bool include_lod) {
	// Spec §8 flattened, face-aligned form: {type, surfaces?, face_semantics?, shells?}.
	// `type` is the CityJSON string type; the int type code / cityjsonType of the
	// old form are dropped. The LoD is normally carried by the column name; it is
	// added here only for an un-suffixed column (include_lod), as a permitted §8
	// extra key.
	json result;
	result["type"] = geometry.type;
	if (include_lod) {
		result["lod"] = geometry.lod;
	}

	const std::string &t = geometry.type;
	const bool is_solid = (t == "Solid");
	const bool is_multisolid = (t == "MultiSolid" || t == "CompositeSolid");

	// shells: per-shell emitted-face counts. Flat for Solid, one array per solid
	// (WKB GeometryCollection member order) for MultiSolid/CompositeSolid. Present
	// for solid-family geometry regardless of semantics; absent for non-solid types.
	if (is_solid && geometry.boundaries.is_array()) {
		json shells = json::array();
		for (const auto &shell : geometry.boundaries) {
			shells.push_back(shell.is_array() ? shell.size() : 0);
		}
		result["shells"] = shells;
	} else if (is_multisolid && geometry.boundaries.is_array()) {
		json shells = json::array();
		for (const auto &solid : geometry.boundaries) {
			json solid_shells = json::array();
			if (solid.is_array()) {
				for (const auto &shell : solid) {
					solid_shells.push_back(shell.is_array() ? shell.size() : 0);
				}
			}
			shells.push_back(solid_shells);
		}
		result["shells"] = shells;
	}

	// surfaces + face_semantics are emitted together, and only when the source
	// carries semantics. surfaces is preserved verbatim (order and content);
	// face_semantics replaces CityJSON's nested `values` with one entry per face.
	if (geometry.semantics.has_value()) {
		const json &sem = geometry.semantics.value();
		if (sem.contains("surfaces") && sem["surfaces"].is_array()) {
			result["surfaces"] = sem["surfaces"];
			const json empty = json::array();
			const json &values = (sem.contains("values") && sem["values"].is_array()) ? sem["values"] : empty;
			result["face_semantics"] = FlattenFaceSemantics(geometry, values);
		}
	}

	return result;
}

json GeometryPropertiesSerializer::SerializeSemantics(const json &semantics_json) {
	json result;

	// Copy the semantics structure directly
	// CityJSON semantics format:
	// {
	//   "surfaces": [{"type": "WallSurface", ...}, ...],
	//   "values": [0, 1, null, 2, ...]
	// }

	if (semantics_json.contains("surfaces")) {
		result["surfaces"] = json::array();

		for (const auto &surface : semantics_json["surfaces"]) {
			json surf_obj;

			// Required: type
			if (surface.contains("type")) {
				surf_obj["type"] = surface["type"];
			}

			// Optional: other attributes (slope, solar-potential, etc.)
			for (auto it = surface.begin(); it != surface.end(); ++it) {
				if (it.key() != "type" && it.key() != "parent" && it.key() != "children") {
					// Skip structural fields, copy attribute fields
					surf_obj[it.key()] = it.value();
				}
			}

			// Optional: parent reference
			if (surface.contains("parent")) {
				surf_obj["parent"] = surface["parent"];
			}

			// Optional: children references
			if (surface.contains("children")) {
				surf_obj["children"] = surface["children"];
			}

			result["surfaces"].push_back(surf_obj);
		}
	}

	if (semantics_json.contains("values")) {
		result["values"] = semantics_json["values"];
	}

	return result;
}

GeometryProperties GeometryPropertiesSerializer::Build(const Geometry &geometry) {
	GeometryProperties props;

	props.type = GetTypeCode(geometry.type);
	props.cityjson_type = geometry.type;
	props.lod = geometry.lod;

	// Build children for complex geometries
	if (props.type >= 9 && props.type <= 11) {
		props.children = BuildChildren(geometry);
	}

	// Parse semantics if present
	if (geometry.semantics.has_value()) {
		const auto &sem_json = geometry.semantics.value();
		GeometrySemantics semantics;

		if (sem_json.contains("surfaces")) {
			for (const auto &surf : sem_json["surfaces"]) {
				SemanticSurface ss;
				if (surf.contains("type")) {
					ss.type = surf["type"].get<std::string>();
				}
				// Store other attributes
				ss.attributes = surf;
				semantics.surfaces.push_back(std::move(ss));
			}
		}

		if (sem_json.contains("values")) {
			semantics.values = sem_json["values"];
		}

		props.semantics = std::move(semantics);
	}

	return props;
}

// =============================================================================
// Private methods
// =============================================================================

std::vector<GeometryPropertyChild> GeometryPropertiesSerializer::BuildChildren(const Geometry &geometry) {
	std::vector<GeometryPropertyChild> children;

	// For Solid: create children for each shell and surface
	if (geometry.type == "Solid" && geometry.boundaries.is_array()) {
		size_t polygon_index = 0;

		for (size_t shell_idx = 0; shell_idx < geometry.boundaries.size(); ++shell_idx) {
			const auto &shell = geometry.boundaries[shell_idx];

			// Create a child for the shell
			GeometryPropertyChild shell_child;
			shell_child.type = 6; // CompositeSurface
			shell_child.cityjson_type = "CompositeSurface";
			shell_child.object_id = (shell_idx == 0) ? "exteriorShell" : "interiorShell" + std::to_string(shell_idx);
			children.push_back(shell_child);

			// Create children for each surface in the shell
			if (shell.is_array()) {
				for (size_t surf_idx = 0; surf_idx < shell.size(); ++surf_idx) {
					GeometryPropertyChild surf_child;
					surf_child.type = 5; // Surface
					surf_child.cityjson_type = "Surface";
					surf_child.parent = shell_idx;
					surf_child.geometry_index = polygon_index++;
					children.push_back(surf_child);
				}
			}
		}
	}
	// For MultiSolid/CompositeSolid: create children for each solid
	else if ((geometry.type == "MultiSolid" || geometry.type == "CompositeSolid") && geometry.boundaries.is_array()) {
		size_t polygon_index = 0;

		for (size_t solid_idx = 0; solid_idx < geometry.boundaries.size(); ++solid_idx) {
			const auto &solid = geometry.boundaries[solid_idx];

			// Create a child for the solid
			GeometryPropertyChild solid_child;
			solid_child.type = 9; // Solid
			solid_child.cityjson_type = "Solid";
			children.push_back(solid_child);

			// Count polygons in this solid for geometry_index mapping
			if (solid.is_array()) {
				for (const auto &shell : solid) {
					if (shell.is_array()) {
						polygon_index += shell.size();
					}
				}
			}
		}
	}

	return children;
}

size_t GeometryPropertiesSerializer::CountPolygonsInSolid(const json &boundaries) {
	size_t count = 0;

	if (boundaries.is_array()) {
		for (const auto &shell : boundaries) {
			if (shell.is_array()) {
				count += shell.size();
			}
		}
	}

	return count;
}

} // namespace cityjson
} // namespace duckdb
