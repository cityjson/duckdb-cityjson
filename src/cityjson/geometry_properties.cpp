#include "cityjson/geometry_properties.hpp"

#include "cityjson/appearance_flatten.hpp"

namespace duckdb {
namespace cityjson {

json GeometryPropertiesSerializer::Serialize(const Geometry &geometry) {
	// Spec §8 flattened, face-aligned form: {type, surfaces?, face_semantics?, shells?}.
	// `type` is the CityJSON string type; the int type code / cityjsonType of the
	// old form are dropped. There is no `lod` key: the LoD is carried by the column
	// name (geometry_properties_lod2_2), and in the single-LoD `lod=` reading mode
	// the caller supplied it and GetGeometryAtLOD matched it exactly.
	json result;
	result["type"] = geometry.type;

	const std::string &t = geometry.type;
	const bool is_solid = (t == "Solid");
	const bool is_multisolid = (t == "MultiSolid" || t == "CompositeSolid");

	// shells: per-solid, then per-shell, emitted-face counts -- always two levels
	// deep, so a lone Solid is [[12, 4]], not [12, 4]. Present for solid-family
	// geometry regardless of semantics; absent for non-solid types.
	if (is_solid && geometry.boundaries.is_array()) {
		json solid_shells = json::array();
		for (const auto &shell : geometry.boundaries) {
			solid_shells.push_back(shell.is_array() ? shell.size() : 0);
		}
		json shells = json::array();
		shells.push_back(std::move(solid_shells));
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
			// The same walk the appearance columns use, so semantics and appearance
			// share one WKB face order (spec: entry `i` is face `i`).
			result["face_semantics"] = FlattenPerFace(geometry, values);
		}
	}

	return result;
}

} // namespace cityjson
} // namespace duckdb
