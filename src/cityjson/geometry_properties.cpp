#include "cityjson/geometry_properties.hpp"

namespace duckdb {
namespace cityjson {

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
			result["face_semantics"] = FlattenFaceSemantics(geometry, values);
		}
	}

	return result;
}

} // namespace cityjson
} // namespace duckdb
