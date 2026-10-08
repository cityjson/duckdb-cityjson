#include "cityjson/city_object_utils.hpp"
#include <set>
#include <algorithm>
#include "cityjson/column_types.hpp"
#include "cityjson/lod_table.hpp"
#include "cityjson/wkb_encoder.hpp"
#include "cityjson/geometry_properties.hpp"
#include <map>
#include <set>
#include <algorithm>
#include <cctype>
#include <cstring>
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {
namespace cityjson {

void ReportDroppedRings(ClientContext &context, const CityJSONFeature &feature) {
	for (const auto &[id, object] : feature.city_objects) {
		for (const auto &geometry : object.geometry) {
			if (geometry.dropped_rings == 0) {
				continue;
			}
			DUCKDB_LOG_WARNING(context,
			                   "cityjson: object '%s' (%s, lod %s): dropped %llu ring(s) with fewer than three "
			                   "vertices, which cannot form a closed WKB ring, and %llu surface(s) whose exterior "
			                   "ring was one of them",
			                   id, geometry.type, geometry.lod, static_cast<unsigned long long>(geometry.dropped_rings),
			                   static_cast<unsigned long long>(geometry.dropped_surfaces));
		}
	}
}

void ReportDroppedRings(ClientContext &context, const std::vector<CityJSONFeature> &features) {
	for (const auto &feature : features) {
		ReportDroppedRings(context, feature);
	}
}

// ============================================================
// CityObjectUtils - Attribute Extraction
// ============================================================

json CityObjectUtils::GetAttributeValue(const CityObject &obj, const Column &col,
                                        const std::set<std::string> &emitted_columns) {
	// Handle predefined columns
	if (col.name == "object_type") {
		return json(obj.type);
	}

	if (col.name == "children") {
		if (obj.children.empty()) {
			return json(nullptr);
		}
		return json(obj.children);
	}

	if (col.name == "parents") {
		if (obj.parents.empty()) {
			return json(nullptr);
		}
		return json(obj.parents);
	}

	if (col.name == "children_roles") {
		if (!obj.children_roles.has_value() || obj.children_roles->empty()) {
			return json(nullptr);
		}
		// Preserve null slots positionally -- WriteVarcharArray already writes a
		// non-string array element as a SQL NULL list entry.
		json roles = json::array();
		for (const auto &role : obj.children_roles.value()) {
			roles.push_back(role.has_value() ? json(role.value()) : json(nullptr));
		}
		return roles;
	}

	// `address` and `implicit_geometry` are written by the scan from the object's own
	// address member and GeometryInstance (they need its vertex pool), never from a
	// same-named source attribute: that is reserved-name-colliding input, preserved in
	// `other`.
	if (col.name == "address" || col.name == "implicit_geometry") {
		return json(nullptr);
	}

	if (col.name == "other") {
		// `other` carries only what no column of its own carries: an attribute
		// whose name collides with a reserved column, and so never got one.
		// Attributes that DID get a column are not repeated here -- a duplicate
		// costs a JSON blob per row and is written nowhere on COPY TO.
		// geographicalExtent is not included: it is carried by `bbox`, which
		// unions it with the extent computed from the object's geometry.
		json other_attrs = json::object();
		for (const auto &[key, value] : obj.attributes) {
			if (emitted_columns.count(key) == 0) {
				other_attrs[key] = value;
			}
		}
		if (other_attrs.empty()) {
			return json(nullptr);
		}
		return other_attrs;
	}

	// Dynamic attribute column - look up in attributes map
	auto it = obj.attributes.find(col.name);
	if (it != obj.attributes.end()) {
		return it->second;
	}

	// Attribute not found
	return json(nullptr);
}

// ============================================================
// CityObjectUtils - Schema Inference
// ============================================================

std::vector<Column> CityObjectUtils::InferAttributeColumns(const std::vector<CityJSONFeature> &features,
                                                           size_t sample_size) {
	if (features.empty()) {
		return {};
	}

	// Determine how many features to sample
	size_t num_to_sample = std::min(sample_size, features.size());

	// Map of attribute name -> list of observed types
	std::map<std::string, std::vector<ColumnType>> attribute_types;

	// Sample features and collect attribute keys
	for (size_t i = 0; i < num_to_sample; i++) {
		const auto &feature = features[i];

		// Iterate through all CityObjects in the feature
		for (const auto &[city_obj_id, city_obj] : feature.city_objects) {
			// Collect all attributes
			for (const auto &[attr_key, attr_value] : city_obj.attributes) {
				// Reserved columns take precedence over dynamic attributes: an attribute
				// whose name collides (case-insensitively) with a reserved column does not
				// get its own column. Its value is still preserved in the `other` JSON.
				if (IsReservedColumnName(attr_key)) {
					continue;
				}

				// Infer type from value
				ColumnType inferred_type = ColumnTypeUtils::InferFromJson(attr_value);
				attribute_types[attr_key].push_back(inferred_type);
			}
		}
	}

	// Resolve final type for each attribute. Attribute names that differ only by case
	// would also produce duplicate DuckDB columns, so keep only the first (the map is
	// ordered, so this is deterministic).
	std::vector<Column> result;
	std::set<std::string> seen_lower;
	for (const auto &[attr_name, types] : attribute_types) {
		std::string lowered = attr_name;
		std::transform(lowered.begin(), lowered.end(), lowered.begin(),
		               [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
		if (!seen_lower.insert(lowered).second) {
			continue;
		}
		ColumnType resolved_type = ColumnTypeUtils::ResolveFromSamples(types);
		result.emplace_back(attr_name, resolved_type);
	}

	// Sort by name for consistent ordering
	std::sort(result.begin(), result.end(), [](const Column &a, const Column &b) { return a.name < b.name; });

	return result;
}

std::vector<Column> CityObjectUtils::InferGeometryColumns(const std::vector<CityJSONFeature> &features,
                                                          size_t sample_size) {
	if (features.empty()) {
		return {};
	}

	// Determine how many features to sample
	size_t num_to_sample = std::min(sample_size, features.size());

	// Set of unique LODs found
	std::set<std::string> lods;

	// Sample features and collect LODs
	for (size_t i = 0; i < num_to_sample; i++) {
		const auto &feature = features[i];

		// Iterate through all CityObjects in the feature
		for (const auto &[city_obj_id, city_obj] : feature.city_objects) {
			// Collect LODs from all geometries
			for (const auto &geom : city_obj.geometry) {
				if (!geom.lod.empty()) {
					lods.insert(LODTableUtils::NormalizeLOD(geom.lod));
				}
			}
		}
	}

	// `bbox` leads the geometry group (spec 02-object-table-schema.mdx, "Reserved
	// columns": bbox precedes every geometry_lod*), and is emitted unconditionally
	// -- unlike the geometry_lod*/geometry_properties_lod*/material_lod*/
	// texture_lod* family, whose per-LoD *set* genuinely is a property of the
	// dataset (spec: "a table whose objects have no analysis geometry at all
	// carries none of them"), bbox is not part of that per-LoD exception: it is a
	// single reserved column like `address` or `implicit_geometry`, column-nullable but
	// always present. cityparquet-rs pushes it unconditionally too.
	std::vector<Column> result;
	result.emplace_back("bbox", ColumnType::GeographicalExtent);

	// Create per-LOD WKB geometry columns (CityParquet-style wide layout):
	// for each LOD, a "geometry_lodX_Y" (WKB BLOB) and "geometry_properties_lodX_Y" (JSON).
	// `lods` is a std::set, so iteration is already sorted; emit the pair per LOD so the
	// geometry and its properties stay adjacent.
	for (const auto &lod : lods) {
		std::string suffix = LODTableUtils::FormatLODAsColumnSuffix(lod);
		result.emplace_back("geometry_" + suffix, ColumnType::GeometryWKB);
		result.emplace_back("geometry_properties_" + suffix, ColumnType::GeometryPropertiesStruct);
		// Per-LoD appearance columns paired to the geometry by name (§11.1). Present
		// for every LoD that has a geometry column, whether or not any row carries
		// appearance for it (nullable).
		result.emplace_back("material_" + suffix, ColumnType::MaterialMap);
		result.emplace_back("texture_" + suffix, ColumnType::TextureMap);
	}

	return result;
}

// ============================================================
// CityObjectUtils - Geometry Encoding
// ============================================================

std::vector<uint8_t> CityObjectUtils::GetGeometryWKB(const Geometry &geometry,
                                                     const std::vector<std::array<double, 3>> &vertices,
                                                     const std::optional<Transform> &transform) {
	return WKBEncoder::Encode(geometry, vertices, transform);
}

json CityObjectUtils::GetGeometryPropertiesStruct(const Geometry &geometry,
                                                  const std::optional<std::string> &object_id) {
	// Note: object_id parameter reserved for future use
	(void)object_id; // Suppress unused parameter warning
	return GeometryPropertiesSerializer::Serialize(geometry);
}

static void CollectExtentRecursive(const json &boundaries, const std::vector<std::array<double, 3>> &vertices,
                                   const std::optional<Transform> &transform, GeographicalExtent &extent, bool &found) {
	if (boundaries.is_number_integer()) {
		auto idx = boundaries.get<int64_t>();
		if (idx < 0 || static_cast<size_t>(idx) >= vertices.size()) {
			return; // skip invalid index
		}
		std::array<double, 3> v = vertices[static_cast<size_t>(idx)];
		if (transform.has_value()) {
			v = transform->Apply(v);
		}
		if (!found) {
			extent.min_x = extent.max_x = v[0];
			extent.min_y = extent.max_y = v[1];
			extent.min_z = extent.max_z = v[2];
			found = true;
		} else {
			extent.min_x = std::min(extent.min_x, v[0]);
			extent.min_y = std::min(extent.min_y, v[1]);
			extent.min_z = std::min(extent.min_z, v[2]);
			extent.max_x = std::max(extent.max_x, v[0]);
			extent.max_y = std::max(extent.max_y, v[1]);
			extent.max_z = std::max(extent.max_z, v[2]);
		}
		return;
	}
	if (boundaries.is_array()) {
		for (const auto &child : boundaries) {
			CollectExtentRecursive(child, vertices, transform, extent, found);
		}
	}
}

namespace {

void MergeExtent(std::optional<GeographicalExtent> &into, const GeographicalExtent &other) {
	if (!into.has_value()) {
		into = other;
		return;
	}
	into->min_x = std::min(into->min_x, other.min_x);
	into->min_y = std::min(into->min_y, other.min_y);
	into->min_z = std::min(into->min_z, other.min_z);
	into->max_x = std::max(into->max_x, other.max_x);
	into->max_y = std::max(into->max_y, other.max_y);
	into->max_z = std::max(into->max_z, other.max_z);
}

void AccumulateObjectExtent(const std::string &object_id, const std::map<std::string, CityObject> &objects,
                            const std::vector<std::array<double, 3>> &vertices,
                            const std::optional<Transform> &transform, std::set<std::string> &visited,
                            std::optional<GeographicalExtent> &result) {
	if (!visited.insert(object_id).second) {
		return; // already folded in; also guards a cyclic hierarchy
	}
	auto entry = objects.find(object_id);
	if (entry == objects.end()) {
		return; // dangling child reference contributes nothing
	}
	const auto &object = entry->second;

	// Every stored LoD, not just the highest.
	for (const auto &geometry : object.geometry) {
		auto extent = CityObjectUtils::GetGeometryExtent(geometry, vertices, transform);
		if (extent.has_value()) {
			MergeExtent(result, extent.value());
		}
	}
	for (const auto &child_id : object.children) {
		AccumulateObjectExtent(child_id, objects, vertices, transform, visited, result);
	}
}

} // namespace

std::optional<GeographicalExtent> CityObjectUtils::GetObjectExtent(const std::string &object_id,
                                                                   const std::map<std::string, CityObject> &objects,
                                                                   const std::vector<std::array<double, 3>> &vertices,
                                                                   const std::optional<Transform> &transform) {
	std::optional<GeographicalExtent> result;
	std::set<std::string> visited;
	AccumulateObjectExtent(object_id, objects, vertices, transform, visited, result);
	return result;
}

std::optional<GeographicalExtent> CityObjectUtils::GetGeometryExtent(const Geometry &geometry,
                                                                     const std::vector<std::array<double, 3>> &vertices,
                                                                     const std::optional<Transform> &transform) {
	// A GeometryInstance's one vertex is the reference point its relative geometry is
	// placed at, not an extent of what it places: it contributes none, as in the
	// reference writer.
	if (geometry.type == "GeometryInstance") {
		return std::nullopt;
	}
	GeographicalExtent extent;
	bool found = false;
	CollectExtentRecursive(geometry.boundaries, vertices, transform, extent, found);
	if (!found) {
		return std::nullopt;
	}
	return extent;
}

} // namespace cityjson
} // namespace duckdb

namespace duckdb {
namespace cityjson {

const std::vector<std::pair<std::string, std::string>> &AddressMemberNames() {
	static const std::vector<std::pair<std::string, std::string>> names = {
	    {"street", "thoroughfareName"}, {"house_number", "thoroughfareNumber"},
	    {"po_box", "postBox"},          {"zip_code", "postcode"},
	    {"city", "locality"},           {"state", "administrativeArea"},
	    {"country", "country"},         {"free_text", "freeText"}};
	return names;
}

Value CityObjectUtils::GetAddressValue(const CityObject &object, const std::vector<std::array<double, 3>> *vertices,
                                       const std::optional<Transform> &transform) {
	const auto list_type = ColumnTypeUtils::ToDuckDBType(ColumnType::AddressList);
	if (!object.address.has_value() || !object.address->is_array()) {
		return Value(list_type);
	}
	const auto &struct_type = ListType::GetChildType(list_type);
	duckdb::vector<Value> entries;
	for (const auto &address : object.address.value()) {
		child_list_t<Value> fields;
		for (const auto &field : AddressMemberNames()) {
			const auto member = address.is_object() ? address.find(field.second) : address.end();
			// A member that is not a string is not an address field this format can hold.
			if (address.is_object() && member != address.end() && member->is_string()) {
				fields.emplace_back(field.first, Value(member->get<std::string>()));
			} else {
				fields.emplace_back(field.first, Value(LogicalType(LogicalTypeId::VARCHAR)));
			}
		}
		// `location` is a MultiPoint indexing the vertex pool; the column holds the
		// geometry itself, so a location that does not resolve -- wrong type, a bad
		// index -- is NULL rather than a dangling reference.
		Value location {LogicalType(LogicalTypeId::BLOB)};
		if (address.is_object() && address.contains("location") && address["location"].is_object() &&
		    vertices != nullptr) {
			const auto &geometry = address["location"];
			if (geometry.value("type", "") == "MultiPoint" && geometry.contains("boundaries")) {
				try {
					auto wkb =
					    WKBEncoder::Encode(Geometry("MultiPoint", "", geometry["boundaries"]), *vertices, transform);
					location = Value::BLOB(wkb.data(), wkb.size());
				} catch (const std::exception &) {
					location = Value(LogicalType(LogicalTypeId::BLOB));
				}
			}
		}
		fields.emplace_back("location", std::move(location));
		entries.push_back(Value::STRUCT(std::move(fields)));
	}
	return Value::LIST(struct_type, std::move(entries));
}

} // namespace cityjson
} // namespace duckdb

namespace duckdb {
namespace cityjson {

std::optional<ImplicitGeometryCell>
CityObjectUtils::GetImplicitGeometry(const CityObject &object, const std::vector<std::array<double, 3>> *vertices,
                                     const std::optional<Transform> &transform) {
	for (const auto &geometry : object.geometry) {
		if (geometry.type != "GeometryInstance") {
			continue;
		}
		// The first instance only: the column holds one (spec open question).
		if (!geometry.template_index.has_value() || vertices == nullptr || !geometry.boundaries.is_array() ||
		    geometry.boundaries.empty() || !geometry.boundaries[0].is_number_unsigned()) {
			return std::nullopt;
		}
		const auto index = geometry.boundaries[0].get<uint64_t>();
		if (index >= vertices->size()) {
			return std::nullopt;
		}
		ImplicitGeometryCell cell;
		cell.id = geometry.template_index.value();
		const auto &vertex = (*vertices)[index];
		cell.point = WKBEncoder::EncodePoint(transform.has_value() ? transform->Apply(vertex) : vertex);
		if (geometry.transformation_matrix.has_value()) {
			const auto &matrix = geometry.transformation_matrix.value();
			if (!matrix.is_array() || matrix.size() != 16 ||
			    !std::all_of(matrix.begin(), matrix.end(), [](const json &v) { return v.is_number(); })) {
				throw CityJSONError::InvalidGeometry("a GeometryInstance's transformationMatrix must be 16 numbers "
				                                     "(a row-major 4x4), got " +
				                                     matrix.dump());
			}
			std::vector<double> values;
			for (const auto &value : matrix) {
				values.push_back(value.get<double>());
			}
			cell.transformation_matrix = std::move(values);
		}
		return cell;
	}
	return std::nullopt;
}

} // namespace cityjson
} // namespace duckdb
