#include "cityjson/cityjson_types.hpp"
#include <algorithm>

namespace duckdb {
namespace cityjson {

using namespace json_utils;

// ============================================================
// Transform
// ============================================================

Transform::Transform() : scale({1.0, 1.0, 1.0}), translate({0.0, 0.0, 0.0}) {
}

Transform::Transform(std::array<double, 3> scale, std::array<double, 3> translate)
    : scale(scale), translate(translate) {
}

std::array<double, 3> Transform::Apply(const std::array<double, 3> &vertex) const {
	return {vertex[0] * scale[0] + translate[0], vertex[1] * scale[1] + translate[1],
	        vertex[2] * scale[2] + translate[2]};
}

Transform Transform::FromJson(const json &obj) {
	if (!obj.is_object()) {
		throw CityJSONError::InvalidTransform("Transform must be a JSON object");
	}

	Transform result;

	if (obj.contains("scale") && obj["scale"].is_array()) {
		const auto &scale_arr = obj["scale"];
		if (scale_arr.size() != 3) {
			throw CityJSONError::InvalidTransform("Transform scale must have exactly 3 elements");
		}
		result.scale = {scale_arr[0].get<double>(), scale_arr[1].get<double>(), scale_arr[2].get<double>()};
	}

	if (obj.contains("translate") && obj["translate"].is_array()) {
		const auto &trans_arr = obj["translate"];
		if (trans_arr.size() != 3) {
			throw CityJSONError::InvalidTransform("Transform translate must have exactly 3 elements");
		}
		result.translate = {trans_arr[0].get<double>(), trans_arr[1].get<double>(), trans_arr[2].get<double>()};
	}

	return result;
}

// ============================================================
// CRS
// ============================================================

CRS::CRS(std::string name) : name(std::move(name)) {
}

CRS::CRS(std::string name, std::string authority, std::string code)
    : name(std::move(name)), authority(std::move(authority)), code(std::move(code)) {
}

CRS CRS::FromJson(const json &obj) {
	// Handle EPSG code format: { "epsg": 4326 } or string "EPSG:4326"
	if (obj.is_number()) {
		int epsg_code = obj.get<int>();
		return CRS("EPSG:" + std::to_string(epsg_code), "EPSG", std::to_string(epsg_code));
	}

	if (obj.is_string()) {
		std::string crs_str = obj.get<std::string>();
		return CRS(crs_str);
	}

	if (obj.is_object()) {
		if (obj.contains("epsg")) {
			int epsg_code = obj["epsg"].get<int>();
			return CRS("EPSG:" + std::to_string(epsg_code), "EPSG", std::to_string(epsg_code));
		}

		std::string name = GetString(obj, "name", "");
		auto authority = GetOptionalString(obj, "authority");
		auto code = GetOptionalString(obj, "code");

		if (!authority.has_value() && !code.has_value()) {
			return CRS(name);
		} else {
			return CRS(name, authority.value_or(""), code.value_or(""));
		}
	}

	throw CityJSONError::InvalidCRS("Invalid CRS format");
}

// ============================================================
// PointOfContact
// ============================================================

PointOfContact::PointOfContact(std::string contact_name, std::string email_address)
    : contact_name(std::move(contact_name)), email_address(std::move(email_address)) {
}

PointOfContact PointOfContact::FromJson(const json &obj) {
	if (!obj.is_object()) {
		throw CityJSONError::InvalidJson("pointOfContact must be a JSON object");
	}

	// According to CityJSON 2.0.1 spec section 5.3:
	// - contactName is required
	// - emailAddress is required
	ValidateRequiredKeys(obj, {"contactName", "emailAddress"});

	PointOfContact result;
	result.contact_name = obj["contactName"].get<std::string>();
	result.email_address = obj["emailAddress"].get<std::string>();
	result.role = GetOptionalString(obj, "role");
	result.website = GetOptionalString(obj, "website");
	result.contact_type = GetOptionalString(obj, "contactType");
	result.address = GetOptionalObject(obj, "address");
	result.phone = GetOptionalString(obj, "phone");
	result.organization = GetOptionalString(obj, "organization");

	return result;
}

json PointOfContact::ToJson() const {
	json result = {{"contactName", contact_name}, {"emailAddress", email_address}};

	if (role)
		result["role"] = *role;
	if (website)
		result["website"] = *website;
	if (contact_type)
		result["contactType"] = *contact_type;
	if (address)
		result["address"] = *address;
	if (phone)
		result["phone"] = *phone;
	if (organization)
		result["organization"] = *organization;

	return result;
}

// ============================================================
// Metadata
// ============================================================

Metadata Metadata::FromJson(const json &obj) {
	if (!obj.is_object()) {
		return Metadata {};
	}

	Metadata result;
	result.title = GetOptionalString(obj, "title");
	result.identifier = GetOptionalString(obj, "identifier");

	// Parse pointOfContact as an object (per CityJSON 2.0.1 spec section 5.3)
	if (obj.contains("pointOfContact") && obj["pointOfContact"].is_object()) {
		result.point_of_contact = PointOfContact::FromJson(obj["pointOfContact"]);
	}

	result.reference_date = GetOptionalString(obj, "referenceDate");
	result.reference_system = GetOptionalString(obj, "referenceSystem");
	result.geographic_location = GetOptionalString(obj, "geographicLocation");

	// geographicalExtent is an array of 6 numbers [minx, miny, minz, maxx, maxy, maxz]
	if (obj.contains("geographicalExtent") && obj["geographicalExtent"].is_array()) {
		result.geographic_extent = GeographicalExtent::FromJson(obj["geographicalExtent"]);
	}
	result.dataset_topic_category = GetOptionalString(obj, "datasetTopicCategory");
	result.feature_type = GetOptionalString(obj, "featureType");
	result.metadata_standard = GetOptionalString(obj, "metadataStandard");
	result.metadata_language = GetOptionalString(obj, "metadataLanguage");
	result.metadata_character_set = GetOptionalString(obj, "metadataCharacterSet");
	result.metadata_date = GetOptionalString(obj, "metadataDate");

	return result;
}

// ============================================================
// GeographicalExtent
// ============================================================

GeographicalExtent::GeographicalExtent(double min_x, double min_y, double min_z, double max_x, double max_y,
                                       double max_z)
    : min_x(min_x), min_y(min_y), min_z(min_z), max_x(max_x), max_y(max_y), max_z(max_z) {
}

GeographicalExtent GeographicalExtent::FromJson(const json &arr) {
	if (!arr.is_array() || arr.size() != 6) {
		throw CityJSONError::InvalidJson("GeographicalExtent must be an array of 6 numbers");
	}

	return GeographicalExtent(arr[0].get<double>(), arr[1].get<double>(), arr[2].get<double>(), arr[3].get<double>(),
	                          arr[4].get<double>(), arr[5].get<double>());
}

json GeographicalExtent::ToJson() const {
	return json::array({min_x, min_y, min_z, max_x, max_y, max_z});
}

// ============================================================
// Geometry
// ============================================================

Geometry::Geometry(std::string type, std::string lod, json boundaries)
    : type(std::move(type)), lod(std::move(lod)), boundaries(std::move(boundaries)) {
}

Geometry Geometry::FromJson(const json &obj) {
	if (!obj.is_object()) {
		throw CityJSONError::InvalidGeometry("Geometry must be a JSON object");
	}

	// lod is optional per the CityJSON spec (especially in CityJSONFeature lines)
	ValidateRequiredKeys(obj, {"type", "boundaries"});

	Geometry result;
	result.type = obj["type"].get<std::string>();

	// LOD can be string or number (optional — default to empty string if missing)
	if (obj.contains("lod")) {
		if (obj["lod"].is_string()) {
			result.lod = obj["lod"].get<std::string>();
		} else if (obj["lod"].is_number()) {
			result.lod = std::to_string(obj["lod"].get<double>());
		} else {
			throw CityJSONError::InvalidGeometry("LOD must be a string or number");
		}
	}

	result.boundaries = obj["boundaries"];
	result.semantics = GetOptionalObject(obj, "semantics");
	result.material = GetOptionalObject(obj, "material");
	result.texture = GetOptionalObject(obj, "texture");

	return result;
}

json Geometry::ToJson() const {
	json result = {{"type", type}, {"lod", lod}, {"boundaries", boundaries}};

	if (semantics.has_value()) {
		result["semantics"] = semantics.value();
	}
	if (material.has_value()) {
		result["material"] = material.value();
	}
	if (texture.has_value()) {
		result["texture"] = texture.value();
	}

	return result;
}

// ============================================================
// CityObject
// ============================================================

CityObject::CityObject(std::string type) : type(std::move(type)) {
}

CityObject CityObject::FromJson(const json &obj) {
	if (!obj.is_object()) {
		throw CityJSONError::InvalidSchema("CityObject must be a JSON object");
	}

	ValidateRequiredKeys(obj, {"type"});

	CityObject result(obj["type"].get<std::string>());

	// Parse attributes (all fields except special ones)
	for (auto &[key, value] : obj.items()) {
		// Handle the special "attributes" object - extract its contents as individual attributes
		if (key == "attributes" && value.is_object()) {
			for (auto &[attr_key, attr_value] : value.items()) {
				result.attributes[attr_key] = attr_value;
			}
		} else if (key != "type" && key != "geometry" && key != "children" && key != "parents" &&
		           key != "geographicalExtent" && key != "children_roles") {
			result.attributes[key] = value;
		}
	}

	// Parse geometry array
	if (obj.contains("geometry") && obj["geometry"].is_array()) {
		for (const auto &geom_obj : obj["geometry"]) {
			result.geometry.push_back(Geometry::FromJson(geom_obj));
		}
	}

	// Parse geographical extent
	if (obj.contains("geographicalExtent") && !obj["geographicalExtent"].is_null()) {
		result.geographical_extent = GeographicalExtent::FromJson(obj["geographicalExtent"]);
	}

	// Parse children
	if (obj.contains("children") && obj["children"].is_array()) {
		for (const auto &child : obj["children"]) {
			result.children.push_back(child.get<std::string>());
		}
	}

	// Parse parents
	if (obj.contains("parents") && obj["parents"].is_array()) {
		for (const auto &parent : obj["parents"]) {
			result.parents.push_back(parent.get<std::string>());
		}
	}

	// Parse children roles
	if (obj.contains("children_roles") && obj["children_roles"].is_array()) {
		std::vector<std::string> roles;
		for (const auto &role : obj["children_roles"]) {
			roles.push_back(role.get<std::string>());
		}
		result.children_roles = roles;
	}

	return result;
}

json CityObject::ToJson() const {
	json result = {{"type", type}};

	// Add attributes
	for (const auto &[key, value] : attributes) {
		result[key] = value;
	}

	// Add geometry
	if (!geometry.empty()) {
		json geom_array = json::array();
		for (const auto &geom : geometry) {
			geom_array.push_back(geom.ToJson());
		}
		result["geometry"] = geom_array;
	}

	// Add geographical extent
	if (geographical_extent.has_value()) {
		result["geographicalExtent"] = geographical_extent->ToJson();
	}

	// Add children
	if (!children.empty()) {
		result["children"] = children;
	}

	// Add parents
	if (!parents.empty()) {
		result["parents"] = parents;
	}

	// Add children roles
	if (children_roles.has_value()) {
		result["children_roles"] = children_roles.value();
	}

	return result;
}

std::optional<Geometry> CityObject::GetGeometryAtLOD(const std::string &lod) const {
	for (const auto &geom : geometry) {
		if (geom.lod == lod) {
			return geom;
		}
	}
	return std::nullopt;
}

// ============================================================
// Extension
// ============================================================

Extension::Extension(std::string url, std::string version) : url(std::move(url)), version(std::move(version)) {
}

Extension Extension::FromJson(const json &obj) {
	if (!obj.is_object()) {
		throw CityJSONError::InvalidSchema("Extension must be a JSON object");
	}

	ValidateRequiredKeys(obj, {"url", "version"});

	Extension result(obj["url"].get<std::string>(), obj["version"].get<std::string>());

	// Parse extra properties (everything except url and version)
	json extra = json::object();
	for (auto &[key, value] : obj.items()) {
		if (key != "url" && key != "version") {
			extra[key] = value;
		}
	}

	if (!extra.empty()) {
		result.extra_properties = extra;
	}

	return result;
}

// ============================================================
// CityJSONFeature
// ============================================================

CityJSONFeature CityJSONFeature::FromJson(const json &obj) {
	if (!obj.is_object()) {
		throw CityJSONError::InvalidSchema("CityJSONFeature must be a JSON object");
	}

	ValidateRequiredKeys(obj, {"type", "CityObjects"});

	std::string type_str = obj["type"].get<std::string>();
	if (type_str != "CityJSONFeature") {
		throw CityJSONError::InvalidSchema("Expected type 'CityJSONFeature', got '" + type_str + "'");
	}

	CityJSONFeature result;
	result.id = GetString(obj, "id", "");
	result.type = "CityJSONFeature";

	// Parse CityObjects
	const auto &city_objs = obj["CityObjects"];
	if (!city_objs.is_object()) {
		throw CityJSONError::InvalidSchema("CityObjects must be an object");
	}

	for (auto &[obj_id, obj_data] : city_objs.items()) {
		result.city_objects[obj_id] = CityObject::FromJson(obj_data);
	}

	// Parse per-feature local vertex pool (CityJSONSeq format)
	// Geometry boundary indices in this feature reference these local vertices
	if (obj.contains("vertices") && obj["vertices"].is_array()) {
		for (const auto &vertex : obj["vertices"]) {
			if (vertex.is_array() && vertex.size() == 3) {
				result.vertices.push_back({vertex[0].get<double>(), vertex[1].get<double>(), vertex[2].get<double>()});
			}
		}
	}

	return result;
}

json CityJSONFeature::ToJson() const {
	json city_objs = json::object();
	for (const auto &[obj_id, obj_data] : city_objects) {
		city_objs[obj_id] = obj_data.ToJson();
	}

	json result = {{"type", "CityJSONFeature"}, {"CityObjects", city_objs}};

	if (!id.empty()) {
		result["id"] = id;
	}

	return result;
}

// ============================================================
// CityJSON
// ============================================================

CityJSON CityJSON::FromJson(const json &obj) {
	if (!obj.is_object()) {
		throw CityJSONError::InvalidSchema("CityJSON must be a JSON object");
	}

	ValidateRequiredKeys(obj, {"type", "version"});

	std::string type_str = obj["type"].get<std::string>();
	if (type_str != "CityJSON") {
		throw CityJSONError::InvalidSchema("Expected type 'CityJSON', got '" + type_str + "'");
	}

	CityJSON result;
	result.version = obj["version"].get<std::string>();

	// Parse transform
	if (obj.contains("transform") && !obj["transform"].is_null()) {
		result.transform = Transform::FromJson(obj["transform"]);
	}

	// Parse CRS
	if (obj.contains("crs") && !obj["crs"].is_null()) {
		result.crs = CRS::FromJson(obj["crs"]);
	}

	// Parse metadata
	if (obj.contains("metadata") && !obj["metadata"].is_null()) {
		result.metadata = Metadata::FromJson(obj["metadata"]);
	}

	// Parse extensions
	if (obj.contains("extensions") && obj["extensions"].is_object()) {
		for (auto &[ext_name, ext_data] : obj["extensions"].items()) {
			result.extensions[ext_name] = Extension::FromJson(ext_data);
		}
	}

	// Parse vertices (optional)
	if (obj.contains("vertices") && obj["vertices"].is_array()) {
		std::vector<std::array<double, 3>> vertices;
		for (const auto &vertex : obj["vertices"]) {
			if (vertex.is_array() && vertex.size() == 3) {
				vertices.push_back({vertex[0].get<double>(), vertex[1].get<double>(), vertex[2].get<double>()});
			}
		}
		result.vertices = vertices;
	}

	return result;
}

json CityJSON::ToJson() const {
	json result = {{"type", "CityJSON"}, {"version", version}};

	if (transform.has_value()) {
		const auto &t = transform.value();
		result["transform"] = {{"scale", t.scale}, {"translate", t.translate}};
	}

	if (crs.has_value()) {
		result["crs"] = crs->name;
	}

	if (metadata.has_value()) {
		json meta = json::object();
		const auto &m = metadata.value();
		if (m.title)
			meta["title"] = *m.title;
		if (m.identifier)
			meta["identifier"] = *m.identifier;
		if (m.point_of_contact)
			meta["pointOfContact"] = m.point_of_contact->ToJson();
		if (m.reference_date)
			meta["referenceDate"] = *m.reference_date;
		if (m.reference_system)
			meta["referenceSystem"] = *m.reference_system;
		if (m.geographic_location)
			meta["geographicLocation"] = *m.geographic_location;
		if (m.geographic_extent)
			meta["geographicalExtent"] = m.geographic_extent->ToJson();
		if (m.dataset_topic_category)
			meta["datasetTopicCategory"] = *m.dataset_topic_category;
		if (m.feature_type)
			meta["featureType"] = *m.feature_type;
		if (m.metadata_standard)
			meta["metadataStandard"] = *m.metadata_standard;
		if (m.metadata_language)
			meta["metadataLanguage"] = *m.metadata_language;
		if (m.metadata_character_set)
			meta["metadataCharacterSet"] = *m.metadata_character_set;
		if (m.metadata_date)
			meta["metadataDate"] = *m.metadata_date;

		if (!meta.empty()) {
			result["metadata"] = meta;
		}
	}

	if (!extensions.empty()) {
		json exts = json::object();
		for (const auto &[name, ext] : extensions) {
			json ext_obj = {{"url", ext.url}, {"version", ext.version}};
			if (ext.extra_properties) {
				for (auto &[key, value] : ext.extra_properties->items()) {
					ext_obj[key] = value;
				}
			}
			exts[name] = ext_obj;
		}
		result["extensions"] = exts;
	}

	if (vertices.has_value()) {
		json verts = json::array();
		for (const auto &v : vertices.value()) {
			verts.push_back(json::array({v[0], v[1], v[2]}));
		}
		result["vertices"] = verts;
	}

	return result;
}

// ============================================================
// CityJSONFeatureChunk
// ============================================================

std::optional<size_t> CityJSONFeatureChunk::CityObjectCount(size_t chunk_idx) const {
	if (chunk_idx >= chunks.size()) {
		return std::nullopt;
	}

	const Range &range = chunks[chunk_idx];
	size_t count = 0;

	for (size_t i = range.start; i < range.end && i < records.size(); i++) {
		count += records[i].CityObjectCount();
	}

	return count;
}

std::optional<std::span<CityJSONFeature>> CityJSONFeatureChunk::GetChunk(size_t chunk_idx) {
	if (chunk_idx >= chunks.size()) {
		return std::nullopt;
	}

	const Range &range = chunks[chunk_idx];
	if (range.start >= records.size()) {
		return std::nullopt;
	}

	size_t end = std::min(range.end, records.size());
	return std::span<CityJSONFeature>(records.data() + range.start, end - range.start);
}

std::optional<std::span<const CityJSONFeature>> CityJSONFeatureChunk::GetChunk(size_t chunk_idx) const {
	if (chunk_idx >= chunks.size()) {
		return std::nullopt;
	}

	const Range &range = chunks[chunk_idx];
	if (range.start >= records.size()) {
		return std::nullopt;
	}

	size_t end = std::min(range.end, records.size());
	return std::span<const CityJSONFeature>(records.data() + range.start, end - range.start);
}

size_t CityJSONFeatureChunk::TotalCityObjectCount() const {
	size_t total = 0;
	for (const auto &feature : records) {
		total += feature.CityObjectCount();
	}
	return total;
}

CityJSONFeatureChunk CityJSONFeatureChunk::CreateChunks(std::vector<CityJSONFeature> features, size_t chunk_size) {
	CityJSONFeatureChunk result;
	result.records = std::move(features);

	if (result.records.empty()) {
		return result;
	}

	// Divide features into chunks based on CityObject count
	size_t current_count = 0;
	size_t chunk_start = 0;

	for (size_t i = 0; i < result.records.size(); i++) {
		size_t feature_obj_count = result.records[i].CityObjectCount();
		current_count += feature_obj_count;

		// Create chunk when we reach chunk_size or at the last feature
		if (current_count >= chunk_size || i == result.records.size() - 1) {
			result.chunks.emplace_back(chunk_start, i + 1);
			chunk_start = i + 1;
			current_count = 0;
		}
	}

	return result;
}

} // namespace cityjson
} // namespace duckdb
