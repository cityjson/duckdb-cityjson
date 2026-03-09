#include "cityjson/metadata_table.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {
namespace cityjson {

LogicalType MetadataTableUtils::GetTransformStructType() {
	child_list_t<LogicalType> struct_children;
	struct_children.push_back({"x", LogicalType::DOUBLE});
	struct_children.push_back({"y", LogicalType::DOUBLE});
	struct_children.push_back({"z", LogicalType::DOUBLE});
	return LogicalType::STRUCT(std::move(struct_children));
}

LogicalType MetadataTableUtils::GetGeographicalExtentStructType() {
	child_list_t<LogicalType> struct_children;
	struct_children.push_back({"min_x", LogicalType::DOUBLE});
	struct_children.push_back({"min_y", LogicalType::DOUBLE});
	struct_children.push_back({"min_z", LogicalType::DOUBLE});
	struct_children.push_back({"max_x", LogicalType::DOUBLE});
	struct_children.push_back({"max_y", LogicalType::DOUBLE});
	struct_children.push_back({"max_z", LogicalType::DOUBLE});
	return LogicalType::STRUCT(std::move(struct_children));
}

LogicalType MetadataTableUtils::GetAddressStructType() {
	child_list_t<LogicalType> struct_children;
	struct_children.push_back({"thoroughfare_number", LogicalType::BIGINT});
	struct_children.push_back({"thoroughfare_name", LogicalType::VARCHAR});
	struct_children.push_back({"locality", LogicalType::VARCHAR});
	struct_children.push_back({"postal_code", LogicalType::VARCHAR});
	struct_children.push_back({"country", LogicalType::VARCHAR});
	return LogicalType::STRUCT(std::move(struct_children));
}

LogicalType MetadataTableUtils::GetReferenceSystemStructType() {
	child_list_t<LogicalType> struct_children;
	struct_children.push_back({"base_url", LogicalType::VARCHAR});
	struct_children.push_back({"authority", LogicalType::VARCHAR});
	struct_children.push_back({"version", LogicalType::VARCHAR});
	struct_children.push_back({"code", LogicalType::VARCHAR});
	return LogicalType::STRUCT(std::move(struct_children));
}

LogicalType MetadataTableUtils::GetPointOfContactStructType() {
	child_list_t<LogicalType> struct_children;
	struct_children.push_back({"contact_name", LogicalType::VARCHAR});
	struct_children.push_back({"email_address", LogicalType::VARCHAR});
	struct_children.push_back({"contact_type", LogicalType::VARCHAR});
	struct_children.push_back({"role", LogicalType::VARCHAR});
	struct_children.push_back({"phone", LogicalType::VARCHAR});
	struct_children.push_back({"website", LogicalType::VARCHAR});
	struct_children.push_back({"address", GetAddressStructType()});
	return LogicalType::STRUCT(std::move(struct_children));
}

vector<LogicalType> MetadataTableUtils::GetMetadataTableTypes() {
	vector<LogicalType> types;
	types.push_back(LogicalType::INTEGER);              // id
	types.push_back(LogicalType::VARCHAR);              // version
	types.push_back(LogicalType::VARCHAR);              // identifier
	types.push_back(LogicalType::VARCHAR);              // title
	types.push_back(LogicalType::DATE);                 // reference_date
	types.push_back(GetTransformStructType());          // transform_scale
	types.push_back(GetTransformStructType());          // transform_translate
	types.push_back(GetGeographicalExtentStructType()); // geographical_extent
	types.push_back(GetReferenceSystemStructType());    // reference_system
	types.push_back(GetPointOfContactStructType());     // point_of_contact
	types.push_back(LogicalType::BIGINT);               // city_objects_count
	return types;
}

vector<string> MetadataTableUtils::GetMetadataTableNames() {
	vector<string> names;
	names.push_back("id");
	names.push_back("version");
	names.push_back("identifier");
	names.push_back("title");
	names.push_back("reference_date");
	names.push_back("transform_scale");
	names.push_back("transform_translate");
	names.push_back("geographical_extent");
	names.push_back("reference_system");
	names.push_back("point_of_contact");
	names.push_back("city_objects_count");
	return names;
}

// Helper to create a transform struct value
static Value CreateTransformValue(const std::optional<Transform> &transform, bool is_scale) {
	if (!transform.has_value()) {
		return Value(MetadataTableUtils::GetTransformStructType());
	}

	child_list_t<Value> children;
	const auto &arr = is_scale ? transform->scale : transform->translate;
	children.push_back({"x", Value::DOUBLE(arr[0])});
	children.push_back({"y", Value::DOUBLE(arr[1])});
	children.push_back({"z", Value::DOUBLE(arr[2])});
	return Value::STRUCT(std::move(children));
}

// Helper to parse a CRS URI string into reference system struct components
static Value ParseCRSUri(const std::string &name) {
	child_list_t<Value> children;
	std::string base_url = "";
	std::string authority = "";
	std::string version = "";
	std::string code = "";

	// Try to parse OGC-style URL: "https://www.opengis.net/def/crs/EPSG/0/7415"
	if (name.find("opengis.net/def/crs/") != std::string::npos) {
		size_t pos = name.find("/def/crs/");
		if (pos != std::string::npos) {
			base_url = name.substr(0, pos + 9);
			std::string rest = name.substr(pos + 9);
			size_t slash1 = rest.find('/');
			if (slash1 != std::string::npos) {
				authority = rest.substr(0, slash1);
				size_t slash2 = rest.find('/', slash1 + 1);
				if (slash2 != std::string::npos) {
					version = rest.substr(slash1 + 1, slash2 - slash1 - 1);
					code = rest.substr(slash2 + 1);
				}
			}
		}
	}

	children.push_back({"base_url", base_url.empty() ? Value() : Value(base_url)});
	children.push_back({"authority", authority.empty() ? Value() : Value(authority)});
	children.push_back({"version", version.empty() ? Value() : Value(version)});
	children.push_back({"code", code.empty() ? Value() : Value(code)});
	return Value::STRUCT(std::move(children));
}

// Helper to create a reference system struct value
// Uses top-level CRS if available, falls back to metadata.referenceSystem URI
static Value CreateReferenceSystemValue(const std::optional<CRS> &crs, const std::optional<Metadata> &metadata) {
	// First try top-level CRS
	if (crs.has_value()) {
		const auto &name = crs->name;
		if (name.find("opengis.net/def/crs/") != std::string::npos) {
			return ParseCRSUri(name);
		}

		// CRS with authority/code fields
		if (crs->authority.has_value()) {
			child_list_t<Value> children;
			children.push_back({"base_url", Value()});
			children.push_back({"authority", Value(crs->authority.value())});
			children.push_back({"version", Value()});
			children.push_back({"code", crs->code.has_value() ? Value(crs->code.value()) : Value()});
			return Value::STRUCT(std::move(children));
		}

		// Plain CRS name string — try to parse as URI
		if (!name.empty()) {
			return ParseCRSUri(name);
		}
	}

	// Fall back to metadata.referenceSystem (CityJSON 2.0 style URI string)
	if (metadata.has_value() && metadata->reference_system.has_value()) {
		return ParseCRSUri(metadata->reference_system.value());
	}

	return Value(MetadataTableUtils::GetReferenceSystemStructType());
}

// Helper to create an address struct value
static Value CreateAddressValue(const std::optional<json> &address_json) {
	child_list_t<Value> children;

	if (!address_json.has_value() || address_json->is_null()) {
		children.push_back({"thoroughfare_number", Value()});
		children.push_back({"thoroughfare_name", Value()});
		children.push_back({"locality", Value()});
		children.push_back({"postal_code", Value()});
		children.push_back({"country", Value()});
	} else {
		const auto &addr = address_json.value();
		children.push_back({"thoroughfare_number", addr.contains("thoroughfareNumber")
		                                               ? Value::BIGINT(addr["thoroughfareNumber"].get<int64_t>())
		                                               : Value()});
		children.push_back({"thoroughfare_name", addr.contains("thoroughfareName")
		                                             ? Value(addr["thoroughfareName"].get<std::string>())
		                                             : Value()});
		children.push_back(
		    {"locality", addr.contains("locality") ? Value(addr["locality"].get<std::string>()) : Value()});
		children.push_back(
		    {"postal_code", addr.contains("postalCode") ? Value(addr["postalCode"].get<std::string>()) : Value()});
		children.push_back({"country", addr.contains("country") ? Value(addr["country"].get<std::string>()) : Value()});
	}
	return Value::STRUCT(std::move(children));
}

// Helper to create a point of contact struct value
static Value CreatePointOfContactValue(const std::optional<PointOfContact> &poc) {
	if (!poc.has_value()) {
		return Value(MetadataTableUtils::GetPointOfContactStructType());
	}

	child_list_t<Value> children;
	children.push_back({"contact_name", Value(poc->contact_name)});
	children.push_back({"email_address", Value(poc->email_address)});
	children.push_back({"contact_type", poc->contact_type.has_value() ? Value(poc->contact_type.value()) : Value()});
	children.push_back({"role", poc->role.has_value() ? Value(poc->role.value()) : Value()});
	children.push_back({"phone", poc->phone.has_value() ? Value(poc->phone.value()) : Value()});
	children.push_back({"website", poc->website.has_value() ? Value(poc->website.value()) : Value()});
	children.push_back({"address", CreateAddressValue(poc->address)});
	return Value::STRUCT(std::move(children));
}

unique_ptr<DataChunk> MetadataTableUtils::CreateMetadataChunk(const CityJSON &cityjson, idx_t city_objects_count) {
	auto types = GetMetadataTableTypes();
	auto chunk = make_uniq<DataChunk>();
	chunk->Initialize(Allocator::DefaultAllocator(), types);

	// Create a single row
	chunk->SetCardinality(1);

	// id = 1
	chunk->data[0].SetValue(0, Value::INTEGER(1));

	// version
	chunk->data[1].SetValue(0, Value(cityjson.version));

	// identifier
	if (cityjson.metadata.has_value() && cityjson.metadata->identifier.has_value()) {
		chunk->data[2].SetValue(0, Value(cityjson.metadata->identifier.value()));
	} else {
		chunk->data[2].SetValue(0, Value());
	}

	// title
	if (cityjson.metadata.has_value() && cityjson.metadata->title.has_value()) {
		chunk->data[3].SetValue(0, Value(cityjson.metadata->title.value()));
	} else {
		chunk->data[3].SetValue(0, Value());
	}

	// reference_date
	if (cityjson.metadata.has_value() && cityjson.metadata->reference_date.has_value()) {
		// Parse date string to DATE type
		try {
			chunk->data[4].SetValue(0,
			                        Value(cityjson.metadata->reference_date.value()).DefaultCastAs(LogicalType::DATE));
		} catch (...) {
			chunk->data[4].SetValue(0, Value());
		}
	} else {
		chunk->data[4].SetValue(0, Value());
	}

	// transform_scale
	chunk->data[5].SetValue(0, CreateTransformValue(cityjson.transform, true));

	// transform_translate
	chunk->data[6].SetValue(0, CreateTransformValue(cityjson.transform, false));

	// geographical_extent — from metadata if available
	if (cityjson.metadata.has_value() && cityjson.metadata->geographic_extent.has_value()) {
		const auto &ext = cityjson.metadata->geographic_extent.value();
		child_list_t<Value> extent_children;
		extent_children.push_back({"min_x", Value::DOUBLE(ext.min_x)});
		extent_children.push_back({"min_y", Value::DOUBLE(ext.min_y)});
		extent_children.push_back({"min_z", Value::DOUBLE(ext.min_z)});
		extent_children.push_back({"max_x", Value::DOUBLE(ext.max_x)});
		extent_children.push_back({"max_y", Value::DOUBLE(ext.max_y)});
		extent_children.push_back({"max_z", Value::DOUBLE(ext.max_z)});
		chunk->data[7].SetValue(0, Value::STRUCT(std::move(extent_children)));
	} else {
		chunk->data[7].SetValue(0, Value(GetGeographicalExtentStructType()));
	}

	// reference_system — uses top-level CRS with fallback to metadata.referenceSystem
	chunk->data[8].SetValue(0, CreateReferenceSystemValue(cityjson.crs, cityjson.metadata));

	// point_of_contact
	if (cityjson.metadata.has_value()) {
		chunk->data[9].SetValue(0, CreatePointOfContactValue(cityjson.metadata->point_of_contact));
	} else {
		chunk->data[9].SetValue(0, Value(GetPointOfContactStructType()));
	}

	// city_objects_count
	chunk->data[10].SetValue(0, Value::BIGINT(static_cast<int64_t>(city_objects_count)));

	return chunk;
}

string MetadataTableUtils::GetCreateTableSQL(const string &table_name) {
	return "CREATE TABLE IF NOT EXISTS " + table_name +
	       " ("
	       "id INTEGER PRIMARY KEY, "
	       "version VARCHAR NOT NULL, "
	       "identifier VARCHAR, "
	       "title VARCHAR, "
	       "reference_date DATE, "
	       "transform_scale STRUCT(x DOUBLE, y DOUBLE, z DOUBLE), "
	       "transform_translate STRUCT(x DOUBLE, y DOUBLE, z DOUBLE), "
	       "geographical_extent STRUCT(min_x DOUBLE, min_y DOUBLE, min_z DOUBLE, max_x DOUBLE, max_y DOUBLE, max_z "
	       "DOUBLE), "
	       "reference_system STRUCT(base_url VARCHAR, authority VARCHAR, version VARCHAR, code VARCHAR), "
	       "point_of_contact STRUCT(contact_name VARCHAR, email_address VARCHAR, contact_type VARCHAR, role VARCHAR, "
	       "phone VARCHAR, website VARCHAR, address STRUCT(thoroughfare_number BIGINT, thoroughfare_name VARCHAR, "
	       "locality VARCHAR, postal_code VARCHAR, country VARCHAR)), "
	       "city_objects_count BIGINT"
	       ")";
}

} // namespace cityjson
} // namespace duckdb
