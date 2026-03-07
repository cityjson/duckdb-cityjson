#include "cityjson/copy_function.hpp"
#include "cityjson/cityjson_writer.hpp"
#include "cityjson/wkb_decoder.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {
namespace cityjson {

// json typedef is available from duckdb::cityjson namespace via json_utils.hpp

// ============================================================
// Column role detection
// ============================================================

CopyColumnRole DetectColumnRole(const std::string &name) {
	if (name == "id") return CopyColumnRole::Id;
	if (name == "feature_id") return CopyColumnRole::FeatureId;
	if (name == "object_type") return CopyColumnRole::ObjectType;
	if (name == "children") return CopyColumnRole::Children;
	if (name == "parents") return CopyColumnRole::Parents;
	if (name == "children_roles") return CopyColumnRole::ChildrenRoles;
	if (name == "geometry" || name.substr(0, 8) == "geom_lod") return CopyColumnRole::GeometryWKB;
	if (name == "geometry_properties") return CopyColumnRole::GeometryProperties;
	if (name == "other") return CopyColumnRole::Other;
	return CopyColumnRole::Attribute;
}

// ============================================================
// CityJSONCopyBindData
// ============================================================

unique_ptr<FunctionData> CityJSONCopyBindData::Copy() const {
	auto result = make_uniq<CityJSONCopyBindData>();
	result->file_path = file_path;
	result->is_seq = is_seq;
	result->is_fcb = is_fcb;
	result->version = version;
	result->crs = crs;
	result->transform = transform;
	result->title = title;
	result->identifier = identifier;
	result->reference_date = reference_date;
	result->geographical_extent = geographical_extent;
	result->point_of_contact = point_of_contact;
	result->column_names = column_names;
	result->column_types = column_types;
	result->column_roles = column_roles;
	result->id_col = id_col;
	result->feature_id_col = feature_id_col;
	result->object_type_col = object_type_col;
	result->children_col = children_col;
	result->parents_col = parents_col;
	result->children_roles_col = children_roles_col;
	result->geometry_col = geometry_col;
	result->geometry_properties_col = geometry_properties_col;
	return std::move(result);
}

bool CityJSONCopyBindData::Equals(const FunctionData &other) const {
	auto &o = other.Cast<CityJSONCopyBindData>();
	return file_path == o.file_path && is_seq == o.is_seq && is_fcb == o.is_fcb;
}

// ============================================================
// Helper: parse metadata_query result
// ============================================================

static void ParseMetadataFromQuery(ClientContext &context, const std::string &query, CityJSONCopyBindData &bind_data) {
	// Use a separate connection to avoid deadlock — the COPY bind already holds a lock
	// on the current connection, so running a nested query on it would deadlock.
	Connection conn(*context.db);
	auto result = conn.Query(query);
	if (result->HasError()) {
		throw BinderException("metadata_query failed: " + result->GetError());
	}

	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		throw BinderException("metadata_query returned no rows");
	}

	// Get column names from the result
	auto &col_names = result->names;

	// Helper to extract {x, y, z} struct into array<double, 3>
	auto extract_xyz_struct = [](const Value &v) -> std::optional<std::array<double, 3>> {
		if (v.IsNull() || v.type().id() != LogicalTypeId::STRUCT) return std::nullopt;
		auto &children = StructValue::GetChildren(v);
		if (children.size() < 3) return std::nullopt;
		// Fields are x, y, z
		if (children[0].IsNull() || children[1].IsNull() || children[2].IsNull()) return std::nullopt;
		return std::array<double, 3>{
		    children[0].GetValue<double>(),
		    children[1].GetValue<double>(),
		    children[2].GetValue<double>()
		};
	};

	for (idx_t col = 0; col < col_names.size(); col++) {
		auto &name = col_names[col];
		auto val = chunk->data[col].GetValue(0);

		if (val.IsNull()) continue;

		if (name == "version") {
			bind_data.version = val.ToString();
		} else if (name == "title") {
			bind_data.title = val.ToString();
		} else if (name == "identifier") {
			bind_data.identifier = val.ToString();
		} else if (name == "reference_date") {
			bind_data.reference_date = val.ToString();
		} else if (name == "reference_system" || name == "crs") {
			// Handle STRUCT {base_url, authority, version, code} or plain string
			if (val.type().id() == LogicalTypeId::STRUCT) {
				auto &children = StructValue::GetChildren(val);
				// Reconstruct CRS URI: base_url + authority/version/code
				std::string base_url = children.size() > 0 && !children[0].IsNull() ? children[0].ToString() : "";
				std::string authority = children.size() > 1 && !children[1].IsNull() ? children[1].ToString() : "";
				std::string version_str = children.size() > 2 && !children[2].IsNull() ? children[2].ToString() : "";
				std::string code = children.size() > 3 && !children[3].IsNull() ? children[3].ToString() : "";
				if (!base_url.empty() && !authority.empty()) {
					bind_data.crs = base_url + authority + "/" + version_str + "/" + code;
				} else if (!authority.empty() && !code.empty()) {
					bind_data.crs = "https://www.opengis.net/def/crs/" + authority + "/" + version_str + "/" + code;
				}
			} else {
				bind_data.crs = val.ToString();
			}
		} else if (name == "transform_scale") {
			if (val.type().id() == LogicalTypeId::STRUCT) {
				auto parsed = extract_xyz_struct(val);
				if (parsed.has_value()) {
					if (!bind_data.transform.has_value()) bind_data.transform = Transform();
					bind_data.transform->scale = parsed.value();
				}
			} else {
				auto s = val.ToString();
				std::array<double, 3> scale;
				if (sscanf(s.c_str(), "%lf,%lf,%lf", &scale[0], &scale[1], &scale[2]) == 3 ||
				    sscanf(s.c_str(), "[%lf,%lf,%lf]", &scale[0], &scale[1], &scale[2]) == 3) {
					if (!bind_data.transform.has_value()) bind_data.transform = Transform();
					bind_data.transform->scale = scale;
				}
			}
		} else if (name == "transform_translate") {
			if (val.type().id() == LogicalTypeId::STRUCT) {
				auto parsed = extract_xyz_struct(val);
				if (parsed.has_value()) {
					if (!bind_data.transform.has_value()) bind_data.transform = Transform();
					bind_data.transform->translate = parsed.value();
				}
			} else {
				auto s = val.ToString();
				std::array<double, 3> translate;
				if (sscanf(s.c_str(), "%lf,%lf,%lf", &translate[0], &translate[1], &translate[2]) == 3 ||
				    sscanf(s.c_str(), "[%lf,%lf,%lf]", &translate[0], &translate[1], &translate[2]) == 3) {
					if (!bind_data.transform.has_value()) bind_data.transform = Transform();
					bind_data.transform->translate = translate;
				}
			}
		} else if (name == "geographical_extent") {
			if (val.type().id() == LogicalTypeId::STRUCT) {
				auto &children = StructValue::GetChildren(val);
				if (children.size() >= 6 &&
				    !children[0].IsNull() && !children[1].IsNull() && !children[2].IsNull() &&
				    !children[3].IsNull() && !children[4].IsNull() && !children[5].IsNull()) {
					bind_data.geographical_extent = GeographicalExtent(
					    children[0].GetValue<double>(), children[1].GetValue<double>(),
					    children[2].GetValue<double>(), children[3].GetValue<double>(),
					    children[4].GetValue<double>(), children[5].GetValue<double>());
				}
			}
		} else if (name == "point_of_contact") {
			if (val.type().id() == LogicalTypeId::STRUCT) {
				auto &children = StructValue::GetChildren(val);
				// Fields: contact_name, email_address, contact_type, role, phone, website, address
				if (children.size() >= 2 && !children[0].IsNull() && !children[1].IsNull()) {
					PointOfContact poc(children[0].ToString(), children[1].ToString());
					if (children.size() > 2 && !children[2].IsNull()) poc.contact_type = children[2].ToString();
					if (children.size() > 3 && !children[3].IsNull()) poc.role = children[3].ToString();
					if (children.size() > 4 && !children[4].IsNull()) poc.phone = children[4].ToString();
					if (children.size() > 5 && !children[5].IsNull()) poc.website = children[5].ToString();
					bind_data.point_of_contact = poc;
				}
			}
		}
	}
}

// ============================================================
// Helper: parse comma-separated doubles
// ============================================================

static std::optional<std::array<double, 3>> ParseDoubleTriple(const std::string &s) {
	std::array<double, 3> result;
	if (sscanf(s.c_str(), "%lf,%lf,%lf", &result[0], &result[1], &result[2]) == 3) {
		return result;
	}
	return std::nullopt;
}

// ============================================================
// COPY TO Bind (shared between cityjson and cityjsonseq)
// ============================================================

static unique_ptr<FunctionData> CityJSONCopyToBind(ClientContext &context, CopyFunctionBindInput &input,
                                                    const vector<string> &names,
                                                    const vector<LogicalType> &sql_types) {
	auto bind_data = make_uniq<CityJSONCopyBindData>();
	bind_data->file_path = input.info.file_path;
	bind_data->is_seq = (input.info.format == "cityjsonseq");
	bind_data->is_fcb = (input.info.format == "flatcitybuf");

	// Parse options
	for (auto &option : input.info.options) {
		auto loption = StringUtil::Lower(option.first);
		if (option.second.empty()) continue;

		auto &val = option.second[0];

		if (loption == "version") {
			bind_data->version = val.ToString();
		} else if (loption == "crs") {
			bind_data->crs = val.ToString();
		} else if (loption == "metadata_query") {
			ParseMetadataFromQuery(context, val.ToString(), *bind_data);
		} else if (loption == "transform_scale") {
			auto parsed = ParseDoubleTriple(val.ToString());
			if (parsed.has_value()) {
				if (!bind_data->transform.has_value()) {
					bind_data->transform = Transform();
				}
				bind_data->transform->scale = parsed.value();
			}
		} else if (loption == "transform_translate") {
			auto parsed = ParseDoubleTriple(val.ToString());
			if (parsed.has_value()) {
				if (!bind_data->transform.has_value()) {
					bind_data->transform = Transform();
				}
				bind_data->transform->translate = parsed.value();
			}
		}
	}

	// Map columns to roles
	bind_data->column_names = names;
	bind_data->column_types.assign(sql_types.begin(), sql_types.end());

	for (idx_t i = 0; i < names.size(); i++) {
		auto role = DetectColumnRole(names[i]);
		bind_data->column_roles.push_back(role);

		switch (role) {
		case CopyColumnRole::Id:
			bind_data->id_col = i;
			break;
		case CopyColumnRole::FeatureId:
			bind_data->feature_id_col = i;
			break;
		case CopyColumnRole::ObjectType:
			bind_data->object_type_col = i;
			break;
		case CopyColumnRole::Children:
			bind_data->children_col = i;
			break;
		case CopyColumnRole::Parents:
			bind_data->parents_col = i;
			break;
		case CopyColumnRole::ChildrenRoles:
			bind_data->children_roles_col = i;
			break;
		case CopyColumnRole::GeometryWKB:
			bind_data->geometry_col = i;
			break;
		case CopyColumnRole::GeometryProperties:
			bind_data->geometry_properties_col = i;
			break;
		default:
			break;
		}
	}

	// Validate mandatory columns
	if (bind_data->id_col == DConstants::INVALID_INDEX) {
		throw BinderException("COPY TO cityjson requires an 'id' column");
	}
	if (bind_data->feature_id_col == DConstants::INVALID_INDEX) {
		throw BinderException("COPY TO cityjson requires a 'feature_id' column");
	}
	if (bind_data->object_type_col == DConstants::INVALID_INDEX) {
		throw BinderException("COPY TO cityjson requires an 'object_type' column");
	}

	return std::move(bind_data);
}

// ============================================================
// COPY TO Initialize Global
// ============================================================

static unique_ptr<GlobalFunctionData> CityJSONCopyToInitGlobal(ClientContext &context, FunctionData &bind_data,
                                                                 const string &file_path) {
	auto gstate = make_uniq<CityJSONCopyGlobalState>();
	gstate->temp_file_path = file_path;
	return std::move(gstate);
}

// ============================================================
// COPY TO Initialize Local
// ============================================================

static unique_ptr<LocalFunctionData> CityJSONCopyToInitLocal(ExecutionContext &context, FunctionData &bind_data) {
	return make_uniq<CityJSONCopyLocalState>();
}

// ============================================================
// Helper: convert DuckDB Value to JSON
// ============================================================

static json ValueToJson(const Value &val) {
	if (val.IsNull()) {
		return json();
	}

	auto &type = val.type();
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return json(BooleanValue::Get(val));
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
		return json(val.GetValue<int64_t>());
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		return json(val.GetValue<double>());
	case LogicalTypeId::VARCHAR:
		return json(StringValue::Get(val));
	default:
		// For complex types, try to convert to string
		return json(val.ToString());
	}
}

// Helper: parse JSON string array from a DuckDB list/varchar value
static json ParseJsonArrayValue(const Value &val) {
	if (val.IsNull()) return json::array();

	auto str = val.ToString();
	try {
		auto parsed = json_utils::ParseJson(str);
		if (parsed.is_array()) return parsed;
	} catch (...) {
	}

	// Try as DuckDB list
	return json::array();
}

// ============================================================
// COPY TO Sink
// ============================================================

static void CityJSONCopyToSink(ExecutionContext &context, FunctionData &bind_data_p,
                                 GlobalFunctionData &gstate_p, LocalFunctionData &lstate_p,
                                 DataChunk &input) {
	auto &bind_data = bind_data_p.Cast<CityJSONCopyBindData>();
	auto &lstate = lstate_p.Cast<CityJSONCopyLocalState>();

	for (idx_t row = 0; row < input.size(); row++) {
		// Extract key columns
		auto id_val = input.data[bind_data.id_col].GetValue(row);
		auto feature_id_val = input.data[bind_data.feature_id_col].GetValue(row);
		auto object_type_val = input.data[bind_data.object_type_col].GetValue(row);

		if (id_val.IsNull() || feature_id_val.IsNull() || object_type_val.IsNull()) {
			continue; // Skip rows with null key columns
		}

		std::string city_obj_id = id_val.ToString();
		std::string feature_id = feature_id_val.ToString();
		std::string object_type = object_type_val.ToString();

		// Build CityObject JSON
		json city_obj;
		city_obj["type"] = object_type;

		// Children
		if (bind_data.children_col != DConstants::INVALID_INDEX) {
			auto val = input.data[bind_data.children_col].GetValue(row);
			if (!val.IsNull()) {
				city_obj["children"] = ParseJsonArrayValue(val);
			}
		}

		// Parents
		if (bind_data.parents_col != DConstants::INVALID_INDEX) {
			auto val = input.data[bind_data.parents_col].GetValue(row);
			if (!val.IsNull()) {
				city_obj["parents"] = ParseJsonArrayValue(val);
			}
		}

		// Children roles
		if (bind_data.children_roles_col != DConstants::INVALID_INDEX) {
			auto val = input.data[bind_data.children_roles_col].GetValue(row);
			if (!val.IsNull()) {
				city_obj["children_roles"] = ParseJsonArrayValue(val);
			}
		}

		// Geometry columns (geom_lod* or geometry)
		json geometries = json::array();
		for (idx_t col = 0; col < bind_data.column_roles.size(); col++) {
			if (bind_data.column_roles[col] != CopyColumnRole::GeometryWKB) {
				continue;
			}
			auto val = input.data[col].GetValue(row);
			if (val.IsNull()) {
				continue;
			}

			auto &col_type = bind_data.column_types[col];
			auto &col_name = bind_data.column_names[col];

			if (col_type.id() == LogicalTypeId::STRUCT) {
				// Non-LOD STRUCT geometry: {lod, type, boundaries, semantics, material, texture}
				auto &children = StructValue::GetChildren(val);
				auto &struct_type = StructType::GetChildTypes(col_type);

				json geom;
				for (idx_t c = 0; c < struct_type.size(); c++) {
					auto &field_name = struct_type[c].first;
					auto &child_val = children[c];
					if (child_val.IsNull()) continue;

					if (field_name == "type") {
						geom["type"] = child_val.ToString();
					} else if (field_name == "lod") {
						geom["lod"] = child_val.ToString();
					} else if (field_name == "boundaries") {
						try {
							geom["boundaries"] = json_utils::ParseJson(child_val.ToString());
						} catch (...) {
							geom["boundaries"] = json::array();
						}
					} else if (field_name == "semantics") {
						try {
							geom["semantics"] = json_utils::ParseJson(child_val.ToString());
						} catch (...) {}
					} else if (field_name == "material") {
						try {
							geom["material"] = json_utils::ParseJson(child_val.ToString());
						} catch (...) {}
					} else if (field_name == "texture") {
						try {
							geom["texture"] = json_utils::ParseJson(child_val.ToString());
						} catch (...) {}
					}
				}
				if (!geom.contains("type")) {
					geom["type"] = "MultiSurface";
				}
				if (!geom.contains("boundaries")) {
					geom["boundaries"] = json::array();
				}
				geometries.push_back(geom);

			} else if (col_type.id() == LogicalTypeId::BLOB) {
				// WKB BLOB geometry: decode back to CityJSON boundaries
				auto blob_str = val.GetValueUnsafe<string_t>();
				auto decoded = WKBDecoder::Decode(
				    reinterpret_cast<const uint8_t *>(blob_str.GetData()),
				    blob_str.GetSize());

				json geom;
				geom["type"] = decoded.cityjson_type;
				geom["boundaries"] = decoded.boundaries;

				// Extract LOD from column name (geom_lod2_2 → "2.2")
				if (col_name.size() > 8 && col_name.substr(0, 8) == "geom_lod") {
					std::string lod = col_name.substr(8);
					// Replace underscores with dots for LOD (e.g., "2_2" → "2.2")
					std::replace(lod.begin(), lod.end(), '_', '.');
					geom["lod"] = lod;
				}

				geometries.push_back(geom);
			}
		}

		// Geometry properties (may contain LOD, semantics, material, texture, cityjsonType)
		if (bind_data.geometry_properties_col != DConstants::INVALID_INDEX) {
			auto val = input.data[bind_data.geometry_properties_col].GetValue(row);
			if (!val.IsNull() && !geometries.empty()) {
				try {
					auto props = json_utils::ParseJson(val.ToString());
					// Extract LOD if not already set from column name
					if (!geometries[0].contains("lod") && props.contains("lod")) {
						geometries[0]["lod"] = props["lod"].get<std::string>();
					}
					// Use cityjsonType if available (more precise than WKB-inferred type)
					if (props.contains("cityjsonType")) {
						geometries[0]["type"] = props["cityjsonType"].get<std::string>();
					}
					if (props.contains("semantics")) {
						geometries[0]["semantics"] = props["semantics"];
					}
					if (props.contains("material")) {
						geometries[0]["material"] = props["material"];
					}
					if (props.contains("texture")) {
						geometries[0]["texture"] = props["texture"];
					}
				} catch (...) {
					// Ignore parse errors in geometry properties
				}
			}
		}

		if (!geometries.empty()) {
			city_obj["geometry"] = geometries;
		} else {
			city_obj["geometry"] = json::array();
		}

		// Attributes (all non-reserved columns)
		json attributes = json::object();
		for (idx_t col = 0; col < bind_data.column_roles.size(); col++) {
			if (bind_data.column_roles[col] == CopyColumnRole::Attribute) {
				auto val = input.data[col].GetValue(row);
				if (!val.IsNull()) {
					attributes[bind_data.column_names[col]] = ValueToJson(val);
				}
			}
		}
		if (!attributes.empty()) {
			city_obj["attributes"] = attributes;
		}

		// Add to local buffer
		if (lstate.local_objects.find(feature_id) == lstate.local_objects.end()) {
			lstate.local_feature_order.push_back(feature_id);
		}
		lstate.local_objects[feature_id].emplace_back(city_obj_id, std::move(city_obj));
	}
}

// ============================================================
// COPY TO Combine
// ============================================================

static void CityJSONCopyToCombine(ExecutionContext &context, FunctionData &bind_data_p,
                                    GlobalFunctionData &gstate_p, LocalFunctionData &lstate_p) {
	auto &gstate = gstate_p.Cast<CityJSONCopyGlobalState>();
	auto &lstate = lstate_p.Cast<CityJSONCopyLocalState>();

	std::lock_guard<std::mutex> lock(gstate.mutex);

	for (const auto &fid : lstate.local_feature_order) {
		if (gstate.feature_objects.find(fid) == gstate.feature_objects.end()) {
			gstate.feature_order.push_back(fid);
		}

		auto &global_objs = gstate.feature_objects[fid];
		auto &local_objs = lstate.local_objects[fid];
		global_objs.insert(global_objs.end(),
		                   std::make_move_iterator(local_objs.begin()),
		                   std::make_move_iterator(local_objs.end()));
	}

	lstate.local_objects.clear();
	lstate.local_feature_order.clear();
}

// ============================================================
// COPY TO Finalize
// ============================================================

static void CityJSONCopyToFinalize(ClientContext &context, FunctionData &bind_data_p,
                                     GlobalFunctionData &gstate_p) {
	auto &bind_data = bind_data_p.Cast<CityJSONCopyBindData>();
	auto &gstate = gstate_p.Cast<CityJSONCopyGlobalState>();

	// Build write metadata from bind data
	CityJSONWriteMetadata write_meta;
	write_meta.version = bind_data.version;
	write_meta.crs = bind_data.crs;
	write_meta.transform = bind_data.transform;
	write_meta.title = bind_data.title;
	write_meta.identifier = bind_data.identifier;
	write_meta.reference_date = bind_data.reference_date;
	write_meta.geographical_extent = bind_data.geographical_extent;
	write_meta.point_of_contact = bind_data.point_of_contact;

	// Write to the temp file path — DuckDB will rename it to the final path after Finalize
	auto &output_path = gstate.temp_file_path;

	if (bind_data.is_seq) {
		CityJSONWriter::WriteCityJSONSeq(
		    output_path,
		    write_meta,
		    gstate.feature_objects,
		    gstate.feature_order);
#ifdef CITYJSON_HAS_FCB
	} else if (bind_data.is_fcb) {
		CityJSONWriter::WriteFlatCityBuf(
		    output_path,
		    write_meta,
		    gstate.feature_objects,
		    gstate.feature_order);
#endif
	} else {
		CityJSONWriter::WriteCityJSON(
		    output_path,
		    write_meta,
		    gstate.feature_objects,
		    gstate.feature_order);
	}
}

// ============================================================
// Registration
// ============================================================

void RegisterCityJSONCopyFunction(ExtensionLoader &loader) {
	CopyFunction function("cityjson");
	function.extension = "city.json";
	function.copy_to_bind = CityJSONCopyToBind;
	function.copy_to_initialize_global = CityJSONCopyToInitGlobal;
	function.copy_to_initialize_local = CityJSONCopyToInitLocal;
	function.copy_to_sink = CityJSONCopyToSink;
	function.copy_to_combine = CityJSONCopyToCombine;
	function.copy_to_finalize = CityJSONCopyToFinalize;
	loader.RegisterFunction(function);
}

void RegisterCityJSONSeqCopyFunction(ExtensionLoader &loader) {
	CopyFunction function("cityjsonseq");
	function.extension = "city.jsonl";
	function.copy_to_bind = CityJSONCopyToBind;
	function.copy_to_initialize_global = CityJSONCopyToInitGlobal;
	function.copy_to_initialize_local = CityJSONCopyToInitLocal;
	function.copy_to_sink = CityJSONCopyToSink;
	function.copy_to_combine = CityJSONCopyToCombine;
	function.copy_to_finalize = CityJSONCopyToFinalize;
	loader.RegisterFunction(function);
}

#ifdef CITYJSON_HAS_FCB
void RegisterFlatCityBufCopyFunction(ExtensionLoader &loader) {
	CopyFunction function("flatcitybuf");
	function.extension = "fcb";
	function.copy_to_bind = CityJSONCopyToBind;
	function.copy_to_initialize_global = CityJSONCopyToInitGlobal;
	function.copy_to_initialize_local = CityJSONCopyToInitLocal;
	function.copy_to_sink = CityJSONCopyToSink;
	function.copy_to_combine = CityJSONCopyToCombine;
	function.copy_to_finalize = CityJSONCopyToFinalize;
	loader.RegisterFunction(function);
}
#endif

} // namespace cityjson
} // namespace duckdb
