#include "cityjson/copy_function.hpp"
#include "cityjson/cityjson_writer.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/client_context.hpp"
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
	result->version = version;
	result->crs = crs;
	result->transform = transform;
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
	return file_path == o.file_path && is_seq == o.is_seq;
}

// ============================================================
// Helper: parse metadata_query result
// ============================================================

static void ParseMetadataFromQuery(ClientContext &context, const std::string &query, CityJSONCopyBindData &bind_data) {
	auto result = context.Query(query, false);
	if (result->HasError()) {
		throw BinderException("metadata_query failed: " + result->GetError());
	}

	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		throw BinderException("metadata_query returned no rows");
	}

	// Get column names from the result
	auto &col_names = result->names;

	for (idx_t col = 0; col < col_names.size(); col++) {
		auto &name = col_names[col];
		auto val = chunk->data[col].GetValue(0);

		if (val.IsNull()) continue;

		if (name == "version") {
			bind_data.version = val.ToString();
		} else if (name == "reference_system" || name == "crs") {
			bind_data.crs = val.ToString();
		} else if (name == "transform_scale") {
			// Parse "x,y,z" or JSON array
			auto s = val.ToString();
			// Try as comma-separated
			std::array<double, 3> scale;
			if (sscanf(s.c_str(), "%lf,%lf,%lf", &scale[0], &scale[1], &scale[2]) == 3 ||
			    sscanf(s.c_str(), "[%lf,%lf,%lf]", &scale[0], &scale[1], &scale[2]) == 3) {
				if (!bind_data.transform.has_value()) {
					bind_data.transform = Transform();
				}
				bind_data.transform->scale = scale;
			}
		} else if (name == "transform_translate") {
			auto s = val.ToString();
			std::array<double, 3> translate;
			if (sscanf(s.c_str(), "%lf,%lf,%lf", &translate[0], &translate[1], &translate[2]) == 3 ||
			    sscanf(s.c_str(), "[%lf,%lf,%lf]", &translate[0], &translate[1], &translate[2]) == 3) {
				if (!bind_data.transform.has_value()) {
					bind_data.transform = Transform();
				}
				bind_data.transform->translate = translate;
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
	return make_uniq<CityJSONCopyGlobalState>();
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
			if (bind_data.column_roles[col] == CopyColumnRole::GeometryWKB) {
				auto val = input.data[col].GetValue(row);
				if (!val.IsNull()) {
					// For now, store geometry as a placeholder
					// WKB decoding back to CityJSON boundaries would be complex
					// Instead, if there's geometry_properties with the full geometry JSON, use that
					json geom;
					geom["type"] = "MultiSurface";
					geom["boundaries"] = json::array();

					// Check column name for LOD
					auto &col_name = bind_data.column_names[col];
					if (col_name.size() > 8 && col_name.substr(0, 8) == "geom_lod") {
						geom["lod"] = col_name.substr(8);
					}

					geometries.push_back(geom);
				}
			}
		}

		// Geometry properties (may contain full geometry info)
		if (bind_data.geometry_properties_col != DConstants::INVALID_INDEX) {
			auto val = input.data[bind_data.geometry_properties_col].GetValue(row);
			if (!val.IsNull()) {
				try {
					auto props = json_utils::ParseJson(val.ToString());
					if (props.contains("semantics") && !geometries.empty()) {
						geometries[0]["semantics"] = props["semantics"];
					}
					if (props.contains("material") && !geometries.empty()) {
						geometries[0]["material"] = props["material"];
					}
					if (props.contains("texture") && !geometries.empty()) {
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

	if (bind_data.is_seq) {
		CityJSONWriter::WriteCityJSONSeq(
		    bind_data.file_path,
		    bind_data.version,
		    bind_data.crs,
		    bind_data.transform,
		    gstate.feature_objects,
		    gstate.feature_order);
	} else {
		CityJSONWriter::WriteCityJSON(
		    bind_data.file_path,
		    bind_data.version,
		    bind_data.crs,
		    bind_data.transform,
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

} // namespace cityjson
} // namespace duckdb
