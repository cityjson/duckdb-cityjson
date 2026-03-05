#pragma once

#include "cityjson/cityjson_types.hpp"
#include "cityjson/json_utils.hpp"
#include "duckdb.hpp"
#include "duckdb/function/copy_function.hpp"
#include <string>
#include <vector>
#include <map>
#include <mutex>
#include <optional>

namespace duckdb {

class ExtensionLoader;

namespace cityjson {

// ============================================================
// Column role detection for COPY TO
// ============================================================

enum class CopyColumnRole {
	Id,                 // CityObject ID
	FeatureId,          // Feature grouping key
	ObjectType,         // CityObject type
	Children,           // children array
	Parents,            // parents array
	ChildrenRoles,      // children_roles array
	GeometryWKB,        // geometry (WKB blob)
	GeometryProperties, // geometry_properties JSON
	Other,              // extension fields
	Attribute           // everything else -> attributes map
};

CopyColumnRole DetectColumnRole(const std::string &name);

// ============================================================
// Bind data for COPY TO
// ============================================================

struct CityJSONCopyBindData : public FunctionData {
	std::string file_path;
	bool is_seq = false;      // true for cityjsonseq format

	// Metadata (from options or metadata_query)
	std::string version = "2.0";
	std::optional<std::string> crs;
	std::optional<Transform> transform;

	// Column mapping
	std::vector<std::string> column_names;
	std::vector<LogicalType> column_types;
	std::vector<CopyColumnRole> column_roles;

	// Index of key columns (-1 if not found)
	idx_t id_col = DConstants::INVALID_INDEX;
	idx_t feature_id_col = DConstants::INVALID_INDEX;
	idx_t object_type_col = DConstants::INVALID_INDEX;
	idx_t children_col = DConstants::INVALID_INDEX;
	idx_t parents_col = DConstants::INVALID_INDEX;
	idx_t children_roles_col = DConstants::INVALID_INDEX;
	idx_t geometry_col = DConstants::INVALID_INDEX;
	idx_t geometry_properties_col = DConstants::INVALID_INDEX;

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other) const override;
};

// ============================================================
// Global state for COPY TO
// ============================================================

struct CityJSONCopyGlobalState : public GlobalFunctionData {
	std::mutex mutex;

	// Accumulated CityObjects grouped by feature_id
	// feature_id -> [(city_object_id, CityObject json)]
	std::map<std::string, std::vector<std::pair<std::string, json>>> feature_objects;

	// All unique feature IDs in order
	std::vector<std::string> feature_order;
};

// ============================================================
// Local state for COPY TO
// ============================================================

struct CityJSONCopyLocalState : public LocalFunctionData {
	// Local buffer before combine
	std::map<std::string, std::vector<std::pair<std::string, json>>> local_objects;
	std::vector<std::string> local_feature_order;
};

// ============================================================
// Registration
// ============================================================

void RegisterCityJSONCopyFunction(ExtensionLoader &loader);
void RegisterCityJSONSeqCopyFunction(ExtensionLoader &loader);

} // namespace cityjson
} // namespace duckdb
