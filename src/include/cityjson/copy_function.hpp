#pragma once

#include "cityjson/cityjson_types.hpp"
#include "cityjson/json_utils.hpp"
#include "duckdb.hpp"
#include "duckdb/function/copy_function.hpp"
#include <string>
#include <vector>
#include <map>
#include <unordered_map>
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
	GeometryWKB,        // geometry / geometry_lod* / geom_lod* (WKB blob)
	GeometryProperties, // geometry_properties / geometry_properties_lod* JSON
	Appearance,         // material_lod* / texture_lod* (per-LoD appearance, §11)
	Bbox,               // derived bounding box — recomputed on read, ignored on write
	Other,              // extension fields
	Attribute           // everything else -> attributes map
};

CopyColumnRole DetectColumnRole(const std::string &name);

// ============================================================
// Bind data for COPY TO
// ============================================================

struct CityJSONCopyBindData : public FunctionData {
	std::string file_path;
	bool is_seq = false; // true for cityjsonseq format
	bool is_fcb = false; // true for flatcitybuf format

	// FlatCityBuf write-only options (COPY TO ... FORMAT flatcitybuf).
	std::vector<std::string> fcb_attr_index_columns; // parsed from attr_index, empty = none
	std::optional<uint16_t> fcb_branching_factor;
	std::optional<uint16_t> fcb_index_node_size;

	// Metadata (from options or metadata_query)
	std::string version = "2.0";
	std::optional<std::string> crs;
	std::optional<Transform> transform;
	std::optional<std::string> title;
	std::optional<std::string> identifier;
	std::optional<std::string> reference_date;
	std::optional<GeographicalExtent> geographical_extent;
	std::optional<PointOfContact> point_of_contact;

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
	// Legacy single properties column / fallback when a geometry column has no per-LOD
	// properties counterpart (e.g. the old geom_lod* layout).
	idx_t geometry_properties_col = DConstants::INVALID_INDEX;
	// Wide CityParquet layout: one properties column per LOD. Keyed by the properties
	// column name (e.g. "geometry_properties_lod2_2") so a geometry column can find its
	// matching properties without assuming a single shared column.
	std::unordered_map<std::string, idx_t> geometry_properties_by_name;
	// material_lod*/texture_lod* columns by name, for re-attaching appearance to
	// the matching geometry on export (§11).
	std::unordered_map<std::string, idx_t> appearance_by_name;

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other) const override;
};

// ============================================================
// Global state for COPY TO
// ============================================================

struct CityJSONCopyGlobalState : public GlobalFunctionData {
	std::mutex mutex;

	// The temp file path provided by DuckDB (DuckDB renames it to the final path after Finalize)
	std::string temp_file_path;

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

#ifdef CITYJSON_HAS_FCB
void RegisterFlatCityBufCopyFunction(ExtensionLoader &loader);
#endif

} // namespace cityjson
} // namespace duckdb
