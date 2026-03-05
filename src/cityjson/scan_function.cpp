#include "cityjson/table_function.hpp"
#include "cityjson/vector_writer.hpp"
#include "cityjson/city_object_utils.hpp"

namespace duckdb {
namespace cityjson {

void CityJSONScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<CityJSONBindData>();
	auto &local_state = data.local_state->Cast<CityJSONLocalState>();
	auto &global_state = data.global_state->Cast<CityJSONGlobalState>();

	// Get next batch index atomically
	size_t batch_index = global_state.batch_index.fetch_add(1);

	// Calculate starting position in flattened CityObject sequence
	size_t start_position = batch_index * STANDARD_VECTOR_SIZE;

	// Find which chunk and feature contains this position
	size_t current_position = 0;
	size_t chunk_idx = 0;
	size_t feature_idx = 0;
	size_t city_object_offset = 0; // Offset within the current feature's CityObjects
	bool found = false;

	for (chunk_idx = 0; chunk_idx < bind_data.chunks.ChunkCount(); chunk_idx++) {
		auto chunk = bind_data.chunks.GetChunk(chunk_idx);
		if (!chunk)
			break;

		// Iterate through features to find exact position
		for (feature_idx = 0; feature_idx < chunk->size(); feature_idx++) {
			size_t feature_obj_count = (*chunk)[feature_idx].city_objects.size();

			if (current_position + feature_obj_count > start_position) {
				// Found the feature containing start_position
				city_object_offset = start_position - current_position;
				found = true;
				break;
			}
			current_position += feature_obj_count;
		}

		if (found)
			break;
	}

	// Check if exhausted
	if (!found || chunk_idx >= bind_data.chunks.ChunkCount()) {
		output.SetCardinality(0);
		return;
	}

	// Create vector wrappers for projected columns
	// Use projection_ids if available (projection pushdown), otherwise use all column_ids
	const auto &projected_cols =
	    local_state.projection_ids.empty() ? local_state.column_ids : local_state.projection_ids;
	auto wrappers = CreateVectors(output, bind_data.columns, projected_cols);

	// Track output row
	size_t output_row = 0;
	size_t remaining = STANDARD_VECTOR_SIZE;

	// Iterate across chunks if necessary
	while (remaining > 0 && chunk_idx < bind_data.chunks.ChunkCount()) {
		auto chunk = bind_data.chunks.GetChunk(chunk_idx);
		if (!chunk)
			break;

		// Iterate through features in chunk
		for (; feature_idx < chunk->size() && remaining > 0; feature_idx++) {
			const auto &feature = (*chunk)[feature_idx];

			// Create an iterator to skip to city_object_offset
			size_t obj_idx = 0;
			for (const auto &[city_obj_id, city_obj] : feature.city_objects) {
				// Skip objects until we reach the offset
				if (obj_idx < city_object_offset) {
					obj_idx++;
					continue;
				}

				if (remaining == 0)
					break;

				// For WKB encoding mode, find the geometry matching the target LOD
				std::optional<Geometry> target_geom;
				if (bind_data.use_wkb_encoding && bind_data.target_lod.has_value()) {
					target_geom = city_obj.GetGeometryAtLOD(bind_data.target_lod.value());
				}

				// Resolve which vertex pool to use for WKB encoding:
				// - CityJSONSeq: each feature has its own local vertex pool (feature.vertices)
				// - Regular CityJSON: global vertex pool in bind_data.metadata.vertices
				const std::vector<std::array<double, 3>> *vertex_pool = nullptr;
				if (!feature.vertices.empty()) {
					vertex_pool = &feature.vertices;
				} else if (bind_data.metadata.vertices.has_value() && !bind_data.metadata.vertices->empty()) {
					vertex_pool = &bind_data.metadata.vertices.value();
				}

				// Write data for each projected column
				for (size_t col_idx = 0; col_idx < projected_cols.size(); col_idx++) {
					size_t schema_idx = projected_cols[col_idx];
					const Column &col = bind_data.columns[schema_idx];

					// Handle WKB geometry column
					if (col.kind == ColumnType::GeometryWKB) {
						if (target_geom.has_value() && vertex_pool != nullptr) {
							// Encode geometry to WKB using the resolved vertex pool
							auto wkb_data = CityObjectUtils::GetGeometryWKB(target_geom.value(), *vertex_pool,
							                                                bind_data.metadata.transform);
							WriteGeometryWKB(wrappers[col_idx].AsFlatMut(), wkb_data, output_row);
						} else {
							FlatVector::SetNull(*wrappers[col_idx].AsFlatMut(), output_row, true);
						}
						continue;
					}

					// Handle geometry properties column
					if (col.kind == ColumnType::GeometryPropertiesJson) {
						if (target_geom.has_value()) {
							auto props = CityObjectUtils::GetGeometryPropertiesJson(target_geom.value());
							WriteGeometryProperties(wrappers[col_idx].AsFlatMut(), props, output_row);
						} else {
							FlatVector::SetNull(*wrappers[col_idx].AsFlatMut(), output_row, true);
						}
						continue;
					}

					// Get value based on column type (standard handling)
					json value;

					if (col.name == "id") {
						value = json(city_obj_id);
					} else if (col.name == "feature_id") {
						value = json(feature.id);
					} else if (IsGeometryColumn(col.name)) {
						value = CityObjectUtils::GetGeometryValue(city_obj, col);
					} else {
						value = CityObjectUtils::GetAttributeValue(city_obj, col);
					}

					// Write to vector
					try {
						WriteToVector(col, value, wrappers[col_idx], output_row);
					} catch (const CityJSONError &e) {
						throw InternalException("Failed to write value for column '" + col.name +
						                        "': " + std::string(e.what()));
					}
				}

				output_row++;
				remaining--;
				obj_idx++;
			}

			// Reset city_object_offset after first feature (only applies to starting feature)
			city_object_offset = 0;
		}

		// Move to next chunk
		chunk_idx++;
		feature_idx = 0;
	}

	// Set output cardinality
	output.SetCardinality(output_row);

	// Verify output
	output.Verify();
}

} // namespace cityjson
} // namespace duckdb
