#include "cityjson/table_function.hpp"
#include "cityjson/vector_writer.hpp"
#include "cityjson/city_object_utils.hpp"

namespace duckdb {
namespace cityjson {

static void WriteCityObjectRow(const CityJSONBindData &bind_data, const CityJSONFeature &feature,
                               const std::string &city_obj_id, const CityObject &city_obj,
                               std::vector<VectorWrapper> &wrappers, const std::vector<idx_t> &projected_cols,
                               size_t output_row) {
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

		// Handle WKB geometry column. In per-LOD mode (lod => ...) the single "geometry"
		// column uses target_geom; in default mode each "geometry_lodX_Y" column resolves
		// its own LOD from the column name.
		if (col.kind == ColumnType::GeometryWKB) {
			std::optional<Geometry> geom = bind_data.target_lod.has_value()
			                                   ? target_geom
			                                   : city_obj.GetGeometryAtLOD(ParseLODFromGeometryColumn(col.name));
			if (geom.has_value() && vertex_pool != nullptr) {
				auto wkb_data =
				    CityObjectUtils::GetGeometryWKB(geom.value(), *vertex_pool, bind_data.metadata.transform);
				WriteGeometryWKB(wrappers[col_idx].AsFlatMut(), wkb_data, output_row);
			} else {
				wrappers[col_idx].SetNull(output_row);
			}
			continue;
		}

		// Handle geometry properties column (single "geometry_properties" in per-LOD mode,
		// or per-LOD "geometry_properties_lodX_Y" in default mode).
		if (col.kind == ColumnType::GeometryPropertiesJson) {
			std::optional<Geometry> geom = bind_data.target_lod.has_value()
			                                   ? target_geom
			                                   : city_obj.GetGeometryAtLOD(ParseLODFromGeometryColumn(col.name));
			if (geom.has_value()) {
				// The LoD is carried by a suffixed column name (geometry_properties_lod*);
				// an un-suffixed "geometry_properties" column (single-LoD mode) has no
				// name to carry it, so the LoD is added inside the JSON instead (spec §8).
				bool include_lod = (col.name == "geometry_properties");
				auto props = CityObjectUtils::GetGeometryPropertiesJson(geom.value(), include_lod);
				WriteGeometryProperties(wrappers[col_idx].AsFlatMut(), props, output_row);
			} else {
				wrappers[col_idx].SetNull(output_row);
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
			// Write geometry struct directly without JSON round-trip
			std::string lod = ParseLODFromColumnName(col.name);
			auto geom_opt = city_obj.GetGeometryAtLOD(lod);
			if (geom_opt.has_value()) {
				WriteGeometry(wrappers[col_idx].AsStructMut(), geom_opt.value(), output_row);
			} else {
				wrappers[col_idx].SetNull(output_row);
			}
			continue;
		} else if (col.name == "bbox") {
			// Per-LOD mode uses target_geom; default (wide) mode has no target LOD, so the
			// bbox is computed from the city object's highest-LOD geometry.
			std::optional<Geometry> bbox_geom =
			    bind_data.target_lod.has_value() ? target_geom : city_obj.GetHighestLODGeometry();
			if (bbox_geom.has_value() && vertex_pool != nullptr) {
				auto extent =
				    CityObjectUtils::GetGeometryExtent(bbox_geom.value(), *vertex_pool, bind_data.metadata.transform);
				value = extent.has_value() ? extent->ToJson() : json(nullptr);
			} else {
				value = json(nullptr);
			}
		} else {
			value = CityObjectUtils::GetAttributeValue(city_obj, col);
		}

		// Write to vector
		try {
			WriteToVector(col, value, wrappers[col_idx], output_row);
		} catch (const CityJSONError &e) {
			throw InternalException("Failed to write value for column '" + col.name + "': " + std::string(e.what()));
		}
	}
}

static bool MatchesFilters(const CityJSONBindData &bind_data, const CityJSONFeature &feature,
                           const std::string &city_obj_id, const CityObject &city_obj) {
	for (const auto &[column, expected] : bind_data.equality_filters) {
		if (column == "id" && city_obj_id != expected) {
			return false;
		}
		if (column == "feature_id" && feature.id != expected) {
			return false;
		}
		if (column == "object_type" && city_obj.type != expected) {
			return false;
		}
	}
	return true;
}

static void MaterializedScan(const CityJSONBindData &bind_data, CityJSONGlobalState &global_state,
                             CityJSONLocalState &local_state, DataChunk &output) {
	const auto &active_chunks = bind_data.chunks;
	const auto &active_plan = bind_data.scan_plan;

	const auto &projected_cols =
	    local_state.projection_ids.empty() ? local_state.column_ids : local_state.projection_ids;
	auto wrappers = CreateVectors(output, bind_data.columns, projected_cols);

	if (bind_data.equality_filters.empty()) {
		// No filters: use the precomputed batch-based scan plan.
		size_t batch_index = global_state.batch_index.fetch_add(1);

		if (batch_index >= active_plan.BatchCount()) {
			output.SetCardinality(0);
			return;
		}

		const auto &start_pos = active_plan.batch_starts[batch_index];
		size_t start_row = start_pos.start_row;
		size_t end_row = (batch_index + 1 < active_plan.BatchCount())
		                     ? active_plan.batch_starts[batch_index + 1].start_row
		                     : active_plan.total_rows;
		size_t rows_to_write = end_row - start_row;

		if (rows_to_write == 0) {
			output.SetCardinality(0);
			return;
		}

		size_t output_row = 0;
		size_t remaining = rows_to_write;

		size_t chunk_idx = start_pos.chunk_idx;
		size_t feature_idx = start_pos.feature_idx;
		size_t city_object_offset = start_pos.city_object_offset;

		while (remaining > 0 && chunk_idx < active_chunks.ChunkCount()) {
			auto chunk = active_chunks.GetChunk(chunk_idx);
			if (!chunk) {
				break;
			}

			for (; feature_idx < chunk->size() && remaining > 0; feature_idx++) {
				const auto &feature = (*chunk)[feature_idx];

				size_t obj_idx = 0;
				for (const auto &[city_obj_id, city_obj] : feature.city_objects) {
					if (obj_idx < city_object_offset) {
						obj_idx++;
						continue;
					}

					if (remaining == 0)
						break;

					WriteCityObjectRow(bind_data, feature, city_obj_id, city_obj, wrappers, projected_cols, output_row);

					output_row++;
					remaining--;
					obj_idx++;
				}

				city_object_offset = 0;
			}

			chunk_idx++;
			feature_idx = 0;
		}

		output.SetCardinality(output_row);
	} else {
		// Filters are active: scan sequentially from the shared source position and
		// emit only matching rows until the output chunk is full.
		size_t output_row = 0;

		while (output_row < STANDARD_VECTOR_SIZE && global_state.filter_chunk_idx < active_chunks.ChunkCount()) {
			auto chunk = active_chunks.GetChunk(global_state.filter_chunk_idx);
			if (!chunk) {
				break;
			}

			if (global_state.filter_feature_idx >= chunk->size()) {
				global_state.filter_chunk_idx++;
				global_state.filter_feature_idx = 0;
				global_state.filter_obj_offset = 0;
				continue;
			}

			const auto &feature = (*chunk)[global_state.filter_feature_idx];

			size_t obj_idx = 0;
			for (const auto &[city_obj_id, city_obj] : feature.city_objects) {
				if (obj_idx < global_state.filter_obj_offset) {
					obj_idx++;
					continue;
				}

				global_state.filter_obj_offset = obj_idx + 1;

				if (MatchesFilters(bind_data, feature, city_obj_id, city_obj)) {
					WriteCityObjectRow(bind_data, feature, city_obj_id, city_obj, wrappers, projected_cols, output_row);
					output_row++;
					break;
				}

				obj_idx++;
			}

			if (global_state.filter_obj_offset >= feature.city_objects.size()) {
				global_state.filter_feature_idx++;
				global_state.filter_obj_offset = 0;
			}
		}

		output.SetCardinality(output_row);
	}

	output.Verify();
}

static void StreamingScan(const CityJSONBindData &bind_data, CityJSONGlobalState &global_state,
                          CityJSONLocalState &local_state, DataChunk &output) {
	if (!global_state.streaming_reader) {
		output.SetCardinality(0);
		return;
	}

	const auto &projected_cols =
	    local_state.projection_ids.empty() ? local_state.column_ids : local_state.projection_ids;
	auto wrappers = CreateVectors(output, bind_data.columns, projected_cols);

	size_t output_row = 0;

	while (output_row < STANDARD_VECTOR_SIZE) {
		// Ensure we have a feature with remaining city objects
		if (!global_state.streaming_feature.has_value() ||
		    global_state.streaming_obj_it == global_state.streaming_feature->city_objects.end()) {
			auto next = global_state.streaming_reader->ReadNextFeature();
			if (!next.has_value()) {
				break;
			}
			global_state.streaming_feature = std::move(next.value());
			global_state.streaming_obj_it = global_state.streaming_feature->city_objects.begin();
		}

		const auto &feature = global_state.streaming_feature.value();
		const auto &[city_obj_id, city_obj] = *global_state.streaming_obj_it;

		// Advance iterator before potentially writing, so we continue correctly
		// regardless of whether this row passes the filter.
		++global_state.streaming_obj_it;

		if (bind_data.equality_filters.empty() || MatchesFilters(bind_data, feature, city_obj_id, city_obj)) {
			WriteCityObjectRow(bind_data, feature, city_obj_id, city_obj, wrappers, projected_cols, output_row);
			output_row++;
		}
	}

	output.SetCardinality(output_row);
	output.Verify();
}

void CityJSONScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<CityJSONBindData>();
	auto &local_state = data.local_state->Cast<CityJSONLocalState>();
	auto &global_state = data.global_state->Cast<CityJSONGlobalState>();

	if (bind_data.streaming) {
		StreamingScan(bind_data, global_state, local_state, output);
	} else {
		MaterializedScan(bind_data, global_state, local_state, output);
	}
}

} // namespace cityjson
} // namespace duckdb
