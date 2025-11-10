#include "cityjson/table_function.hpp"
#include "cityjson/vector_writer.hpp"
#include "cityjson/city_object_utils.hpp"

namespace duckdb {
namespace cityjson {

void CityJSONScan(
    ClientContext &context,
    TableFunctionInput &data,
    DataChunk &output) {

    auto &bind_data = data.bind_data->Cast<CityJSONBindData>();
    auto &local_state = data.local_state->Cast<CityJSONLocalState>();
    auto &global_state = data.global_state->Cast<CityJSONGlobalState>();

    // Get next batch index atomically
    size_t batch_index = global_state.batch_index.fetch_add(1);

    // Calculate starting position in flattened CityObject sequence
    size_t start_position = batch_index * STANDARD_VECTOR_SIZE;

    // Find which chunk contains this position
    size_t current_position = 0;
    size_t chunk_idx = 0;
    size_t offset_in_chunk = 0;

    for (chunk_idx = 0; chunk_idx < bind_data.chunks.ChunkCount(); chunk_idx++) {
        auto chunk_size = bind_data.chunks.CityObjectCount(chunk_idx);
        if (!chunk_size) break;

        if (current_position + *chunk_size > start_position) {
            offset_in_chunk = start_position - current_position;
            break;
        }
        current_position += *chunk_size;
    }

    // Check if exhausted
    if (chunk_idx >= bind_data.chunks.ChunkCount()) {
        output.SetCardinality(0);
        return;
    }

    // Create vector wrappers for projected columns
    auto wrappers = CreateVectors(output, bind_data.columns, local_state.projection_ids);

    // Track output row
    size_t output_row = 0;
    size_t remaining = STANDARD_VECTOR_SIZE;
    size_t feature_idx = offset_in_chunk;

    // Iterate across chunks if necessary
    while (remaining > 0 && chunk_idx < bind_data.chunks.ChunkCount()) {
        auto chunk = bind_data.chunks.GetChunk(chunk_idx);
        if (!chunk) break;

        // Iterate through features in chunk
        for (; feature_idx < chunk->size() && remaining > 0; feature_idx++) {
            const auto& feature = (*chunk)[feature_idx];

            // Iterate through CityObjects in feature
            for (const auto& [city_obj_id, city_obj] : feature.city_objects) {
                if (remaining == 0) break;

                // Write data for each projected column
                for (size_t col_idx = 0; col_idx < local_state.projection_ids.size(); col_idx++) {
                    size_t schema_idx = local_state.projection_ids[col_idx];
                    const Column& col = bind_data.columns[schema_idx];

                    // Get value based on column type
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
                    } catch (const CityJSONError& e) {
                        throw InternalException(
                            "Failed to write value for column '" + col.name + "': " + std::string(e.what())
                        );
                    }
                }

                output_row++;
                remaining--;
            }
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
