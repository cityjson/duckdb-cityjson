#include "cityjson/reader.hpp"
#include "cityjson/city_object_utils.hpp"
#include <fstream>
#include <algorithm>

namespace duckdb {
namespace cityjson {

using namespace json_utils;

// Standard vector size for DuckDB
constexpr size_t STANDARD_VECTOR_SIZE = 2048;

// ============================================================
// Constructor
// ============================================================

LocalCityJSONReader::LocalCityJSONReader(const std::string& file_path, size_t sample_lines)
    : file_path_(file_path), sample_lines_(sample_lines) {}

// ============================================================
// Name
// ============================================================

std::string LocalCityJSONReader::Name() const {
    return file_path_;
}

// ============================================================
// ReadMetadata
// ============================================================

CityJSON LocalCityJSONReader::ReadMetadata() const {
    // Check cache
    if (cached_metadata_.has_value()) {
        return cached_metadata_.value();
    }

    // Parse the JSON file
    json obj = ParseJsonFile(file_path_);

    // Extract metadata (without CityObjects)
    CityJSON metadata = CityJSON::FromJson(obj);

    // Cache the result
    cached_metadata_ = metadata;

    return metadata;
}

// ============================================================
// ReadNFeatures
// ============================================================

std::vector<CityJSONFeature> LocalCityJSONReader::ReadNFeatures(size_t n) const {
    // Parse the JSON file
    json obj = ParseJsonFile(file_path_);

    // Validate structure
    if (!obj.contains("CityObjects") || !obj["CityObjects"].is_object()) {
        throw CityJSONError::InvalidSchema("CityJSON file missing 'CityObjects' field");
    }

    // For CityJSON format, all CityObjects are in one implicit feature
    // We'll create a single feature containing up to N CityObjects
    CityJSONFeature feature;
    feature.id = file_path_;  // Use file path as feature ID
    feature.type = "CityJSONFeature";

    const auto& city_objects = obj["CityObjects"];
    size_t count = 0;

    for (auto& [obj_id, obj_data] : city_objects.items()) {
        if (count >= n) {
            break;
        }
        feature.city_objects[obj_id] = CityObject::FromJson(obj_data);
        count++;
    }

    return {feature};
}

// ============================================================
// ReadAllChunks
// ============================================================

CityJSONFeatureChunk LocalCityJSONReader::ReadAllChunks() const {
    // Parse the JSON file
    json obj = ParseJsonFile(file_path_);

    // Validate structure
    if (!obj.contains("CityObjects") || !obj["CityObjects"].is_object()) {
        throw CityJSONError::InvalidSchema("CityJSON file missing 'CityObjects' field");
    }

    // Convert all CityObjects to a single feature
    CityJSONFeature feature;
    feature.id = file_path_;
    feature.type = "CityJSONFeature";

    const auto& city_objects = obj["CityObjects"];
    for (auto& [obj_id, obj_data] : city_objects.items()) {
        feature.city_objects[obj_id] = CityObject::FromJson(obj_data);
    }

    // Create chunks
    std::vector<CityJSONFeature> features = {feature};
    return CityJSONFeatureChunk::CreateChunks(std::move(features), STANDARD_VECTOR_SIZE);
}

// ============================================================
// ReadNthChunk
// ============================================================

CityJSONFeatureChunk LocalCityJSONReader::ReadNthChunk(size_t n) const {
    // For CityJSON format, we need to load all data first then extract the Nth chunk
    // This is because CityObjects are stored as a single JSON object, not line-delimited
    CityJSONFeatureChunk all_chunks = ReadAllChunks();

    if (n >= all_chunks.ChunkCount()) {
        // Return empty chunk
        return CityJSONFeatureChunk();
    }

    // Extract the Nth chunk
    auto chunk_opt = all_chunks.GetChunk(n);
    if (!chunk_opt.has_value()) {
        return CityJSONFeatureChunk();
    }

    // Create a new chunk containing only the requested chunk
    CityJSONFeatureChunk result;
    result.records = std::vector<CityJSONFeature>(chunk_opt->begin(), chunk_opt->end());
    result.chunks = {Range(0, result.records.size())};

    return result;
}

// ============================================================
// Columns
// ============================================================

std::vector<Column> LocalCityJSONReader::Columns() const {
    // Check cache
    if (cached_columns_.has_value()) {
        return cached_columns_.value();
    }

    // Start with predefined columns
    std::vector<Column> columns = GetDefinedColumns();

    // Sample features for schema inference
    std::vector<CityJSONFeature> sample_features = ReadNFeatures(sample_lines_);

    // Infer attribute columns
    std::vector<Column> attr_columns = CityObjectUtils::InferAttributeColumns(
        sample_features,
        sample_lines_
    );

    // Infer geometry columns
    std::vector<Column> geom_columns = CityObjectUtils::InferGeometryColumns(
        sample_features,
        sample_lines_
    );

    // Merge all columns: predefined + attributes + geometries
    columns.insert(columns.end(), attr_columns.begin(), attr_columns.end());
    columns.insert(columns.end(), geom_columns.begin(), geom_columns.end());

    // Cache the result
    cached_columns_ = columns;

    return columns;
}

} // namespace cityjson
} // namespace duckdb
