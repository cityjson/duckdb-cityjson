#include "cityjson/reader.hpp"
#include "cityjson/city_object_utils.hpp"
#include <fstream>
#include <sstream>
#include <algorithm>

namespace duckdb {
namespace cityjson {

using namespace json_utils;

// ============================================================
// Constructor
// ============================================================

LocalCityJSONSeqReader::LocalCityJSONSeqReader(const std::string& file_path, size_t sample_lines)
    : file_path_(file_path), sample_lines_(sample_lines) {}

// ============================================================
// Name
// ============================================================

std::string LocalCityJSONSeqReader::Name() const {
    return file_path_;
}

// ============================================================
// ReadMetadata
// ============================================================

CityJSON LocalCityJSONSeqReader::ReadMetadata() const {
    // Check cache
    if (cached_metadata_.has_value()) {
        return cached_metadata_.value();
    }

    // Open file
    std::ifstream file(file_path_);
    if (!file.is_open()) {
        throw CityJSONError::FileRead("Failed to open file: " + file_path_);
    }

    // Read first line (metadata record)
    std::string line;
    if (!std::getline(file, line)) {
        throw CityJSONError::Sequence("CityJSONSeq file is empty");
    }

    // Parse metadata
    json obj = ParseJson(line);

    // Validate it's a CityJSON metadata record
    if (!obj.contains("type") || obj["type"] != "CityJSON") {
        throw CityJSONError::Sequence("First line must be CityJSON metadata");
    }

    CityJSON metadata = CityJSON::FromJson(obj);

    // Cache the result
    cached_metadata_ = metadata;

    return metadata;
}

// ============================================================
// ReadNFeatures
// ============================================================

std::vector<CityJSONFeature> LocalCityJSONSeqReader::ReadNFeatures(size_t n) const {
    // Open file
    std::ifstream file(file_path_);
    if (!file.is_open()) {
        throw CityJSONError::FileRead("Failed to open file: " + file_path_);
    }

    std::vector<CityJSONFeature> features;
    std::string line;

    // Skip first line (metadata)
    if (!std::getline(file, line)) {
        throw CityJSONError::Sequence("CityJSONSeq file is empty");
    }

    // Read next N feature lines
    size_t count = 0;
    while (count < n && std::getline(file, line)) {
        if (line.empty()) {
            continue;  // Skip empty lines
        }

        try {
            json feature_obj = ParseJson(line);
            CityJSONFeature feature = CityJSONFeature::FromJson(feature_obj);
            features.push_back(std::move(feature));
            count++;
        } catch (const CityJSONError& e) {
            throw CityJSONError::Sequence(
                "Failed to parse feature at line " + std::to_string(count + 2) + ": " + std::string(e.what()),
                file_path_
            );
        }
    }

    return features;
}

// ============================================================
// ReadAllChunks
// ============================================================

CityJSONFeatureChunk LocalCityJSONSeqReader::ReadAllChunks() const {
    // Open file
    std::ifstream file(file_path_);
    if (!file.is_open()) {
        throw CityJSONError::FileRead("Failed to open file: " + file_path_);
    }

    std::vector<CityJSONFeature> features;
    std::string line;
    size_t line_number = 0;

    // Skip first line (metadata)
    if (!std::getline(file, line)) {
        throw CityJSONError::Sequence("CityJSONSeq file is empty");
    }
    line_number++;

    // Read all remaining lines
    while (std::getline(file, line)) {
        line_number++;

        if (line.empty()) {
            continue;  // Skip empty lines
        }

        try {
            json feature_obj = ParseJson(line);
            CityJSONFeature feature = CityJSONFeature::FromJson(feature_obj);
            features.push_back(std::move(feature));
        } catch (const CityJSONError& e) {
            throw CityJSONError::Sequence(
                "Failed to parse feature at line " + std::to_string(line_number) + ": " + std::string(e.what()),
                file_path_
            );
        }
    }

    // Create chunks
    return CityJSONFeatureChunk::CreateChunks(std::move(features), STANDARD_VECTOR_SIZE);
}

// ============================================================
// ReadNthChunk
// ============================================================

CityJSONFeatureChunk LocalCityJSONSeqReader::ReadNthChunk(size_t n) const {
    // For line-delimited format, we need to read all features to determine chunk boundaries
    // This is because chunks are based on CityObject count, not feature count
    // A more optimized implementation could cache line positions, but for now we read all
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

std::vector<Column> LocalCityJSONSeqReader::Columns() const {
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
