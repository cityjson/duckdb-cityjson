#pragma once

#include "cityjson/types.hpp"
#include "cityjson/cityjson_types.hpp"
#include "cityjson/column_types.hpp"
#include <string>
#include <vector>
#include <memory>

namespace duckdb {
namespace cityjson {

/**
 * Abstract base class for CityJSON readers
 * Provides interface for reading CityJSON and CityJSONSeq formats
 */
class CityJSONReader {
public:
    virtual ~CityJSONReader() = default;

    /**
     * Get reader name/identifier
     * Typically returns the file path
     *
     * @return Reader name
     */
    virtual std::string Name() const = 0;

    /**
     * Read CityJSON metadata (version, transform, CRS, etc.)
     * Does not read CityObjects, only metadata fields
     *
     * @return CityJSON struct with metadata populated
     * @throws CityJSONError on read or parse failure
     */
    virtual CityJSON ReadMetadata() const = 0;

    /**
     * Read the Nth chunk from the file
     * Chunks are divided by STANDARD_VECTOR_SIZE (2048 CityObjects per chunk)
     *
     * @param n Chunk index (0-based)
     * @return CityJSONFeatureChunk containing the Nth chunk
     * @throws CityJSONError on read or parse failure
     */
    virtual CityJSONFeatureChunk ReadNthChunk(size_t n) const = 0;

    /**
     * Read all chunks from the file
     * Loads entire file and divides into chunks
     *
     * @return CityJSONFeatureChunk containing all data
     * @throws CityJSONError on read or parse failure
     */
    virtual CityJSONFeatureChunk ReadAllChunks() const = 0;

    /**
     * Read first N features from the file
     * Used for schema inference and sampling
     *
     * @param n Number of features to read
     * @return Vector of CityJSONFeature records
     * @throws CityJSONError on read or parse failure
     */
    virtual std::vector<CityJSONFeature> ReadNFeatures(size_t n) const = 0;

    /**
     * Get complete column schema
     * Includes both predefined columns and inferred attribute columns
     * Performs schema inference by sampling features
     *
     * @return Vector of Column definitions
     * @throws CityJSONError on schema inference failure
     */
    virtual std::vector<Column> Columns() const = 0;
};

/**
 * Reader for standard CityJSON format (.city.json)
 * Loads entire JSON file into memory
 */
class LocalCityJSONReader : public CityJSONReader {
public:
    /**
     * Construct reader for local CityJSON file
     *
     * @param file_path Path to .city.json file
     * @param sample_lines Number of features to sample for schema inference
     */
    LocalCityJSONReader(const std::string& file_path, size_t sample_lines = 100);

    std::string Name() const override;
    CityJSON ReadMetadata() const override;
    CityJSONFeatureChunk ReadNthChunk(size_t n) const override;
    CityJSONFeatureChunk ReadAllChunks() const override;
    std::vector<CityJSONFeature> ReadNFeatures(size_t n) const override;
    std::vector<Column> Columns() const override;

private:
    std::string file_path_;         // Path to CityJSON file
    size_t sample_lines_;           // Number of features to sample for schema inference

    // Caching fields (mutable for lazy initialization in const methods)
    mutable std::optional<CityJSON> cached_metadata_;
    mutable std::optional<std::vector<Column>> cached_columns_;
};

/**
 * Reader for CityJSONSeq format (.city.jsonl)
 * Newline-delimited JSON format with streaming support
 */
class LocalCityJSONSeqReader : public CityJSONReader {
public:
    /**
     * Construct reader for local CityJSONSeq file
     *
     * @param file_path Path to .city.jsonl file
     * @param sample_lines Number of features to sample for schema inference
     */
    LocalCityJSONSeqReader(const std::string& file_path, size_t sample_lines = 100);

    std::string Name() const override;
    CityJSON ReadMetadata() const override;
    CityJSONFeatureChunk ReadNthChunk(size_t n) const override;
    CityJSONFeatureChunk ReadAllChunks() const override;
    std::vector<CityJSONFeature> ReadNFeatures(size_t n) const override;
    std::vector<Column> Columns() const override;

private:
    std::string file_path_;         // Path to CityJSONSeq file
    size_t sample_lines_;           // Number of features to sample for schema inference

    // Caching fields (mutable for lazy initialization in const methods)
    mutable std::optional<CityJSON> cached_metadata_;
    mutable std::optional<std::vector<Column>> cached_columns_;
};

/**
 * Factory function to open any CityJSON file format
 * Automatically detects format from file extension:
 * - .city.json or .json → LocalCityJSONReader
 * - .city.jsonl or .jsonl → LocalCityJSONSeqReader
 *
 * @param file_name Path to CityJSON file
 * @return Unique pointer to appropriate reader implementation
 * @throws CityJSONError if format cannot be determined or file doesn't exist
 */
std::unique_ptr<CityJSONReader> OpenAnyCityJSONFile(const std::string& file_name);

} // namespace cityjson
} // namespace duckdb
