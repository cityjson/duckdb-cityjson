#pragma once

#include "cityjson/types.hpp"
#include "cityjson/cityjson_types.hpp"
#include "cityjson/column_types.hpp"
#include "cityjson/json_utils.hpp"
#include <string>
#include <vector>
#include <memory>
#include <istream>

namespace duckdb {
class ClientContext;
}

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

	/**
	 * Count total city objects without retaining all features
	 * Default implementation reads all chunks; streaming readers should override
	 *
	 * @return Total number of city objects
	 * @throws CityJSONError on read or parse failure
	 */
	virtual size_t CountCityObjects() const;

	/**
	 * Read the next feature incrementally.
	 * Default implementation returns nullopt (not supported by this reader).
	 */
	virtual std::optional<CityJSONFeature> ReadNextFeature() const {
		return std::nullopt;
	}
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
	explicit LocalCityJSONReader(const std::string &file_path, size_t sample_lines = 100);

	/**
	 * Construct reader from pre-loaded content
	 *
	 * @param name Display name (e.g. file path or URL)
	 * @param content File content as string
	 * @param sample_lines Number of features to sample for schema inference
	 */
	LocalCityJSONReader(const std::string &name, std::string content, size_t sample_lines);

	std::string Name() const override;
	CityJSON ReadMetadata() const override;
	CityJSONFeatureChunk ReadNthChunk(size_t n) const override;
	CityJSONFeatureChunk ReadAllChunks() const override;
	std::vector<CityJSONFeature> ReadNFeatures(size_t n) const override;
	std::vector<Column> Columns() const override;

private:
	std::string file_path_;              // Path to CityJSON file
	size_t sample_lines_;                // Number of features to sample for schema inference
	std::optional<std::string> content_; // Pre-loaded file content (for remote files)

	// Caching fields (mutable for lazy initialization in const methods)
	mutable std::optional<CityJSON> cached_metadata_;
	mutable std::optional<std::vector<Column>> cached_columns_;

	// Internal helper: get JSON object (from content_ or file)
	json LoadJson() const;
};

/**
 * Reader for CityJSONSeq format (.city.jsonl)
 * Newline-delimited JSON format with streaming support
 *
 * Uses DuckDB's FileHandle so local files, HTTP, S3, GCS, etc. are read
 * incrementally rather than loaded entirely into memory.
 */
class LocalCityJSONSeqReader : public CityJSONReader {
public:
	/**
	 * Construct reader using DuckDB FileSystem
	 *
	 * @param context DuckDB client context (provides FileSystem access)
	 * @param file_path Path or URL to .city.jsonl file
	 * @param sample_lines Number of features to sample for schema inference
	 */
	LocalCityJSONSeqReader(duckdb::ClientContext &context, const std::string &file_path, size_t sample_lines = 100);

	std::string Name() const override;
	CityJSON ReadMetadata() const override;
	CityJSONFeatureChunk ReadNthChunk(size_t n) const override;
	CityJSONFeatureChunk ReadAllChunks() const override;
	std::vector<CityJSONFeature> ReadNFeatures(size_t n) const override;
	std::vector<Column> Columns() const override;
	size_t CountCityObjects() const override;

	/**
	 * Read the next feature line from the stream.
	 * Returns nullopt when the stream is exhausted.
	 */
	std::optional<CityJSONFeature> ReadNextFeature() const;

private:
	std::string file_path_;              // Path to CityJSONSeq file
	size_t sample_lines_;                // Number of features to sample for schema inference
	duckdb::ClientContext &context_;     // DuckDB client context for FileSystem access

	// Caching fields (mutable for lazy initialization in const methods)
	mutable std::optional<CityJSON> cached_metadata_;
	mutable std::optional<std::vector<Column>> cached_columns_;
	mutable std::unique_ptr<duckdb::FileHandle> handle_; // FileHandle for incremental reads
	mutable bool metadata_read_ = false;                 // Whether the metadata line has been consumed

	// Internal helper: open or reopen the file handle positioned at the start
	void OpenHandle() const;
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
/**
 * Factory function using DuckDB FileSystem API for remote/local files.
 * CityJSONSeq files are opened incrementally via FileHandle rather than
 * loaded entirely into memory.
 *
 * @param context DuckDB client context
 * @param file_name Path or URL to CityJSON file
 * @param sample_lines Number of features to sample for schema inference
 * @return Unique pointer to appropriate reader implementation
 */
std::unique_ptr<CityJSONReader> OpenAnyCityJSONFile(duckdb::ClientContext &context, const std::string &file_name,
                                                    size_t sample_lines = 100);

} // namespace cityjson
} // namespace duckdb
