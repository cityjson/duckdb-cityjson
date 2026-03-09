#pragma once

#ifdef CITYJSON_HAS_FCB

#include "cityjson/reader.hpp"
#include <string>
#include <vector>
#include <memory>
#include <optional>

namespace duckdb {
class ClientContext;

namespace cityjson {

/**
 * Reader for FlatCityBuf (.fcb) format
 * Cloud-optimized binary CityJSON format
 * Each feature is returned as CityJSONFeature JSON for reuse with existing parsing
 *
 * Uses the fcb:: C++ API which reads directly from file paths.
 */
class FlatCityBufReader : public CityJSONReader {
public:
	/**
	 * Construct reader from a local file path
	 *
	 * @param name Display name (file path or URL)
	 * @param file_path Local file path the FCB API will open directly
	 * @param sample_lines Number of features to sample for schema inference
	 */
	FlatCityBufReader(const std::string &name, const std::string &file_path, size_t sample_lines = 100);

	std::string Name() const override;
	CityJSON ReadMetadata() const override;
	CityJSONFeatureChunk ReadNthChunk(size_t n) const override;
	CityJSONFeatureChunk ReadAllChunks() const override;
	std::vector<CityJSONFeature> ReadNFeatures(size_t n) const override;
	std::vector<Column> Columns() const override;

	/**
	 * Get bounding box of the FCB file
	 * @return Optional array of [minx, miny, maxx, maxy]
	 */
	std::optional<std::array<double, 4>> GetBBox() const;

private:
	std::string name_;
	std::string file_path_;
	size_t sample_lines_;

	mutable std::optional<CityJSON> cached_metadata_;
	mutable std::optional<std::vector<Column>> cached_columns_;

	// Parse all features from the FCB file
	std::vector<CityJSONFeature> ParseAllFeatures() const;
};

} // namespace cityjson
} // namespace duckdb

#endif // CITYJSON_HAS_FCB
