#ifdef CITYJSON_HAS_FCB

#include "cityjson/flatcitybuf_reader.hpp"
#include "cityjson/city_object_utils.hpp"
#include "cityjson/json_utils.hpp"
#include <flatcitybuf/fcb_reader.h>

namespace duckdb {
namespace cityjson {

using namespace json_utils;

// ============================================================
// Constructor
// ============================================================

FlatCityBufReader::FlatCityBufReader(const std::string &name, std::string content, size_t sample_lines)
    : name_(name), content_(std::move(content)), sample_lines_(sample_lines) {
}

// ============================================================
// Name
// ============================================================

std::string FlatCityBufReader::Name() const {
	return name_;
}

// ============================================================
// ReadMetadata
// ============================================================

CityJSON FlatCityBufReader::ReadMetadata() const {
	if (cached_metadata_.has_value()) {
		return cached_metadata_.value();
	}

	// Use FCB API to read metadata from content buffer
	auto reader = fcb_reader_open_buffer(
	    reinterpret_cast<const uint8_t *>(content_.data()), content_.size());
	if (!reader) {
		throw CityJSONError::FileRead("Failed to open FCB content: " + name_);
	}

	auto meta = fcb_reader_metadata(reader);

	CityJSON metadata;
	metadata.version = meta.version ? std::string(meta.version) : "2.0";

	if (meta.crs) {
		metadata.reference_system = std::string(meta.crs);
	}

	if (meta.has_transform) {
		Transform t;
		t.scale = {meta.transform_scale[0], meta.transform_scale[1], meta.transform_scale[2]};
		t.translate = {meta.transform_translate[0], meta.transform_translate[1], meta.transform_translate[2]};
		metadata.transform = t;
	}

	fcb_reader_close(reader);

	cached_metadata_ = metadata;
	return metadata;
}

// ============================================================
// ParseAllFeatures
// ============================================================

std::vector<CityJSONFeature> FlatCityBufReader::ParseAllFeatures() const {
	auto reader = fcb_reader_open_buffer(
	    reinterpret_cast<const uint8_t *>(content_.data()), content_.size());
	if (!reader) {
		throw CityJSONError::FileRead("Failed to open FCB content: " + name_);
	}

	std::vector<CityJSONFeature> features;

	// Select all features
	auto iter = fcb_reader_select_all(reader);
	while (fcb_reader_iter_has_next(iter)) {
		auto feat = fcb_reader_iter_next(iter);
		if (feat.json_str) {
			try {
				json feature_obj = ParseJson(std::string(feat.json_str));
				CityJSONFeature feature = CityJSONFeature::FromJson(feature_obj);
				features.push_back(std::move(feature));
			} catch (const CityJSONError &e) {
				// Skip malformed features
			}
		}
		fcb_reader_feature_free(feat);
	}

	fcb_reader_iter_free(iter);
	fcb_reader_close(reader);

	return features;
}

// ============================================================
// ReadNFeatures
// ============================================================

std::vector<CityJSONFeature> FlatCityBufReader::ReadNFeatures(size_t n) const {
	auto all = ParseAllFeatures();
	if (all.size() > n) {
		all.resize(n);
	}
	return all;
}

// ============================================================
// ReadAllChunks
// ============================================================

CityJSONFeatureChunk FlatCityBufReader::ReadAllChunks() const {
	auto features = ParseAllFeatures();
	return CityJSONFeatureChunk::CreateChunks(std::move(features), STANDARD_VECTOR_SIZE);
}

// ============================================================
// ReadNthChunk
// ============================================================

CityJSONFeatureChunk FlatCityBufReader::ReadNthChunk(size_t n) const {
	CityJSONFeatureChunk all_chunks = ReadAllChunks();

	if (n >= all_chunks.ChunkCount()) {
		return CityJSONFeatureChunk();
	}

	auto chunk_opt = all_chunks.GetChunk(n);
	if (!chunk_opt.has_value()) {
		return CityJSONFeatureChunk();
	}

	CityJSONFeatureChunk result;
	result.records = std::vector<CityJSONFeature>(chunk_opt->begin(), chunk_opt->end());
	result.chunks = {Range(0, result.records.size())};

	return result;
}

// ============================================================
// Columns
// ============================================================

std::vector<Column> FlatCityBufReader::Columns() const {
	if (cached_columns_.has_value()) {
		return cached_columns_.value();
	}

	std::vector<Column> columns = GetDefinedColumns();
	std::vector<CityJSONFeature> sample_features = ReadNFeatures(sample_lines_);
	std::vector<Column> attr_columns = CityObjectUtils::InferAttributeColumns(sample_features, sample_lines_);
	std::vector<Column> geom_columns = CityObjectUtils::InferGeometryColumns(sample_features, sample_lines_);

	columns.insert(columns.end(), attr_columns.begin(), attr_columns.end());
	columns.insert(columns.end(), geom_columns.begin(), geom_columns.end());

	cached_columns_ = columns;
	return columns;
}

// ============================================================
// GetBBox
// ============================================================

std::optional<std::array<double, 4>> FlatCityBufReader::GetBBox() const {
	auto reader = fcb_reader_open_buffer(
	    reinterpret_cast<const uint8_t *>(content_.data()), content_.size());
	if (!reader) {
		return std::nullopt;
	}

	auto meta = fcb_reader_metadata(reader);
	std::optional<std::array<double, 4>> result;
	if (meta.has_bbox) {
		result = std::array<double, 4>{meta.bbox[0], meta.bbox[1], meta.bbox[2], meta.bbox[3]};
	}

	fcb_reader_close(reader);
	return result;
}

} // namespace cityjson
} // namespace duckdb

#endif // CITYJSON_HAS_FCB
