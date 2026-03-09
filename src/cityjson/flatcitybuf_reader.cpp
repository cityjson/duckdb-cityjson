#ifdef CITYJSON_HAS_FCB

#include "cityjson/flatcitybuf_reader.hpp"
#include "cityjson/city_object_utils.hpp"
#include "cityjson/json_utils.hpp"
#include "fcb.h"

namespace duckdb {
namespace cityjson {

using namespace json_utils;

// ============================================================
// Constructor
// ============================================================

FlatCityBufReader::FlatCityBufReader(const std::string &name, const std::string &file_path, size_t sample_lines)
    : name_(name), file_path_(file_path), sample_lines_(sample_lines) {
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

	// Open the FCB file and read metadata
	auto reader = fcb::fcb_reader_open(file_path_);
	auto meta = fcb::fcb_reader_metadata(*reader);

	CityJSON metadata;
	metadata.version = std::string(meta.cityjson_version);

	// Extract transform from typed fields
	if (meta.has_transform) {
		Transform t;
		t.scale = {meta.transform.scale_x, meta.transform.scale_y, meta.transform.scale_z};
		t.translate = {meta.transform.translate_x, meta.transform.translate_y, meta.transform.translate_z};
		metadata.transform = t;
	}

	// Parse the full CityJSON header JSON for remaining metadata fields
	std::string metadata_json_str(meta.metadata_json);
	if (!metadata_json_str.empty()) {
		try {
			json cj = ParseJson(metadata_json_str);

			// Parse metadata section (title, identifier, referenceDate, pointOfContact, etc.)
			if (cj.contains("metadata") && cj["metadata"].is_object()) {
				metadata.metadata = Metadata::FromJson(cj["metadata"]);
			}

			// Parse geographical extent from typed fields (more reliable) or from metadata
			if (meta.has_geographical_extent) {
				GeographicalExtent extent(meta.geographical_extent.min_x, meta.geographical_extent.min_y,
				                          meta.geographical_extent.min_z, meta.geographical_extent.max_x,
				                          meta.geographical_extent.max_y, meta.geographical_extent.max_z);
				if (!metadata.metadata.has_value()) {
					metadata.metadata = Metadata();
				}
				metadata.metadata->geographic_extent = extent;
			}

			// Parse CRS from metadata.referenceSystem or top-level
			if (cj.contains("metadata") && cj["metadata"].contains("referenceSystem")) {
				auto &rs = cj["metadata"]["referenceSystem"];
				if (rs.is_string()) {
					metadata.crs = CRS(rs.get<std::string>());
				}
			}

			// Parse extensions
			if (cj.contains("extensions") && cj["extensions"].is_object()) {
				for (auto &[name, ext_data] : cj["extensions"].items()) {
					metadata.extensions[name] = Extension::FromJson(ext_data);
				}
			}
		} catch (const std::exception &) {
			// If metadata_json parsing fails, we still have the typed fields above
		}
	}

	cached_metadata_ = metadata;
	return metadata;
}

// ============================================================
// ParseAllFeatures
// ============================================================

std::vector<CityJSONFeature> FlatCityBufReader::ParseAllFeatures() const {
	auto reader = fcb::fcb_reader_open(file_path_);

	std::vector<CityJSONFeature> features;

	// Select all features — this consumes the reader (Rust ownership)
	auto iter = fcb::fcb_reader_select_all(std::move(reader));
	while (fcb::fcb_iterator_next(*iter)) {
		auto feat_data = fcb::fcb_iterator_current(*iter);
		std::string json_str(feat_data.json.data(), feat_data.json.size());
		try {
			json feature_obj = ParseJson(json_str);
			CityJSONFeature feature = CityJSONFeature::FromJson(feature_obj);
			features.push_back(std::move(feature));
		} catch (const CityJSONError &e) {
			// Skip malformed features
		}
	}

	return features;
}

// ============================================================
// ReadNFeatures
// ============================================================

std::vector<CityJSONFeature> FlatCityBufReader::ReadNFeatures(size_t n) const {
	auto reader = fcb::fcb_reader_open(file_path_);

	std::vector<CityJSONFeature> features;

	auto iter = fcb::fcb_reader_select_all(std::move(reader));
	while (fcb::fcb_iterator_next(*iter) && features.size() < n) {
		auto feat_data = fcb::fcb_iterator_current(*iter);
		std::string json_str(feat_data.json.data(), feat_data.json.size());
		try {
			json feature_obj = ParseJson(json_str);
			CityJSONFeature feature = CityJSONFeature::FromJson(feature_obj);
			features.push_back(std::move(feature));
		} catch (const CityJSONError &e) {
			// Skip malformed features
		}
	}

	return features;
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
	auto reader = fcb::fcb_reader_open(file_path_);
	auto meta = fcb::fcb_reader_metadata(*reader);
	if (meta.has_geographical_extent) {
		return std::array<double, 4> {meta.geographical_extent.min_x, meta.geographical_extent.min_y,
		                              meta.geographical_extent.max_x, meta.geographical_extent.max_y};
	}
	return std::nullopt;
}

} // namespace cityjson
} // namespace duckdb

#endif // CITYJSON_HAS_FCB
