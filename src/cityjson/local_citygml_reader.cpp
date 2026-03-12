#include "cityjson/reader.hpp"
#include "cityjson/citygml_parser.hpp"
#include "cityjson/city_object_utils.hpp"
#include "cityjson/column_types.hpp"
#include "cityjson/error.hpp"
#include <fstream>
#include <sstream>

namespace duckdb {
namespace cityjson {

// ============================================================
// LocalCityGMLReader
// ============================================================

LocalCityGMLReader::LocalCityGMLReader(const std::string &file_path, size_t sample_lines)
    : file_path_(file_path), sample_lines_(sample_lines) {
}

LocalCityGMLReader::LocalCityGMLReader(const std::string &name, std::string content, size_t sample_lines)
    : file_path_(name), sample_lines_(sample_lines), content_(std::move(content)) {
}

std::string LocalCityGMLReader::Name() const {
	return file_path_;
}

const LocalCityGMLReader::CachedParse &LocalCityGMLReader::EnsureParsed() const {
	if (!cached_parse_.has_value()) {
		std::string xml_content;
		if (content_.has_value()) {
			xml_content = content_.value();
		} else {
			std::ifstream file(file_path_);
			if (!file.is_open()) {
				throw CityJSONError::FileRead("Cannot open CityGML file: " + file_path_);
			}
			std::ostringstream ss;
			ss << file.rdbuf();
			xml_content = ss.str();
		}

		auto result = CityGMLParser::Parse(xml_content);

		CachedParse cp;
		cp.metadata = std::move(result.metadata);
		cp.features = std::move(result.features);
		cached_parse_ = std::move(cp);
	}
	return cached_parse_.value();
}

CityJSON LocalCityGMLReader::ReadMetadata() const {
	return EnsureParsed().metadata;
}

CityJSONFeatureChunk LocalCityGMLReader::ReadAllChunks() const {
	const auto &parsed = EnsureParsed();

	// Copy features for chunking (CreateChunks takes ownership)
	std::vector<CityJSONFeature> features_copy = parsed.features;
	return CityJSONFeatureChunk::CreateChunks(std::move(features_copy), 2048);
}

CityJSONFeatureChunk LocalCityGMLReader::ReadNthChunk(size_t n) const {
	auto all = ReadAllChunks();

	if (n >= all.ChunkCount()) {
		return CityJSONFeatureChunk();
	}
	return all;
}

std::vector<CityJSONFeature> LocalCityGMLReader::ReadNFeatures(size_t n) const {
	const auto &parsed = EnsureParsed();

	if (n >= parsed.features.size()) {
		return parsed.features;
	}

	return std::vector<CityJSONFeature>(parsed.features.begin(), parsed.features.begin() + n);
}

std::vector<Column> LocalCityGMLReader::Columns() const {
	if (!cached_columns_.has_value()) {
		auto columns = GetDefinedColumns();

		auto features = ReadNFeatures(sample_lines_);

		auto attr_columns = CityObjectUtils::InferAttributeColumns(features);
		columns.insert(columns.end(), attr_columns.begin(), attr_columns.end());

		auto geom_columns = CityObjectUtils::InferGeometryColumns(features);
		columns.insert(columns.end(), geom_columns.begin(), geom_columns.end());

		cached_columns_ = columns;
	}

	return cached_columns_.value();
}

} // namespace cityjson
} // namespace duckdb
