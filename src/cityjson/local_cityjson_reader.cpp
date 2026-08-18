#include "cityjson/reader.hpp"
#include "cityjson/city_object_utils.hpp"
#include <fstream>
#include <algorithm>

namespace duckdb {
namespace cityjson {

using namespace json_utils; // NOLINT(google-build-using-namespace)

// ============================================================
// Constructors
// ============================================================

LocalCityJSONReader::LocalCityJSONReader(const std::string &file_path, size_t sample_lines)
    : file_path_(file_path), sample_lines_(sample_lines) {
}

LocalCityJSONReader::LocalCityJSONReader(const std::string &name, std::string content, size_t sample_lines)
    : file_path_(name), sample_lines_(sample_lines), content_(std::move(content)) {
}

// ============================================================
// Name
// ============================================================

std::string LocalCityJSONReader::Name() const {
	return file_path_;
}

// ============================================================
// LoadJson (internal helper)
// ============================================================

const json &LocalCityJSONReader::LoadJson() const {
	if (!cached_json_.has_value()) {
		cached_json_.emplace(content_.has_value() ? ParseJson(content_.value()) : ParseJsonFile(file_path_));
	}
	// Engaged by the branch above on exactly the path where it was not already.
	// clang-tidy's optional model does not track that for a mutable member reached
	// through `this` in a const method, so it reads the return as unchecked.
	// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
	return *cached_json_;
}

// ============================================================
// ReadMetadata
// ============================================================

CityJSON LocalCityJSONReader::ReadMetadata() const {
	// Check cache
	if (cached_metadata_.has_value()) {
		return cached_metadata_.value();
	}

	const json &obj = LoadJson();
	CityJSON metadata = CityJSON::FromJson(obj);

	// Cache the result
	cached_metadata_ = metadata;

	return metadata;
}

// ============================================================
// ReadNFeatures
// ============================================================

// The spec feature_id rule (02-object-table-schema.mdx): a root object's feature_id
// is its own id; a child's is the id of the root reached by following the FIRST
// entry of `parents`. The walk stops at the deepest resolvable object when a parent
// reference dangles, and is bounded by the object count so a malformed cycle
// terminates.
static std::string ResolveRootId(const std::map<std::string, CityObject> &objects, const std::string &id) {
	std::string current = id;
	for (size_t step = 0; step < objects.size(); step++) {
		auto found = objects.find(current);
		if (found == objects.end() || found->second.parents.empty()) {
			return current;
		}
		const auto &parent = found->second.parents.front();
		if (objects.find(parent) == objects.end()) {
			return current;
		}
		current = parent;
	}
	return current;
}

std::vector<CityJSONFeature> LocalCityJSONReader::ReadNFeatures(size_t n) const {
	const json &obj = LoadJson();

	// Validate structure
	if (!obj.contains("CityObjects") || !obj["CityObjects"].is_object()) {
		throw CityJSONError::InvalidSchema("CityJSON file missing 'CityObjects' field");
	}

	// For CityJSON format, all CityObjects are in one implicit feature
	// We'll create a single feature containing up to N CityObjects
	CityJSONFeature feature;
	feature.id = file_path_; // Use file path as feature ID
	feature.type = "CityJSONFeature";

	const auto &city_objects = obj["CityObjects"];
	size_t count = 0;

	for (auto &[obj_id, obj_data] : city_objects.items()) {
		if (count >= n) {
			break;
		}
		feature.city_objects[obj_id] = CityObject::FromJson(obj_data);
		count++;
	}

	for (auto &[obj_id, obj] : feature.city_objects) {
		obj.feature_id = ResolveRootId(feature.city_objects, obj_id);
	}

	return {feature};
}

// ============================================================
// ReadAllChunks
// ============================================================

CityJSONFeatureChunk LocalCityJSONReader::ReadAllChunks() const {
	const json &obj = LoadJson();

	// Validate structure
	if (!obj.contains("CityObjects") || !obj["CityObjects"].is_object()) {
		throw CityJSONError::InvalidSchema("CityJSON file missing 'CityObjects' field");
	}

	// Convert all CityObjects to a single feature
	CityJSONFeature feature;
	feature.id = file_path_;
	feature.type = "CityJSONFeature";

	const auto &city_objects = obj["CityObjects"];
	for (auto &[obj_id, obj_data] : city_objects.items()) {
		feature.city_objects[obj_id] = CityObject::FromJson(obj_data);
	}

	for (auto &[obj_id, obj] : feature.city_objects) {
		obj.feature_id = ResolveRootId(feature.city_objects, obj_id);
	}

	// Create chunks
	std::vector<CityJSONFeature> features = {feature};
	return CityJSONFeatureChunk::CreateChunks(std::move(features), STANDARD_VECTOR_SIZE);
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
	std::vector<Column> attr_columns = CityObjectUtils::InferAttributeColumns(sample_features, sample_lines_);

	// Infer geometry columns
	std::vector<Column> geom_columns = CityObjectUtils::InferGeometryColumns(sample_features, sample_lines_);

	// Merge all columns: predefined + attributes + geometries
	columns.insert(columns.end(), attr_columns.begin(), attr_columns.end());
	columns.insert(columns.end(), geom_columns.begin(), geom_columns.end());

	// Cache the result
	cached_columns_ = columns;

	return columns;
}

} // namespace cityjson
} // namespace duckdb
