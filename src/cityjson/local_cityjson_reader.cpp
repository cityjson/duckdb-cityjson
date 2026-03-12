#include "cityjson/reader.hpp"
#include "cityjson/city_object_utils.hpp"
#include <fstream>
#include <algorithm>
#include <unordered_map>
#include <unordered_set>
#include <queue>

namespace duckdb {
namespace cityjson {

using namespace json_utils; // NOLINT(google-build-using-namespace)

// ============================================================
// GroupCityObjectsIntoFeatures
// ============================================================
// Groups CityObjects into CityJSONFeatures based on parent/child
// relationships, following the CityJSONFeature specification.
// Root objects (those with no parents) become feature roots.
// Each feature contains the root and all its descendants.

static std::vector<CityJSONFeature>
GroupCityObjectsIntoFeatures(const std::map<std::string, CityObject> &city_objects) {
	// Step 1: Find root objects (objects with no parents)
	std::vector<std::string> root_ids;
	for (const auto &[obj_id, obj] : city_objects) {
		if (obj.parents.empty()) {
			root_ids.push_back(obj_id);
		}
	}

	// Step 2: For each root, collect all descendants via BFS on children
	std::vector<CityJSONFeature> features;
	std::unordered_set<std::string> assigned; // track which objects are already assigned

	for (const auto &root_id : root_ids) {
		CityJSONFeature feature;
		feature.id = root_id;
		feature.type = "CityJSONFeature";

		// BFS to collect root + all descendants
		std::queue<std::string> queue;
		queue.push(root_id);

		while (!queue.empty()) {
			std::string current = queue.front();
			queue.pop();

			if (assigned.count(current)) {
				continue;
			}

			auto it = city_objects.find(current);
			if (it == city_objects.end()) {
				continue;
			}

			assigned.insert(current);
			feature.city_objects[current] = it->second;

			// Enqueue children
			for (const auto &child_id : it->second.children) {
				if (!assigned.count(child_id)) {
					queue.push(child_id);
				}
			}
		}

		features.push_back(std::move(feature));
	}

	// Step 3: Handle orphan objects (parents reference non-existent objects)
	// These become their own single-object features
	for (const auto &[obj_id, obj] : city_objects) {
		if (!assigned.count(obj_id)) {
			CityJSONFeature feature;
			feature.id = obj_id;
			feature.type = "CityJSONFeature";
			feature.city_objects[obj_id] = obj;
			features.push_back(std::move(feature));
		}
	}

	return features;
}

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

json LocalCityJSONReader::LoadJson() const {
	if (content_.has_value()) {
		return ParseJson(content_.value());
	}
	return ParseJsonFile(file_path_);
}

// ============================================================
// ReadMetadata
// ============================================================

CityJSON LocalCityJSONReader::ReadMetadata() const {
	// Check cache
	if (cached_metadata_.has_value()) {
		return cached_metadata_.value();
	}

	json obj = LoadJson();
	CityJSON metadata = CityJSON::FromJson(obj);

	// Cache the result
	cached_metadata_ = metadata;

	return metadata;
}

// ============================================================
// ReadNFeatures
// ============================================================

std::vector<CityJSONFeature> LocalCityJSONReader::ReadNFeatures(size_t n) const {
	json obj = LoadJson();

	// Validate structure
	if (!obj.contains("CityObjects") || !obj["CityObjects"].is_object()) {
		throw CityJSONError::InvalidSchema("CityJSON file missing 'CityObjects' field");
	}

	// Parse all CityObjects first
	std::map<std::string, CityObject> all_objects;
	const auto &city_objects = obj["CityObjects"];
	for (auto &[obj_id, obj_data] : city_objects.items()) {
		all_objects[obj_id] = CityObject::FromJson(obj_data);
	}

	// Group into features based on parent/child relationships
	auto features = GroupCityObjectsIntoFeatures(all_objects);

	// Return up to n features
	if (features.size() > n) {
		features.resize(n);
	}

	return features;
}

// ============================================================
// ReadAllChunks
// ============================================================

CityJSONFeatureChunk LocalCityJSONReader::ReadAllChunks() const {
	json obj = LoadJson();

	// Validate structure
	if (!obj.contains("CityObjects") || !obj["CityObjects"].is_object()) {
		throw CityJSONError::InvalidSchema("CityJSON file missing 'CityObjects' field");
	}

	// Parse all CityObjects
	std::map<std::string, CityObject> all_objects;
	const auto &city_objects = obj["CityObjects"];
	for (auto &[obj_id, obj_data] : city_objects.items()) {
		all_objects[obj_id] = CityObject::FromJson(obj_data);
	}

	// Group into features based on parent/child relationships
	auto features = GroupCityObjectsIntoFeatures(all_objects);

	// Create chunks
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
