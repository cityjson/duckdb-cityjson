#include "cityjson/reader.hpp"
#include "cityjson/json_utils.hpp"
#ifdef CITYJSON_HAS_FCB
#include "cityjson/flatcitybuf_reader.hpp"
#endif
#include <algorithm>
#include <fstream>

namespace duckdb {
namespace cityjson {

// Default sample lines for schema inference
constexpr size_t DEFAULT_SAMPLE_LINES = 100;

/**
 * Check if string ends with suffix (case-insensitive)
 */
static bool EndsWith(const std::string &str, const std::string &suffix) {
	if (suffix.length() > str.length()) {
		return false;
	}

	auto it1 = str.end() - suffix.length();
	auto it2 = suffix.begin();

	while (it2 != suffix.end()) {
		if (std::tolower(*it1) != std::tolower(*it2)) {
			return false;
		}
		++it1;
		++it2;
	}

	return true;
}

/**
 * Try to detect format by reading first line of file
 * Returns true if file appears to be CityJSONSeq format
 */
static bool IsLikelyCityJSONSeq(const std::string &file_name) {
	std::ifstream file(file_name);
	if (!file.is_open()) {
		return false;
	}

	std::string first_line;
	if (!std::getline(file, first_line)) {
		return false;
	}

	// CityJSONSeq first line should contain "type":"CityJSON" and no "CityObjects"
	// This is a heuristic check
	bool has_cityjson_type =
	    first_line.find("\"type\"") != std::string::npos && first_line.find("\"CityJSON\"") != std::string::npos;
	bool has_city_objects = first_line.find("\"CityObjects\"") != std::string::npos;

	// If it has CityJSON type but no CityObjects, likely metadata line
	return has_cityjson_type && !has_city_objects;
}

/**
 * Detect format from content string (first line heuristic)
 */
static bool IsLikelyCityJSONSeqFromContent(const std::string &content) {
	// Find the first newline
	auto pos = content.find('\n');
	std::string first_line = (pos != std::string::npos) ? content.substr(0, pos) : content;

	bool has_cityjson_type =
	    first_line.find("\"type\"") != std::string::npos && first_line.find("\"CityJSON\"") != std::string::npos;
	bool has_city_objects = first_line.find("\"CityObjects\"") != std::string::npos;

	return has_cityjson_type && !has_city_objects;
}

std::unique_ptr<CityJSONReader> OpenAnyCityJSONFile(const std::string &file_name) {
	// Check if file exists
	std::ifstream test_file(file_name);
	if (!test_file.is_open()) {
		throw CityJSONError::FileRead("File not found: " + file_name);
	}
	test_file.close();

	// Try to detect format from extension first
	if (EndsWith(file_name, ".city.jsonl") || EndsWith(file_name, ".jsonl")) {
		// CityJSONSeq format
		return std::make_unique<LocalCityJSONSeqReader>(file_name, DEFAULT_SAMPLE_LINES);
	}

	if (EndsWith(file_name, ".city.json") || EndsWith(file_name, ".json")) {
		// Could be either format - check content to be sure
		if (IsLikelyCityJSONSeq(file_name)) {
			return std::make_unique<LocalCityJSONSeqReader>(file_name, DEFAULT_SAMPLE_LINES);
		} else {
			return std::make_unique<LocalCityJSONReader>(file_name, DEFAULT_SAMPLE_LINES);
		}
	}

	// Unknown extension - try to auto-detect from content
	if (IsLikelyCityJSONSeq(file_name)) {
		return std::make_unique<LocalCityJSONSeqReader>(file_name, DEFAULT_SAMPLE_LINES);
	} else {
		// Default to CityJSON format
		return std::make_unique<LocalCityJSONReader>(file_name, DEFAULT_SAMPLE_LINES);
	}
}

std::unique_ptr<CityJSONReader> OpenAnyCityJSONFile(duckdb::ClientContext &context, const std::string &file_name) {
	// Read file content using DuckDB FileSystem (supports HTTP, S3, GCS, etc.)
	std::string content = json_utils::ReadFileContent(context, file_name);

#ifdef CITYJSON_HAS_FCB
	// FlatCityBuf format — FCB API reads directly from file path
	if (EndsWith(file_name, ".fcb")) {
		return std::make_unique<FlatCityBufReader>(file_name, file_name, DEFAULT_SAMPLE_LINES);
	}
#endif

	// Try to detect format from extension first
	if (EndsWith(file_name, ".city.jsonl") || EndsWith(file_name, ".jsonl")) {
		return std::make_unique<LocalCityJSONSeqReader>(file_name, std::move(content), DEFAULT_SAMPLE_LINES);
	}

	if (EndsWith(file_name, ".city.json") || EndsWith(file_name, ".json")) {
		if (IsLikelyCityJSONSeqFromContent(content)) {
			return std::make_unique<LocalCityJSONSeqReader>(file_name, std::move(content), DEFAULT_SAMPLE_LINES);
		} else {
			return std::make_unique<LocalCityJSONReader>(file_name, std::move(content), DEFAULT_SAMPLE_LINES);
		}
	}

	// Unknown extension - detect from content
	if (IsLikelyCityJSONSeqFromContent(content)) {
		return std::make_unique<LocalCityJSONSeqReader>(file_name, std::move(content), DEFAULT_SAMPLE_LINES);
	} else {
		return std::make_unique<LocalCityJSONReader>(file_name, std::move(content), DEFAULT_SAMPLE_LINES);
	}
}

} // namespace cityjson
} // namespace duckdb
