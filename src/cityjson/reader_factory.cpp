#include "cityjson/reader.hpp"
#include "cityjson/json_utils.hpp"
#ifdef CITYJSON_HAS_FCB
#include "cityjson/flatcitybuf_reader.hpp"
#endif
#include <algorithm>

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
 * Try to detect format from the first line of file content.
 * Returns true if file appears to be CityJSONSeq format
 */
static bool IsLikelyCityJSONSeqContent(const std::string &content) {
	std::string first_line;
	auto newline = content.find('\n');
	if (newline == std::string::npos) {
		first_line = content;
	} else {
		first_line = content.substr(0, newline);
	}
	if (first_line.empty()) {
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

std::unique_ptr<CityJSONReader> OpenCityJSONSeqFile(duckdb::ClientContext &context, const std::string &file_name,
                                                    size_t sample_lines) {
	// read_cityjsonseq must only ever construct a sequence reader. Auto-detecting the
	// format here (as OpenAnyCityJSONFile does) would let a regular CityJSON document
	// fall through to LocalCityJSONReader, whose streaming ReadNextFeature() yields no
	// rows — silently turning malformed input into an empty result. The sequence reader
	// rejects non-sequence content during ReadMetadata() instead.
	return std::make_unique<LocalCityJSONSeqReader>(context, file_name, sample_lines);
}

std::unique_ptr<CityJSONReader> OpenAnyCityJSONFile(duckdb::ClientContext &context, const std::string &file_name,
                                                    size_t sample_lines) {
#ifdef CITYJSON_HAS_FCB
	// FlatCityBuf format — FCB API reads directly from file path
	if (EndsWith(file_name, ".fcb")) {
		return std::make_unique<FlatCityBufReader>(context, file_name, file_name, sample_lines);
	}
#endif

	// Try to detect format from extension first
	if (EndsWith(file_name, ".city.jsonl") || EndsWith(file_name, ".jsonl")) {
		return std::make_unique<LocalCityJSONSeqReader>(context, file_name, sample_lines);
	}

	if (EndsWith(file_name, ".city.json") || EndsWith(file_name, ".json")) {
		// Could be either format - check content to be sure
		auto content = json_utils::ReadFileContent(context, file_name);
		if (IsLikelyCityJSONSeqContent(content)) {
			return std::make_unique<LocalCityJSONSeqReader>(context, file_name, sample_lines);
		} else {
			return std::make_unique<LocalCityJSONReader>(file_name, std::move(content), sample_lines);
		}
	}

	// Unknown extension - try to auto-detect from content
	auto content = json_utils::ReadFileContent(context, file_name);
	if (IsLikelyCityJSONSeqContent(content)) {
		return std::make_unique<LocalCityJSONSeqReader>(context, file_name, sample_lines);
	} else {
		// Default to CityJSON format
		return std::make_unique<LocalCityJSONReader>(file_name, std::move(content), sample_lines);
	}
}

std::unique_ptr<CityJSONReader> OpenCityJSONFileOfKind(duckdb::ClientContext &context, ReaderKind kind,
                                                       const std::string &file_name, size_t sample_lines) {
	switch (kind) {
	case ReaderKind::CityJSONSeq:
		return OpenCityJSONSeqFile(context, file_name, sample_lines);
	case ReaderKind::Auto:
	default:
		return OpenAnyCityJSONFile(context, file_name, sample_lines);
	}
}

} // namespace cityjson
} // namespace duckdb
