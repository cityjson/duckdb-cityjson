#include "cityjson/crs_projjson.hpp"
#include "cityjson/epsg_projjson_data.hpp"
#include "cityjson/json_utils.hpp"
#include "duckdb/common/gzip_file_system.hpp"

#include <cctype>
#include <mutex>

namespace duckdb {
namespace cityjson {

namespace {

// The embedded table, decompressed and parsed exactly once.
const json &EpsgTable() {
	static json table = [] {
		std::string compressed(reinterpret_cast<const char *>(EPSG_PROJJSON_GZ), EPSG_PROJJSON_GZ_LEN);
		std::string decompressed = GZipFileSystem::UncompressGZIPString(compressed);
		return json_utils::ParseJson(decompressed);
	}();
	return table;
}

std::string ToLower(const std::string &s) {
	std::string out = s;
	for (auto &c : out) {
		c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
	}
	return out;
}

// The last maximal run of decimal digits in `s`, or empty if none.
std::string LastDigitRun(const std::string &s) {
	size_t end = s.size();
	while (end > 0 && !std::isdigit(static_cast<unsigned char>(s[end - 1]))) {
		end--;
	}
	if (end == 0) {
		return "";
	}
	size_t start = end;
	while (start > 0 && std::isdigit(static_cast<unsigned char>(s[start - 1]))) {
		start--;
	}
	return s.substr(start, end - start);
}

bool IsAllDigits(const std::string &s) {
	if (s.empty()) {
		return false;
	}
	for (char c : s) {
		if (!std::isdigit(static_cast<unsigned char>(c))) {
			return false;
		}
	}
	return true;
}

bool IsCrs84(const std::string &lower) {
	// OGC:CRS84 in its short, URN, and URL forms (2D lon/lat WGS84).
	return lower == "ogc:crs84" || lower.find("crs84") != std::string::npos;
}

} // namespace

std::optional<int> EpsgCodeFromReferenceSystem(const std::string &reference_system) {
	const std::string lower = ToLower(reference_system);
	std::string digits;
	if (lower.find("epsg") != std::string::npos) {
		digits = LastDigitRun(reference_system);
	} else if (IsAllDigits(reference_system)) {
		digits = reference_system;
	}
	if (digits.empty()) {
		return std::nullopt;
	}
	try {
		return std::stoi(digits);
	} catch (...) {
		return std::nullopt;
	}
}

std::optional<std::string> ProjjsonForEpsg(int code) {
	const json &table = EpsgTable();
	const std::string key = std::to_string(code);
	auto it = table.find(key);
	if (it == table.end()) {
		return std::nullopt;
	}
	return it->dump();
}

std::optional<std::string> ProjjsonForReferenceSystem(const std::string &reference_system) {
	const std::string lower = ToLower(reference_system);
	// CRS84 must be checked before EPSG-code extraction: some URL forms also
	// contain digits (version segments) that must not be read as a code.
	if (IsCrs84(lower)) {
		const json &table = EpsgTable();
		auto it = table.find("OGC:CRS84");
		if (it != table.end()) {
			return it->dump();
		}
		return std::nullopt;
	}
	auto code = EpsgCodeFromReferenceSystem(reference_system);
	if (!code.has_value()) {
		return std::nullopt;
	}
	return ProjjsonForEpsg(code.value());
}

} // namespace cityjson
} // namespace duckdb
