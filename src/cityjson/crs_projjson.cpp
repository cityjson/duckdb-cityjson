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

// Drop a URL query string / fragment (`?…`, `#…`) so a `?version=2`-style suffix
// is not mistaken for the authority code.
std::string StripQueryFragment(const std::string &s) {
	size_t cut = s.find_first_of("?#");
	return cut == std::string::npos ? s : s.substr(0, cut);
}

// The final `/`- or `:`-separated component (an authority code sits at the end of
// every accepted CRS form: EPSG:7415, urn:…:EPSG::7415, …/EPSG/0/7415, OGC:CRS84).
std::string LastComponent(const std::string &s) {
	size_t pos = s.find_last_of("/:");
	return pos == std::string::npos ? s : s.substr(pos + 1);
}

// The recognised CRS84 forms map to distinct table keys: CRS84 (2D lon/lat) and
// CRS84h (3D). Matched as the whole final component so `CRS840` / `CRS84h` are not
// conflated and a stray `crs84` inside a query string does not trigger.
std::optional<std::string> Crs84Key(const std::string &stripped_lower) {
	std::string lc = LastComponent(stripped_lower);
	if (lc == "crs84") {
		return std::string("OGC:CRS84");
	}
	if (lc == "crs84h") {
		return std::string("OGC:CRS84h");
	}
	return std::nullopt;
}

} // namespace

std::optional<int> EpsgCodeFromReferenceSystem(const std::string &reference_system) {
	const std::string stripped = StripQueryFragment(reference_system);
	const std::string lower = ToLower(stripped);
	std::string digits;
	if (lower.find("epsg") != std::string::npos) {
		digits = LastDigitRun(stripped);
	} else if (IsAllDigits(stripped)) {
		digits = stripped;
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
	const std::string stripped_lower = ToLower(StripQueryFragment(reference_system));
	// CRS84/CRS84h must be checked before EPSG-code extraction: some URL forms also
	// contain digits (version segments) that must not be read as a code.
	if (auto key = Crs84Key(stripped_lower)) {
		const json &table = EpsgTable();
		auto it = table.find(key.value());
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
