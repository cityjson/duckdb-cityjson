#include "cityjson/json_utils.hpp"
#include <fstream>
#include <sstream>

namespace duckdb {
namespace cityjson {
namespace json_utils {

json ParseJson(const std::string &str) {
	try {
		return json::parse(str);
	} catch (const json::parse_error &e) {
		throw CityJSONError::InvalidJson("Failed to parse JSON: " + std::string(e.what()));
	}
}

json ParseJsonFile(const std::string &file_path) {
	std::ifstream file(file_path);
	if (!file.is_open()) {
		throw CityJSONError::FileRead("Failed to open file: " + file_path);
	}

	try {
		json result;
		file >> result;
		return result;
	} catch (const json::parse_error &e) {
		throw CityJSONError::InvalidJson("Failed to parse JSON from file: " + std::string(e.what()), file_path);
	} catch (const std::exception &e) {
		throw CityJSONError::FileRead("Failed to read file: " + std::string(e.what()), file_path);
	}
}

std::string JsonToString(const json &value, bool pretty_print) {
	if (pretty_print) {
		return value.dump(2); // 2-space indentation
	} else {
		return value.dump();
	}
}

bool IsNullOrMissing(const json &value) {
	return value.is_null();
}

std::string GetString(const json &obj, const std::string &key, const std::string &default_value) {
	if (!obj.is_object() || !obj.contains(key) || obj[key].is_null()) {
		return default_value;
	}

	try {
		return obj[key].get<std::string>();
	} catch (const json::type_error &e) {
		throw CityJSONError::InvalidJson("Expected string for key '" + key + "': " + std::string(e.what()));
	}
}

std::optional<std::string> GetOptionalString(const json &obj, const std::string &key) {
	if (!obj.is_object() || !obj.contains(key) || obj[key].is_null()) {
		return std::nullopt;
	}

	try {
		return obj[key].get<std::string>();
	} catch (const json::type_error &e) {
		throw CityJSONError::InvalidJson("Expected string for key '" + key + "': " + std::string(e.what()));
	}
}

int64_t GetInt(const json &obj, const std::string &key, int64_t default_value) {
	if (!obj.is_object() || !obj.contains(key) || obj[key].is_null()) {
		return default_value;
	}

	try {
		return obj[key].get<int64_t>();
	} catch (const json::type_error &e) {
		throw CityJSONError::InvalidJson("Expected integer for key '" + key + "': " + std::string(e.what()));
	}
}

std::optional<int64_t> GetOptionalInt(const json &obj, const std::string &key) {
	if (!obj.is_object() || !obj.contains(key) || obj[key].is_null()) {
		return std::nullopt;
	}

	try {
		return obj[key].get<int64_t>();
	} catch (const json::type_error &e) {
		throw CityJSONError::InvalidJson("Expected integer for key '" + key + "': " + std::string(e.what()));
	}
}

double GetDouble(const json &obj, const std::string &key, double default_value) {
	if (!obj.is_object() || !obj.contains(key) || obj[key].is_null()) {
		return default_value;
	}

	try {
		return obj[key].get<double>();
	} catch (const json::type_error &e) {
		throw CityJSONError::InvalidJson("Expected number for key '" + key + "': " + std::string(e.what()));
	}
}

std::optional<double> GetOptionalDouble(const json &obj, const std::string &key) {
	if (!obj.is_object() || !obj.contains(key) || obj[key].is_null()) {
		return std::nullopt;
	}

	try {
		return obj[key].get<double>();
	} catch (const json::type_error &e) {
		throw CityJSONError::InvalidJson("Expected number for key '" + key + "': " + std::string(e.what()));
	}
}

bool GetBool(const json &obj, const std::string &key, bool default_value) {
	if (!obj.is_object() || !obj.contains(key) || obj[key].is_null()) {
		return default_value;
	}

	try {
		return obj[key].get<bool>();
	} catch (const json::type_error &e) {
		throw CityJSONError::InvalidJson("Expected boolean for key '" + key + "': " + std::string(e.what()));
	}
}

std::optional<bool> GetOptionalBool(const json &obj, const std::string &key) {
	if (!obj.is_object() || !obj.contains(key) || obj[key].is_null()) {
		return std::nullopt;
	}

	try {
		return obj[key].get<bool>();
	} catch (const json::type_error &e) {
		throw CityJSONError::InvalidJson("Expected boolean for key '" + key + "': " + std::string(e.what()));
	}
}

json GetObject(const json &obj, const std::string &key) {
	if (!obj.is_object()) {
		throw CityJSONError::InvalidJson("Expected JSON object");
	}

	if (!obj.contains(key)) {
		throw CityJSONError::MissingField("Required key '" + key + "' is missing");
	}

	const auto &value = obj[key];
	if (value.is_null()) {
		throw CityJSONError::InvalidJson("Key '" + key + "' is null");
	}

	if (!value.is_object()) {
		throw CityJSONError::InvalidJson("Key '" + key + "' is not an object");
	}

	return value;
}

std::optional<json> GetOptionalObject(const json &obj, const std::string &key) {
	if (!obj.is_object() || !obj.contains(key) || obj[key].is_null()) {
		return std::nullopt;
	}

	const auto &value = obj[key];
	if (!value.is_object()) {
		throw CityJSONError::InvalidJson("Key '" + key + "' is not an object");
	}

	return value;
}

json GetArray(const json &obj, const std::string &key) {
	if (!obj.is_object()) {
		throw CityJSONError::InvalidJson("Expected JSON object");
	}

	if (!obj.contains(key)) {
		throw CityJSONError::MissingField("Required key '" + key + "' is missing");
	}

	const auto &value = obj[key];
	if (value.is_null()) {
		throw CityJSONError::InvalidJson("Key '" + key + "' is null");
	}

	if (!value.is_array()) {
		throw CityJSONError::InvalidJson("Key '" + key + "' is not an array");
	}

	return value;
}

std::optional<json> GetOptionalArray(const json &obj, const std::string &key) {
	if (!obj.is_object() || !obj.contains(key) || obj[key].is_null()) {
		return std::nullopt;
	}

	const auto &value = obj[key];
	if (!value.is_array()) {
		throw CityJSONError::InvalidJson("Key '" + key + "' is not an array");
	}

	return value;
}

bool HasKey(const json &obj, const std::string &key) {
	return obj.is_object() && obj.contains(key) && !obj[key].is_null();
}

void ValidateRequiredKeys(const json &obj, const std::vector<std::string> &required_keys) {
	if (!obj.is_object()) {
		throw CityJSONError::InvalidJson("Expected JSON object for key validation");
	}

	for (const auto &key : required_keys) {
		if (!obj.contains(key)) {
			throw CityJSONError::MissingField("Required key '" + key + "' is missing");
		}
		if (obj[key].is_null()) {
			throw CityJSONError::InvalidJson("Required key '" + key + "' is null");
		}
	}
}

} // namespace json_utils
} // namespace cityjson
} // namespace duckdb
