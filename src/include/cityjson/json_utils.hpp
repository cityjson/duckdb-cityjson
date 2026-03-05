#pragma once

#include "cityjson/error.hpp"
#include <nlohmann/json.hpp>
#include <string>
#include <optional>

namespace duckdb {
class ClientContext;
}

namespace duckdb {
namespace cityjson {

/**
 * Type alias for JSON library
 * Using nlohmann::json as the JSON implementation
 */
using json = nlohmann::json;

/**
 * JSON utility functions for parsing and validation
 */
namespace json_utils {

/**
 * Parse JSON string
 *
 * @param str JSON string to parse
 * @return Parsed JSON value
 * @throws CityJSONError if parsing fails
 */
json ParseJson(const std::string& str);

/**
 * Parse JSON from file
 *
 * @param file_path Path to JSON file
 * @return Parsed JSON value
 * @throws CityJSONError if file cannot be read or parsing fails
 */
json ParseJsonFile(const std::string& file_path);

/**
 * Convert JSON value to string
 *
 * @param value JSON value
 * @param pretty_print Whether to format with indentation (default: false)
 * @return JSON string representation
 */
std::string JsonToString(const json& value, bool pretty_print = false);

/**
 * Check if JSON value is null or missing
 *
 * @param value JSON value to check
 * @return true if value is null or does not exist
 */
bool IsNullOrMissing(const json& value);

/**
 * Get string value from JSON with default
 *
 * @param obj JSON object
 * @param key Key to lookup
 * @param default_value Default value if key is missing or null
 * @return String value or default
 */
std::string GetString(const json& obj, const std::string& key, const std::string& default_value = "");

/**
 * Get optional string value from JSON
 *
 * @param obj JSON object
 * @param key Key to lookup
 * @return Optional string value (nullopt if missing or null)
 */
std::optional<std::string> GetOptionalString(const json& obj, const std::string& key);

/**
 * Get integer value from JSON with default
 *
 * @param obj JSON object
 * @param key Key to lookup
 * @param default_value Default value if key is missing or null
 * @return Integer value or default
 */
int64_t GetInt(const json& obj, const std::string& key, int64_t default_value = 0);

/**
 * Get optional integer value from JSON
 *
 * @param obj JSON object
 * @param key Key to lookup
 * @return Optional integer value (nullopt if missing or null)
 */
std::optional<int64_t> GetOptionalInt(const json& obj, const std::string& key);

/**
 * Get double value from JSON with default
 *
 * @param obj JSON object
 * @param key Key to lookup
 * @param default_value Default value if key is missing or null
 * @return Double value or default
 */
double GetDouble(const json& obj, const std::string& key, double default_value = 0.0);

/**
 * Get optional double value from JSON
 *
 * @param obj JSON object
 * @param key Key to lookup
 * @return Optional double value (nullopt if missing or null)
 */
std::optional<double> GetOptionalDouble(const json& obj, const std::string& key);

/**
 * Get boolean value from JSON with default
 *
 * @param obj JSON object
 * @param key Key to lookup
 * @param default_value Default value if key is missing or null
 * @return Boolean value or default
 */
bool GetBool(const json& obj, const std::string& key, bool default_value = false);

/**
 * Get optional boolean value from JSON
 *
 * @param obj JSON object
 * @param key Key to lookup
 * @return Optional boolean value (nullopt if missing or null)
 */
std::optional<bool> GetOptionalBool(const json& obj, const std::string& key);

/**
 * Get nested JSON object
 *
 * @param obj JSON object
 * @param key Key to lookup
 * @return Nested JSON object
 * @throws CityJSONError if key is missing or value is not an object
 */
json GetObject(const json& obj, const std::string& key);

/**
 * Get optional nested JSON object
 *
 * @param obj JSON object
 * @param key Key to lookup
 * @return Optional JSON object (nullopt if missing or null)
 */
std::optional<json> GetOptionalObject(const json& obj, const std::string& key);

/**
 * Get JSON array
 *
 * @param obj JSON object
 * @param key Key to lookup
 * @return JSON array
 * @throws CityJSONError if key is missing or value is not an array
 */
json GetArray(const json& obj, const std::string& key);

/**
 * Get optional JSON array
 *
 * @param obj JSON object
 * @param key Key to lookup
 * @return Optional JSON array (nullopt if missing or null)
 */
std::optional<json> GetOptionalArray(const json& obj, const std::string& key);

/**
 * Check if JSON object has key
 *
 * @param obj JSON object
 * @param key Key to check
 * @return true if key exists and is not null
 */
bool HasKey(const json& obj, const std::string& key);

/**
 * Read entire file content using DuckDB's FileSystem API
 * Supports local files, HTTP, S3, GCS URLs via registered file systems
 *
 * @param context DuckDB client context (provides FileSystem access)
 * @param file_path Path or URL to the file
 * @return File content as a string
 * @throws CityJSONError if file cannot be read
 */
std::string ReadFileContent(duckdb::ClientContext &context, const std::string &file_path);

/**
 * Validate JSON has required keys
 *
 * @param obj JSON object
 * @param required_keys List of required keys
 * @throws CityJSONError if any required key is missing
 */
void ValidateRequiredKeys(const json& obj, const std::vector<std::string>& required_keys);

} // namespace json_utils
} // namespace cityjson
} // namespace duckdb
