#pragma once

#include <string>
#include <cstdint>

namespace duckdb {
namespace cityjson {

/**
 * Parse date string in YYYY-MM-DD format
 * Returns DuckDB date representation (days since epoch: 1970-01-01)
 *
 * Supported formats:
 * - ISO 8601: YYYY-MM-DD
 * - Variants: YYYY/MM/DD
 *
 * @param date_str Date string to parse
 * @return Days since epoch (1970-01-01)
 * @throws CityJSONError::Conversion if format is invalid
 */
int32_t ParseDateString(const std::string& date_str);

/**
 * Parse timestamp string in ISO 8601 format
 * Returns DuckDB timestamp representation (microseconds since epoch: 1970-01-01 00:00:00 UTC)
 *
 * Supported formats:
 * - ISO 8601 with T separator: YYYY-MM-DDTHH:MM:SS[.ffffff][Z|±HH:MM]
 * - ISO 8601 with space: YYYY-MM-DD HH:MM:SS[.ffffff][Z|±HH:MM]
 * - Without timezone (assumes UTC): YYYY-MM-DDTHH:MM:SS[.ffffff]
 * - With fractional seconds (up to 6 digits for microseconds)
 *
 * Examples:
 * - "2023-12-25T14:30:00Z"
 * - "2023-12-25T14:30:00.123456+01:00"
 * - "2023-12-25 14:30:00"
 *
 * @param timestamp_str Timestamp string to parse
 * @return Microseconds since epoch (1970-01-01 00:00:00 UTC)
 * @throws CityJSONError::Conversion if format is invalid
 */
int64_t ParseTimestampString(const std::string& timestamp_str);

/**
 * Parse time string in HH:MM:SS[.ffffff] format
 * Returns DuckDB time representation (microseconds since midnight)
 *
 * Supported formats:
 * - HH:MM:SS
 * - HH:MM:SS.ffffff (fractional seconds up to 6 digits)
 *
 * Examples:
 * - "14:30:00"
 * - "14:30:00.123456"
 *
 * @param time_str Time string to parse
 * @return Microseconds since midnight
 * @throws CityJSONError::Conversion if format is invalid
 */
int64_t ParseTimeString(const std::string& time_str);

} // namespace cityjson
} // namespace duckdb
