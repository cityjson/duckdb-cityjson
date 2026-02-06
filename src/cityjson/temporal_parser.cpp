#include "cityjson/temporal_parser.hpp"
#include "cityjson/error.hpp"
#include "duckdb.hpp"
#include "duckdb/common/types/time.hpp"
#include <regex>
#include <sstream>
#include <iomanip>

namespace duckdb {
namespace cityjson {

// ============================================================
// Date Parsing
// ============================================================

int32_t ParseDateString(const std::string& date_str) {
    // Parse YYYY-MM-DD format
    std::regex date_pattern(R"(^(\d{4})-(\d{2})-(\d{2})$)");
    std::smatch match;

    if (!std::regex_match(date_str, match, date_pattern)) {
        throw CityJSONError::Conversion("Invalid date format: " + date_str + " (expected YYYY-MM-DD)");
    }

    try {
        int year = std::stoi(match[1].str());
        int month = std::stoi(match[2].str());
        int day = std::stoi(match[3].str());

        // Validate ranges
        if (month < 1 || month > 12) {
            throw CityJSONError::Conversion("Invalid month: " + match[2].str());
        }
        if (day < 1 || day > 31) {
            throw CityJSONError::Conversion("Invalid day: " + match[3].str());
        }

        // Use DuckDB's Date::FromDate to convert to days since epoch
        date_t date = Date::FromDate(year, month, day);
        return date.days;

    } catch (const std::exception& e) {
        throw CityJSONError::Conversion("Failed to parse date: " + date_str + " - " + std::string(e.what()));
    }
}

// ============================================================
// Timestamp Parsing
// ============================================================

int64_t ParseTimestampString(const std::string& timestamp_str) {
    // Parse ISO 8601 timestamp formats:
    // - YYYY-MM-DDTHH:MM:SS
    // - YYYY-MM-DD HH:MM:SS
    // - YYYY-MM-DDTHH:MM:SS.ffffff
    // - YYYY-MM-DDTHH:MM:SS+HH:MM (with timezone)

    // Pattern: YYYY-MM-DD[T ]HH:MM:SS[.ffffff][Z|±HH:MM]
    std::regex timestamp_pattern(
        R"(^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?(?:Z|([+-]\d{2}):?(\d{2}))?$)"
    );
    std::smatch match;

    if (!std::regex_match(timestamp_str, match, timestamp_pattern)) {
        throw CityJSONError::Conversion(
            "Invalid timestamp format: " + timestamp_str +
            " (expected ISO 8601: YYYY-MM-DDTHH:MM:SS[.ffffff][Z|±HH:MM])"
        );
    }

    try {
        int year = std::stoi(match[1].str());
        int month = std::stoi(match[2].str());
        int day = std::stoi(match[3].str());
        int hour = std::stoi(match[4].str());
        int minute = std::stoi(match[5].str());
        int second = std::stoi(match[6].str());

        // Parse fractional seconds (microseconds)
        int64_t microseconds = 0;
        if (match[7].matched) {
            std::string frac_str = match[7].str();
            // Pad or truncate to 6 digits
            if (frac_str.length() > 6) {
                frac_str = frac_str.substr(0, 6);
            } else {
                frac_str.append(6 - frac_str.length(), '0');
            }
            microseconds = std::stoll(frac_str);
        }

        // Validate ranges
        if (month < 1 || month > 12) {
            throw CityJSONError::Conversion("Invalid month: " + match[2].str());
        }
        if (day < 1 || day > 31) {
            throw CityJSONError::Conversion("Invalid day: " + match[3].str());
        }
        if (hour < 0 || hour > 23) {
            throw CityJSONError::Conversion("Invalid hour: " + match[4].str());
        }
        if (minute < 0 || minute > 59) {
            throw CityJSONError::Conversion("Invalid minute: " + match[5].str());
        }
        if (second < 0 || second > 59) {
            throw CityJSONError::Conversion("Invalid second: " + match[6].str());
        }

        // Convert to DuckDB timestamp (microseconds since epoch)
        date_t date = Date::FromDate(year, month, day);
        dtime_t time = Time::FromTime(hour, minute, second, microseconds);

        // Combine date and time
        timestamp_t timestamp = Timestamp::FromDatetime(date, time);

        // Handle timezone offset if present
        if (match[8].matched && match[9].matched) {
            int tz_hour = std::stoi(match[8].str());
            int tz_minute = std::stoi(match[9].str());
            int64_t tz_offset_micros = (tz_hour * 60 + tz_minute) * 60 * 1000000LL;
            timestamp.value -= tz_offset_micros;
        }

        return timestamp.value;

    } catch (const std::exception& e) {
        throw CityJSONError::Conversion(
            "Failed to parse timestamp: " + timestamp_str + " - " + std::string(e.what())
        );
    }
}

// ============================================================
// Time Parsing
// ============================================================

int64_t ParseTimeString(const std::string& time_str) {
    // Parse HH:MM:SS[.ffffff] format
    std::regex time_pattern(R"(^(\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?$)");
    std::smatch match;

    if (!std::regex_match(time_str, match, time_pattern)) {
        throw CityJSONError::Conversion(
            "Invalid time format: " + time_str + " (expected HH:MM:SS[.ffffff])"
        );
    }

    try {
        int hour = std::stoi(match[1].str());
        int minute = std::stoi(match[2].str());
        int second = std::stoi(match[3].str());

        // Parse fractional seconds (microseconds)
        int64_t microseconds = 0;
        if (match[4].matched) {
            std::string frac_str = match[4].str();
            // Pad or truncate to 6 digits
            if (frac_str.length() > 6) {
                frac_str = frac_str.substr(0, 6);
            } else {
                frac_str.append(6 - frac_str.length(), '0');
            }
            microseconds = std::stoll(frac_str);
        }

        // Validate ranges
        if (hour < 0 || hour > 23) {
            throw CityJSONError::Conversion("Invalid hour: " + match[1].str());
        }
        if (minute < 0 || minute > 59) {
            throw CityJSONError::Conversion("Invalid minute: " + match[2].str());
        }
        if (second < 0 || second > 59) {
            throw CityJSONError::Conversion("Invalid second: " + match[3].str());
        }

        // Convert to DuckDB time (microseconds since midnight)
        dtime_t time = Time::FromTime(hour, minute, second, microseconds);
        return time.micros;

    } catch (const std::exception& e) {
        throw CityJSONError::Conversion(
            "Failed to parse time: " + time_str + " - " + std::string(e.what())
        );
    }
}

} // namespace cityjson
} // namespace duckdb
