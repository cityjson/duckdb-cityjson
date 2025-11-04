#pragma once

#include "cityjson/types.hpp"
#include <exception>
#include <string>
#include <optional>
#include <expected>

namespace duckdb {
namespace cityjson {

/**
 * Error class for CityJSON extension with kind, message, and optional context
 */
class CityJSONError : public std::exception {
private:
    CityJSONErrorKind kind_;
    std::string message_;
    std::optional<std::string> context_;

public:
    /**
     * Construct error with kind and message
     */
    CityJSONError(CityJSONErrorKind kind, std::string message);

    /**
     * Construct error with kind, message, and context
     */
    CityJSONError(CityJSONErrorKind kind, std::string message, std::string context);

    /**
     * Get error kind
     */
    CityJSONErrorKind Kind() const { return kind_; }

    /**
     * Get error message (override from std::exception)
     */
    const char* what() const noexcept override { return message_.c_str(); }

    /**
     * Get optional context
     */
    const std::optional<std::string>& Context() const { return context_; }

    // ============================================================
    // Factory methods for creating specific error types
    // ============================================================

    static CityJSONError FileRead(const std::string& msg);
    static CityJSONError FileRead(const std::string& msg, const std::string& context);

    static CityJSONError Parse(const std::string& msg);
    static CityJSONError Parse(const std::string& msg, const std::string& context);

    static CityJSONError InvalidJson(const std::string& msg);
    static CityJSONError InvalidJson(const std::string& msg, const std::string& context);

    static CityJSONError InvalidSchema(const std::string& msg);
    static CityJSONError InvalidSchema(const std::string& msg, const std::string& context);

    static CityJSONError MissingField(const std::string& msg);
    static CityJSONError MissingField(const std::string& msg, const std::string& context);

    static CityJSONError InvalidCRS(const std::string& msg);
    static CityJSONError InvalidCRS(const std::string& msg, const std::string& context);

    static CityJSONError InvalidGeometry(const std::string& msg);
    static CityJSONError InvalidGeometry(const std::string& msg, const std::string& context);

    static CityJSONError InvalidTransform(const std::string& msg);
    static CityJSONError InvalidTransform(const std::string& msg, const std::string& context);

    static CityJSONError FileWrite(const std::string& msg);
    static CityJSONError FileWrite(const std::string& msg, const std::string& context);

    static CityJSONError Conversion(const std::string& msg);
    static CityJSONError Conversion(const std::string& msg, const std::string& context);

    static CityJSONError DuckDB(const std::string& msg);
    static CityJSONError DuckDB(const std::string& msg, const std::string& context);

    static CityJSONError Sequence(const std::string& msg);
    static CityJSONError Sequence(const std::string& msg, const std::string& context);

    static CityJSONError Validation(const std::string& msg);
    static CityJSONError Validation(const std::string& msg, const std::string& context);

    static CityJSONError UnsupportedVersion(const std::string& msg);
    static CityJSONError UnsupportedVersion(const std::string& msg, const std::string& context);

    static CityJSONError UnsupportedFeature(const std::string& msg);
    static CityJSONError UnsupportedFeature(const std::string& msg, const std::string& context);

    static CityJSONError Io(const std::string& msg);
    static CityJSONError Io(const std::string& msg, const std::string& context);

    static CityJSONError Utf8(const std::string& msg);
    static CityJSONError Utf8(const std::string& msg, const std::string& context);

    static CityJSONError Ffi(const std::string& msg);
    static CityJSONError Ffi(const std::string& msg, const std::string& context);

    static CityJSONError ParameterBind(const std::string& msg);
    static CityJSONError ParameterBind(const std::string& msg, const std::string& context);

    static CityJSONError ColumnTypeMismatch(const std::string& column_type, const std::string& value);

    static CityJSONError Other(const std::string& msg);
    static CityJSONError Other(const std::string& msg, const std::string& context);
};

/**
 * Result type for error propagation
 * Uses C++23 std::expected or falls back to alternative implementation
 */
template<typename T>
using Result = std::expected<T, CityJSONError>;

} // namespace cityjson
} // namespace duckdb
