#pragma once

#include "cityjson/types.hpp"
#include <exception>
#include <string>
#include <optional>
#include <variant>

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
	CityJSONErrorKind Kind() const {
		return kind_;
	}

	/**
	 * Get error message (override from std::exception)
	 */
	const char *what() const noexcept override {
		return message_.c_str();
	}

	/**
	 * Get optional context
	 */
	const std::optional<std::string> &Context() const {
		return context_;
	}

	// ============================================================
	// Factory methods for creating specific error types
	// ============================================================

	static CityJSONError FileRead(const std::string &msg);
	static CityJSONError FileRead(const std::string &msg, const std::string &context);

	static CityJSONError Parse(const std::string &msg);
	static CityJSONError Parse(const std::string &msg, const std::string &context);

	static CityJSONError InvalidJson(const std::string &msg);
	static CityJSONError InvalidJson(const std::string &msg, const std::string &context);

	static CityJSONError InvalidSchema(const std::string &msg);
	static CityJSONError InvalidSchema(const std::string &msg, const std::string &context);

	static CityJSONError MissingField(const std::string &msg);
	static CityJSONError MissingField(const std::string &msg, const std::string &context);

	static CityJSONError InvalidCRS(const std::string &msg);
	static CityJSONError InvalidCRS(const std::string &msg, const std::string &context);

	static CityJSONError InvalidGeometry(const std::string &msg);
	static CityJSONError InvalidGeometry(const std::string &msg, const std::string &context);

	static CityJSONError InvalidTransform(const std::string &msg);
	static CityJSONError InvalidTransform(const std::string &msg, const std::string &context);

	static CityJSONError FileWrite(const std::string &msg);
	static CityJSONError FileWrite(const std::string &msg, const std::string &context);

	static CityJSONError Conversion(const std::string &msg);
	static CityJSONError Conversion(const std::string &msg, const std::string &context);

	static CityJSONError DuckDB(const std::string &msg);
	static CityJSONError DuckDB(const std::string &msg, const std::string &context);

	static CityJSONError Sequence(const std::string &msg);
	static CityJSONError Sequence(const std::string &msg, const std::string &context);

	static CityJSONError Validation(const std::string &msg);
	static CityJSONError Validation(const std::string &msg, const std::string &context);

	static CityJSONError UnsupportedVersion(const std::string &msg);
	static CityJSONError UnsupportedVersion(const std::string &msg, const std::string &context);

	static CityJSONError UnsupportedFeature(const std::string &msg);
	static CityJSONError UnsupportedFeature(const std::string &msg, const std::string &context);

	static CityJSONError Io(const std::string &msg);
	static CityJSONError Io(const std::string &msg, const std::string &context);

	static CityJSONError Utf8(const std::string &msg);
	static CityJSONError Utf8(const std::string &msg, const std::string &context);

	static CityJSONError Ffi(const std::string &msg);
	static CityJSONError Ffi(const std::string &msg, const std::string &context);

	static CityJSONError ParameterBind(const std::string &msg);
	static CityJSONError ParameterBind(const std::string &msg, const std::string &context);

	static CityJSONError ColumnTypeMismatch(const std::string &column_type, const std::string &value);

	static CityJSONError Other(const std::string &msg);
	static CityJSONError Other(const std::string &msg, const std::string &context);
};

/**
 * Result type for error propagation
 * Uses std::variant for C++17 compatibility (alternatively to C++23 std::expected)
 */
template <typename T>
class Result {
private:
	std::variant<T, CityJSONError> value_;

public:
	explicit Result(T value) : value_(std::move(value)) {
	}
	explicit Result(CityJSONError error) : value_(std::move(error)) {
	}

	bool HasError() const {
		return std::holds_alternative<CityJSONError>(value_);
	}
	const T &GetValue() const {
		return std::get<T>(value_);
	}
	const CityJSONError &GetError() const {
		return std::get<CityJSONError>(value_);
	}

	explicit operator bool() const {
		return !HasError();
	}
	const T &operator*() const {
		return GetValue();
	}
};

} // namespace cityjson
} // namespace duckdb
