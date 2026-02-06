#include "cityjson/error.hpp"

namespace duckdb {
namespace cityjson {

// ============================================================
// Constructors
// ============================================================

CityJSONError::CityJSONError(CityJSONErrorKind kind, std::string message)
    : kind_(kind), message_(std::move(message)), context_(std::nullopt) {
}

CityJSONError::CityJSONError(CityJSONErrorKind kind, std::string message, std::string context)
    : kind_(kind), message_(std::move(message)), context_(std::move(context)) {
}

// ============================================================
// Factory methods
// ============================================================

CityJSONError CityJSONError::FileRead(const std::string &msg) {
	return CityJSONError(CityJSONErrorKind::FileReadError, msg);
}

CityJSONError CityJSONError::FileRead(const std::string &msg, const std::string &context) {
	return CityJSONError(CityJSONErrorKind::FileReadError, msg, context);
}

CityJSONError CityJSONError::Parse(const std::string &msg) {
	return CityJSONError(CityJSONErrorKind::ParseError, msg);
}

CityJSONError CityJSONError::Parse(const std::string &msg, const std::string &context) {
	return CityJSONError(CityJSONErrorKind::ParseError, msg, context);
}

CityJSONError CityJSONError::InvalidJson(const std::string &msg) {
	return CityJSONError(CityJSONErrorKind::InvalidJson, msg);
}

CityJSONError CityJSONError::InvalidJson(const std::string &msg, const std::string &context) {
	return CityJSONError(CityJSONErrorKind::InvalidJson, msg, context);
}

CityJSONError CityJSONError::InvalidSchema(const std::string &msg) {
	return CityJSONError(CityJSONErrorKind::InvalidSchema, msg);
}

CityJSONError CityJSONError::InvalidSchema(const std::string &msg, const std::string &context) {
	return CityJSONError(CityJSONErrorKind::InvalidSchema, msg, context);
}

CityJSONError CityJSONError::MissingField(const std::string &msg) {
	return CityJSONError(CityJSONErrorKind::MissingField, msg);
}

CityJSONError CityJSONError::MissingField(const std::string &msg, const std::string &context) {
	return CityJSONError(CityJSONErrorKind::MissingField, msg, context);
}

CityJSONError CityJSONError::InvalidCRS(const std::string &msg) {
	return CityJSONError(CityJSONErrorKind::InvalidCRS, msg);
}

CityJSONError CityJSONError::InvalidCRS(const std::string &msg, const std::string &context) {
	return CityJSONError(CityJSONErrorKind::InvalidCRS, msg, context);
}

CityJSONError CityJSONError::InvalidGeometry(const std::string &msg) {
	return CityJSONError(CityJSONErrorKind::InvalidGeometry, msg);
}

CityJSONError CityJSONError::InvalidGeometry(const std::string &msg, const std::string &context) {
	return CityJSONError(CityJSONErrorKind::InvalidGeometry, msg, context);
}

CityJSONError CityJSONError::InvalidTransform(const std::string &msg) {
	return CityJSONError(CityJSONErrorKind::InvalidTransform, msg);
}

CityJSONError CityJSONError::InvalidTransform(const std::string &msg, const std::string &context) {
	return CityJSONError(CityJSONErrorKind::InvalidTransform, msg, context);
}

CityJSONError CityJSONError::FileWrite(const std::string &msg) {
	return CityJSONError(CityJSONErrorKind::FileWriteError, msg);
}

CityJSONError CityJSONError::FileWrite(const std::string &msg, const std::string &context) {
	return CityJSONError(CityJSONErrorKind::FileWriteError, msg, context);
}

CityJSONError CityJSONError::Conversion(const std::string &msg) {
	return CityJSONError(CityJSONErrorKind::ConversionError, msg);
}

CityJSONError CityJSONError::Conversion(const std::string &msg, const std::string &context) {
	return CityJSONError(CityJSONErrorKind::ConversionError, msg, context);
}

CityJSONError CityJSONError::DuckDB(const std::string &msg) {
	return CityJSONError(CityJSONErrorKind::DuckDBError, msg);
}

CityJSONError CityJSONError::DuckDB(const std::string &msg, const std::string &context) {
	return CityJSONError(CityJSONErrorKind::DuckDBError, msg, context);
}

CityJSONError CityJSONError::Sequence(const std::string &msg) {
	return CityJSONError(CityJSONErrorKind::SequenceError, msg);
}

CityJSONError CityJSONError::Sequence(const std::string &msg, const std::string &context) {
	return CityJSONError(CityJSONErrorKind::SequenceError, msg, context);
}

CityJSONError CityJSONError::Validation(const std::string &msg) {
	return CityJSONError(CityJSONErrorKind::ValidationError, msg);
}

CityJSONError CityJSONError::Validation(const std::string &msg, const std::string &context) {
	return CityJSONError(CityJSONErrorKind::ValidationError, msg, context);
}

CityJSONError CityJSONError::UnsupportedVersion(const std::string &msg) {
	return CityJSONError(CityJSONErrorKind::UnsupportedVersion, msg);
}

CityJSONError CityJSONError::UnsupportedVersion(const std::string &msg, const std::string &context) {
	return CityJSONError(CityJSONErrorKind::UnsupportedVersion, msg, context);
}

CityJSONError CityJSONError::UnsupportedFeature(const std::string &msg) {
	return CityJSONError(CityJSONErrorKind::UnsupportedFeature, msg);
}

CityJSONError CityJSONError::UnsupportedFeature(const std::string &msg, const std::string &context) {
	return CityJSONError(CityJSONErrorKind::UnsupportedFeature, msg, context);
}

CityJSONError CityJSONError::Io(const std::string &msg) {
	return CityJSONError(CityJSONErrorKind::IoError, msg);
}

CityJSONError CityJSONError::Io(const std::string &msg, const std::string &context) {
	return CityJSONError(CityJSONErrorKind::IoError, msg, context);
}

CityJSONError CityJSONError::Utf8(const std::string &msg) {
	return CityJSONError(CityJSONErrorKind::Utf8Error, msg);
}

CityJSONError CityJSONError::Utf8(const std::string &msg, const std::string &context) {
	return CityJSONError(CityJSONErrorKind::Utf8Error, msg, context);
}

CityJSONError CityJSONError::Ffi(const std::string &msg) {
	return CityJSONError(CityJSONErrorKind::FfiError, msg);
}

CityJSONError CityJSONError::Ffi(const std::string &msg, const std::string &context) {
	return CityJSONError(CityJSONErrorKind::FfiError, msg, context);
}

CityJSONError CityJSONError::ParameterBind(const std::string &msg) {
	return CityJSONError(CityJSONErrorKind::ParameterBindError, msg);
}

CityJSONError CityJSONError::ParameterBind(const std::string &msg, const std::string &context) {
	return CityJSONError(CityJSONErrorKind::ParameterBindError, msg, context);
}

CityJSONError CityJSONError::ColumnTypeMismatch(const std::string &column_type, const std::string &value) {
	return CityJSONError(CityJSONErrorKind::ColumnTypeMismatch,
	                     "Column type mismatch: expected " + column_type + ", got value: " + value);
}

CityJSONError CityJSONError::Other(const std::string &msg) {
	return CityJSONError(CityJSONErrorKind::Other, msg);
}

CityJSONError CityJSONError::Other(const std::string &msg, const std::string &context) {
	return CityJSONError(CityJSONErrorKind::Other, msg, context);
}

} // namespace cityjson
} // namespace duckdb
