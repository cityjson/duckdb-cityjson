#pragma once

#include <string>
#include <cstddef>

namespace duckdb {
namespace cityjson {

/**
 * Supported column data types for CityJSON data
 * Maps to DuckDB's LogicalType system
 */
enum class ColumnType {
    // Primitive types
    Boolean,              // BOOLEAN
    BigInt,               // BIGINT (64-bit signed integer)
    Double,               // DOUBLE (64-bit float)
    Varchar,              // VARCHAR (variable-length string)

    // Temporal types
    Timestamp,            // TIMESTAMP (datetime with microsecond precision)
    Date,                 // DATE (date without time)
    Time,                 // TIME (time without date)

    // Complex types
    Json,                 // JSON (stored as VARCHAR)
    VarcharArray,         // LIST(VARCHAR) - array of strings

    // CityJSON-specific types
    Geometry,             // STRUCT(lod VARCHAR, type VARCHAR, boundaries VARCHAR,
                         //        semantics VARCHAR, material VARCHAR, texture VARCHAR)
    GeographicalExtent,   // STRUCT(min_x DOUBLE, min_y DOUBLE, min_z DOUBLE,
                         //        max_x DOUBLE, max_y DOUBLE, max_z DOUBLE)
};

/**
 * Error kind enumeration for CityJSON extension
 */
enum class CityJSONErrorKind {
    FileReadError,          // Failed to read file
    ParseError,             // Failed to parse CityJSON
    InvalidJson,            // Invalid JSON syntax
    InvalidSchema,          // CityJSON schema violation
    MissingField,           // Required field missing
    InvalidCRS,             // Invalid coordinate reference system
    InvalidGeometry,        // Invalid geometry definition
    InvalidTransform,       // Invalid transform parameters
    FileWriteError,         // Failed to write file
    ConversionError,        // Type conversion failed
    DuckDBError,            // DuckDB API error
    SequenceError,          // CityJSONSeq format error
    ValidationError,        // Validation failure
    UnsupportedVersion,     // Unsupported CityJSON version
    UnsupportedFeature,     // Feature not implemented
    IoError,                // I/O operation failed
    Utf8Error,              // UTF-8 encoding error
    FfiError,               // FFI/pointer error
    ParameterBindError,     // Parameter binding failed
    ColumnTypeMismatch,     // Column type doesn't match value
    Other,                  // Generic error
};

/**
 * Vector type enumeration for DuckDB vector types
 */
enum class VectorType {
    Flat,      // Standard flat vector (primitives, strings)
    List,      // List/array vector (LIST type)
    Struct,    // Struct vector (STRUCT type)
};

/**
 * Column definition with name and type
 */
struct Column {
    std::string name;          // Column name
    ColumnType kind;           // Column data type

    Column() = default;
    Column(std::string name, ColumnType kind) : name(std::move(name)), kind(kind) {}
};

/**
 * Range structure representing [start, end) indices
 */
struct Range {
    size_t start;          // Starting index (inclusive)
    size_t end;            // Ending index (exclusive)

    Range() : start(0), end(0) {}
    Range(size_t start, size_t end) : start(start), end(end) {}

    size_t Size() const { return end - start; }
    bool IsEmpty() const { return start >= end; }
};

} // namespace cityjson
} // namespace duckdb
