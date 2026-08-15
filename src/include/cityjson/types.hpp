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
	Boolean, // BOOLEAN
	BigInt,  // BIGINT (64-bit signed integer)
	Double,  // DOUBLE (64-bit float)
	Varchar, // VARCHAR (variable-length string)

	// Temporal types
	Timestamp, // TIMESTAMP (datetime with microsecond precision)
	Date,      // DATE (date without time)
	Time,      // TIME (time without date)

	// Complex types
	Json,         // JSON (stored as VARCHAR)
	VarcharArray, // LIST(VARCHAR) - array of strings

	// CityJSON-specific types (legacy)
	Geometry,           // STRUCT(lod VARCHAR, type VARCHAR, boundaries VARCHAR,
	                    //        semantics VARCHAR, material VARCHAR, texture VARCHAR)
	GeographicalExtent, // STRUCT(min_x DOUBLE, min_y DOUBLE, min_z DOUBLE,
	                    //        max_x DOUBLE, max_y DOUBLE, max_z DOUBLE)

	// New WKB-based geometry types
	GeometryWKB,              // BLOB - WKB-encoded geometry (3D)
	GeometryPropertiesStruct, // STRUCT("type" VARCHAR, surfaces JSON,
	                          //        face_semantics INTEGER[], shells INTEGER[][])
	AppearanceJson,           // JSON - per-LoD material_lod*/texture_lod* appearance (§11)

	// Arrow-native geometry encoding (experimental, branch arrow-native-type -- see
	// docs/superpowers/specs/2026-07-25-arrow-native-geometry-design.md in the parent
	// workspace repo). A nested LIST of vertex-pool indices replaces the WKB BLOB,
	// paired with a sibling column holding the row's compacted vertex pool. The shape
	// MUST match cityparquet-rs's arrow_native_geometry_data_type() /
	// arrow_native_vertices_data_type() exactly -- the point of the experiment is that
	// either producer's file is readable by the other.
	GeometryArrowNative,         // solid -> shell -> face -> ring -> INTEGER vertex-pool index
	GeometryVerticesArrowNative, // LIST(STRUCT(x DOUBLE, y DOUBLE, z DOUBLE))
};

/**
 * How the geometry columns are physically encoded.
 *
 * `Wkb` is the default and the only encoding the CityParquet specification
 * currently blesses. `ArrowNative` is the experimental alternative under
 * evaluation on the arrow-native-type branch (see
 * docs/superpowers/specs/2026-07-25-arrow-native-geometry-design.md in the parent
 * workspace repo), where geometry_lod* becomes nested LISTs of vertex-pool indices
 * and gains a geometry_vertices_lod* sibling. The two never mix within one read.
 */
enum class GeometryEncoding { Wkb, ArrowNative };

/**
 * Error kind enumeration for CityJSON extension
 */
enum class CityJSONErrorKind {
	FileReadError,      // Failed to read file
	ParseError,         // Failed to parse CityJSON
	InvalidJson,        // Invalid JSON syntax
	InvalidSchema,      // CityJSON schema violation
	MissingField,       // Required field missing
	InvalidCRS,         // Invalid coordinate reference system
	InvalidGeometry,    // Invalid geometry definition
	InvalidTransform,   // Invalid transform parameters
	FileWriteError,     // Failed to write file
	ConversionError,    // Type conversion failed
	DuckDBError,        // DuckDB API error
	SequenceError,      // CityJSONSeq format error
	ValidationError,    // Validation failure
	UnsupportedVersion, // Unsupported CityJSON version
	UnsupportedFeature, // Feature not implemented
	IoError,            // I/O operation failed
	Utf8Error,          // UTF-8 encoding error
	FfiError,           // FFI/pointer error
	ParameterBindError, // Parameter binding failed
	ColumnTypeMismatch, // Column type doesn't match value
	Other,              // Generic error
};

/**
 * Vector type enumeration for DuckDB vector types
 */
enum class VectorType {
	Flat,   // Standard flat vector (primitives, strings)
	List,   // List/array vector (LIST type)
	Struct, // Struct vector (STRUCT type)
};

/**
 * Column definition with name and type
 */
struct Column {
	std::string name; // Column name
	ColumnType kind;  // Column data type

	// Normalised LoD parsed from the name at bind time ("2.2"); empty when the name
	// carries no LoD component. Filled by InferCityJSONColumns so the scan does not
	// run a regex per column per row.
	std::string lod;

	Column() = default;
	Column(std::string name, ColumnType kind) : name(std::move(name)), kind(kind) {
	}
};

/**
 * Range structure representing [start, end) indices
 */
struct Range {
	size_t start; // Starting index (inclusive)
	size_t end;   // Ending index (exclusive)

	Range() : start(0), end(0) {
	}
	Range(size_t start, size_t end) : start(start), end(end) {
	}

	size_t Size() const {
		return end - start;
	}
	bool IsEmpty() const {
		return start >= end;
	}
};

} // namespace cityjson
} // namespace duckdb
