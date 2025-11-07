#pragma once

#include "cityjson/types.hpp"
#include "cityjson/column_types.hpp"
#include "cityjson/json_utils.hpp"
#include "duckdb.hpp"
#include <vector>
#include <cstddef>

namespace duckdb {
namespace cityjson {

/**
 * Wrapper around DuckDB Vector with type-safe accessors
 * Provides typed access to DuckDB vectors (Flat, List, Struct)
 */
class VectorWrapper {
public:
    /**
     * Construct vector wrapper
     *
     * @param type Type of vector (Flat, List, Struct)
     * @param vector Pointer to DuckDB Vector
     */
    VectorWrapper(VectorType type, Vector* vector);

    /**
     * Get as flat vector (for primitives and strings)
     * @return Pointer to Vector
     * @throws CityJSONError::DuckDBError if vector type is not Flat
     */
    Vector* AsFlatMut();

    /**
     * Get as list vector (for LIST types)
     * @return Pointer to Vector
     * @throws CityJSONError::DuckDBError if vector type is not List
     */
    Vector* AsListMut();

    /**
     * Get as struct vector (for STRUCT types)
     * @return Pointer to Vector
     * @throws CityJSONError::DuckDBError if vector type is not Struct
     */
    Vector* AsStructMut();

    /**
     * Get vector type
     * @return VectorType enum value
     */
    VectorType GetType() const { return type_; }

private:
    VectorType type_;       // Type of vector
    Vector* vector_;        // Pointer to DuckDB vector
};

/**
 * Create vector wrappers for all projected columns in a DataChunk
 * Determines vector type based on column type and wraps vectors appropriately
 *
 * Vector type mapping:
 * - Primitives (Boolean, BigInt, Double, Varchar, Timestamp, Date, Time) → Flat
 * - Json (stored as VARCHAR) → Flat
 * - VarcharArray → List
 * - Geometry → Struct
 * - GeographicalExtent → Struct
 *
 * @param output DataChunk containing output vectors
 * @param columns Complete column schema (all columns)
 * @param projected_column_ids Indices of projected columns in schema
 * @return Vector of VectorWrapper objects (one per projected column)
 */
std::vector<VectorWrapper> CreateVectors(
    DataChunk& output,
    const std::vector<Column>& columns,
    const std::vector<size_t>& projected_column_ids
);

/**
 * Write JSON value to DuckDB vector at specified row
 * Dispatches to appropriate writer based on column type
 *
 * Handles type conversion and NULL values:
 * - NULL JSON → FlatVector::SetNull()
 * - Primitives → WritePrimitive<T>()
 * - Temporal types → Parse string and write as primitive
 * - Json → Serialize to JSON string
 * - VarcharArray → WriteVarcharArray()
 * - Geometry → WriteGeometry()
 * - GeographicalExtent → WriteGeographicalExtent()
 *
 * @param col Column definition (name and type)
 * @param value JSON value to write
 * @param wrapper Vector wrapper for target column
 * @param row Row index to write to (0-based)
 * @throws CityJSONError on conversion failure or type mismatch
 */
void WriteToVector(
    const Column& col,
    const json& value,
    VectorWrapper& wrapper,
    size_t row
);

/**
 * Write primitive value to flat vector
 * Template function for type-safe primitive writes
 *
 * Supported types:
 * - bool → BOOLEAN vector
 * - int32_t → INTEGER/DATE vector
 * - int64_t → BIGINT/TIMESTAMP/TIME vector
 * - double → DOUBLE vector
 * - std::string → VARCHAR vector (special handling with StringVector::AddString)
 *
 * @tparam T Value type (bool, int32_t, int64_t, double, std::string)
 * @param vec Pointer to flat vector
 * @param row Row index to write to
 * @param value Value to write
 */
template<typename T>
void WritePrimitive(Vector* vec, size_t row, T value);

/**
 * Write array of strings to list vector
 * Handles LIST(VARCHAR) type
 *
 * Algorithm:
 * 1. Validate value is JSON array
 * 2. Get list entry metadata and child vector
 * 3. Set list_entry_t with offset and length
 * 4. Reserve space in list vector
 * 5. Write each array element as string to child vector
 * 6. Update list size
 *
 * @param list_vec Pointer to list vector
 * @param value JSON array of strings
 * @param row Row index in parent vector
 * @throws CityJSONError if value is not an array
 */
void WriteVarcharArray(Vector* list_vec, const json& value, size_t row);

/**
 * Write geometry to struct vector
 * Handles Geometry STRUCT type with fields:
 * - lod: VARCHAR
 * - type: VARCHAR
 * - boundaries: VARCHAR (JSON string)
 * - semantics: VARCHAR (JSON string, nullable)
 * - material: VARCHAR (JSON string, nullable)
 * - texture: VARCHAR (JSON string, nullable)
 *
 * @param struct_vec Pointer to struct vector
 * @param value JSON object representing geometry
 * @param row Row index in struct vector
 * @throws CityJSONError if value is not a valid geometry object
 */
void WriteGeometry(Vector* struct_vec, const json& value, size_t row);

/**
 * Write geographical extent to struct vector
 * Handles GeographicalExtent STRUCT type with fields:
 * - min_x: DOUBLE
 * - min_y: DOUBLE
 * - min_z: DOUBLE
 * - max_x: DOUBLE
 * - max_y: DOUBLE
 * - max_z: DOUBLE
 *
 * @param struct_vec Pointer to struct vector
 * @param value JSON array with 6 elements [minx, miny, minz, maxx, maxy, maxz]
 * @param row Row index in struct vector
 * @throws CityJSONError if value is not a valid extent array
 */
void WriteGeographicalExtent(Vector* struct_vec, const json& value, size_t row);

} // namespace cityjson
} // namespace duckdb
