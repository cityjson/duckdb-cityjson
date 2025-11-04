# DuckDB CityJSON Extension - Architecture Overview

**Document Purpose**: This document describes the architectural design and interfaces of the DuckDB CityJSON extension to facilitate reimplementation in C++. It focuses on interface definitions, data types, and architectural patterns rather than implementation details.

**Target Audience**: Developers implementing this extension in C++ for DuckDB

---

## Table of Contents

1. [Extension Overview](#extension-overview)
2. [Core Architecture](#core-architecture)
3. [Column Type System](#column-type-system)
4. [Parameter System](#parameter-system)
5. [CityJSON Reader Interface](#cityjson-reader-interface)
6. [Data Processing Pipeline](#data-processing-pipeline)
7. [Vector Writing System](#vector-writing-system)
8. [Error Handling](#error-handling)

---

## 1. Extension Overview

### Purpose

The extension provides table functions for reading CityJSON and CityJSONSeq files into DuckDB tables, mapping CityJSON properties to typed columns with automatic schema inference.

### Key Features

- **Format Support**: `.city.json` (CityJSON), `.city.jsonl` (CityJSONSeq)
- **Local & Remote Files**: Supports filesystem and URLs (http, https, s3, gs, etc.)
- **Schema Inference**: Automatic column type detection from sampled data
- **Chunked Processing**: Memory-efficient processing via DuckDB's vector-based API
- **Type Safety**: Strong typing with automatic type coercion

### Registration Entry Point

```cpp
// Extension registration (called by DuckDB on load)
extern "C" void extension_entrypoint(Connection& con) {
    // Register the read_cityjson table function
    con.RegisterTableFunction<CityJSONReaderVTab>("read_cityjson");
}
```

---

## 2. Core Architecture

### 2.1 DuckDB Table Function Lifecycle

The extension implements DuckDB's table function interface with three phases:

```cpp
/**
 * Table Function Interface (VTab pattern)
 *
 * DuckDB calls these methods in sequence:
 * 1. BIND:  Parse parameters, infer schema, prepare bind data
 * 2. INIT:  Initialize per-thread state, get column projections
 * 3. FUNC:  Execute function repeatedly to output data chunks
 */
class CityJSONReaderVTab {
public:
    using InitData = CityJSONInitData;
    using BindData = CityJSONReaderBindData;

    /**
     * BIND PHASE: Called once during query planning
     * - Parse function parameters (file path, options)
     * - Open file and infer schema from sampled data
     * - Register output columns with DuckDB
     * - Prepare metadata and chunked data structures
     *
     * @param bind BindInfo containing function parameters
     * @return BindData with metadata, columns, and data chunks
     */
    static BindData Bind(const BindInfo& bind);

    /**
     * INIT PHASE: Called once per thread before execution
     * - Initialize per-thread state (batch counter)
     * - Receive column projections (which columns to output)
     *
     * @param init_info InitInfo with column projection info
     * @return InitData with execution state
     */
    static InitData Init(const InitInfo& init_info);

    /**
     * FUNC PHASE: Called repeatedly to produce output
     * - Called repeatedly until all data is output
     * - Each call outputs up to VECTOR_SIZE rows (typically 2048)
     * - Must respect column projections
     * - Set output.SetLength(0) when done
     *
     * @param func TableFunctionInfo with bind/init data
     * @param output DataChunk to write results to
     */
    static void Func(const TableFunctionInfo<VTab>& func,
                     DataChunk& output);

    /**
     * Parameter definitions for the table function
     * @return Vector of parameter types (positional then named)
     */
    static vector<LogicalType> Parameters();

    /**
     * Whether the function supports filter/projection pushdown
     * @return false (not implemented in current version)
     */
    static bool SupportsPushdown() { return false; }
};
```

### 2.2 Data Structures

```cpp
/**
 * Parameters extracted from SQL function call
 * Example: SELECT * FROM read_cityjson('file.jsonl', sample_lines=100)
 */
struct CityJSONReaderParams {
    string file_name;                // Required: path or URL to CityJSON file
    optional<size_t> sample_lines;   // Optional: number of lines to sample for schema inference (default: 100)

    // Reserved for future use:
    // optional<size_t> max_lines;   // Limit number of lines to read
    // optional<size_t> offset;      // Skip first N lines
};

/**
 * Bind-time data persisted throughout query execution
 * Created during BIND phase, shared across all FUNC calls
 */
struct CityJSONReaderBindData {
    string file_name;                    // File path or URL
    CityJSON metadata;                   // CityJSON metadata (version, CRS, transform, etc.)
    CityJSONFeatureChunk chunks;         // All features chunked for processing
    vector<Column> columns;              // Inferred column schema
};

/**
 * Per-thread execution state
 * Created during INIT phase for each parallel thread
 */
struct CityJSONInitData {
    atomic<size_t> batch_index;          // Current batch being processed (atomic for thread safety)
    vector<size_t> projections;          // Column indices to output (DuckDB projection pushdown)
};
```

---

## 3. Column Type System

### 3.1 Column Type Enumeration

```cpp
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
 * Column definition with name and type
 */
struct Column {
    string name;          // Column name
    ColumnType kind;      // Column data type
};
```

### 3.2 Type Conversion Interface

```cpp
class ColumnTypeUtils {
public:
    /**
     * Convert ColumnType to DuckDB string representation
     * Used for SQL DDL generation
     *
     * @param type Column type enum
     * @return SQL type string (e.g., "BIGINT", "VARCHAR", "STRUCT(...)")
     */
    static const char* ToString(ColumnType type);

    /**
     * Convert ColumnType to DuckDB LogicalTypeId
     * Used for DuckDB API calls
     *
     * @param type Column type enum
     * @return DuckDB logical type ID
     */
    static LogicalTypeId ToLogicalTypeId(ColumnType type);

    /**
     * Parse column type from string (case-insensitive)
     * Supports aliases: INT/INTEGER/BIGINT, FLOAT/DOUBLE, TEXT/STRING/VARCHAR
     *
     * @param name Type name string
     * @return Parsed column type
     * @throws ColumnError if type is invalid
     */
    static ColumnType Parse(const string& name);

    /**
     * Infer column type from JSON value
     * Used during schema inference
     *
     * Rules:
     * - null → VARCHAR (fallback)
     * - bool → Boolean
     * - integer → BigInt
     * - float → Double
     * - string → Varchar (or Timestamp/Date/Time if format matches)
     * - array/object → Json
     *
     * @param value JSON value to analyze
     * @return Inferred column type
     */
    static ColumnType InferFromJson(const json::Value& value);

    /**
     * Resolve final type from multiple samples
     * Handles type promotion and inconsistency
     *
     * Rules:
     * - All same type → that type
     * - Mixed BigInt/Double → Double
     * - Any other mix → VARCHAR (fallback)
     *
     * @param types Vector of observed types from samples
     * @return Resolved column type
     */
    static ColumnType ResolveFromSamples(const vector<ColumnType>& types);
};
```

### 3.3 Predefined Column Schema

```cpp
/**
 * Get predefined columns for CityJSON city objects
 * These columns are always present in the output schema
 *
 * Standard columns:
 * - id: VARCHAR - CityObject ID
 * - feature_id: VARCHAR - Groups CityObjects into CityJSONFeatures
 * - object_type: VARCHAR - CityObject type (Building, Road, Bridge, etc.)
 * - children: LIST(VARCHAR) - Child CityObject IDs
 * - children_roles: LIST(VARCHAR) - Roles of child CityObjects
 * - parents: LIST(VARCHAR) - Parent CityObject IDs
 * - other: JSON - Custom/extension fields
 *
 * Optional (currently commented out in implementation):
 * - geographical_extent: STRUCT - 3D bounding box
 * - geom_lod{X}_{Y}: STRUCT - Geometry at LOD X.Y (e.g., geom_lod2_2)
 *
 * @return Vector of predefined Column definitions
 */
vector<Column> GetDefinedColumns();
```

---

## 4. Parameter System

### 4.1 Parameter Traits

```cpp
/**
 * Trait for positional parameters
 *
 * Template specializations define behavior for each parameter type
 */
template<typename T>
struct PositionalParam {
    /**
     * Get the DuckDB logical type for this parameter
     * @return LogicalType for parameter validation
     */
    static LogicalType Type();

    /**
     * Read parameter value from BindInfo
     * @param bind BindInfo from DuckDB
     * @param index 0-based parameter index
     * @return Parsed parameter value
     * @throws CityJSONError if parameter is invalid
     */
    static T Read(const BindInfo& bind, size_t index);
};

/**
 * Trait for named parameters (keyword arguments)
 *
 * Template specializations define behavior for each parameter type
 */
template<typename T>
struct NamedParam {
    /**
     * Get the parameter name as used in SQL
     * @return Parameter name string
     */
    static const char* Name();

    /**
     * Get the DuckDB logical type for this parameter
     * @return LogicalType for parameter validation
     */
    static LogicalType Type();

    /**
     * Read named parameter value from BindInfo
     * Returns nullopt if parameter not provided
     *
     * @param bind BindInfo from DuckDB
     * @return Optional parameter value
     * @throws CityJSONError if parameter is invalid
     */
    static optional<T> Read(const BindInfo& bind);

    /**
     * Cast DuckDB Value to parameter type
     * @param value DuckDB Value from bind info
     * @return Typed parameter value
     * @throws CityJSONError if cast fails
     */
    static T Cast(const Value& value);
};
```

### 4.2 Concrete Parameter Implementations

```cpp
/**
 * Positional parameter 0: file name
 * Type: VARCHAR (required)
 */
struct FileNameParam {
    using Type = string;
    static LogicalType ParamType() { return LogicalType::VARCHAR; }
    static string Read(const BindInfo& bind, size_t index);
};

/**
 * Named parameter: sample_lines
 * Type: INTEGER (optional, default: 100)
 *
 * Controls how many features to sample for schema inference
 */
struct SampleLinesParam {
    using Type = size_t;
    static const char* Name() { return "sample_lines"; }
    static LogicalType ParamType() { return LogicalType::INTEGER; }
    static optional<size_t> Read(const BindInfo& bind);
};
```

---

## 5. CityJSON Reader Interface

### 5.1 Reader Trait

```cpp
/**
 * Abstract interface for CityJSON readers
 *
 * Implementations:
 * - LocalCityJSONReader: Reads .city.json files
 * - LocalCityJSONSeqReader: Reads .city.jsonl files
 * - Future: UnifiedReader for remote files via DuckDB's read_blob
 */
class CityJSONReader {
public:
    virtual ~CityJSONReader() = default;

    /**
     * Get the file name or URL
     * @return File path or URL string
     */
    virtual string Name() const = 0;

    /**
     * Read CityJSON metadata (without CityObjects)
     * First line in CityJSONSeq, or top-level fields in CityJSON
     *
     * Metadata includes:
     * - version: CityJSON version (e.g., "2.0")
     * - transform: Scale/translate for vertex coordinates
     * - metadata: Author, date, geographic extent, etc.
     * - CRS: Coordinate reference system
     * - extensions: Used extensions
     *
     * @return CityJSON metadata object
     * @throws CityJSONError if read fails
     */
    virtual CityJSON ReadMetadata() const = 0;

    /**
     * Read a specific chunk of features
     *
     * @param n Chunk index (0-based)
     * @return Chunk containing features in range [n*chunk_size, (n+1)*chunk_size)
     * @throws CityJSONError if read fails
     */
    virtual CityJSONFeatureChunk ReadNthChunk(size_t n) const = 0;

    /**
     * Read all features from file, divided into chunks
     *
     * Current implementation loads all features into memory.
     * Future: Implement true streaming with lazy chunk loading.
     *
     * @return Chunk container with all features
     * @throws CityJSONError if read fails
     */
    virtual CityJSONFeatureChunk ReadAllChunks() const = 0;

    /**
     * Read first N features for schema inference
     *
     * Used during BIND phase to infer column types without
     * loading the entire file.
     *
     * @param n Number of features to read
     * @return Vector of sampled features
     * @throws CityJSONError if read fails
     */
    virtual vector<CityJSONFeature> ReadNFeatures(size_t n) const = 0;

    /**
     * Infer column schema from file
     *
     * Process:
     * 1. Read sample_lines features
     * 2. Extract all unique attribute keys
     * 3. Infer type for each attribute from sample values
     * 4. Combine with predefined columns
     *
     * @return Vector of inferred columns (predefined + attributes)
     * @throws CityJSONError if inference fails
     */
    virtual vector<Column> Columns() const = 0;
};
```

### 5.2 Feature Chunk Container

```cpp
/**
 * Container for CityJSON features divided into chunks
 *
 * Structure:
 * - records: All features in memory (flat vector)
 * - chunks: Ranges defining chunk boundaries in records
 *
 * Example:
 *   records = [F0, F1, F2, F3, F4, F5]  (6 features)
 *   chunks = [0..3, 3..6]                (2 chunks of 3 features)
 */
struct CityJSONFeatureChunk {
    vector<CityJSONFeature> records;    // All features
    vector<Range> chunks;                // Chunk boundaries (start..end indices)

    /**
     * Get number of chunks
     */
    size_t ChunkCount() const { return chunks.size(); }

    /**
     * Get number of CityObjects in a specific chunk
     *
     * Note: A CityJSONFeature can contain multiple CityObjects
     *
     * @param chunk_idx Chunk index
     * @return Total CityObjects across all features in chunk
     */
    optional<size_t> CityObjectCount(size_t chunk_idx) const;

    /**
     * Get features in a specific chunk
     *
     * @param chunk_idx Chunk index
     * @return Slice of records for this chunk, or nullopt if invalid
     */
    optional<span<CityJSONFeature>> GetChunk(size_t chunk_idx) const;

    /**
     * Get iterator over chunk ranges (for debugging)
     */
    auto ChunkRanges() const { return chunks | views::all; }
};
```

### 5.3 Reader Factory

```cpp
/**
 * Factory function to create appropriate reader for file
 *
 * Logic:
 * 1. Check file extension
 * 2. .city.jsonl / .jsonl → CityJSONSeqReader
 * 3. .city.json / .json → CityJSONReader
 * 4. Future: .city.parquet → CityParquetReader
 *
 * @param file_name Local path or URL
 * @return Unique pointer to appropriate reader implementation
 * @throws CityJSONError if format unsupported or file inaccessible
 */
unique_ptr<CityJSONReader> OpenAnyCityJSONFile(const string& file_name);
```

### 5.4 Attribute Extraction Utilities

```cpp
/**
 * Shared utilities for extracting data from CityObjects
 */
class CityObjectUtils {
public:
    /**
     * Get attribute value from a CityObject for a specific column
     *
     * Extraction rules:
     * - "id" → CityObject ID (special handling at caller level)
     * - "feature_id" → Parent feature ID (special handling at caller level)
     * - "object_type" → CityObject.type
     * - "geographical_extent" → CityObject.geographicalExtent (6-element array)
     * - "children" → CityObject.children (array of IDs)
     * - "parents" → CityObject.parents (array of IDs)
     * - "children_roles" → CityObject.children_roles (array of strings)
     * - "geom_lod{X}_{Y}" → Search CityObject.geometry for matching LOD
     * - "other" → Reserved for extensions
     * - Else → Search CityObject.attributes[column.name]
     *
     * @param city_object CityObject to extract from
     * @param column Column definition
     * @return JSON value (may be null)
     * @throws CityJSONError on extraction failure
     */
    static json::Value GetAttributeValue(
        const CityObject& city_object,
        const Column& column
    );

    /**
     * Get geometry value for a specific LOD column
     *
     * Column format: "geom_lod{X}_{Y}" where X.Y is LOD (e.g., "geom_lod2_2")
     *
     * Process:
     * 1. Parse LOD from column name (e.g., "2.2")
     * 2. Search CityObject.geometry array for matching LOD
     * 3. Return geometry as JSON value or null
     *
     * @param city_object CityObject to search
     * @param column Geometry column definition
     * @return Geometry JSON or null
     * @throws CityJSONError if column name invalid
     */
    static json::Value GetGeometryValue(
        const CityObject& city_object,
        const Column& column
    );

    /**
     * Infer attribute columns from sampled features
     *
     * Process:
     * 1. Iterate through all CityObjects in features
     * 2. Collect all attribute keys
     * 3. Infer type for each key using InferFromJson
     * 4. Resolve final type with ResolveFromSamples
     * 5. Sort columns alphabetically
     *
     * @param features Vector of sampled features
     * @return Vector of inferred attribute columns
     * @throws CityJSONError on inference failure
     */
    static vector<Column> InferAttributeColumns(
        const vector<CityJSONFeature>& features
    );
};
```

---

## 6. Data Processing Pipeline

### 6.1 BIND Phase Flow

```cpp
/**
 * BIND phase processes SQL function call into execution plan
 *
 * Execution order:
 * 1. Parse parameters from SQL
 * 2. Open file and create appropriate reader
 * 3. Sample N features for schema inference
 * 4. Infer column types from samples
 * 5. Read all chunks into memory
 * 6. Register columns with DuckDB
 * 7. Return bind data
 */
CityJSONReaderBindData CityJSONReaderVTab::Bind(const BindInfo& bind) {
    // Step 1: Parse parameters
    CityJSONReaderParams params = CityJSONReaderParams::FromBindInfo(bind);

    // Step 2: Open file (validates accessibility)
    auto reader = OpenAnyCityJSONFile(params.file_name);

    // Step 3: Infer schema
    size_t sample_size = params.sample_lines.value_or(100);
    vector<Column> columns = reader->Columns();  // Uses sample_size internally

    // Step 4: Load all chunks
    CityJSONFeatureChunk chunks = reader->ReadAllChunks();

    // Step 5: Read metadata
    CityJSON metadata = reader->ReadMetadata();

    // Step 6: Register columns with DuckDB
    for (const auto& column : columns) {
        LogicalTypeId type_id = ColumnTypeUtils::ToLogicalTypeId(column.kind);
        bind.AddResultColumn(column.name, LogicalType(type_id));
    }

    // Step 7: Return bind data
    return CityJSONReaderBindData{
        .file_name = params.file_name,
        .metadata = metadata,
        .chunks = chunks,
        .columns = columns
    };
}
```

### 6.2 INIT Phase Flow

```cpp
/**
 * INIT phase initializes per-thread execution state
 *
 * Execution order:
 * 1. Get column projections from DuckDB
 * 2. Initialize atomic batch counter
 * 3. Return init data
 */
CityJSONInitData CityJSONReaderVTab::Init(const InitInfo& init_info) {
    // Get column indices to output (projection pushdown)
    vector<size_t> projections;
    for (idx_t idx : init_info.GetColumnIndices()) {
        projections.push_back(static_cast<size_t>(idx));
    }

    return CityJSONInitData{
        .batch_index = 0,
        .projections = projections
    };
}
```

### 6.3 FUNC Phase Flow

```cpp
/**
 * FUNC phase outputs data in batches of VECTOR_SIZE rows
 *
 * Execution order:
 * 1. Get next batch index (atomic increment)
 * 2. Calculate starting position in chunks
 * 3. Create output vectors for projected columns
 * 4. Iterate through city objects across chunks
 * 5. Write data to vectors
 * 6. Set output length (0 means done)
 *
 * Key constraints:
 * - Maximum VECTOR_SIZE rows per call (typically 2048)
 * - Must respect column projections
 * - Must handle multi-chunk iteration
 * - Must track position across calls
 */
void CityJSONReaderVTab::Func(
    const TableFunctionInfo<VTab>& func,
    DataChunk& output
) {
    const auto& bind_data = func.GetBindData();
    auto& init_data = func.GetInitData();

    const size_t VECTOR_SIZE = 2048;  // DuckDB standard vector size

    // Step 1: Get next batch
    size_t batch_idx = init_data.batch_index.fetch_add(1);

    // Step 2: Calculate starting position
    size_t global_offset = batch_idx * VECTOR_SIZE;
    size_t remaining_to_skip = global_offset;
    size_t chunk_idx = 0;
    size_t start_row_in_chunk = 0;

    // Find which chunk contains our starting position
    while (chunk_idx < bind_data.chunks.ChunkCount()) {
        size_t chunk_size = bind_data.chunks.CityObjectCount(chunk_idx).value();
        if (remaining_to_skip < chunk_size) {
            start_row_in_chunk = remaining_to_skip;
            break;
        }
        remaining_to_skip -= chunk_size;
        chunk_idx++;
    }

    // If exhausted all chunks, signal completion
    if (chunk_idx >= bind_data.chunks.ChunkCount()) {
        output.SetLength(0);
        return;
    }

    // Step 3: Create output vectors
    vector<Vector*> vectors = CreateVectors(
        output,
        bind_data.columns,
        init_data.projections
    );

    size_t output_row = 0;

    // Step 4: Iterate through city objects
    while (chunk_idx < bind_data.chunks.ChunkCount() &&
           output_row < VECTOR_SIZE) {

        auto chunk_opt = bind_data.chunks.GetChunk(chunk_idx);
        if (!chunk_opt) break;

        auto& chunk = *chunk_opt;
        size_t current_row_in_chunk = 0;

        // Iterate features in chunk
        for (const auto& feature : chunk) {
            const string& feature_id = feature.id;

            // Iterate city objects in feature
            for (const auto& [city_object_id, city_object] : feature.city_objects) {
                // Skip rows before starting position
                if (current_row_in_chunk < start_row_in_chunk) {
                    current_row_in_chunk++;
                    continue;
                }

                // Stop if batch full
                if (output_row >= VECTOR_SIZE) {
                    goto batch_complete;
                }

                // Step 5: Write data for projected columns
                for (size_t vec_idx = 0; vec_idx < init_data.projections.size(); vec_idx++) {
                    size_t col_idx = init_data.projections[vec_idx];
                    const Column& column = bind_data.columns[col_idx];
                    Vector* vector = vectors[vec_idx];

                    // Special handling for id and feature_id
                    if (column.name == "id") {
                        FlatVector::GetData<string_t>(vector)[output_row] =
                            StringVector::AddString(vector, city_object_id);
                    } else if (column.name == "feature_id") {
                        FlatVector::GetData<string_t>(vector)[output_row] =
                            StringVector::AddString(vector, feature_id);
                    } else {
                        // Extract and write attribute
                        json::Value value = CityObjectUtils::GetAttributeValue(
                            city_object, column);
                        WriteToVector(column, value, vector, output_row);
                    }
                }

                output_row++;
                current_row_in_chunk++;
            }
        }

        // Move to next chunk
        chunk_idx++;
        start_row_in_chunk = 0;
    }

batch_complete:
    // Step 6: Set output length
    output.SetLength(output_row);
}
```

---

## 7. Vector Writing System

### 7.1 Vector Type Abstraction

```cpp
/**
 * Enum representing different DuckDB vector types
 * Used to handle different column types uniformly
 */
enum class VectorType {
    Flat,      // Standard flat vector (primitives, strings)
    List,      // List/array vector (LIST type)
    Struct,    // Struct vector (STRUCT type)
};

/**
 * Vector wrapper for type-safe vector operations
 */
class VectorWrapper {
private:
    VectorType type_;
    Vector* vector_;

public:
    VectorWrapper(VectorType type, Vector* vector);

    /**
     * Get as flat vector (panics if not flat)
     */
    Vector* AsFlatMut();

    /**
     * Get as list vector (panics if not list)
     */
    Vector* AsListMut();

    /**
     * Get as struct vector (panics if not struct)
     */
    Vector* AsStructMut();
};
```

### 7.2 Vector Creation

```cpp
/**
 * Create appropriate vector types for projected columns
 *
 * Logic:
 * - VarcharArray → ListVector
 * - Geometry / GeographicalExtent → StructVector
 * - Everything else → FlatVector
 *
 * @param output Output DataChunk
 * @param columns All columns from bind data
 * @param projections Indices of columns to output
 * @return Vector of VectorWrappers for each projected column
 */
vector<VectorWrapper> CreateVectors(
    DataChunk& output,
    const vector<Column>& columns,
    const vector<size_t>& projections
);
```

### 7.3 Value Writing Interface

```cpp
/**
 * Write JSON value to appropriate vector type at specified row
 *
 * Dispatches based on column type:
 * - Primitive types → WriteFlat
 * - VarcharArray → WriteVarcharArray
 * - Geometry → WriteGeometry
 * - GeographicalExtent → WriteGeographicalExtent
 *
 * @param column Column definition
 * @param value JSON value to write
 * @param vector Output vector
 * @param row Row index to write to
 * @throws CityJSONError if type mismatch or write fails
 */
void WriteToVector(
    const Column& column,
    const json::Value& value,
    VectorWrapper& vector,
    size_t row
);

/**
 * Write primitive value directly to flat vector memory
 * Uses pointer arithmetic for performance
 *
 * Supported types: bool, i64, f64, i32 (dates), i64 (timestamps/times)
 *
 * @param vector Flat vector
 * @param row Row index
 * @param value Value to write
 */
template<typename T>
void WritePrimitive(Vector* vector, size_t row, T value);

/**
 * Write VARCHAR array (LIST of VARCHAR) to list vector
 *
 * Process:
 * 1. Validate JSON is array
 * 2. Get list vector child (VARCHAR vector)
 * 3. Write each string element to child
 * 4. Update list entry metadata (offset, length)
 *
 * @param list_vec List vector
 * @param value JSON array value
 * @param row Row index
 * @throws CityJSONError if not an array or write fails
 */
void WriteVarcharArray(Vector* list_vec, const json::Value& value, size_t row);

/**
 * Write CityJSON geometry to struct vector
 *
 * Struct fields (in order):
 * 0. lod: VARCHAR
 * 1. type: VARCHAR
 * 2. boundaries: VARCHAR (JSON as string)
 * 3. semantics: VARCHAR (JSON as string, optional)
 * 4. material: VARCHAR (JSON as string, optional)
 * 5. texture: VARCHAR (JSON as string, optional)
 *
 * @param struct_vec Struct vector
 * @param value JSON object with geometry fields
 * @param row Row index
 * @throws CityJSONError if not an object or missing required fields
 */
void WriteGeometry(Vector* struct_vec, const json::Value& value, size_t row);

/**
 * Write geographical extent (3D bounding box) to struct vector
 *
 * Struct fields (in order):
 * 0. min_x: DOUBLE
 * 1. min_y: DOUBLE
 * 2. min_z: DOUBLE
 * 3. max_x: DOUBLE
 * 4. max_y: DOUBLE
 * 5. max_z: DOUBLE
 *
 * @param struct_vec Struct vector
 * @param value JSON array with 6 numeric elements
 * @param row Row index
 * @throws CityJSONError if not array of 6 numbers
 */
void WriteGeographicalExtent(
    Vector* struct_vec,
    const json::Value& value,
    size_t row
);
```

### 7.4 Temporal Type Parsing

```cpp
/**
 * Parse date string (YYYY-MM-DD) to days since Unix epoch
 *
 * @param date_str Date string
 * @return Days since 1970-01-01
 * @throws Error if parse fails
 */
int32_t ParseDateString(const string& date_str);

/**
 * Parse timestamp string to microseconds since Unix epoch
 *
 * Supported formats:
 * - RFC3339: "2023-01-15T10:30:00Z", "2023-01-15T10:30:00+02:00"
 * - ISO 8601: "2023-01-15T10:30:00", "2023-01-15T10:30:00.123456"
 * - Space-separated: "2023-01-15 10:30:00", "2023-01-15 10:30:00.123"
 *
 * @param timestamp_str Timestamp string
 * @return Microseconds since 1970-01-01 00:00:00 UTC
 * @throws Error if parse fails
 */
int64_t ParseTimestampString(const string& timestamp_str);

/**
 * Parse time string (HH:MM:SS[.ffffff]) to microseconds since midnight
 *
 * @param time_str Time string
 * @return Microseconds since 00:00:00
 * @throws Error if parse fails
 */
int64_t ParseTimeString(const string& time_str);
```

---

## 8. Error Handling

### 8.1 Error Type Hierarchy

```cpp
/**
 * Custom error types for CityJSON extension
 * Use a variant or inheritance-based approach in C++
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
 * Error class with kind, message, and optional context
 */
class CityJSONError : public std::exception {
private:
    CityJSONErrorKind kind_;
    string message_;
    optional<string> context_;

public:
    CityJSONError(CityJSONErrorKind kind, string message);
    CityJSONError(CityJSONErrorKind kind, string message, string context);

    CityJSONErrorKind Kind() const { return kind_; }
    const char* what() const noexcept override { return message_.c_str(); }

    /**
     * Create specific error types
     */
    static CityJSONError FileRead(const string& msg);
    static CityJSONError Parse(const string& msg);
    static CityJSONError InvalidSchema(const string& msg);
    static CityJSONError ColumnTypeMismatch(const string& column_type,
                                           const string& value);
    // ... other factory methods
};

/**
 * Result type alias for error propagation
 */
template<typename T>
using Result = std::expected<T, CityJSONError>;  // C++23
// Or: using Result = tl::expected<T, CityJSONError>;  // C++17 with library
```

---

## 9. Implementation Notes

### 9.1 Memory Management

- **Bind Data Lifetime**: Created once during BIND, shared across all FUNC calls
- **Init Data Lifetime**: Created once per thread during INIT, used in all FUNC calls for that thread
- **String Storage**: Use DuckDB's `StringVector::AddString` for proper lifetime management
- **Vector Size**: Always respect `VECTOR_SIZE` (typically 2048) to avoid memory corruption

### 9.2 Concurrency

- **Thread Safety**: Batch index uses atomic operations for thread-safe increments
- **Shared State**: Bind data is read-only after creation, safe for concurrent access
- **Per-Thread State**: Init data is thread-local, no synchronization needed

### 9.3 Performance Considerations

- **Schema Inference**: Sample only N features (default 100) to avoid reading entire file
- **Chunking**: Process data in VECTOR_SIZE batches for efficient SIMD operations
- **Pointer Arithmetic**: Use direct memory writes for primitives instead of API calls
- **String Interning**: Consider string dictionary for repeated values

### 9.4 Testing Strategy

- **Unit Tests**: Test each component in isolation
  - Column type inference
  - Parameter parsing
  - Attribute extraction
  - Vector writing
  - Temporal parsing

- **Integration Tests**: Test full pipeline with sample files
  - CityJSON files
  - CityJSONSeq files
  - Remote files (if supported)
  - Edge cases (empty files, malformed data)

- **SQL Tests**: Use DuckDB's SQLLogicTest framework
  - See `test/sql/*.test` files in repository
  - Test various SQL queries and projections

### 9.5 Future Enhancements

- **True Streaming**: Implement lazy chunk loading for large files
- **Projection Pushdown**: Implement `SupportsPushdown()` to avoid reading unused columns
- **Filter Pushdown**: Support WHERE clause pushdown to reader
- **Parallel Reading**: Multiple threads reading different chunks
- **CityParquet Support**: Add reader for Parquet-encoded CityJSON

---

## 10. References

### Documentation

- [CityJSON Specification](https://www.cityjson.org/specs/)
- [CityJSON Text Sequences](https://www.cityjson.org/specs/#text-sequences-cityjsonfeature)
- [DuckDB Extension API](https://duckdb.org/docs/dev/api_overview)
- [DuckDB C++ API](https://github.com/duckdb/duckdb/tree/master/src/include/duckdb)

### Related Projects

- [cjseq Library](https://github.com/hugoledoux/cjseq) - CityJSON parsing
- [cityparquet](https://github.com/VCityTeam/cityparquet) - CityParquet schema reference

### Code References

- Rust implementation: `/Users/hideba/tudelft/repos/duckdb-cityjson-extension-rs/src/`
- Current schema: `cityparquet/schema/duckdb.rs`
- Test files: `test/sql/*.test`

---

**Document Version**: 1.0
**Last Updated**: 2025-11-04
**Author**: Architecture extracted from Rust implementation for C++ port
