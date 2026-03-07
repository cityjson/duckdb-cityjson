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

The extension implements DuckDB's table function interface through function pointers and state objects:

```cpp
/**
 * Table Function Interface
 *
 * DuckDB TableFunction consists of:
 * 1. BIND:  table_function_bind_t - Parse parameters, infer schema, return FunctionData
 * 2. INIT_GLOBAL: table_function_init_global_t - Initialize shared global state
 * 3. INIT_LOCAL: table_function_init_local_t - Initialize thread-local state
 * 4. FUNCTION: table_function_t - Execute function repeatedly to output data chunks
 *
 * Key DuckDB types used:
 * - TableFunction: Main function descriptor class
 * - FunctionData: Base class for bind-time data (must be const after bind)
 * - GlobalTableFunctionState: Base class for global execution state
 * - LocalTableFunctionState: Base class for thread-local execution state
 */

/**
 * BIND PHASE: Called once during query planning
 * Signature: unique_ptr<FunctionData> (*table_function_bind_t)(
 *     ClientContext &context,
 *     TableFunctionBindInput &input,
 *     vector<LogicalType> &return_types,
 *     vector<string> &names
 * );
 *
 * Responsibilities:
 * - Parse function parameters from input.inputs (positional) and input.named_parameters
 * - Open file and infer schema from sampled data
 * - Populate return_types and names vectors with output columns
 * - Return custom FunctionData subclass with metadata, columns, and data chunks
 *
 * @param context DuckDB client context
 * @param input Contains inputs, named_parameters, and table_function reference
 * @param return_types [OUT] Output column types
 * @param names [OUT] Output column names
 * @return Unique pointer to custom FunctionData implementation
 */
unique_ptr<FunctionData> CityJSONBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<string> &names
);

/**
 * INIT_GLOBAL PHASE: Called once before parallel execution
 * Signature: unique_ptr<GlobalTableFunctionState> (*table_function_init_global_t)(
 *     ClientContext &context,
 *     TableFunctionInitInput &input
 * );
 *
 * Responsibilities:
 * - Initialize shared state (e.g., atomic counters for chunk distribution)
 * - Access bind data via input.bind_data
 * - Determine parallelism via MaxThreads() method
 *
 * @param context DuckDB client context
 * @param input Contains bind_data, column_ids, projection_ids, filters
 * @return Unique pointer to custom GlobalTableFunctionState implementation
 */
unique_ptr<GlobalTableFunctionState> CityJSONInitGlobal(
    ClientContext &context,
    TableFunctionInitInput &input
);

/**
 * INIT_LOCAL PHASE: Called once per thread before execution
 * Signature: unique_ptr<LocalTableFunctionState> (*table_function_init_local_t)(
 *     ExecutionContext &context,
 *     TableFunctionInitInput &input,
 *     GlobalTableFunctionState *global_state
 * );
 *
 * Responsibilities:
 * - Initialize thread-local state
 * - Store column projections from input.projection_ids
 * - Access global state for coordination
 *
 * @param context DuckDB execution context
 * @param input Contains bind_data, column_ids, projection_ids, filters
 * @param global_state Shared global state
 * @return Unique pointer to custom LocalTableFunctionState implementation
 */
unique_ptr<LocalTableFunctionState> CityJSONInitLocal(
    ExecutionContext &context,
    TableFunctionInitInput &input,
    GlobalTableFunctionState *global_state
);

/**
 * FUNCTION PHASE: Called repeatedly to produce output
 * Signature: void (*table_function_t)(
 *     ClientContext &context,
 *     TableFunctionInput &data,
 *     DataChunk &output
 * );
 *
 * Responsibilities:
 * - Called repeatedly until all data is output
 * - Each call outputs up to STANDARD_VECTOR_SIZE rows (typically 2048)
 * - Must respect column projections from local state
 * - Call output.SetCardinality(0) when done
 *
 * @param context DuckDB client context
 * @param data Contains bind_data, local_state, global_state
 * @param output DataChunk to write results to
 */
void CityJSONFunction(
    ClientContext &context,
    TableFunctionInput &data,
    DataChunk &output
);

/**
 * Registration Example
 */
void RegisterCityJSONFunction(ExtensionLoader &loader) {
    // Create TableFunction with name, parameter types, and function pointer
    TableFunction read_cityjson("read_cityjson",
                                {LogicalType::VARCHAR},  // positional parameters
                                CityJSONFunction,        // main function
                                CityJSONBind,            // bind function
                                CityJSONInitGlobal,      // global init (optional)
                                CityJSONInitLocal);      // local init (optional)

    // Add named parameters
    read_cityjson.named_parameters["sample_lines"] = LogicalType::INTEGER;

    // Configure pushdown capabilities
    read_cityjson.projection_pushdown = true;
    read_cityjson.filter_pushdown = false;

    // Register the function
    loader.RegisterFunction(read_cityjson);
}
```

### 2.2 Data Structures

```cpp
/**
 * Bind-time data persisted throughout query execution
 * Must inherit from FunctionData and be immutable after bind
 * Created during BIND phase, shared across all threads
 */
struct CityJSONBindData : public FunctionData {
    string file_name;                    // File path or URL
    CityJSON metadata;                   // CityJSON metadata (version, CRS, transform, etc.)
    CityJSONFeatureChunk chunks;         // All features chunked for processing
    vector<Column> columns;              // Inferred column schema

    // Required: FunctionData copy method for serialization
    unique_ptr<FunctionData> Copy() const override {
        auto result = make_uniq<CityJSONBindData>();
        result->file_name = file_name;
        result->metadata = metadata;
        result->chunks = chunks;
        result->columns = columns;
        return std::move(result);
    }

    // Required: FunctionData equality method
    bool Equals(const FunctionData &other_p) const override {
        auto &other = other_p.Cast<CityJSONBindData>();
        return file_name == other.file_name;
    }
};

/**
 * Global execution state shared across all threads
 * Created during INIT_GLOBAL phase
 * Use atomic operations for thread-safe coordination
 */
struct CityJSONGlobalState : public GlobalTableFunctionState {
    atomic<size_t> batch_index;          // Current batch being processed (atomic for thread safety)

    CityJSONGlobalState() : batch_index(0) {}

    // Optional: Control parallelism
    idx_t MaxThreads() const override {
        // Return number of chunks or MAX_THREADS for unlimited
        return GlobalTableFunctionState::MAX_THREADS;
    }
};

/**
 * Thread-local execution state
 * Created during INIT_LOCAL phase for each parallel thread
 */
struct CityJSONLocalState : public LocalTableFunctionState {
    vector<column_t> column_ids;         // Column IDs being read
    vector<idx_t> projection_ids;        // Column indices to output (projection pushdown)

    CityJSONLocalState() = default;
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

### 4.1 Parameter Declaration

Parameters are declared in the TableFunction constructor and named_parameters map:

```cpp
/**
 * Declare table function parameters
 */
TableFunction CreateReadCityJSONFunction() {
    // Positional parameters: vector<LogicalType>
    vector<LogicalType> arguments = {LogicalType::VARCHAR};

    TableFunction func("read_cityjson", arguments, CityJSONFunction,
                       CityJSONBind, CityJSONInitGlobal, CityJSONInitLocal);

    // Named parameters: named_parameter_map_t (unordered_map<string, LogicalType>)
    func.named_parameters["sample_lines"] = LogicalType::INTEGER;
    func.named_parameters["max_lines"] = LogicalType::BIGINT;

    return func;
}
```

### 4.2 Parameter Reading in Bind Function

Parameters are accessed through `TableFunctionBindInput`:

```cpp
/**
 * Read parameters in bind function
 */
unique_ptr<FunctionData> CityJSONBind(ClientContext &context,
                                       TableFunctionBindInput &input,
                                       vector<LogicalType> &return_types,
                                       vector<string> &names) {
    auto result = make_uniq<CityJSONBindData>();

    // Read positional parameters from input.inputs (vector<Value>&)
    if (input.inputs.empty()) {
        throw BinderException("read_cityjson requires a file name argument");
    }
    result->file_name = input.inputs[0].ToString();

    // Read named parameters from input.named_parameters (named_parameter_map_t&)
    size_t sample_lines = 100;  // default
    auto sample_it = input.named_parameters.find("sample_lines");
    if (sample_it != input.named_parameters.end()) {
        sample_lines = sample_it->second.GetValue<int64_t>();
    }

    // Alternative: use helper for named parameters with defaults
    auto max_lines_it = input.named_parameters.find("max_lines");
    optional<size_t> max_lines;
    if (max_lines_it != input.named_parameters.end()) {
        max_lines = max_lines_it->second.GetValue<int64_t>();
    }

    // ... rest of bind logic
    return std::move(result);
}
```

### 4.3 Value Type Casting

DuckDB's Value class provides type-safe casting:

```cpp
/**
 * Common Value casting patterns
 */
void ParameterExamples(const Value &value) {
    // Basic types
    auto str = value.ToString();                    // VARCHAR
    auto i64 = value.GetValue<int64_t>();          // BIGINT
    auto i32 = value.GetValue<int32_t>();          // INTEGER
    auto dbl = value.GetValue<double>();           // DOUBLE
    auto boolean = value.GetValue<bool>();         // BOOLEAN

    // Check type before casting
    if (value.type().id() == LogicalTypeId::VARCHAR) {
        string str_val = value.ToString();
    }

    // Handle NULL values
    if (value.IsNull()) {
        // Use default value
    }

    // List parameters (e.g., list of strings)
    if (value.type().id() == LogicalTypeId::LIST) {
        auto list = ListValue::GetChildren(value);
        for (auto &item : list) {
            string item_str = item.ToString();
        }
    }
}
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
 * 1. Parse parameters from TableFunctionBindInput
 * 2. Open file and create appropriate reader
 * 3. Sample N features for schema inference
 * 4. Infer column types from samples
 * 5. Read all chunks into memory (or prepare for lazy loading)
 * 6. Populate return_types and names vectors
 * 7. Return FunctionData
 */
unique_ptr<FunctionData> CityJSONBind(ClientContext &context,
                                       TableFunctionBindInput &input,
                                       vector<LogicalType> &return_types,
                                       vector<string> &names) {
    auto bind_data = make_uniq<CityJSONBindData>();

    // Step 1: Parse parameters
    if (input.inputs.empty()) {
        throw BinderException("read_cityjson requires a file name");
    }
    bind_data->file_name = input.inputs[0].ToString();

    size_t sample_lines = 100;
    auto sample_it = input.named_parameters.find("sample_lines");
    if (sample_it != input.named_parameters.end()) {
        sample_lines = sample_it->second.GetValue<int64_t>();
    }

    // Step 2: Open file (validates accessibility)
    auto reader = OpenAnyCityJSONFile(bind_data->file_name);

    // Step 3: Read metadata
    bind_data->metadata = reader->ReadMetadata();

    // Step 4: Infer schema from sample
    bind_data->columns = reader->InferColumns(sample_lines);

    // Step 5: Load all chunks (future: lazy loading)
    bind_data->chunks = reader->ReadAllChunks();

    // Step 6: Register columns with DuckDB via output parameters
    for (const auto& column : bind_data->columns) {
        LogicalType duckdb_type = ColumnTypeUtils::ToDuckDBType(column.kind);
        return_types.push_back(duckdb_type);
        names.push_back(column.name);
    }

    // Step 7: Return bind data
    return std::move(bind_data);
}
```

### 6.2 INIT_GLOBAL Phase Flow

```cpp
/**
 * INIT_GLOBAL phase initializes shared state for parallel execution
 *
 * Execution order:
 * 1. Create global state with atomic batch counter
 * 2. Optionally configure parallelism via MaxThreads()
 */
unique_ptr<GlobalTableFunctionState> CityJSONInitGlobal(
    ClientContext &context,
    TableFunctionInitInput &input) {

    auto global_state = make_uniq<CityJSONGlobalState>();

    // Initialize atomic counter for chunk distribution
    global_state->batch_index = 0;

    // Access bind data if needed
    auto &bind_data = input.bind_data->Cast<CityJSONBindData>();
    // Could use bind_data to determine optimal parallelism

    return std::move(global_state);
}
```

### 6.3 INIT_LOCAL Phase Flow

```cpp
/**
 * INIT_LOCAL phase initializes thread-local execution state
 *
 * Execution order:
 * 1. Create local state
 * 2. Store column IDs and projection IDs from input
 * 3. Access global state for coordination if needed
 */
unique_ptr<LocalTableFunctionState> CityJSONInitLocal(
    ExecutionContext &context,
    TableFunctionInitInput &input,
    GlobalTableFunctionState *global_state) {

    auto local_state = make_uniq<CityJSONLocalState>();

    // Store column IDs being scanned
    local_state->column_ids = input.column_ids;

    // Store projection IDs (which columns to actually output)
    local_state->projection_ids = input.projection_ids;

    // Access global state if coordination needed
    auto &global = global_state->Cast<CityJSONGlobalState>();

    return std::move(local_state);
}
```

### 6.4 FUNCTION Phase Flow

```cpp
/**
 * FUNCTION phase outputs data in batches of STANDARD_VECTOR_SIZE rows
 *
 * Execution order:
 * 1. Cast and access bind_data, local_state, global_state
 * 2. Get next batch index (atomic increment from global state)
 * 3. Calculate starting position in chunks
 * 4. Iterate through city objects across chunks
 * 5. Write data to output DataChunk vectors
 * 6. Set output cardinality (0 means done)
 *
 * Key constraints:
 * - Maximum STANDARD_VECTOR_SIZE rows per call (typically 2048)
 * - Must respect column projections from local_state
 * - Must handle multi-chunk iteration
 * - Thread-safe through atomic batch_index
 */
void CityJSONFunction(ClientContext &context,
                      TableFunctionInput &data,
                      DataChunk &output) {
    // Step 1: Access state objects
    auto &bind_data = data.bind_data->Cast<CityJSONBindData>();
    auto &local_state = data.local_state->Cast<CityJSONLocalState>();
    auto &global_state = data.global_state->Cast<CityJSONGlobalState>();

    // Step 2: Get next batch (thread-safe atomic increment)
    size_t batch_idx = global_state.batch_index.fetch_add(1);

    const size_t VECTOR_SIZE = STANDARD_VECTOR_SIZE;  // DuckDB constant, typically 2048
    size_t global_offset = batch_idx * VECTOR_SIZE;

    // Step 3: Calculate starting position
    size_t remaining_to_skip = global_offset;
    size_t chunk_idx = 0;
    size_t start_row_in_chunk = 0;

    // Find which chunk contains our starting position
    while (chunk_idx < bind_data.chunks.ChunkCount()) {
        size_t chunk_size = bind_data.chunks.CityObjectCount(chunk_idx).value_or(0);
        if (remaining_to_skip < chunk_size) {
            start_row_in_chunk = remaining_to_skip;
            break;
        }
        remaining_to_skip -= chunk_size;
        chunk_idx++;
    }

    // If exhausted all chunks, signal completion
    if (chunk_idx >= bind_data.chunks.ChunkCount()) {
        output.SetCardinality(0);
        return;
    }

    // Step 4: Initialize output vectors
    output.Reset();
    idx_t output_row = 0;

    // Step 5: Iterate through city objects
    while (chunk_idx < bind_data.chunks.ChunkCount() && output_row < VECTOR_SIZE) {
        auto chunk_opt = bind_data.chunks.GetChunk(chunk_idx);
        if (!chunk_opt) break;

        auto &chunk = *chunk_opt;
        size_t current_row_in_chunk = 0;

        // Iterate features in chunk
        for (const auto &feature : chunk) {
            const string &feature_id = feature.id;

            // Iterate city objects in feature
            for (const auto &[city_object_id, city_object] : feature.city_objects) {
                // Skip rows before starting position
                if (current_row_in_chunk < start_row_in_chunk) {
                    current_row_in_chunk++;
                    continue;
                }

                // Stop if batch full
                if (output_row >= VECTOR_SIZE) {
                    goto batch_complete;
                }

                // Write data for each projected column
                for (idx_t proj_idx = 0; proj_idx < local_state.projection_ids.size(); proj_idx++) {
                    idx_t col_idx = local_state.projection_ids[proj_idx];
                    const Column &column = bind_data.columns[col_idx];

                    // Get output vector for this column
                    auto &output_vector = output.data[proj_idx];

                    // Special handling for predefined columns
                    if (column.name == "id") {
                        FlatVector::GetData<string_t>(output_vector)[output_row] =
                            StringVector::AddString(output_vector, city_object_id);
                    } else if (column.name == "feature_id") {
                        FlatVector::GetData<string_t>(output_vector)[output_row] =
                            StringVector::AddString(output_vector, feature_id);
                    } else {
                        // Extract and write attribute
                        json::Value value = CityObjectUtils::GetAttributeValue(city_object, column);
                        WriteToVector(column, value, output_vector, output_row);
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
    // Step 6: Set output cardinality
    output.SetCardinality(output_row);
}
```

---

## 7. Optional Table Function Callbacks

DuckDB's `TableFunction` class provides many optional callback functions for advanced features. This section documents the most relevant ones for the CityJSON extension.

### 7.1 Pushdown Capabilities

```cpp
/**
 * Configure pushdown support in table function
 */
void ConfigurePushdown(TableFunction &func) {
    // Projection pushdown: Only read columns that are actually used
    func.projection_pushdown = true;

    // Filter pushdown: Push WHERE clauses into the scan
    func.filter_pushdown = false;  // Not yet implemented

    // Filter prune: Remove filter columns from output if not used elsewhere
    func.filter_prune = false;

    // Sampling pushdown: Push SAMPLE clause into scan
    func.sampling_pushdown = false;

    // Late materialization: Delay reading columns until needed
    func.late_materialization = false;
}
```

### 7.2 Statistics and Cardinality

```cpp
/**
 * table_function_cardinality_t: Estimate number of rows
 * Used by query optimizer for planning
 */
unique_ptr<NodeStatistics> CityJSONCardinality(
    ClientContext &context,
    const FunctionData *bind_data_p) {

    auto &bind_data = bind_data_p->Cast<CityJSONBindData>();

    // Count total city objects across all chunks
    size_t total_rows = 0;
    for (size_t i = 0; i < bind_data.chunks.ChunkCount(); i++) {
        total_rows += bind_data.chunks.CityObjectCount(i).value_or(0);
    }

    // Return estimated cardinality
    return make_uniq<NodeStatistics>(total_rows);
}

/**
 * table_statistics_t: Provide column-level statistics
 * Used for filter selectivity estimation
 */
unique_ptr<BaseStatistics> CityJSONStatistics(
    ClientContext &context,
    const FunctionData *bind_data,
    column_t column_index) {

    // Return statistics for specific column (min, max, null count, etc.)
    // Not implemented in initial version
    return nullptr;
}
```

### 7.3 Progress Reporting

```cpp
/**
 * table_function_progress_t: Report scan progress
 * Used for progress bars and query monitoring
 */
double CityJSONProgress(ClientContext &context,
                        const FunctionData *bind_data_p,
                        const GlobalTableFunctionState *global_state_p) {

    auto &bind_data = bind_data_p->Cast<CityJSONBindData>();
    auto &global_state = global_state_p->Cast<CityJSONGlobalState>();

    size_t total_batches = (bind_data.total_rows + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE;
    size_t completed_batches = global_state.batch_index.load();

    // Return percentage complete (0.0 to 1.0)
    return std::min(1.0, static_cast<double>(completed_batches) / total_batches);
}
```

### 7.4 Serialization (for distributed execution)

```cpp
/**
 * table_function_serialize_t: Serialize bind data
 * Used when shipping queries to remote nodes
 */
void CityJSONSerialize(Serializer &serializer,
                       const optional_ptr<FunctionData> bind_data_p,
                       const TableFunction &function) {

    auto &bind_data = bind_data_p->Cast<CityJSONBindData>();

    // Serialize only necessary data (not full chunks)
    serializer.WriteProperty("file_name", bind_data.file_name);
    // ... serialize other metadata
}

/**
 * table_function_deserialize_t: Deserialize bind data
 * Reconstruct state on remote node
 */
unique_ptr<FunctionData> CityJSONDeserialize(
    Deserializer &deserializer,
    TableFunction &function) {

    auto result = make_uniq<CityJSONBindData>();

    // Deserialize metadata
    result->file_name = deserializer.ReadProperty<string>("file_name");

    // Re-open file and load chunks on remote node
    // ... reconstruction logic

    return std::move(result);
}
```

### 7.5 Filter and Type Pushdown

```cpp
/**
 * table_function_pushdown_complex_filter_t: Push complex filters into scan
 * Allows pushing arbitrary expressions like: WHERE ST_Intersects(geom, bbox)
 */
void CityJSONPushdownComplexFilter(
    ClientContext &context,
    LogicalGet &get,
    FunctionData *bind_data_p,
    vector<unique_ptr<Expression>> &filters) {

    auto &bind_data = bind_data_p->Cast<CityJSONBindData>();

    // Iterate through filters and extract pushable ones
    for (auto it = filters.begin(); it != filters.end();) {
        auto &filter = *it;

        // Example: Push bounding box filters
        if (IsBoundingBoxFilter(filter.get())) {
            // Store filter in bind_data for use in scan
            bind_data.spatial_filter = ExtractBoundingBox(filter.get());
            // Remove from filters list (we'll handle it)
            it = filters.erase(it);
        } else {
            ++it;
        }
    }
}

/**
 * table_function_type_pushdown_t: Adapt to requested types
 * Called when query requests specific types (e.g., after CAST)
 */
void CityJSONTypePushdown(
    ClientContext &context,
    optional_ptr<FunctionData> bind_data_p,
    const unordered_map<idx_t, LogicalType> &new_column_types) {

    auto &bind_data = bind_data_p->Cast<CityJSONBindData>();

    // Update bind data to reflect requested types
    for (auto &[col_idx, new_type] : new_column_types) {
        // Adjust column types if compatible
        bind_data.columns[col_idx].requested_type = new_type;
    }
}
```

### 7.6 Multi-File Support

```cpp
/**
 * table_function_get_multi_file_reader_t: Custom multi-file reader
 * For table functions that support reading multiple files with globbing
 */
unique_ptr<MultiFileReader> CityJSONGetMultiFileReader(const TableFunction &) {
    // Return custom MultiFileReader implementation
    // Enables patterns like: read_cityjson('data/*.city.jsonl')
    return make_uniq<CityJSONMultiFileReader>();
}
```

### 7.7 Complete Registration Example

```cpp
/**
 * Register table function with all optional callbacks
 */
void RegisterCityJSONWithCallbacks(ExtensionLoader &loader) {
    TableFunction func("read_cityjson",
                       {LogicalType::VARCHAR},
                       CityJSONFunction,
                       CityJSONBind,
                       CityJSONInitGlobal,
                       CityJSONInitLocal);

    // Named parameters
    func.named_parameters["sample_lines"] = LogicalType::INTEGER;

    // Pushdown configuration
    func.projection_pushdown = true;
    func.filter_pushdown = false;

    // Optional callbacks
    func.cardinality = CityJSONCardinality;
    func.statistics = CityJSONStatistics;
    func.table_scan_progress = CityJSONProgress;

    // Serialization (for distributed execution)
    func.serialize = CityJSONSerialize;
    func.deserialize = CityJSONDeserialize;

    // Advanced pushdown
    func.pushdown_complex_filter = CityJSONPushdownComplexFilter;
    func.type_pushdown = CityJSONTypePushdown;

    // Multi-file support
    func.get_multi_file_reader = CityJSONGetMultiFileReader;

    loader.RegisterFunction(func);
}
```

---

## 8. Vector Writing System

### 8.1 Vector Type Abstraction

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

### 8.2 Vector Creation

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

### 8.3 Value Writing Interface

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

### 8.4 Temporal Type Parsing

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

## 9. Error Handling

### 9.1 Error Type Hierarchy

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

## 10. Implementation Notes

### 10.1 Memory Management

- **Bind Data Lifetime**: Created once during BIND, shared across all FUNC calls
- **Init Data Lifetime**: Created once per thread during INIT, used in all FUNC calls for that thread
- **String Storage**: Use DuckDB's `StringVector::AddString` for proper lifetime management
- **Vector Size**: Always respect `VECTOR_SIZE` (typically 2048) to avoid memory corruption

### 10.2 Concurrency

- **Thread Safety**: Batch index uses atomic operations for thread-safe increments
- **Shared State**: Bind data is read-only after creation, safe for concurrent access
- **Per-Thread State**: Init data is thread-local, no synchronization needed

### 10.3 Performance Considerations

- **Schema Inference**: Sample only N features (default 100) to avoid reading entire file
- **Chunking**: Process data in VECTOR_SIZE batches for efficient SIMD operations
- **Pointer Arithmetic**: Use direct memory writes for primitives instead of API calls
- **String Interning**: Consider string dictionary for repeated values

### 10.4 Testing Strategy

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

### 10.5 Future Enhancements

- **True Streaming**: Implement lazy chunk loading for large files
- **Projection Pushdown**: Implement `SupportsPushdown()` to avoid reading unused columns
- **Filter Pushdown**: Support WHERE clause pushdown to reader
- **Parallel Reading**: Multiple threads reading different chunks
- **CityParquet Support**: Add reader for Parquet-encoded CityJSON

---

## 11. References

### Documentation

- [CityJSON Specification](https://www.cityjson.org/specs/)
- [CityJSON Text Sequences](https://www.cityjson.org/specs/#text-sequences-cityjsonfeature)
- [DuckDB C++ API](https://duckdb.org/docs/stable/clients/c/api)

### Code References

- Rust implementation: `/Users/hideba/tudelft/repos/duckdb-cityjson-extension-rs/src/`
- Current schema: `cityparquet/schema/duckdb.rs`
- Test files: `test/sql/*.test`

---
