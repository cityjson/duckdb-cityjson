# DuckDB CityJSON Extension - C++ Implementation Plan

> **Status**: Phase 1 partially complete (5/8 header signatures defined)
> **Last Updated**: 2025-11-07
> **Based on**: DESIGN_DOC.md

## Table of Contents

1. [Overview](#overview)
2. [Current Progress](#current-progress)
3. [Architecture Summary](#architecture-summary)
4. [Detailed Task Breakdown](#detailed-task-breakdown)
5. [Implementation Phases](#implementation-phases)
6. [File Structure](#file-structure)
7. [Dependencies](#dependencies)
8. [Testing Strategy](#testing-strategy)
9. [Build Configuration](#build-configuration)
10. [Critical Implementation Notes](#critical-implementation-notes)

---

## Overview

This document outlines the complete implementation plan for the DuckDB CityJSON extension in C++. The extension provides table functions to read CityJSON and CityJSONSeq files, exposing city object data through DuckDB's SQL interface.

### Key Features

- ✅ Read `.city.json` files (CityJSON format)
- ✅ Read `.city.jsonl` files (CityJSONSeq format)
- ✅ Automatic schema inference from sampled data
- ✅ 7 predefined columns (id, feature_id, object_type, etc.)
- ✅ Dynamic attribute columns based on data
- ✅ Geometry column support with LOD handling
- ✅ Projection pushdown optimization
- 🔄 Multi-threaded parallel scanning
- 🔄 Filter pushdown (future)
- 🔄 Statistics and cardinality estimation (future)

### Estimated Scope

- **Total Tasks**: 50
- **Complexity**: High (DuckDB C++ API, vectorized processing, multi-threading)

---

## Current Progress

### ✅ Completed (5 tasks)

#### Phase 1: Foundation - File Signatures

1. ✅ **types.hpp** - Core type definitions
   - `ColumnType` enum (13 variants)
   - `CityJSONErrorKind` enum (20+ variants)
   - `VectorType` enum (3 variants)
   - `Column`, `Range` structs

2. ✅ **error.hpp** - Error handling system
   - `CityJSONError` class with factory methods
   - `Result<T>` type alias (std::expected)

3. ✅ **json_utils.hpp** - JSON utilities
   - JSON parsing and serialization
   - Safe getter functions with defaults
   - Validation helpers

4. ✅ **cityjson_types.hpp** - CityJSON data structures
   - `Transform`, `CRS`, `Metadata`
   - `GeographicalExtent`, `Geometry`, `CityObject`
   - `CityJSONFeature`, `CityJSON`
   - `CityJSONFeatureChunk` with chunking logic

5. ✅ **column_types.hpp** - Column type system
   - `ColumnTypeUtils` class (type conversion, inference)
   - `GetDefinedColumns()` function
   - Predefined column definitions

### 🔄 In Progress (0 tasks)

None currently.

### ⏳ Remaining (45 tasks)

See [Detailed Task Breakdown](#detailed-task-breakdown) below.

---

## Architecture Summary

### Component Hierarchy

```
cityjson_extension/
├── Foundation Layer
│   ├── Error System (error.hpp/cpp)
│   ├── Type Definitions (types.hpp/cpp)
│   ├── JSON Utilities (json_utils.hpp/cpp)
│   └── CityJSON Data Structures (cityjson_types.hpp/cpp)
├── Schema Layer
│   ├── Column Type System (column_types.hpp/cpp)
│   ├── Temporal Parsers (temporal_parser.hpp/cpp)
│   └── CityObject Utilities (city_object_utils.hpp/cpp)
├── Data Access Layer
│   ├── Reader Interface (reader.hpp/cpp)
│   ├── LocalCityJSONReader (local_cityjson_reader.cpp)
│   ├── LocalCityJSONSeqReader (local_cityjsonseq_reader.cpp)
│   └── Reader Factory (reader_factory.cpp)
├── Output Layer
│   └── Vector Writer System (vector_writer.hpp/cpp)
└── DuckDB Integration Layer
    ├── Table Function System (table_function.hpp)
    ├── Bind Logic (bind_data.cpp, bind_function.cpp)
    ├── State Management (global_state.cpp, local_state.cpp)
    ├── Scan Logic (scan_function.cpp, init_*.cpp)
    ├── Optional Callbacks (optional_callbacks.cpp)
    └── Registration (table_function_registration.cpp)
```

### Data Flow

```
CityJSON File → Reader → CityJSONFeatureChunk → Table Function → DuckDB Vectors → SQL Result
                  ↓                                      ↓
            Schema Inference                      Vector Writers
                  ↓                                      ↓
            Column Definitions                  Type Conversion
```

---

## Detailed Task Breakdown

### Phase 1: Foundation - File Signatures (Tasks 1-8)

#### ✅ Task 1: Define Core Type Header Signatures

**File**: `src/include/cityjson/types.hpp`
**Status**: ✅ Complete
**Details**:

- ✅ `ColumnType` enum (Boolean, BigInt, Double, Varchar, Timestamp, Date, Time, Json, VarcharArray, Geometry, GeographicalExtent)
- ✅ `CityJSONErrorKind` enum (FileReadError, ParseError, InvalidJson, etc.)
- ✅ `VectorType` enum (Flat, List, Struct)
- ✅ `Column` struct (name, kind)
- ✅ `Range` struct (start, end)

#### ✅ Task 2: Define Error System Header Signatures

**File**: `src/include/cityjson/error.hpp`
**Status**: ✅ Complete
**Details**:

- ✅ `CityJSONError` class declaration
- ✅ Factory methods (FileRead, Parse, InvalidSchema, ColumnTypeMismatch, etc.)
- ✅ `Result<T>` type alias using std::expected

#### ✅ Task 3: Define JSON Utilities Header Signatures

**File**: `src/include/cityjson/json_utils.hpp`
**Status**: ✅ Complete
**Details**:

- ✅ JSON library integration (nlohmann::json)
- ✅ Parsing functions (ParseJson, ParseJsonFile)
- ✅ Safe getter functions (GetString, GetInt, GetDouble, GetBool)
- ✅ Optional getters (GetOptionalString, GetOptionalObject, GetOptionalArray)
- ✅ Validation helpers (HasKey, ValidateRequiredKeys)

#### ✅ Task 4: Define CityJSON Data Structures Header Signatures

**File**: `src/include/cityjson/cityjson_types.hpp`
**Status**: ✅ Complete
**Details**:

- ✅ `Transform` struct with Apply() method
- ✅ `CRS`, `Metadata`, `Extension` structs
- ✅ `GeographicalExtent` struct
- ✅ `Geometry` struct with ToJson()/FromJson()
- ✅ `CityObject` struct with attributes, geometry, children
- ✅ `CityJSONFeature` struct
- ✅ `CityJSON` struct (metadata container)
- ✅ `CityJSONFeatureChunk` with ChunkCount(), GetChunk(), CityObjectCount()

#### ✅ Task 5: Define Column Type System Header Signatures

**File**: `src/include/cityjson/column_types.hpp`
**Status**: ✅ Complete
**Details**:

- ✅ `ColumnTypeUtils` class declaration
- ✅ Static methods: ToString(), ToLogicalTypeId(), ToDuckDBType(), Parse()
- ✅ Inference methods: InferFromJson(), ResolveFromSamples(), InferTemporalType()
- ✅ Helper methods: IsNumeric(), IsTemporal(), IsComplex()
- ✅ `GetDefinedColumns()` function
- ✅ Helper functions: IsPredefinedColumn(), IsGeometryColumn(), ParseLODFromColumnName()

#### ⏳ Task 6: Define Reader System Header Signatures

**File**: `src/include/cityjson/reader.hpp`
**Status**: ⏳ Pending
**Details**:

- [ ] Abstract `CityJSONReader` interface (pure virtual base class)
  - `virtual ~CityJSONReader() = default`
  - `virtual std::string Name() const = 0`
  - `virtual CityJSON ReadMetadata() const = 0`
  - `virtual CityJSONFeatureChunk ReadNthChunk(size_t n) const = 0`
  - `virtual CityJSONFeatureChunk ReadAllChunks() const = 0`
  - `virtual std::vector<CityJSONFeature> ReadNFeatures(size_t n) const = 0`
  - `virtual std::vector<Column> Columns() const = 0`
- [ ] `LocalCityJSONReader` class declaration
  - Private fields: `file_path_`, `sample_lines_`, caching fields
  - Constructor: `LocalCityJSONReader(const std::string& path, size_t sample_lines)`
  - Override all 7 virtual methods
- [ ] `LocalCityJSONSeqReader` class declaration
  - Private fields: `file_path_`, `sample_lines_`
  - Constructor: `LocalCityJSONSeqReader(const std::string& path, size_t sample_lines)`
  - Override all 7 virtual methods
- [ ] Factory function: `std::unique_ptr<CityJSONReader> OpenAnyCityJSONFile(const std::string& file_name)`

**Dependencies**: types.hpp, cityjson_types.hpp, column_types.hpp

#### ⏳ Task 7: Define Utility Headers Signatures

**Files**: `src/include/cityjson/temporal_parser.hpp`, `src/include/cityjson/city_object_utils.hpp`
**Status**: ⏳ Pending
**Details**:

**temporal_parser.hpp**:

- [ ] `int32_t ParseDateString(const std::string& date_str)`
- [ ] `int64_t ParseTimestampString(const std::string& timestamp_str)`
- [ ] `int64_t ParseTimeString(const std::string& time_str)`

**city_object_utils.hpp**:

- [ ] `CityObjectUtils` class (static utility class)
  - `static json GetAttributeValue(const CityObject&, const Column&)`
  - `static json GetGeometryValue(const CityObject&, const Column&)`
  - `static std::vector<Column> InferAttributeColumns(const std::vector<CityJSONFeature>&)`

**Dependencies**: types.hpp, cityjson_types.hpp, column_types.hpp

#### ⏳ Task 8: Define Vector Writing System Header Signatures

**File**: `src/include/cityjson/vector_writer.hpp`
**Status**: ⏳ Pending
**Details**:

- [ ] `VectorWrapper` class
  - Private fields: `VectorType type_`, `Vector* vector_`
  - Constructor: `VectorWrapper(VectorType, Vector*)`
  - Methods: `AsFlatMut()`, `AsListMut()`, `AsStructMut()`
- [ ] Function declarations:
  - `std::vector<VectorWrapper> CreateVectors(DataChunk&, const std::vector<Column>&, const std::vector<size_t>&)`
  - `void WriteToVector(const Column&, const json&, VectorWrapper&, size_t row)`
  - `template<typename T> void WritePrimitive(Vector*, size_t row, T value)`
  - `void WriteVarcharArray(Vector* list_vec, const json&, size_t row)`
  - `void WriteGeometry(Vector* struct_vec, const json&, size_t row)`
  - `void WriteGeographicalExtent(Vector* struct_vec, const json&, size_t row)`

**Dependencies**: types.hpp, column_types.hpp, DuckDB vector API

---

### Phase 2: Foundation - Basic Implementations (Tasks 9-15)

#### ⏳ Task 9: Implement Error System

**File**: `src/cityjson/error.cpp`
**Status**: ⏳ Pending
**Estimated LOC**: ~150
**Details**:

- [ ] Implement constructors

  ```cpp
  CityJSONError::CityJSONError(CityJSONErrorKind kind, std::string message)
      : kind_(kind), message_(std::move(message)) {}

  CityJSONError::CityJSONError(CityJSONErrorKind kind, std::string message, std::string context)
      : kind_(kind), message_(std::move(message)), context_(std::move(context)) {}
  ```

- [ ] Implement all 21+ factory methods (2 overloads each)

  ```cpp
  CityJSONError CityJSONError::FileRead(const std::string& msg) {
      return CityJSONError(CityJSONErrorKind::FileReadError, msg);
  }

  CityJSONError CityJSONError::FileRead(const std::string& msg, const std::string& context) {
      return CityJSONError(CityJSONErrorKind::FileReadError, msg, context);
  }
  // ... repeat for all error kinds
  ```

- [ ] Special case: `ColumnTypeMismatch` factory

  ```cpp
  CityJSONError CityJSONError::ColumnTypeMismatch(const std::string& column_type, const std::string& value) {
      return CityJSONError(CityJSONErrorKind::ColumnTypeMismatch,
                          "Column type mismatch: expected " + column_type + ", got value: " + value);
  }
  ```

**Testing**: Create unit test to verify error messages and kinds

#### ⏳ Task 10: Implement Core Types

**File**: `src/cityjson/types.cpp`
**Status**: ⏳ Pending
**Estimated LOC**: ~50
**Details**:

- [ ] Implement any helper methods for `Column` struct (if needed)
- [ ] Implement any helper methods for `Range` struct (if needed)
- [ ] Add validation logic (e.g., Range::IsValid())
- Most likely minimal implementation since structs are simple

#### ⏳ Task 11: Implement JSON Utilities

**File**: `src/cityjson/json_utils.cpp`
**Status**: ⏳ Pending
**Estimated LOC**: ~300
**Details**:

- [ ] Implement `ParseJson()`

  ```cpp
  json json_utils::ParseJson(const std::string& str) {
      try {
          return json::parse(str);
      } catch (const json::parse_error& e) {
          throw CityJSONError::InvalidJson("Failed to parse JSON: " + std::string(e.what()));
      }
  }
  ```

- [ ] Implement `ParseJsonFile()`
- [ ] Implement `JsonToString()` with pretty_print option
- [ ] Implement all getter functions (GetString, GetInt, GetDouble, GetBool, etc.)
- [ ] Implement optional getters (GetOptionalString, GetOptionalObject, GetOptionalArray)
- [ ] Implement `HasKey()` and `ValidateRequiredKeys()`
- [ ] Add comprehensive error handling with context

**Testing**: Unit tests for all getter functions with various JSON inputs

#### ⏳ Task 12: Implement CityJSON Data Structures

**File**: `src/cityjson/cityjson_types.cpp`
**Status**: ⏳ Pending
**Estimated LOC**: ~500
**Details**:

- [ ] Implement `Transform` methods
  - Constructors
  - `Apply()` method: `result[i] = vertex[i] * scale[i] + translate[i]`
  - `FromJson()` parser
- [ ] Implement `CRS` methods
  - Constructors
  - `FromJson()` parser (handle both object and EPSG code formats)
- [ ] Implement `Metadata::FromJson()`
- [ ] Implement `GeographicalExtent` methods
  - Constructors
  - `FromJson()` parser (array of 6 doubles)
  - `ToJson()` serializer
- [ ] Implement `Geometry` methods
  - Constructors
  - `FromJson()` parser
  - `ToJson()` serializer
- [ ] Implement `CityObject` methods
  - Constructors
  - `FromJson()` parser (with attributes, geometry, children, parents)
  - `ToJson()` serializer
  - `GetGeometryAtLOD()` method
- [ ] Implement `CityJSONFeature` methods
  - Constructors
  - `FromJson()` parser
  - `ToJson()` serializer
- [ ] Implement `CityJSON::FromJson()` (metadata only)
- [ ] Implement `CityJSONFeatureChunk` methods
  - `CityObjectCount()`: iterate through chunk range and sum
  - `GetChunk()`: return span or nullopt
  - `TotalCityObjectCount()`: sum across all chunks
  - `CreateChunks()`: factory method

**Testing**: Unit tests for FromJson/ToJson round-trips

#### ⏳ Task 13: Implement Column Type System - Part 1

**File**: `src/cityjson/column_types.cpp`
**Status**: ⏳ Pending
**Estimated LOC**: ~200
**Details**:

- [ ] Implement `ToString()`

  ```cpp
  const char* ColumnTypeUtils::ToString(ColumnType type) {
      switch (type) {
          case ColumnType::Boolean: return "BOOLEAN";
          case ColumnType::BigInt: return "BIGINT";
          case ColumnType::Double: return "DOUBLE";
          case ColumnType::Varchar: return "VARCHAR";
          case ColumnType::Timestamp: return "TIMESTAMP";
          case ColumnType::Date: return "DATE";
          case ColumnType::Time: return "TIME";
          case ColumnType::Json: return "JSON";
          case ColumnType::VarcharArray: return "LIST(VARCHAR)";
          case ColumnType::Geometry: return "STRUCT(...)";
          case ColumnType::GeographicalExtent: return "STRUCT(...)";
          default: return "UNKNOWN";
      }
  }
  ```

- [ ] Implement `ToLogicalTypeId()`

  ```cpp
  LogicalTypeId ColumnTypeUtils::ToLogicalTypeId(ColumnType type) {
      switch (type) {
          case ColumnType::Boolean: return LogicalTypeId::BOOLEAN;
          case ColumnType::BigInt: return LogicalTypeId::BIGINT;
          case ColumnType::Double: return LogicalTypeId::DOUBLE;
          case ColumnType::Varchar: return LogicalTypeId::VARCHAR;
          // ... etc
      }
  }
  ```

- [ ] Implement `Parse()` with case-insensitive matching and aliases

**Testing**: Unit test all conversions with valid/invalid inputs

#### ⏳ Task 14: Implement Column Type System - Part 2

**File**: `src/cityjson/column_types.cpp` (continued)
**Status**: ⏳ Pending
**Estimated LOC**: ~300
**Details**:

- [ ] Implement `ToDuckDBType()` for complex types

  ```cpp
  LogicalType ColumnTypeUtils::ToDuckDBType(ColumnType type) {
      switch (type) {
          case ColumnType::VarcharArray:
              return LogicalType::LIST(LogicalType::VARCHAR);
          case ColumnType::Geometry:
              // STRUCT(lod VARCHAR, type VARCHAR, boundaries VARCHAR, ...)
              child_list_t<LogicalType> children;
              children.push_back(make_pair("lod", LogicalType::VARCHAR));
              children.push_back(make_pair("type", LogicalType::VARCHAR));
              // ... add all fields
              return LogicalType::STRUCT(children);
          case ColumnType::GeographicalExtent:
              // STRUCT(min_x DOUBLE, min_y DOUBLE, ...)
              child_list_t<LogicalType> children;
              children.push_back(make_pair("min_x", LogicalType::DOUBLE));
              // ... add all 6 fields
              return LogicalType::STRUCT(children);
          default:
              return LogicalType(ToLogicalTypeId(type));
      }
  }
  ```

- [ ] Implement `InferFromJson()`
  - Check for null → Varchar
  - Check for boolean → Boolean
  - Check for integer → BigInt
  - Check for float → Double
  - Check for string → Varchar (or Timestamp/Date/Time if matches format)
  - Check for array → Json or VarcharArray (if all strings)
  - Check for object → Json
- [ ] Implement `InferTemporalType()` with regex patterns
  - ISO 8601 timestamp: `YYYY-MM-DDTHH:MM:SS`
  - Date: `YYYY-MM-DD`
  - Time: `HH:MM:SS`
- [ ] Implement `ResolveFromSamples()`
  - All same → that type
  - Mix of BigInt/Double → Double
  - Any other mix → Varchar
  - Empty → Varchar
- [ ] Implement `GetDefinedColumns()`

  ```cpp
  std::vector<Column> GetDefinedColumns() {
      return {
          Column("id", ColumnType::Varchar),
          Column("feature_id", ColumnType::Varchar),
          Column("object_type", ColumnType::Varchar),
          Column("children", ColumnType::VarcharArray),
          Column("children_roles", ColumnType::VarcharArray),
          Column("parents", ColumnType::VarcharArray),
          Column("other", ColumnType::Json),
      };
  }
  ```

- [ ] Implement helper functions (IsPredefinedColumn, IsGeometryColumn, ParseLODFromColumnName)

**Testing**: Extensive unit tests for type inference with various JSON samples

#### ⏳ Task 15: Implement Temporal Parsers

**File**: `src/cityjson/temporal_parser.cpp`
**Status**: ⏳ Pending
**Estimated LOC**: ~200
**Details**:

- [ ] Implement `ParseDateString()`
  - Parse `YYYY-MM-DD` format
  - Return DuckDB date representation (days since epoch)
  - Throw CityJSONError::Conversion on invalid format
- [ ] Implement `ParseTimestampString()`
  - Parse ISO 8601 formats (with/without timezone)
  - Return DuckDB timestamp (microseconds since epoch)
  - Handle formats: `YYYY-MM-DDTHH:MM:SS`, `YYYY-MM-DD HH:MM:SS.ffffff`
  - Throw CityJSONError::Conversion on invalid format
- [ ] Implement `ParseTimeString()`
  - Parse `HH:MM:SS.ffffff` format
  - Return DuckDB time representation (microseconds since midnight)
  - Throw CityJSONError::Conversion on invalid format

**Testing**: Unit tests with valid/invalid temporal strings, edge cases (leap years, timezones)

---

### Phase 3: Utility Systems (Tasks 16-17)

#### ⏳ Task 16: Implement CityObject Utilities - Part 1

**File**: `src/cityjson/city_object_utils.cpp`
**Status**: ⏳ Pending
**Estimated LOC**: ~250
**Details**:

- [ ] Implement `GetAttributeValue()` for predefined columns

  ```cpp
  json CityObjectUtils::GetAttributeValue(const CityObject& obj, const Column& col) {
      if (col.name == "object_type") {
          return json(obj.type);
      } else if (col.name == "children") {
          return json(obj.children);
      } else if (col.name == "parents") {
          return json(obj.parents);
      } else if (col.name == "children_roles") {
          return obj.children_roles ? json(*obj.children_roles) : json(nullptr);
      } else if (col.name == "geographical_extent") {
          return obj.geographical_extent ? obj.geographical_extent->ToJson() : json(nullptr);
      } else if (col.name == "other") {
          // Return attributes not in standard columns
          json other_attrs = json::object();
          for (const auto& [key, value] : obj.attributes) {
              if (!IsPredefinedColumn(key)) {
                  other_attrs[key] = value;
              }
          }
          return other_attrs;
      } else {
          // Dynamic attribute column
          auto it = obj.attributes.find(col.name);
          return it != obj.attributes.end() ? it->second : json(nullptr);
      }
  }
  ```

**Testing**: Unit tests with various CityObject configurations

#### ⏳ Task 17: Implement CityObject Utilities - Part 2

**File**: `src/cityjson/city_object_utils.cpp` (continued)
**Status**: ⏳ Pending
**Estimated LOC**: ~300
**Details**:

- [ ] Implement `GetGeometryValue()`
  - Parse column name to extract LOD (e.g., `geom_lod2_1` → `"2.1"`)
  - Find geometry with matching LOD in `obj.geometry`
  - Return geometry as STRUCT or null if not found
- [ ] Implement `InferAttributeColumns()`
  - Sample N features (configurable, default 100)
  - Collect all attribute keys across sampled CityObjects
  - For each attribute key:
    - Collect all observed values
    - Infer type using `ColumnTypeUtils::InferFromJson()`
    - Resolve final type using `ColumnTypeUtils::ResolveFromSamples()`
  - Return vector of inferred Column definitions
  - Exclude predefined column names

**Testing**: Unit tests with various feature sets, edge cases (empty attributes, mixed types)

---

### Phase 4: Reader System (Tasks 18-21)

#### ⏳ Task 18: Implement Reader Base Class

**File**: `src/cityjson/reader.cpp`
**Status**: ⏳ Pending
**Estimated LOC**: ~50
**Details**:

- [ ] Implement any shared reader utilities
- [ ] Common error handling helpers
- [ ] File validation helpers
- May be minimal if all logic is in concrete classes

#### ⏳ Task 19: Implement LocalCityJSONReader

**File**: `src/cityjson/local_cityjson_reader.cpp`
**Status**: ⏳ Pending
**Estimated LOC**: ~600
**Details**:

- [ ] Implement constructor
  - Store file_path and sample_lines
  - Initialize caching fields
- [ ] Implement `Name()`
  - Return file path
- [ ] Implement `ReadMetadata()`
  - Open file
  - Parse JSON
  - Extract metadata (version, transform, crs, metadata, extensions)
  - Return CityJSON struct (without CityObjects)
  - Throw CityJSONError on failure
- [ ] Implement `ReadNFeatures()`
  - Parse CityJSON file
  - Extract first N CityObjects
  - Convert to CityJSONFeature format
  - Return vector of features
- [ ] Implement `ReadAllChunks()`
  - Parse entire CityJSON file
  - Extract all CityObjects
  - Convert to CityJSONFeature list
  - Divide into chunks of STANDARD_VECTOR_SIZE (2048)
  - Return CityJSONFeatureChunk
- [ ] Implement `ReadNthChunk()`
  - Similar to ReadAllChunks but return only Nth chunk
- [ ] Implement `Columns()`
  - Read metadata
  - Sample N features using ReadNFeatures()
  - Call InferAttributeColumns()
  - Merge with predefined columns
  - Return complete column list

**Testing**: Integration tests with sample .city.json files

#### ⏳ Task 20: Implement LocalCityJSONSeqReader

**File**: `src/cityjson/local_cityjsonseq_reader.cpp`
**Status**: ⏳ Pending
**Estimated LOC**: ~500
**Details**:

- [ ] Implement constructor
- [ ] Implement `Name()`
- [ ] Implement `ReadMetadata()`
  - Read first line (metadata record)
  - Parse as CityJSON metadata
  - Return CityJSON struct
- [ ] Implement `ReadNFeatures()`
  - Open file
  - Skip first line (metadata)
  - Read next N lines
  - Parse each line as CityJSONFeature
  - Return vector of features
- [ ] Implement `ReadAllChunks()`
  - Open file
  - Skip first line
  - Read all remaining lines
  - Parse each as CityJSONFeature
  - Divide into chunks
  - Return CityJSONFeatureChunk
- [ ] Implement `ReadNthChunk()`
  - Calculate line offset for Nth chunk
  - Seek to position
  - Read chunk_size lines
  - Parse and return
- [ ] Implement `Columns()`
  - Similar to LocalCityJSONReader

**Testing**: Integration tests with sample .city.jsonl files

#### ⏳ Task 21: Implement Reader Factory

**File**: `src/cityjson/reader_factory.cpp`
**Status**: ⏳ Pending
**Estimated LOC**: ~80
**Details**:

- [ ] Implement `OpenAnyCityJSONFile()`

  ```cpp
  std::unique_ptr<CityJSONReader> OpenAnyCityJSONFile(const std::string& file_name) {
      // Default sample lines
      const size_t DEFAULT_SAMPLE_LINES = 100;

      // Detect format from extension
      if (file_name.ends_with(".city.jsonl") || file_name.ends_with(".jsonl")) {
          return std::make_unique<LocalCityJSONSeqReader>(file_name, DEFAULT_SAMPLE_LINES);
      } else if (file_name.ends_with(".city.json") || file_name.ends_with(".json")) {
          return std::make_unique<LocalCityJSONReader>(file_name, DEFAULT_SAMPLE_LINES);
      } else {
          // Try to auto-detect by reading first line
          // If first line is metadata record → CityJSONSeq
          // Otherwise → CityJSON
          throw CityJSONError::UnsupportedFeature("Unable to determine file format from extension");
      }
  }
  ```

**Testing**: Unit tests with various file extensions and formats

---

### Phase 5: Vector Writing System (Tasks 22-27)

#### ⏳ Task 22: Implement VectorWrapper Class

**File**: `src/cityjson/vector_writer.cpp`
**Status**: ⏳ Pending
**Estimated LOC**: ~100
**Details**:

- [ ] Implement constructor

  ```cpp
  VectorWrapper::VectorWrapper(VectorType type, Vector* vector)
      : type_(type), vector_(vector) {}
  ```

- [ ] Implement `AsFlatMut()`
  - Validate type_ == VectorType::Flat
  - Return vector_
  - Throw if type mismatch
- [ ] Implement `AsListMut()`
  - Validate type_ == VectorType::List
  - Return vector_
  - Throw if type mismatch
- [ ] Implement `AsStructMut()`
  - Validate type_ == VectorType::Struct
  - Return vector_
  - Throw if type mismatch

#### ⏳ Task 23: Implement CreateVectors Function

**File**: `src/cityjson/vector_writer.cpp` (continued)
**Status**: ⏳ Pending
**Estimated LOC**: ~150
**Details**:

- [ ] Implement `CreateVectors()`

  ```cpp
  std::vector<VectorWrapper> CreateVectors(
      DataChunk& output,
      const std::vector<Column>& columns,
      const std::vector<size_t>& projected_column_ids) {

      std::vector<VectorWrapper> wrappers;

      for (size_t i = 0; i < projected_column_ids.size(); i++) {
          size_t col_idx = projected_column_ids[i];
          const Column& col = columns[col_idx];
          Vector& vec = output.data[i];

          // Determine vector type
          VectorType vec_type;
          if (ColumnTypeUtils::IsComplex(col.kind)) {
              if (col.kind == ColumnType::VarcharArray) {
                  vec_type = VectorType::List;
              } else if (col.kind == ColumnType::Geometry ||
                        col.kind == ColumnType::GeographicalExtent) {
                  vec_type = VectorType::Struct;
              } else {
                  vec_type = VectorType::Flat; // Json stored as VARCHAR
              }
          } else {
              vec_type = VectorType::Flat;
          }

          wrappers.emplace_back(vec_type, &vec);
      }

      return wrappers;
  }
  ```

#### ⏳ Task 24: Implement Primitive Writers

**File**: `src/cityjson/vector_writer.cpp` (continued)
**Status**: ⏳ Pending
**Estimated LOC**: ~100
**Details**:

- [ ] Implement `WritePrimitive<T>` template

  ```cpp
  template<typename T>
  void WritePrimitive(Vector* vec, size_t row, T value) {
      auto data = FlatVector::GetData<T>(vec);
      data[row] = value;
  }

  // Specialization for VARCHAR
  template<>
  void WritePrimitive<std::string>(Vector* vec, size_t row, std::string value) {
      auto &validity = FlatVector::Validity(vec);
      validity.Set(row, true);
      FlatVector::GetData<string_t>(vec)[row] = StringVector::AddString(vec, value);
  }
  ```

- [ ] Explicit template instantiations for bool, int32_t, int64_t, double, std::string

#### ⏳ Task 25: Implement WriteToVector Dispatcher

**File**: `src/cityjson/vector_writer.cpp` (continued)
**Status**: ⏳ Pending
**Estimated LOC**: ~200
**Details**:

- [ ] Implement `WriteToVector()`

  ```cpp
  void WriteToVector(const Column& col, const json& value, VectorWrapper& wrapper, size_t row) {
      // Handle NULL
      if (value.is_null()) {
          FlatVector::SetNull(wrapper.AsFlatMut(), row, true);
          return;
      }

      // Dispatch based on column type
      switch (col.kind) {
          case ColumnType::Boolean:
              WritePrimitive(wrapper.AsFlatMut(), row, value.get<bool>());
              break;
          case ColumnType::BigInt:
              WritePrimitive(wrapper.AsFlatMut(), row, value.get<int64_t>());
              break;
          case ColumnType::Double:
              WritePrimitive(wrapper.AsFlatMut(), row, value.get<double>());
              break;
          case ColumnType::Varchar:
              WritePrimitive(wrapper.AsFlatMut(), row, value.get<std::string>());
              break;
          case ColumnType::Timestamp:
              WritePrimitive(wrapper.AsFlatMut(), row, ParseTimestampString(value.get<std::string>()));
              break;
          case ColumnType::Date:
              WritePrimitive(wrapper.AsFlatMut(), row, ParseDateString(value.get<std::string>()));
              break;
          case ColumnType::Time:
              WritePrimitive(wrapper.AsFlatMut(), row, ParseTimeString(value.get<std::string>()));
              break;
          case ColumnType::Json:
              WritePrimitive(wrapper.AsFlatMut(), row, value.dump());
              break;
          case ColumnType::VarcharArray:
              WriteVarcharArray(wrapper.AsListMut(), value, row);
              break;
          case ColumnType::Geometry:
              WriteGeometry(wrapper.AsStructMut(), value, row);
              break;
          case ColumnType::GeographicalExtent:
              WriteGeographicalExtent(wrapper.AsStructMut(), value, row);
              break;
          default:
              throw CityJSONError::Other("Unsupported column type");
      }
  }
  ```

#### ⏳ Task 26: Implement Complex Type Writers - Part 1

**File**: `src/cityjson/vector_writer.cpp` (continued)
**Status**: ⏳ Pending
**Estimated LOC**: ~150
**Details**:

- [ ] Implement `WriteVarcharArray()`

  ```cpp
  void WriteVarcharArray(Vector* list_vec, const json& value, size_t row) {
      if (!value.is_array()) {
          FlatVector::SetNull(list_vec, row, true);
          return;
      }

      auto list_data = FlatVector::GetData<list_entry_t>(list_vec);
      auto& child_vec = ListVector::GetEntry(list_vec);
      auto list_size = ListVector::GetListSize(list_vec);

      // Set list entry metadata
      list_data[row].offset = list_size;
      list_data[row].length = value.size();

      // Reserve space
      ListVector::Reserve(list_vec, list_size + value.size());

      // Write elements
      for (size_t i = 0; i < value.size(); i++) {
          const auto& elem = value[i];
          if (elem.is_string()) {
              auto str = elem.get<std::string>();
              FlatVector::GetData<string_t>(&child_vec)[list_size + i] =
                  StringVector::AddString(&child_vec, str);
          } else {
              FlatVector::SetNull(&child_vec, list_size + i, true);
          }
      }

      ListVector::SetListSize(list_vec, list_size + value.size());
  }
  ```

#### ⏳ Task 27: Implement Complex Type Writers - Part 2

**File**: `src/cityjson/vector_writer.cpp` (continued)
**Status**: ⏳ Pending
**Estimated LOC**: ~200
**Details**:

- [ ] Implement `WriteGeometry()`

  ```cpp
  void WriteGeometry(Vector* struct_vec, const json& value, size_t row) {
      if (!value.is_object()) {
          FlatVector::SetNull(struct_vec, row, true);
          return;
      }

      // Parse geometry
      Geometry geom = Geometry::FromJson(value);

      // Get child vectors
      auto& children = StructVector::GetEntries(struct_vec);

      // Write fields: lod, type, boundaries, semantics, material, texture
      WritePrimitive(children[0], row, geom.lod);
      WritePrimitive(children[1], row, geom.type);
      WritePrimitive(children[2], row, geom.boundaries.dump());

      if (geom.semantics) {
          WritePrimitive(children[3], row, geom.semantics->dump());
      } else {
          FlatVector::SetNull(children[3], row, true);
      }

      // ... handle material and texture similarly
  }
  ```

- [ ] Implement `WriteGeographicalExtent()`

  ```cpp
  void WriteGeographicalExtent(Vector* struct_vec, const json& value, size_t row) {
      if (!value.is_array() || value.size() != 6) {
          FlatVector::SetNull(struct_vec, row, true);
          return;
      }

      GeographicalExtent extent = GeographicalExtent::FromJson(value);

      auto& children = StructVector::GetEntries(struct_vec);

      WritePrimitive(children[0], row, extent.min_x);
      WritePrimitive(children[1], row, extent.min_y);
      WritePrimitive(children[2], row, extent.min_z);
      WritePrimitive(children[3], row, extent.max_x);
      WritePrimitive(children[4], row, extent.max_y);
      WritePrimitive(children[5], row, extent.max_z);
  }
  ```

**Testing**: Unit tests for each writer function with various inputs

---

### Phase 6: Table Function System - Data Structures (Tasks 28-30)

#### ⏳ Task 28: Define Table Function Header Signatures

**File**: `src/include/cityjson/table_function.hpp`
**Status**: ⏳ Pending
**Details**:

- [ ] Define `CityJSONBindData` struct

  ```cpp
  struct CityJSONBindData : public FunctionData {
      std::string file_name;
      CityJSON metadata;
      CityJSONFeatureChunk chunks;
      std::vector<Column> columns;

      unique_ptr<FunctionData> Copy() const override;
      bool Equals(const FunctionData &other) const override;
  };
  ```

- [ ] Define `CityJSONGlobalState` struct

  ```cpp
  struct CityJSONGlobalState : public GlobalTableFunctionState {
      std::atomic<size_t> batch_index;

      CityJSONGlobalState();
      idx_t MaxThreads() const override;
  };
  ```

- [ ] Define `CityJSONLocalState` struct

  ```cpp
  struct CityJSONLocalState : public LocalTableFunctionState {
      vector<column_t> column_ids;
      vector<idx_t> projection_ids;
  };
  ```

- [ ] Declare callback functions:
  - `unique_ptr<FunctionData> CityJSONBind(...)`
  - `unique_ptr<GlobalTableFunctionState> CityJSONInitGlobal(...)`
  - `unique_ptr<LocalTableFunctionState> CityJSONInitLocal(...)`
  - `void CityJSONFunction(...)`
  - Optional: `unique_ptr<NodeStatistics> CityJSONCardinality(...)`
  - Optional: `unique_ptr<BaseStatistics> CityJSONStatistics(...)`
  - Optional: `double CityJSONProgress(...)`
- [ ] Declare registration functions:
  - `TableFunction CreateReadCityJSONFunction()`
  - `void RegisterCityJSONFunction(DatabaseInstance&)`

#### ⏳ Task 29: Implement Bind Data Structure

**File**: `src/cityjson/bind_data.cpp`
**Status**: ⏳ Pending
**Estimated LOC**: ~100
**Details**:

- [ ] Implement `CityJSONBindData::Copy()`

  ```cpp
  unique_ptr<FunctionData> CityJSONBindData::Copy() const {
      auto result = make_uniq<CityJSONBindData>();
      result->file_name = file_name;
      result->metadata = metadata;
      result->chunks = chunks;
      result->columns = columns;
      return std::move(result);
  }
  ```

- [ ] Implement `CityJSONBindData::Equals()`

  ```cpp
  bool CityJSONBindData::Equals(const FunctionData &other_p) const {
      auto &other = other_p.Cast<CityJSONBindData>();
      return file_name == other.file_name;
  }
  ```

#### ⏳ Task 30: Implement State Structures

**Files**: `src/cityjson/global_state.cpp`, `src/cityjson/local_state.cpp`
**Status**: ⏳ Pending
**Estimated LOC**: ~80
**Details**:

- [ ] Implement `CityJSONGlobalState` constructor

  ```cpp
  CityJSONGlobalState::CityJSONGlobalState() : batch_index(0) {}
  ```

- [ ] Implement `MaxThreads()`

  ```cpp
  idx_t CityJSONGlobalState::MaxThreads() const {
      // Allow multi-threading
      return DConstants::INVALID_INDEX;
  }
  ```

- [ ] Implement `CityJSONLocalState` constructor (default is fine)

---

### Phase 7: Table Function System - Core Callbacks (Tasks 31-34)

#### ⏳ Task 31: Implement Bind Function - Part 1

**File**: `src/cityjson/bind_function.cpp`
**Status**: ⏳ Pending
**Estimated LOC**: ~150
**Details**:

- [ ] Parse positional parameter (file_name)

  ```cpp
  unique_ptr<FunctionData> CityJSONBind(
      ClientContext &context,
      TableFunctionBindInput &input,
      vector<LogicalType> &return_types,
      vector<string> &names) {

      auto result = make_uniq<CityJSONBindData>();

      // Get file_name from first positional parameter
      if (input.inputs.empty()) {
          throw BinderException("read_cityjson requires a file path");
      }
      result->file_name = StringValue::Get(input.inputs[0]);

      // Parse named parameters
      size_t sample_lines = 100; // default
      for (auto &kv : input.named_parameters) {
          if (kv.first == "sample_lines") {
              sample_lines = BigIntValue::Get(kv.second);
          }
      }

      // Continue in Part 2...
  }
  ```

#### ⏳ Task 32: Implement Bind Function - Part 2

**File**: `src/cityjson/bind_function.cpp` (continued)
**Status**: ⏳ Pending
**Estimated LOC**: ~150
**Details**:

- [ ] Open file and read metadata

  ```cpp
  // Open reader
  auto reader = OpenAnyCityJSONFile(result->file_name);

  // Read metadata
  try {
      result->metadata = reader->ReadMetadata();
  } catch (const CityJSONError& e) {
      throw BinderException("Failed to read CityJSON metadata: " + std::string(e.what()));
  }

  // Infer schema from samples
  try {
      result->columns = reader->Columns();
  } catch (const CityJSONError& e) {
      throw BinderException("Failed to infer schema: " + std::string(e.what()));
  }
  ```

#### ⏳ Task 33: Implement Bind Function - Part 3

**File**: `src/cityjson/bind_function.cpp` (continued)
**Status**: ⏳ Pending
**Estimated LOC**: ~150
**Details**:

- [ ] Load all chunks

  ```cpp
  // Load all data
  try {
      result->chunks = reader->ReadAllChunks();
  } catch (const CityJSONError& e) {
      throw BinderException("Failed to read CityJSON data: " + std::string(e.what()));
  }

  // Populate return types and names
  for (const auto& col : result->columns) {
      names.push_back(col.name);
      return_types.push_back(ColumnTypeUtils::ToDuckDBType(col.kind));
  }

  return std::move(result);
  ```

#### ⏳ Task 34: Implement Init Callbacks

**Files**: `src/cityjson/init_global.cpp`, `src/cityjson/init_local.cpp`
**Status**: ⏳ Pending
**Estimated LOC**: ~100
**Details**:

- [ ] Implement `CityJSONInitGlobal()`

  ```cpp
  unique_ptr<GlobalTableFunctionState> CityJSONInitGlobal(
      ClientContext &context,
      TableFunctionInitInput &input) {
      return make_uniq<CityJSONGlobalState>();
  }
  ```

- [ ] Implement `CityJSONInitLocal()`

  ```cpp
  unique_ptr<LocalTableFunctionState> CityJSONInitLocal(
      ExecutionContext &context,
      TableFunctionInitInput &input,
      GlobalTableFunctionState *global_state) {

      auto result = make_uniq<CityJSONLocalState>();
      result->column_ids = input.column_ids;
      result->projection_ids = input.projection_ids;
      return std::move(result);
  }
  ```

---

### Phase 8: Table Function System - Scan Function (Tasks 35-39)

#### ⏳ Task 35: Implement Scan Function - Part 1

**File**: `src/cityjson/scan_function.cpp`
**Status**: ⏳ Pending
**Estimated LOC**: ~100
**Details**:

- [ ] Cast input structures

  ```cpp
  void CityJSONFunction(
      ClientContext &context,
      TableFunctionInput &data,
      DataChunk &output) {

      auto &bind_data = data.bind_data->Cast<CityJSONBindData>();
      auto &local_state = data.local_state->Cast<CityJSONLocalState>();
      auto &global_state = data.global_state->Cast<CityJSONGlobalState>();

      // Get next batch index atomically
      size_t batch_index = global_state.batch_index.fetch_add(1);

      // Continue in Part 2...
  }
  ```

#### ⏳ Task 36: Implement Scan Function - Part 2

**File**: `src/cityjson/scan_function.cpp` (continued)
**Status**: ⏳ Pending
**Estimated LOC**: ~150
**Details**:

- [ ] Calculate position and find chunk

  ```cpp
  // Calculate starting position in flattened CityObject sequence
  size_t start_position = batch_index * STANDARD_VECTOR_SIZE;

  // Find which chunk contains this position
  size_t current_position = 0;
  size_t chunk_idx = 0;
  size_t offset_in_chunk = 0;

  for (chunk_idx = 0; chunk_idx < bind_data.chunks.ChunkCount(); chunk_idx++) {
      auto chunk_size = bind_data.chunks.CityObjectCount(chunk_idx);
      if (!chunk_size) break;

      if (current_position + *chunk_size > start_position) {
          offset_in_chunk = start_position - current_position;
          break;
      }
      current_position += *chunk_size;
  }

  // Check if exhausted
  if (chunk_idx >= bind_data.chunks.ChunkCount()) {
      output.SetCardinality(0);
      return;
  }
  ```

#### ⏳ Task 37: Implement Scan Function - Part 3

**File**: `src/cityjson/scan_function.cpp` (continued)
**Status**: ⏳ Pending
**Estimated LOC**: ~200
**Details**:

- [ ] Initialize output vectors

  ```cpp
  // Create vector wrappers
  auto wrappers = CreateVectors(output, bind_data.columns, local_state.projection_ids);

  // Track output row
  size_t output_row = 0;
  size_t remaining = STANDARD_VECTOR_SIZE;
  ```

- [ ] Iterate through chunks and CityObjects

  ```cpp
  // Iterate across chunks if necessary
  while (remaining > 0 && chunk_idx < bind_data.chunks.ChunkCount()) {
      auto chunk = bind_data.chunks.GetChunk(chunk_idx);
      if (!chunk) break;

      // Iterate through features in chunk
      for (size_t feat_idx = offset_in_chunk; feat_idx < chunk->size() && remaining > 0; feat_idx++) {
          const auto& feature = (*chunk)[feat_idx];

          // Iterate through CityObjects in feature
          for (const auto& [city_obj_id, city_obj] : feature.city_objects) {
              if (remaining == 0) break;

              // Write data for this CityObject (Part 4)
              // ...

              output_row++;
              remaining--;
          }
      }

      // Move to next chunk
      chunk_idx++;
      offset_in_chunk = 0;
  }
  ```

#### ⏳ Task 38: Implement Scan Function - Part 4

**File**: `src/cityjson/scan_function.cpp` (continued)
**Status**: ⏳ Pending
**Estimated LOC**: ~200
**Details**:

- [ ] Write data for each projected column

  ```cpp
  // For each projected column
  for (size_t col_idx = 0; col_idx < local_state.projection_ids.size(); col_idx++) {
      size_t schema_idx = local_state.projection_ids[col_idx];
      const Column& col = bind_data.columns[schema_idx];

      // Get value
      json value;

      if (col.name == "id") {
          value = json(city_obj_id);
      } else if (col.name == "feature_id") {
          value = json(feature.id);
      } else {
          // Get from CityObject
          value = CityObjectUtils::GetAttributeValue(city_obj, col);
      }

      // Write to vector
      try {
          WriteToVector(col, value, wrappers[col_idx], output_row);
      } catch (const CityJSONError& e) {
          throw InternalException("Failed to write value: " + std::string(e.what()));
      }
  }
  ```

#### ⏳ Task 39: Implement Scan Function - Part 5

**File**: `src/cityjson/scan_function.cpp` (continued)
**Status**: ⏳ Pending
**Estimated LOC**: ~50
**Details**:

- [ ] Finalize output

  ```cpp
  // Set output cardinality
  output.SetCardinality(output_row);

  // Verify output
  output.Verify();
  ```

- [ ] Add comprehensive error handling throughout

---

### Phase 9: Table Function System - Optional Callbacks (Tasks 40-41)

#### ⏳ Task 40: Implement Optional Callbacks - Part 1

**File**: `src/cityjson/optional_callbacks.cpp`
**Status**: ⏳ Pending
**Estimated LOC**: ~150
**Details**:

- [ ] Implement `CityJSONCardinality()`

  ```cpp
  unique_ptr<NodeStatistics> CityJSONCardinality(
      ClientContext &context,
      const FunctionData *bind_data_p) {

      auto &bind_data = bind_data_p->Cast<CityJSONBindData>();

      // Count total CityObjects
      size_t total = bind_data.chunks.TotalCityObjectCount();

      auto stats = make_uniq<NodeStatistics>();
      stats->has_estimated_cardinality = true;
      stats->estimated_cardinality = total;
      stats->has_max_cardinality = true;
      stats->max_cardinality = total;

      return stats;
  }
  ```

- [ ] Implement `CityJSONProgress()`

  ```cpp
  double CityJSONProgress(
      ClientContext &context,
      const FunctionData *bind_data_p,
      const GlobalTableFunctionState *global_state_p) {

      auto &bind_data = bind_data_p->Cast<CityJSONBindData>();
      auto &global_state = global_state_p->Cast<CityJSONGlobalState>();

      size_t total = bind_data.chunks.TotalCityObjectCount();
      if (total == 0) return 1.0;

      size_t processed = global_state.batch_index.load() * STANDARD_VECTOR_SIZE;
      return std::min(1.0, static_cast<double>(processed) / static_cast<double>(total));
  }
  ```

#### ⏳ Task 41: Implement Optional Callbacks - Part 2

**File**: `src/cityjson/optional_callbacks.cpp` (continued)
**Status**: ⏳ Pending
**Estimated LOC**: ~50
**Details**:

- [ ] Stub out `CityJSONStatistics()`

  ```cpp
  unique_ptr<BaseStatistics> CityJSONStatistics(
      ClientContext &context,
      const FunctionData *bind_data_p,
      column_t column_index) {
      // Return nullptr for MVP
      // Future: could return min/max for numeric columns
      return nullptr;
  }
  ```

- [ ] Stub out serialization callbacks (for future distributed execution)

---

### Phase 10: Registration & Integration (Tasks 42-44)

#### ⏳ Task 42: Implement Table Function Registration

**File**: `src/cityjson/table_function_registration.cpp`
**Status**: ⏳ Pending
**Estimated LOC**: ~200
**Details**:

- [ ] Implement `CreateReadCityJSONFunction()`

  ```cpp
  TableFunction CreateReadCityJSONFunction() {
      TableFunction func("read_cityjson", {LogicalType::VARCHAR}, CityJSONFunction, CityJSONBind);

      // Set description
      func.description = "Read CityJSON or CityJSONSeq files";

      // Named parameters
      func.named_parameters["sample_lines"] = LogicalType::BIGINT;

      // Set callbacks
      func.init_global = CityJSONInitGlobal;
      func.init_local = CityJSONInitLocal;
      func.cardinality = CityJSONCardinality;
      func.get_progress = CityJSONProgress;
      func.statistics = CityJSONStatistics;

      // Enable projection pushdown
      func.projection_pushdown = true;

      // Enable filter pushdown (future)
      // func.filter_pushdown = true;
      // func.filter_prune = true;
      // func.pushdown_complex_filter = CityJSONPushdownComplexFilter;

      return func;
  }
  ```

#### ⏳ Task 43: Implement RegisterCityJSONFunction

**File**: `src/cityjson/table_function_registration.cpp` (continued)
**Status**: ⏳ Pending
**Estimated LOC**: ~50
**Details**:

- [ ] Implement registration function

  ```cpp
  void RegisterCityJSONFunction(DatabaseInstance &db) {
      auto func = CreateReadCityJSONFunction();
      ExtensionUtil::RegisterFunction(db, func);
  }
  ```

- [ ] Handle registration errors

#### ⏳ Task 44: Update Extension Entry Point

**Files**: `src/include/cityjson_extension.hpp`, `src/cityjson_extension.cpp`
**Status**: ⏳ Pending
**Estimated LOC**: ~50
**Details**:

- [ ] Update `cityjson_extension.hpp`
  - Add include for `cityjson/table_function.hpp`
- [ ] Update `cityjson_extension.cpp`
  - Remove dummy scalar functions
  - Call `RegisterCityJSONFunction(db)` in `LoadInternal()`

  ```cpp
  void CityjsonExtension::Load(DuckDB &db) {
      LoadInternal(*db.instance);
  }

  void CityjsonExtension::LoadInternal(DatabaseInstance &db) {
      // Register table function
      RegisterCityJSONFunction(db);
  }
  ```

---

### Phase 11: Build System & Testing (Tasks 45-50)

#### ⏳ Task 45: Update CMakeLists.txt

**File**: `CMakeLists.txt`
**Status**: ⏳ Pending
**Details**:

- [ ] Add all new source files to `EXTENSION_SOURCES`

  ```cmake
  set(EXTENSION_SOURCES
      src/cityjson_extension.cpp
      src/cityjson/error.cpp
      src/cityjson/types.cpp
      src/cityjson/json_utils.cpp
      src/cityjson/cityjson_types.cpp
      src/cityjson/column_types.cpp
      src/cityjson/temporal_parser.cpp
      src/cityjson/city_object_utils.cpp
      src/cityjson/reader.cpp
      src/cityjson/local_cityjson_reader.cpp
      src/cityjson/local_cityjsonseq_reader.cpp
      src/cityjson/reader_factory.cpp
      src/cityjson/vector_writer.cpp
      src/cityjson/bind_data.cpp
      src/cityjson/global_state.cpp
      src/cityjson/local_state.cpp
      src/cityjson/bind_function.cpp
      src/cityjson/init_global.cpp
      src/cityjson/init_local.cpp
      src/cityjson/scan_function.cpp
      src/cityjson/optional_callbacks.cpp
      src/cityjson/table_function_registration.cpp
  )
  ```

- [ ] Add nlohmann-json dependency

  ```cmake
  find_package(nlohmann_json REQUIRED)
  target_link_libraries(${EXTENSION_NAME} nlohmann_json::nlohmann_json)
  target_link_libraries(${LOADABLE_EXTENSION_NAME} nlohmann_json::nlohmann_json)
  ```

- [ ] Update `vcpkg.json` to include nlohmann-json

  ```json
  {
    "dependencies": [
      "openssl",
      "nlohmann-json"
    ]
  }
  ```

#### ⏳ Task 46: Create Test Infrastructure

**File**: `test/cpp/test_common.hpp`
**Status**: ⏳ Pending
**Estimated LOC**: ~200
**Details**:

- [ ] Setup test fixtures

  ```cpp
  #pragma once
  #include "duckdb.hpp"
  #include "cityjson/types.hpp"
  #include "cityjson/cityjson_types.hpp"
  #include <string>

  namespace duckdb {
  namespace cityjson {
  namespace test {

  // Sample CityJSON data
  const std::string SAMPLE_CITYJSON_MINIMAL = R"({
    "type": "CityJSON",
    "version": "2.0",
    "CityObjects": {
      "building1": {
        "type": "Building",
        "attributes": {
          "yearOfConstruction": 2020
        }
      }
    },
    "vertices": []
  })";

  // Helper functions
  void CreateTestFile(const std::string& path, const std::string& content);
  void CleanupTestFile(const std::string& path);

  } // namespace test
  } // namespace cityjson
  } // namespace duckdb
  ```

#### ⏳ Task 47: Write Unit Tests - Column System

**File**: `test/cpp/test_column_types.cpp`
**Status**: ⏳ Pending
**Estimated LOC**: ~400
**Details**:

- [ ] Test `ToString()` for all ColumnType values
- [ ] Test `ToLogicalTypeId()` for all types
- [ ] Test `ToDuckDBType()` for complex types
- [ ] Test `Parse()` with valid names
- [ ] Test `Parse()` with aliases (INT, INTEGER, BIGINT → BigInt)
- [ ] Test `Parse()` with invalid names (should throw)
- [ ] Test `InferFromJson()` with various JSON values
  - null → Varchar
  - boolean → Boolean
  - integer → BigInt
  - float → Double
  - string → Varchar (or Timestamp/Date/Time)
  - array of strings → VarcharArray
  - array of mixed → Json
  - object → Json
- [ ] Test `ResolveFromSamples()`
  - All BigInt → BigInt
  - All Double → Double
  - Mix BigInt/Double → Double
  - Mix BigInt/Varchar → Varchar
  - Empty → Varchar
- [ ] Test `GetDefinedColumns()` returns 7 columns

#### ⏳ Task 48: Write Unit Tests - Utilities

**Files**: `test/cpp/test_temporal_parser.cpp`, `test/cpp/test_city_object_utils.cpp`
**Status**: ⏳ Pending
**Estimated LOC**: ~400
**Details**:

**test_temporal_parser.cpp**:

- [ ] Test `ParseDateString()` with valid dates
- [ ] Test `ParseDateString()` with invalid dates (should throw)
- [ ] Test `ParseTimestampString()` with various ISO 8601 formats
- [ ] Test `ParseTimestampString()` with invalid formats (should throw)
- [ ] Test `ParseTimeString()` with valid times
- [ ] Test `ParseTimeString()` with invalid times (should throw)
- [ ] Test edge cases: leap years, timezone offsets, microseconds

**test_city_object_utils.cpp**:

- [ ] Test `GetAttributeValue()` for predefined columns
  - object_type, children, parents, children_roles
- [ ] Test `GetAttributeValue()` for dynamic attributes
- [ ] Test `GetGeometryValue()` with various LODs
- [ ] Test `InferAttributeColumns()` with sample features
  - Empty features → no columns
  - Features with attributes → inferred columns
  - Mixed types → correct resolution

#### ⏳ Task 49: Write Integration Tests - Reader

**File**: `test/cpp/test_reader.cpp`
**Status**: ⏳ Pending
**Estimated LOC**: ~400
**Details**:

- [ ] Test `LocalCityJSONReader` with sample .city.json file
  - ReadMetadata() returns correct CityJSON struct
  - ReadNFeatures() returns N features
  - ReadAllChunks() returns all data correctly chunked
  - Columns() returns correct schema
- [ ] Test `LocalCityJSONSeqReader` with sample .city.jsonl file
  - ReadMetadata() parses first line
  - ReadNFeatures() reads N features
  - ReadAllChunks() reads all features
  - Columns() returns correct schema
- [ ] Test `OpenAnyCityJSONFile()` factory
  - Returns LocalCityJSONReader for .city.json
  - Returns LocalCityJSONSeqReader for .city.jsonl
  - Throws for unsupported extensions

#### ⏳ Task 50: Write SQL Integration Tests

**File**: `test/sql/read_cityjson.test`
**Status**: ⏳ Pending
**Estimated LOC**: ~300
**Details**:

- [ ] Test basic table function call

  ```sql
  -- Test basic read
  SELECT * FROM read_cityjson('test/data/sample.city.json');
  ```

- [ ] Test column projection

  ```sql
  SELECT id, object_type FROM read_cityjson('test/data/sample.city.json');
  ```

- [ ] Test with CityJSONSeq format

  ```sql
  SELECT * FROM read_cityjson('test/data/sample.city.jsonl');
  ```

- [ ] Test with named parameters

  ```sql
  SELECT * FROM read_cityjson('test/data/sample.city.json', sample_lines=50);
  ```

- [ ] Test schema inference

  ```sql
  DESCRIBE SELECT * FROM read_cityjson('test/data/sample.city.json');
  ```

- [ ] Test with various CityJSON files
  - Buildings
  - Roads
  - Mixed object types
  - With geometries at multiple LODs
  - With custom attributes
- [ ] Test error cases
  - Invalid file path
  - Malformed JSON
  - Invalid CityJSON schema

---

## File Structure

### Created Files ✅

```
src/include/cityjson/
├── types.hpp                      ✅ Core types & enums
├── error.hpp                      ✅ Error handling
├── json_utils.hpp                 ✅ JSON utilities
├── cityjson_types.hpp             ✅ CityJSON data structures
└── column_types.hpp               ✅ Column type system
```

### To Be Created ⏳

```
src/include/cityjson/
├── temporal_parser.hpp            ⏳ Date/time parsing
├── city_object_utils.hpp          ⏳ Attribute extraction
├── reader.hpp                     ⏳ Reader interface
├── vector_writer.hpp              ⏳ Vector writing system
└── table_function.hpp             ⏳ Table function declarations

src/cityjson/
├── error.cpp                      ⏳
├── types.cpp                      ⏳
├── json_utils.cpp                 ⏳
├── cityjson_types.cpp             ⏳
├── column_types.cpp               ⏳
├── temporal_parser.cpp            ⏳
├── city_object_utils.cpp          ⏳
├── reader.cpp                     ⏳
├── local_cityjson_reader.cpp      ⏳
├── local_cityjsonseq_reader.cpp   ⏳
├── reader_factory.cpp             ⏳
├── vector_writer.cpp              ⏳
├── bind_data.cpp                  ⏳
├── global_state.cpp               ⏳
├── local_state.cpp                ⏳
├── bind_function.cpp              ⏳
├── init_global.cpp                ⏳
├── init_local.cpp                 ⏳
├── scan_function.cpp              ⏳
├── optional_callbacks.cpp         ⏳
└── table_function_registration.cpp ⏳

test/cpp/
├── test_common.hpp                ⏳
├── test_column_types.cpp          ⏳
├── test_temporal_parser.cpp       ⏳
├── test_city_object_utils.cpp     ⏳
└── test_reader.cpp                ⏳

test/sql/
└── read_cityjson.test             ⏳

test/data/
├── sample.city.json               ⏳ (sample data file)
└── sample.city.jsonl              ⏳ (sample data file)
```

**Total Files**:

- ✅ Created: 5 headers
- ⏳ Remaining: 28 files (10 headers + 18 implementations)
- **Total**: 33 files

---

## Dependencies

### Component Dependencies

```
Phase 1 (Foundation)
└─ No dependencies

Phase 2 (CityJSON Data)
└─ Depends on: Phase 1

Phase 3 (Column System)
└─ Depends on: Phases 1, 2

Phase 4 (Utilities)
└─ Depends on: Phases 1, 2, 3

Phase 5 (Readers)
└─ Depends on: Phases 1, 2, 3, 4

Phase 6 (Vector Writers)
└─ Depends on: Phases 1, 2, 3

Phase 7-10 (Table Function)
└─ Depends on: ALL previous phases
```

### External Dependencies

1. **DuckDB**
   - Version: 1.4.1+ (from submodule)
   - Headers: `duckdb.hpp`, `duckdb/common/types.hpp`, etc.
   - API: Table function system, Vector API

2. **nlohmann/json**
   - Purpose: JSON parsing and serialization
   - Installation: via vcpkg
   - Header: `<nlohmann/json.hpp>`

3. **C++ Standard Library**
   - C++23 features: `std::expected` (or C++17 alternative)
   - C++20 features: `std::span`, concepts
   - C++17 features: `std::optional`, `std::variant`

---

## Testing Strategy

### Unit Tests (Per Component)

| Component | Test File | Key Test Cases |
|-----------|-----------|----------------|
| Column Types | `test_column_types.cpp` | ToString, Parse, InferFromJson, ResolveFromSamples |
| Temporal Parser | `test_temporal_parser.cpp` | Date/Time/Timestamp parsing, edge cases |
| CityObject Utils | `test_city_object_utils.cpp` | Attribute extraction, schema inference |
| Readers | `test_reader.cpp` | CityJSON/CityJSONSeq parsing, chunking |
| Vector Writers | (inline in scan tests) | Primitive/complex type writing |

### Integration Tests

| Test Type | Test File | Purpose |
|-----------|-----------|---------|
| SQL Tests | `read_cityjson.test` | End-to-end SQL functionality |
| C++ Integration | `test_reader.cpp` | Multi-component integration |

### Test Data

Create sample CityJSON files:

1. **Minimal** (`test/data/minimal.city.json`)
   - Single building, no geometry
   - Tests basic parsing

2. **Buildings** (`test/data/buildings.city.json`)
   - Multiple buildings with geometries at LOD 1.0, 2.0
   - Tests geometry handling, LOD parsing

3. **Mixed** (`test/data/mixed.city.json`)
   - Buildings, roads, bridges
   - Tests multiple object types

4. **Attributes** (`test/data/attributes.city.json`)
   - Rich custom attributes
   - Tests schema inference, type resolution

5. **CityJSONSeq** (`test/data/sample.city.jsonl`)
   - Multiple features in newline-delimited format
   - Tests CityJSONSeq reader

### Testing Workflow

```bash
# Build and run tests
make test_debug

# Run specific SQL test
./build/debug/test/unittest test/sql/read_cityjson.test

# Run C++ unit tests
./build/debug/test/unittest --test-dir test/cpp/
```

---

## Build Configuration

### CMake Changes Required

1. **Add source files** (see Task 45)
2. **Add nlohmann-json dependency**
3. **Update vcpkg.json**
4. **Configure include directories** (already done: `include_directories(src/include)`)

### Build Commands

```bash
# Initial setup
GEN=ninja make

# Build debug version
make test_debug

# Build release version
make

# Run tests
make test

# Clean build
make clean
```

### Output Artifacts

- Debug: `build/debug/extension/cityjson/cityjson.duckdb_extension`
- Release: `build/release/extension/cityjson/cityjson.duckdb_extension`

---

## Critical Implementation Notes

### DuckDB-Specific Constraints

1. **STANDARD_VECTOR_SIZE**: Maximum 2048 rows per DataChunk
   - Never exceed this limit in scan function
   - Use for chunking CityObjects

2. **Thread Safety**
   - Use `std::atomic` for `batch_index` in global state
   - Bind data must be const/immutable after bind phase
   - Local state is thread-local

3. **Memory Management**
   - Use `StringVector::AddString()` for strings (DuckDB manages memory)
   - Use `ListVector::Reserve()` before writing lists
   - Use `FlatVector::SetNull()` for NULL values

4. **Error Handling**
   - Throw DuckDB exceptions in callbacks:
     - `BinderException` in bind function
     - `InternalException` in scan function
   - Provide context in error messages

5. **Type System**
   - Complex types require `child_list_t<LogicalType>`
   - List types: `LogicalType::LIST(child_type)`
   - Struct types: `LogicalType::STRUCT(children)`

### CityJSON-Specific Notes

1. **Vertex Compression**
   - Apply transform if present: `real = vertex * scale + translate`
   - Handle missing transform (identity)

2. **LOD Handling**
   - LOD format: `"X.Y"` (e.g., `"2.1"`)
   - Store as VARCHAR, not numeric
   - Column name pattern: `geom_lod{X}_{Y}`

3. **Feature vs CityObject**
   - CityJSONSeq: 1 feature = N CityObjects
   - CityJSON: entire file = 1 implicit feature with N CityObjects
   - Feature ID stored in `feature_id` column

4. **Schema Inference**
   - Sample first N lines/features (default 100)
   - Infer types from all sampled values
   - Resolve conflicts (promote BigInt→Double, fall back to Varchar)

### Performance Considerations

1. **Chunking**
   - Chunk data during bind phase
   - Atomic batch assignment for parallelism
   - Balance chunk size vs memory usage

2. **Projection Pushdown**
   - Only create vectors for projected columns
   - Skip reading non-projected attributes
   - Significant performance gain for wide tables

3. **Vectorized Operations**
   - Write entire batch in scan function
   - Avoid row-by-row processing
   - Use DuckDB's optimized vector operations

---

## Next Steps

### Immediate Next Tasks (Resume from Task 6)

1. ⏳ **Task 6**: Define reader system header signatures ([reader.hpp](src/include/cityjson/reader.hpp:1))
2. ⏳ **Task 7**: Define utility headers signatures ([temporal_parser.hpp](src/include/cityjson/temporal_parser.hpp:1), [city_object_utils.hpp](src/include/cityjson/city_object_utils.hpp:1))
3. ⏳ **Task 8**: Define vector writing system header signatures ([vector_writer.hpp](src/include/cityjson/vector_writer.hpp:1))

Then proceed with Phase 2 implementations (Tasks 9-15).

### Recommended Workflow

1. **Complete all header signatures first** (Tasks 6-8)
   - Provides complete API surface
   - Enables parallel implementation

2. **Implement foundation layer** (Tasks 9-15)
   - Bottom-up approach
   - Each component can be unit tested immediately

3. **Implement one vertical slice** (Tasks 16-21, 28-39)
   - Get basic end-to-end functionality working
   - Helps validate architecture early

4. **Fill in remaining features** (Tasks 22-27, 40-44)
   - Add complex type writers
   - Add optional callbacks
   - Polish and optimize

5. **Testing and integration** (Tasks 45-50)
   - Unit tests throughout
   - Integration tests at end
   - SQL tests validate complete functionality

---

## Glossary

- **CityJSON**: Specification for encoding 3D city models as JSON
- **CityJSONSeq**: Newline-delimited variant (`.city.jsonl`)
- **CityObject**: Individual entity (building, road, etc.)
- **CityJSONFeature**: Container for CityObjects (1:N relationship)
- **LOD**: Level of Detail (geometry resolution)
- **DataChunk**: DuckDB's batch of rows (max 2048)
- **Vector**: DuckDB's columnar data structure
- **Bind Phase**: Schema inference and planning
- **Scan Phase**: Data reading and vectorization
- **Projection Pushdown**: Reading only requested columns

---

## References

- **Design Document**: [DESIGN_DOC.md](DESIGN_DOC.md)
- **DuckDB C++ API**: <https://duckdb.org/docs/stable/clients/c/api>
- **CityJSON Specification**: <https://www.cityjson.org/specs/2.0.1/>
- **DuckDB Extension Template**: <https://github.com/duckdb/extension-template>
- **nlohmann/json**: <https://json.nlohmann.me/>

---
