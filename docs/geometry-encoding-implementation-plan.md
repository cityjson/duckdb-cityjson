# CityJSON Geometry Encoding Implementation Plan

## Document Status

**Version:** 1.1
**Last Updated:** 2026-02-06
**Related Specification:** `docs/geometry-encoding-spec.md`

### Implementation Progress

| Phase   | Status      | Description                                                    |
| ------- | ----------- | -------------------------------------------------------------- |
| Phase 0 | ✅ Complete | Documentation Discovery                                        |
| Phase 1 | ✅ Complete | WKB Encoding Library (`wkb_encoder.hpp/cpp`)                   |
| Phase 2 | ✅ Complete | Geometry Properties Serializer (`geometry_properties.hpp/cpp`) |
| Phase 3 | ✅ Complete | Per-LOD Table Schema (`lod_table.hpp/cpp`)                     |
| Phase 4 | ✅ Complete | Scan Function Modification (`lod` parameter + WKB columns)     |
| Phase 5 | ✅ Complete | Table Function Registration (`lod` named parameter)            |
| Phase 6 | ✅ Complete | Testing (24 tests passing)                                     |
| Phase 7 | ✅ Complete | Documentation (README updated)                                 |

---

## Phase 0: Documentation Discovery Summary

### Libraries and Dependencies

| Library                  | Purpose                                              | Source                           |
| ------------------------ | ---------------------------------------------------- | -------------------------------- |
| **geozero**              | WKB encoding with 3D, PolyhedralSurface, TIN support | https://crates.io/crates/geozero |
| **nlohmann/json**        | Already used for JSON parsing                        | Existing dependency              |
| **DuckDB GEOMETRY type** | Native spatial type with WKB functions               | `ST_GeomFromWKB()`, `ST_AsWKB()` |

### Current Code Structure

| File                                 | Purpose                    | Key Functions                                    |
| ------------------------------------ | -------------------------- | ------------------------------------------------ |
| `src/cityjson/cityjson_types.cpp`    | Geometry struct definition | `Geometry::ToJson()`                             |
| `src/cityjson/city_object_utils.cpp` | Geometry extraction        | `GetGeometryValue()`, `InferGeometryColumns()`   |
| `src/cityjson/column_types.cpp`      | Column type handling       | `IsGeometryColumn()`, `ParseLODFromColumnName()` |
| `src/cityjson/scan_function.cpp`     | Main scan function         | `CityJSONScan()`                                 |

### Current Data Flow

```
CityJSON File → Parse JSON → Extract vertices & boundaries
                                     ↓
                    Store as JSON string in STRUCT columns
                                     ↓
                    DuckDB returns: geom_lod2_1 STRUCT {lod, type, boundaries, ...}
```

### Target Data Flow

```
CityJSON File → Parse JSON → Dereference vertices → Encode as WKB
                                                      ↓
                                         geometry: WKB_BLOB / GEOMETRY
                                                      ↓
                         geometry_properties: JSON {type, semantics, ...}
                                                      ↓
                    DuckDB returns: buildings_lod2 (id, geometry, geometry_properties, ...)
```

---

## Phase 1: WKB Encoding Library

### 1.1 Add WKB Dependency

**File:** `CMakeLists.txt`

```cmake
# Option 1: Use geozero via Rust FFI (add to cjseq/Cargo.toml)
# Option 2: Implement WKB encoding directly in C++ (recommended for this codebase)
```

**Decision:** Implement WKB encoding in C++ to match existing codebase pattern.

### 1.2 Create WKB Encoder Module

**New Files:**

- `src/cityjson/wkb_encoder.hpp` - WKB encoder declaration
- `src/cityjson/wkb_encoder.cpp` - WKB encoder implementation

**Interface (copy from specification):**

```cpp
// wkb_encoder.hpp
#pragma once

#include <vector>
#include <string>
#include <cstdint>
#include "cityjson_types.hpp"

namespace cityjson {

// OGC WKB geometry type codes
enum class WKBGeometryType : uint32_t {
    Point = 1,
    LineString = 2,
    Polygon = 3,
    MultiPoint = 4,
    MultiLineString = 5,
    MultiPolygon = 6,
    GeometryCollection = 7,
    PolyhedralSurface = 15,
    TIN = 16,
    Triangle = 17
};

// Byte order markers
constexpr uint8_t WKB_XDR = 0;  // Big Endian
constexpr uint8_t WKB_NDR = 1;  // Little Endian

class WKBEncoder {
public:
    // Encode CityJSON Geometry to WKB
    // Returns: WKB bytes
    static std::vector<uint8_t> Encode(
        const Geometry& geometry,
        const std::vector<std::array<double, 3>>& vertices,
        const std::optional<Transform>& transform = std::nullopt
    );

    // Encode with explicit geometry type override
    static std::vector<uint8_t> EncodeAsType(
        const Geometry& geometry,
        WKBGeometryType target_type,
        const std::vector<std::array<double, 3>>& vertices,
        const::std::optional<Transform>& transform = std::nullopt
    );

    // Get OGC geometry type for CityJSON type
    static WKBGeometryType GetOGCType(const std::string& cityjson_type);

private:
    // Helper methods for encoding specific geometry types
    static void EncodePoint(std::vector<uint8_t>& out, const std::array<double, 3>& vertex, bool has_z);
    static void MultiPoint(std::vector<uint8_t>& out, ...);
    static void EncodeLineString(std::vector<uint8_t>& out, ...);
    static void EncodePolygon(std::vector<uint8_t>& out, ...);
    static void EncodeMultiPolygon(std::vector<uint8_t>& out, ...);
    static void EncodePolyhedralSurface(std::vector<uint8_t>& out, ...);

    // Apply transform to vertex
    static std::array<double, 3> ApplyTransform(
        const std::array<double, 3>& vertex,
        const Transform& transform
    );

    // Reverse ring orientation (CityGML → OGC)
    static void ReverseRing(std::vector<uint32_t>& ring);
};

} // namespace cityjson
```

### 1.3 Implement Geometry Type Encoders

**Reference for boundary structure:** `src/cityjson/cityjson_types.cpp:202-247`

**Implementation Tasks:**

1. **Point** (`[vertex_index]`)

   ```cpp
   // Single vertex, output as POINT Z
   ```

2. **MultiPoint** (`[[v1, v2, ...]]`)

   ```cpp
   // Array of vertices, output as MULTIPOINT Z
   ```

3. **LineString** (`[[v1, v2, ...]]`)

   ```cpp
   // Array of vertices, output as LINESTRING Z
   ```

4. **MultiLineString** (`[[[v1, v2]], [[v3, v4, v5]]]`)

   ```cpp
   // Array of LineStrings, output as MULTILINESTRING Z
   ```

5. **Polygon / Surface** (`[[[exterior]], [[interior]]]`)

   ```cpp
   // Exterior + interior rings, output as POLYGON Z
   // Reverse ring orientation for OGC compatibility
   ```

6. **MultiSurface** (`[[[surface1]], [[surface2]]]`)

   ```cpp
   // Array of surfaces, output as MULTIPOLYGON Z
   ```

7. **Solid** (`[[[[shell]]], [[[interior]]]]`)

   ```cpp
   // Array of shells, output as POLYHEDRALSURFACE Z
   // Flatten: all shell polygons in one PolyhedralSurface
   ```

8. **TIN** (`[[[tri1]], [[tri2]]]`)
   ```cpp
   // Array of triangles, output as TIN Z
   ```

### 1.4 Vertex Dereferencing

**Reference:** Current vertex handling in `src/cityjson/cityjson_types.cpp`

```cpp
// Current: vertices stored as JSON boundaries with indices
// Target: Look up actual coordinates during encoding

std::array<double, 3> GetVertex(const std::vector<std::array<double, 3>>& vertices,
                                 const json& boundary_ref,
                                 const Transform& transform) {
    uint32_t index = boundary_ref.get<uint32_t>();
    auto& vertex = vertices[index];
    return {transform.scale[0] * vertex[0] + transform.translate[0],
            transform.scale[1] * vertex[1] + transform.translate[1],
            transform.scale[2] * vertex[2] + transform.translate[2]};
}
```

### 1.5 Verification Checklist

- [ ] WKB encoder module compiles
- [ ] Unit test for each geometry type
- [ ] WKB output validates with `ST_GeomFromWKB()` in DuckDB
- [ ] 3D coordinates preserved (Z dimension)
- [ ] Transform metadata applied correctly
- [ ] Ring orientation reversed for OGC compatibility

**Test Command:**

```sql
-- After building extension
LOAD cityjson;
CREATE TABLE test AS SELECT * FROM read_cityjson('test/data/minimal.city.json');
SELECT id, ST_GeomFromWKB(geometry) as geom FROM buildings_lod2;
```

---

## Phase 2: Geometry Properties JSON Schema

### 2.1 Create Properties Serializer

**New Files:**

- `src/cityjson/geometry_properties.hpp`
- `src/cityjson/geometry_properties.cpp`

**Interface:**

```cpp
// geometry_properties.hpp
#pragma once

#include "cityjson_types.hpp"
#include <json.hpp>

namespace cityjson {

struct GeometryProperties {
    int type;                          // Geometry type code (1-17)
    std::string cityjsonType;          // Original CityJSON type
    std::string lod;                   // Level of detail
    std::optional<std::string> objectId;
    std::vector<GeometryPropertyChild> children;
    std::optional<Semantics> semantics;
};

struct GeometryPropertyChild {
    int type;
    std::string cityjsonType;
    std::optional<std::string> objectId;
    std::optional<size_t> parent;      // Index in parent array
    std::optional<size_t> geometryIndex; // Index in WKB geometry
};

struct Semantics {
    std::vector<SemanticSurface> surfaces;
    json values;  // Maps to boundary indices
};

struct SemanticSurface {
    std::string type;                  // "WallSurface", "RoofSurface", etc.
    json attributes;                   // Optional additional attributes
};

class GeometryPropertiesSerializer {
public:
    // Serialize CityJSON Geometry to properties JSON
    static json Serialize(const Geometry& geometry);

    // Get geometry type code
    static int GetTypeCode(const std::string& cityjson_type);

private:
    // Build hierarchical children structure
    static std::vector<GeometryPropertyChild> BuildChildren(
        const Geometry& geometry
    );
};

} // namespace cityjson
```

### 2.2 Type Code Mapping

**Reference:** Specification section 4.3

```cpp
int GeometryPropertiesSerializer::GetTypeCode(const std::string& cityjson_type) {
    static const std::unordered_map<std::string, int> codes = {
        {"Point", 1},
        {"MultiPoint", 2},
        {"LineString", 3},
        {"MultiLineString", 4},
        {"Surface", 5},
        {"CompositeSurface", 6},
        {"TIN", 7},
        {"MultiSurface", 8},
        {"Solid", 9},
        {"CompositeSolid", 10},
        {"MultiSolid", 11}
    };
    auto it = codes.find(cityjson_type);
    return (it != codes.end()) ? it->second : -1;
}
```

### 2.3 Verification Checklist

- [ ] Properties serializer compiles
- [ ] Output matches specification schema
- [ ] Type codes correct for all geometry types
- [ ] Semantics preserved correctly
- [ ] Children hierarchy built for Solid/MultiSolid

---

## Phase 3: Per-LOD Table Schema

### 3.1 Modify Schema Inference

**File to modify:** `src/cityjson/city_object_utils.cpp:143-190`

**Current behavior:** Creates `geom_lodX_Y` columns in single table

**Target behavior:** Returns separate table definitions per LOD

```cpp
// New function
struct LODTableDefinition {
    std::string table_name;           // e.g., "buildings_lod2"
    std::string lod_value;            // e.g., "2.2"
    std::vector<Column> columns;
};

std::vector<LODTableDefinition> InferLODTables(
    const std::vector<CityJSONFeature>& features,
    size_t sample_size = 100
) {
    // 1. Collect all unique LODs
    std::set<std::string> lods;
    // ... sampling logic ...

    // 2. Create table definition for each LOD
    std::vector<LODTableDefinition> tables;
    for (const auto& lod : lods) {
        LODTableDefinition table;
        table.lod_value = lod;
        table.table_name = GetTableNameForLOD(lod);
        table.columns = GetBaseColumns();  // id, feature_id, object_type, attributes

        // Add geometry column (single GEOMETRY column, not multiple)
        table.columns.push_back(Column{
            .name = "geometry",
            .type = ColumnType::GeometryWKB  // New type for WKB
        });
        table.columns.push_back(Column{
            .name = "geometry_properties",
            .type = ColumnType::JSON
        });
        tables.push_back(table);
    }

    return tables;
}
```

### 3.2 Add New Column Type

**File:** `src/cityjson/include/cityjson/column_types.hpp`

```cpp
enum class ColumnType {
    // ... existing types ...
    GeometryWKB,      // WKB-encoded geometry (BLOB)
    GeometryProps     // Geometry properties JSON
};
```

### 3.3 Table Naming Convention

**Options to decide:**

1. Generic: `cityobjects_lod0`, `cityobjects_lod1`, `cityobjects_lod2`
2. Type-specific: `buildings_lod2`, `roads_lod2`, `vegetation_lod2`

**Recommendation:** Generic tables with `object_type` column for filtering.

### 3.4 Verification Checklist

- [ ] Schema inference returns separate table definitions
- [ ] Table names follow consistent convention
- [ ] Geometry column type is WKB_BLOB
- [ ] Geometry properties column is JSON

---

## Phase 4: Scan Function Modification

### 4.1 Update Scan Function

**File:** `src/cityjson/scan_function.cpp`

**Current:** Single table with dynamic LOD columns

**Target:** Multiple table functions, one per LOD

```cpp
// New approach: Register separate table function for each LOD
// or use a parameter to select LOD

// Option A: Parameter-based
CREATE TABLE buildings_lod2 AS
SELECT * FROM read_cityjson('file.city.json', lod => '2.2');

// Option B: Auto-generate all LOD tables
-- read_cityjson returns multiple result sets
```

**Implementation:**

```cpp
// Modified scan function
duckdb::unique_ptr<duckdb::GlobalTableFunctionState>
CityJSONScanGlobalState(duckdb::ClientContext& context,
                        duckdb::TableFunctionInitInput& input) {
    // Parse LOD parameter if provided
    auto& bind_data = input.bind_data->Cast<CityJSONBindData>();

    if (input.parameters.find("lod") != input.parameters.end()) {
        bind_data.target_lod = input.parameters["lod"].ToString();
    }

    // Filter CityObjects by LOD
    // Only process objects with geometry at target LOD
}
```

### 4.2 Data Writing

**File:** `src/cityjson/vector_writer.cpp`

```cpp
void WriteGeometryToVector(
    const Geometry& geometry,
    const std::vector<std::array<double, 3>>& vertices,
    const Transform& transform,
    duckdb::Vector& geometry_out,      // WKB_BLOB
    duckdb::Vector& properties_out     // JSON
) {
    // 1. Encode to WKB
    auto wkb = WKBEncoder::Encode(geometry, vertices, transform);

    // 2. Serialize properties
    auto props = GeometryPropertiesSerializer::Serialize(geometry);

    // 3. Write to vectors
    WriteWKBToVector(geometry_out, wkb);
    WriteJSONToVector(properties_out, props);
}
```

### 4.3 Verification Checklist

- [ ] Scan function filters by LOD parameter
- [ ] WKB bytes written correctly to BLOB column
- [ ] Properties JSON written correctly
- [ ] Objects without target LOD are skipped

---

## Phase 5: Table Function Registration

### 5.1 Update Registration

**File:** `src/cityjson/table_function_registration.cpp`

```cpp
void RegisterCityJSONTableFunction(duckdb::DatabaseInstance& db) {
    // Register with optional LOD parameter
    duckdb::TableFunctionSet function_set("read_cityjson");
    function_set.AddFunction(duckdb::TableFunction({
        duckdb::LogicalType::VARCHAR,  // file_path
        duckdb::LogicalType::VARCHAR   // lod (optional)
    }, CityJSONScanFunction, CityJSONBind, CityJSONInitGlobal, CityJSONInitLocal));

    extension->RegisterFunction(function_set);
}
```

### 5.2 SQL Interface

```sql
-- Read specific LOD
SELECT * FROM read_cityjson('citymodel.city.json', '2.2');

-- Create table from specific LOD
CREATE TABLE buildings_lod2 AS
SELECT * FROM read_cityjson('citymodel.city.json', '2.2');

-- Query with spatial functions
SELECT id, ST_Area(ST_GeomFromWKB(geometry)) as area
FROM buildings_lod2;
```

### 5.3 Verification Checklist

- [ ] Table function registered with LOD parameter
- [ ] SQL queries work with LOD parameter
- [ ] Spatial functions can consume WKB output

---

## Phase 6: Testing

### 6.1 Unit Tests

**New test file:** `test/cpp/test_wkb_encoder.cpp`

```cpp
TEST(WKBEncoder, Point) {
    Geometry geom = { .type = "Point", .boundaries = json(0) };
    std::vector<std::array<double, 3>> vertices = {{{1.0, 2.0, 3.0}}};

    auto wkb = WKBEncoder::Encode(geom, vertices);

    // Verify: byte order, type code, coordinates
    EXPECT_EQ(wkb[0], WKB_NDR);  // Little endian
    EXPECT_EQ(*reinterpret_cast<uint32_t*>(&wkb[1]), 1);  // Point type
    // ... verify coordinates
}

TEST(WKBEncoder, Solid) {
    // Test Solid → PolyhedralSurface conversion
}

TEST(WKBEncoder, TransformApplication) {
    // Test transform metadata is applied
}

TEST(WKBEncoder, RingOrientation) {
    // Test exterior ring is reversed for OGC
}
```

### 6.2 Integration Tests

**New test file:** `test/sql/cityjson_wkb.test`

```sql
-- Test WKB encoding
CREATE TABLE test AS SELECT * FROM read_cityjson('test/data/minimal.city.json', '2.2');

-- Verify WKB is valid
SELECT id, ST_GeomFromWKB(geometry) IS NOT NULL FROM test;

-- Verify geometry type
SELECT ST_GeometryType(ST_GeomFromWKB(geometry)) FROM test;

-- Verify Z dimension
SELECT ST_HasZ(ST_GeomFromWKB(geometry)) FROM test;

-- Verify geometry properties
SELECT geometry_properties->>'type' as cityjson_type FROM test;
```

### 6.3 Performance Benchmarks

**Metrics to track:**

- Import time (vs current JSON approach)
- File size (WKB vs JSON)
- Query performance with spatial functions

### 6.4 Verification Checklist

- [ ] All unit tests pass
- [ ] Integration tests pass
- [ ] Performance benchmarks acceptable
- [ ] Round-trip test (CityJSON → WKB → verify with spatial functions)

---

## Phase 7: Documentation

### 7.1 Update README

**File:** `README.md`

Add section on WKB encoding:

- How to use per-LOD tables
- Spatial function examples
- Geometry properties schema

### 7.2 Update Design Doc

**File:** `DESIGN_DOC.md`

Document new architecture with diagrams.

---

## Implementation Order

1. **Phase 1** (WKB Encoder) - Foundation, no breaking changes
2. **Phase 2** (Properties) - Depends on Phase 1
3. **Phase 6** (Unit Tests) - Parallel with Phase 1-2
4. **Phase 3** (Schema) - Non-breaking, can coexist with current
5. **Phase 4** (Scan) - Core integration
6. **Phase 5** (Registration) - Finalize API
7. **Phase 6** (Integration Tests) - Full end-to-end
8. **Phase 7** (Documentation) - Throughout

---

## Risk Mitigation

| Risk                            | Mitigation                               |
| ------------------------------- | ---------------------------------------- |
| DuckDB spatial not available    | Fall back to WKB_BLOB type               |
| PolyhedralSurface not supported | Store as WKB_BLOB, document limitation   |
| Performance regression          | Benchmark, optimize WKB encoding         |
| Breaking existing queries       | Support old API with deprecation warning |

---

## Open Decisions Needed

1. **Table naming**: Generic (`cityobjects_lod2`) or type-specific (`buildings_lod2`)?
2. **Multi-LOD objects**: If object has LOD1 AND LOD2, appears in both tables?
3. **API design**: Parameter-based (`lod => '2.2'`) or auto-generate all tables?
4. **DuckDB spatial dependency**: Require extension or support WKB_BLOB only?
