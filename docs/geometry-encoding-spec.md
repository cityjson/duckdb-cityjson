# CityJSON Geometry Encoding Specification

## 1. Current Implementation Summary

### 1.1 Current Storage Approach

The current DuckDB CityJSON extension stores all CityJSON data in a single table with dynamic LOD columns:

```sql
-- Current table structure
CREATE TABLE buildings AS
SELECT * FROM read_cityjson('citymodel.city.json');

-- Columns:
-- id: VARCHAR
-- feature_id: VARCHAR
-- object_type: VARCHAR
-- geom_lod0_1: STRUCT(lod VARCHAR, type VARCHAR, boundaries VARCHAR, ...)
-- geom_lod1_2: STRUCT(lod VARCHAR, type VARCHAR, boundaries VARCHAR, ...)
-- geom_lod2_2: STRUCT(lod VARCHAR, type VARCHAR, boundaries VARCHAR, ...)
-- ... (one column per LOD level found in data)
```

### 1.2 Current Geometry Encoding

Geometry is stored as JSON strings preserving the original CityJSON structure:

```json
{
  "lod": "2.2",
  "type": "Solid",
  "boundaries": "[[[[0, 1, 2, 3]]]]",
  "semantics": "{\"surfaces\":[...],\"values\":[...]}",
  "material": null,
  "texture": null
}
```

**Issues with current approach:**
1. Geometry as JSON is not compatible with standard GIS toolchains (QGIS, ArcGIS, PostGIS, etc.)
2. Multiple LOD columns in one table creates sparse data (most objects only have one LOD)
3. Cannot leverage DuckDB's spatial functions or indexes
4. Cannot interoperate with other spatial formats via WKB/WKT

---

## 2. Proposed Architecture

### 2.1 Per-LOD Table Structure

Instead of one table with LOD columns, create separate tables for each LOD level:

```sql
-- Instead of:
-- buildings (with geom_lod0, geom_lod1, geom_lod2 columns)

-- Create:
CREATE TABLE buildings_lod0 AS ...;
CREATE TABLE buildings_lod1 AS ...;
CREATE TABLE buildings_lod2 AS ...;

-- Each table has:
-- id: VARCHAR              -- CityObject identifier
-- feature_id: VARCHAR      -- Feature identifier
-- object_type: VARCHAR     -- Building, Road, etc.
-- attributes: JSON         -- Other CityObject attributes
-- geometry: GEOMETRY       -- WKB-encoded OGC geometry
-- geometry_properties: JSON -- CityJSON semantics mapping
```

**Benefits:**
- Each LOD table contains only objects with that LOD (no sparse columns)
- Can query only the LOD level you need
- Can join across LODs if needed
- Each row has exactly one geometry (simpler than multiple LOD columns)

### 2.2 WKB Geometry Encoding

Store geometry as standard OGC WKB (Well-Known Binary) for GIS compatibility:

```sql
-- DuckDB GEOMETRY type (WKB internally)
-- Access via ST_AsText() for WKT representation
SELECT
    id,
    ST_AsText(geometry) as wkt_geometry,
    geometry_properties
FROM buildings_lod2;
```

Example output:
```
id  | wkt_geometry                           | geometry_properties
----+----------------------------------------+---------------------
b1  | POLYHEDRALSURFACE Z ((0 0 0, 1 0 0, ...)) | {"type": "Solid", ...}
```

---

## 3. CityJSON to OGC Geometry Type Mapping

### 3.1 Mapping Table

| CityJSON Type | Boundaries Depth | OGC/PostGIS Type | WKB Type Code | Notes |
|---------------|------------------|------------------|---------------|-------|
| `Point` | 0 | `POINT Z` | 1 | Single 3D point |
| `MultiPoint` | 1 | `MULTIPOINT Z` | 4 | Array of points |
| `LineString` | 2 | `LINESTRING Z` | 2 | Linear geometry |
| `MultiLineString` | 2 | `MULTILINESTRING Z` | 5 | Array of LineStrings |
| `Surface` | 3 | `POLYGON Z` | 3 | Single polygon with holes |
| `MultiSurface` | 3 | `MULTIPOLYGON Z` | 6 | Array of polygons |
| `TIN` | 3 | `TIN Z` | - | Triangulated surface (PostGIS extension) |
| `CompositeSurface` | 3 | `MULTIPOLYGON Z` | 6 | Treated as MultiPolygon |
| `Solid` | 4 | `POLYHEDRALSURFACE Z` | - | Array of polygons (PostGIS) |
| `MultiSolid` | 5 | `GEOMETRYCOLLECTION Z` | 7 | Collection of solids |
| `CompositeSolid` | 5 | `GEOMETRYCOLLECTION Z` | 7 | Collection of solids |

### 3.2 Boundary Structure Reference

CityJSON uses vertex references into a shared vertex array. During WKB encoding, we **dereference these indices** and store actual coordinates for maximum query readiness.

```
CityJSON Structure (input):
---------------------------
vertices: [[x1,y1,z1], [x2,y2,z2], [x3,y3,z3], ...]
boundaries: [[1, 2, 3]]  → references vertices[1], vertices[2], vertices[3]

WKB Encoding (output):
----------------------
Store actual coordinates: (x1, y1, z1), (x2, y2, z2), (x3, y3, z3)

Mapping Examples:
----------------
Point:         [1]                    → POINT Z (x1, y1, z1)
MultiPoint:    [[1], [2], [3]]        → MULTIPOINT Z ((x1,y1,z1), (x2,y2,z2), (x3,y3,z3))
LineString:    [[1,2,3]]              → LINESTRING Z ((x1,y1,z1), (x2,y2,z2), (x3,y3,z3))
MultiLineString: [[1,2], [3,4,5]]     → MULTILINESTRING Z
Surface:       [[[1,2,3,4]], [[5,6,7,8]]]  → POLYGON Z with exterior and interior rings
MultiSurface:  [[[1,2,3,4]], [[5,6,7,8]]]  → MULTIPOLYGON Z
Solid:         [[[[1,2,3,4]]], [[[5,6,7,8]]]] → POLYHEDRALSURFACE Z
```

**Key Design Decision:** By storing actual coordinates in WKB rather than vertex references:
- Data is immediately queryable with spatial functions (ST_Intersects, ST_Contains, etc.)
- No joins needed to retrieve coordinates
- Standard GIS tools can work with the data without CityJSON-specific knowledge
- Coordinate transformation (if needed) happens once at import time

---

## 4. Geometry Properties Schema

### 4.1 Purpose

The `geometry_properties` column stores JSON metadata that:

1. Preserves CityJSON/CityGML semantics lost in WKB conversion
2. Enables round-trip conversion (WKB → CityJSON)
3. Stores semantic surface information

**Note:** Materials and textures are stored in separate tables (to be designed later) and referenced via foreign keys.

### 4.2 Schema Structure (Based on 3DCityDB)

```json
{
  "type": 9,                      // Geometry type code (see table below)
  "cityjsonType": "Solid",        // Original CityJSON type name
  "lod": "2.2",                   // Level of Detail
  "objectId": "mySolid",          // Optional geometry identifier
  "children": [                   // Child geometry elements
    {
      "type": 6,                  // CompositeSurface
      "cityjsonType": "CompositeSurface",
      "objectId": "myOuterShell",
      "parent": 0                 // Index in parent array
    },
    {
      "type": 5,                  // Polygon
      "cityjsonType": "Surface",
      "objectId": "first",
      "parent": 0,
      "geometryIndex": 0          // Index in WKB geometry
    }
  ],
  "semantics": {                  // CityJSON surface semantics
    "surfaces": [
      {"type": "WallSurface"},
      {"type": "RoofSurface"},
      {"type": "GroundSurface"}
    ],
    "values": [[0, 0, 1, 2, 2]]   // Maps to boundary indices
  }
}
```

### 4.3 Geometry Type Codes

| Code | CityJSON Type | OGC Type |
|------|---------------|----------|
| 1 | Point | Point |
| 2 | MultiPoint | MultiPoint |
| 3 | LineString | LineString |
| 4 | MultiLineString | MultiLineString |
| 5 | Surface | Polygon |
| 6 | CompositeSurface | MultiPolygon |
| 7 | TIN | TIN |
| 8 | MultiSurface | MultiPolygon |
| 9 | Solid | PolyhedralSurface |
| 10 | CompositeSolid | GeometryCollection |
| 11 | MultiSolid | GeometryCollection |

### 4.4 Example: Building with Semantics

**Input CityJSON:**
```json
{
  "type": "Building",
  "geometry": [{
    "type": "Solid",
    "lod": "2.2",
    "boundaries": [
      [[[0, 1, 2, 3]]],      // Wall surface
      [[[4, 5, 6, 7]]],      // Roof surface
      [[[8, 9, 10, 11]]]     // Ground surface
    ],
    "semantics": {
      "surfaces": [
        {"type": "WallSurface"},
        {"type": "RoofSurface"},
        {"type": "GroundSurface"}
      ],
      "values": [[0], [1], [2]]
    }
  }]
}
```

**Output in DuckDB:**

```sql
-- geometry column (WKT representation for readability):
'POLYHEDRALSURFACE Z (
  ((0 0 0, 10 0 0, 10 10 0, 0 10 0, 0 0 0)),   -- Wall
  ((0 10 0, 10 10 0, 10 10 5, 0 10 5, 0 10 0)), -- Roof
  ((0 0 0, 0 10 0, 0 10 5, 0 0 5, 0 0 0))      -- Ground
)'

-- geometry_properties column:
{
  "type": 9,
  "cityjsonType": "Solid",
  "lod": "2.2",
  "semantics": {
    "surfaces": [
      {"type": "WallSurface"},
      {"type": "RoofSurface"},
      {"type": "GroundSurface"}
    ],
    "values": [[0], [1], [2]]
  }
}
```

**Note:** Actual coordinates are stored in the WKB geometry. The vertex indices from CityJSON boundaries have been dereferenced during import.

---

## 5. Implementation Strategy

### 5.1 Phases

**Phase 1: WKB Encoding Library**
- Implement CityJSON to WKB conversion
- Support all geometry types in mapping table
- Handle 3D coordinates (Z dimension)
- Implement vertex dereferencing (indices → actual coordinates)
- Apply transform metadata if present
- Output GEOMETRY type compatible with DuckDB spatial

**Phase 2: Geometry Properties JSON Schema**
- Define the exact JSON schema for geometry_properties
- Implement serializer from CityJSON semantics
- Include surface type mappings and shell structures

**Phase 3: Per-LOD Table Generation**
- Modify schema inference to detect LOD levels
- Generate separate table for each LOD
- Filter objects by LOD presence
- Update read_cityjson() function signature

**Phase 4: Testing & Validation**
- Unit tests for WKB encoding correctness
- Round-trip tests (CityJSON → WKB → CityJSON)
- Integration tests with DuckDB spatial functions
- Performance benchmarks

### 5.2 Key Implementation Considerations

1. **Vertex Dereferencing**
   - CityJSON stores vertices in a shared array: `vertices: [[x1,y1,z1], ...]`
   - Boundaries reference indices: `boundaries: [[0, 1, 2]]`
   - During WKB encoding: look up each vertex and store actual coordinates
   - Result: WKB contains (x1,y1,z1), (x2,y2,z2), (x3,y3,z3) - no indices

2. **Transform Application**
   - CityJSON may have transform metadata (scale, translate)
   - Apply transform during vertex dereferencing
   - Result: WKB contains world coordinates, not scaled/translated ones

3. **Coordinate Reference System (CRS)**
   - CityJSON can specify CRS (EPSG code)
   - WKB does not encode CRS
   - Store CRS in geometry_properties or table metadata

4. **Hole Orientation**
   - CityJSON follows CityGML: exterior rings clockwise, holes counterclockwise
   - OGC/PostGIS: exterior counterclockwise, holes clockwise
   - Must reverse ring orientation during conversion

5. **Solid → PolyhedralSurface Mapping**
   - CityJSON Solid = array of shells (exterior + interiors)
   - PostGIS POLYHEDRALSURFACE = array of polygons (no shell concept)
   - Flatten: store all shell polygons in single PolyhedralSurface
   - Use geometry_properties to preserve shell structure

---

## 6. API Changes

### 6.1 Current API

```sql
-- Read CityJSON into single table
SELECT * FROM read_cityjson('citymodel.city.json');
```

### 6.2 Proposed API Options

**Option A: Automatic LOD Tables**
```sql
-- Creates buildings_lod0, buildings_lod1, buildings_lod2 automatically
SELECT * FROM read_cityjson('citymodel.city.json');
```

**Option B: Explicit LOD Parameter**
```sql
-- Read only specific LOD
SELECT * FROM read_cityjson('citymodel.city.json', lod => '2.2');
```

**Option C: Union All LODs**
```sql
-- Read all LODs into single table with LOD column
SELECT * FROM read_cityjson('citymodel.city.json', mode => 'union');
```

### 6.3 Write Support

```sql
-- Write back to CityJSON (with WKB → CityJSON conversion)
COPY buildings_lod2 TO 'citymodel.city.json'
WITH (FORMAT 'cityjson', lod => '2.2');
```

---

## 7. References

1. **CityJSON Specification**: https://cityjson.org/specs/
2. **OGC Simple Features**: https://portal.ogc.org/files/?artifact_id=13227
3. **3DCityDB Geometry Module**: https://docs.3dcitydb.org/1.1/3dcitydb/geometry-module/
4. **PostGIS Geometry Types**: https://postgis.net/docs/using_postgis_dbmanagement.html
5. **WKB Wikipedia**: https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry

---

## 8. Open Questions

1. **Table Naming Convention**: `{object_type}_lod{X}_{Y}` or generic `cityobjects_lod{X}_{Y}`?
2. **Implicit Geometry**: Since we dereference vertices, implicit geometry (template reuse) is handled automatically - the coordinates are stored directly.
3. **Multi-LOD Objects**: Objects with geometry at multiple LODs - duplicate across tables or choose highest?
4. **DuckDB Spatial Extension**: Is GEOMETRY type available or need to implement WKB as BLOB?
5. **Materials/Textures Schema**: Design separate tables for appearance data (future work).
