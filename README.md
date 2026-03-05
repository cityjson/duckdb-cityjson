# DuckDB CityJSON Extension

A DuckDB extension for reading and querying [CityJSON](https://www.cityjson.org/) and [CityJSONSeq](https://www.cityjson.org/cityjsonseq/) files directly in DuckDB.

## Features

- **Read CityJSON files** (`.city.json`) — full CityJSON documents
- **Read CityJSONSeq files** (`.city.jsonl`) — line-delimited CityJSONFeature streams
- **Metadata functions** for both formats — transform, CRS, object counts, etc.
- **Automatic schema inference** from CityJSON attributes
- **Per-LOD geometry encoding** with WKB (Well-Known Binary) format for GIS compatibility
- **Multiple geometry LODs** (Levels of Detail) support
- **Semantic surface metadata** preservation

## Functions

| Function                     | Input         | Description                           |
| ---------------------------- | ------------- | ------------------------------------- |
| `read_cityjson(path)`        | `.city.json`  | Read all CityObjects as rows          |
| `read_cityjsonseq(path)`     | `.city.jsonl` | Read CityJSONFeature records as rows  |
| `cityjson_metadata(path)`    | `.city.json`  | Dataset metadata as a single row      |
| `cityjsonseq_metadata(path)` | `.city.jsonl` | Dataset metadata from the header line |

## Usage

### Reading CityJSON

```sql
LOAD cityjson;

-- Read all city objects
SELECT * FROM read_cityjson('path/to/file.city.json');

-- Filter by type
SELECT id, object_type, measuredHeight
FROM read_cityjson('buildings.city.json')
WHERE object_type = 'Building';

-- Get metadata
SELECT * FROM cityjson_metadata('buildings.city.json');
```

### Reading CityJSONSeq

CityJSONSeq (`.city.jsonl`) is a line-delimited format where:

- Line 1: CityJSON metadata header
- Line 2+: One `CityJSONFeature` per line, each with its own local vertex pool

```sql
-- Read all city objects from a CityJSONSeq file
SELECT * FROM read_cityjsonseq('path/to/file.city.jsonl');

-- Filter by feature and object type
SELECT feature_id, id, object_type
FROM read_cityjsonseq('railway.city.jsonl')
WHERE object_type = 'Railway';

-- Get metadata from the header line
SELECT version, city_objects_count FROM cityjsonseq_metadata('railway.city.jsonl');
```

### Per-LOD Reading with WKB Geometry (Recommended for GIS)

Use the `lod` parameter to get geometry encoded as WKB (BLOB), compatible with spatial extensions.
Works with both `read_cityjson` and `read_cityjsonseq`.

```sql
-- Read with a specific LOD — geometry returned as WKB BLOB
SELECT id, object_type, geometry, geometry_properties
FROM read_cityjson('buildings.city.json', lod => '2.2');

-- Same for CityJSONSeq
SELECT id, feature_id, geometry
FROM read_cityjsonseq('railway.city.jsonl', lod => '3');
```

This mode produces:

- **`geometry`** (BLOB): WKB-encoded geometry, compatible with PostGIS / DuckDB Spatial
- **`geometry_properties`** (VARCHAR): JSON with geometry metadata (type, LOD, semantics)

To use with DuckDB Spatial:

```sql
LOAD spatial;
SELECT id, ST_GeomFromWKB(geometry) AS geom
FROM read_cityjsonseq('railway.city.jsonl', lod => '3')
WHERE geometry IS NOT NULL;
```

### Schema Modes

| Mode                     | Geometry Column | Format                      | Use Case                         |
| ------------------------ | --------------- | --------------------------- | -------------------------------- |
| Default                  | `geom_lodX_Y`   | STRUCT with JSON boundaries | Full CityJSON preservation       |
| Per-LOD (`lod => '...'`) | `geometry`      | WKB BLOB                    | GIS analysis, spatial operations |

### Metadata Functions

```sql
-- CityJSON metadata (single row)
SELECT version, city_objects_count, transform_scale, reference_system
FROM cityjson_metadata('buildings.city.json');

-- CityJSONSeq metadata (reads header line only)
SELECT version, city_objects_count
FROM cityjsonseq_metadata('railway.city.jsonl');

-- Access nested struct fields
SELECT
    transform_scale.x AS scale_x,
    transform_translate.x AS translate_x,
    reference_system.authority AS crs_authority,
    reference_system.code AS crs_code
FROM cityjson_metadata('buildings.city.json');
```

Metadata columns:

| Column                | Type             | Description                      |
| --------------------- | ---------------- | -------------------------------- |
| `version`             | VARCHAR          | CityJSON version (e.g., `"2.0"`) |
| `identifier`          | VARCHAR          | Dataset identifier               |
| `title`               | VARCHAR          | Dataset title                    |
| `reference_date`      | DATE             | Reference date                   |
| `transform_scale`     | STRUCT(x,y,z)    | Coordinate transform scale       |
| `transform_translate` | STRUCT(x,y,z)    | Coordinate transform offset      |
| `geographical_extent` | STRUCT(6 fields) | Bounding box                     |
| `reference_system`    | STRUCT           | CRS information                  |
| `point_of_contact`    | STRUCT           | Contact information              |
| `city_objects_count`  | BIGINT           | Total number of city objects     |

### Multi-Table Pattern

```sql
-- Metadata table
CREATE TABLE meta AS SELECT * FROM cityjson_metadata('buildings.city.json');

-- City objects table
CREATE TABLE buildings AS SELECT * FROM read_cityjson('buildings.city.json');

-- Join
SELECT b.*, m.version, m.reference_system.code AS epsg
FROM buildings b, meta m
WHERE b.object_type = 'Building';
```

## Building

### Prerequisites

- CMake 3.10+
- C++17 compatible compiler
- DuckDB source (git submodule)

### Build Steps

```sh
# Clone with submodules
git clone --recurse-submodules https://github.com/your-repo/duckdb-cityjson-extension.git
cd duckdb-cityjson-extension

# Build (ninja is faster)
GEN=ninja make

# Incremental rebuild after changes
cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb
```

### Running Tests

```sh
make test
```

SQL tests live in `test/sql/`. Always run tests after making changes.

## Development

See [`CLAUDE.md`](CLAUDE.md) for the agent guide covering architecture, file layout, build workflow, and contribution patterns.
