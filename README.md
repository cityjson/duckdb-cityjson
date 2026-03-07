# DuckDB CityJSON Extension

A DuckDB extension for reading, querying, and writing [CityJSON](https://www.cityjson.org/) and [CityJSONSeq](https://www.cityjson.org/cityjsonseq/) files directly in SQL.

## Features

- **Read CityJSON** (`.city.json`) and **CityJSONSeq** (`.city.jsonl`) files as tables
- **Write CityJSON / CityJSONSeq** files via `COPY TO`
- **Remote file support** — read from HTTP, HTTPS, S3, GCS URLs (requires `httpfs` extension)
- **Metadata functions** — inspect dataset version, CRS, transform, object counts
- **Automatic schema inference** — CityJSON attributes are mapped to DuckDB columns
- **Per-LOD geometry** with WKB encoding for GIS/spatial compatibility
- **FlatCityBuf** (`.fcb`) support (optional, compile-time flag)

## Quick Start

```sql
LOAD cityjson;

-- Read a local CityJSON file
SELECT * FROM read_cityjson('buildings.city.json');

-- Read a remote CityJSONSeq file
SELECT * FROM read_cityjsonseq('https://storage.googleapis.com/cityjson/delft.city.jsonl');

-- Get dataset metadata
SELECT * FROM cityjson_metadata('buildings.city.json');

-- Write query results to a CityJSON file
COPY (SELECT * FROM read_cityjson('input.city.json'))
TO 'output.city.json' (FORMAT cityjson);

-- Read a FlatCityBuf file (requires -DCITYJSON_ENABLE_FCB=ON)
SELECT * FROM read_flatcitybuf('buildings.fcb');
```

## Table Functions

### `read_cityjson(path [, lod => 'X.Y'])`

Reads a CityJSON (`.city.json`) file. Each CityObject becomes a row.

```sql
SELECT * FROM read_cityjson('buildings.city.json');

-- Filter by object type
SELECT id, object_type, measuredHeight
FROM read_cityjson('buildings.city.json')
WHERE object_type = 'Building';
```

**Parameters:**

| Parameter      | Type    | Description                                  |
| -------------- | ------- | -------------------------------------------- |
| `path`         | VARCHAR | File path or URL to a `.city.json` file      |
| `lod`          | VARCHAR | Optional. LOD to extract (e.g., `'2.2'`)     |
| `sample_lines` | BIGINT  | Optional. Number of features to sample for schema inference (default: 100) |

### `read_cityjsonseq(path [, lod => 'X.Y'])`

Reads a CityJSONSeq (`.city.jsonl`) file. Each CityObject from each CityJSONFeature line becomes a row.

CityJSONSeq format:
- Line 1: CityJSON metadata header (`"type": "CityJSON"`)
- Line 2+: One `CityJSONFeature` per line, each with its own local vertex pool

```sql
SELECT * FROM read_cityjsonseq('delft.city.jsonl');

-- Filter by feature and object type
SELECT feature_id, id, object_type
FROM read_cityjsonseq('railway.city.jsonl')
WHERE object_type = 'Railway';
```

**Parameters:** Same as `read_cityjson`.

### `cityjson_metadata(path)`

Returns a single row with dataset-level metadata from a CityJSON file.

```sql
SELECT version, city_objects_count, reference_system
FROM cityjson_metadata('buildings.city.json');
```

### `cityjsonseq_metadata(path)`

Returns a single row with metadata from the header line of a CityJSONSeq file.

```sql
SELECT version, city_objects_count
FROM cityjsonseq_metadata('delft.city.jsonl');
```

### `read_flatcitybuf(path [, lod => 'X.Y'])` (optional)

Reads a [FlatCityBuf](https://github.com/cityjson/flatcitybuf) (`.fcb`) file. FlatCityBuf is a cloud-optimized binary format for CityJSON data.

Only available when compiled with `-DCITYJSON_ENABLE_FCB=ON`.

```sql
SELECT * FROM read_flatcitybuf('buildings.fcb');

-- With LOD selection
SELECT id, object_type, ST_GeomFromWKB(geometry) AS geom
FROM read_flatcitybuf('buildings.fcb', lod => '2.2');
```

**Parameters:** Same as `read_cityjson`.

### `flatcitybuf_metadata(path)` (optional)

Returns metadata from a FlatCityBuf file. Same schema as `cityjson_metadata`.

```sql
SELECT version, reference_system, city_objects_count
FROM flatcitybuf_metadata('buildings.fcb');
```

## Output Schema

### Default Mode

In default mode (no `lod` parameter), the schema includes:

**Predefined columns** (always present):

| Column           | Type          | Description                                |
| ---------------- | ------------- | ------------------------------------------ |
| `id`             | VARCHAR       | CityObject identifier                      |
| `feature_id`     | VARCHAR       | Feature identifier (file path for CityJSON, feature ID for CityJSONSeq) |
| `object_type`    | VARCHAR       | CityJSON type (e.g., `Building`, `Road`)   |
| `children`       | VARCHAR[]     | Child CityObject IDs                       |
| `children_roles` | VARCHAR[]     | Roles of child objects                     |
| `parents`        | VARCHAR[]     | Parent CityObject IDs                      |
| `other`          | JSON (VARCHAR)| Attributes not mapped to their own columns |

**Dynamic attribute columns** — inferred from the data. CityJSON attributes like `measuredHeight`, `yearOfConstruction`, etc. become their own columns with inferred types (BIGINT, DOUBLE, VARCHAR, BOOLEAN, TIMESTAMP, DATE, TIME, or JSON).

**Geometry columns** — one per LOD found in the data, named `geom_lodX_Y` (e.g., `geom_lod2_2`). Each is a STRUCT:

```
STRUCT(lod VARCHAR, type VARCHAR, boundaries VARCHAR, semantics VARCHAR, material VARCHAR, texture VARCHAR)
```

### Per-LOD Mode (`lod => '...'`)

When `lod` is specified, the schema switches to:

| Column                | Type          | Description                                     |
| --------------------- | ------------- | ----------------------------------------------- |
| `id`                  | VARCHAR       | CityObject identifier                           |
| `feature_id`          | VARCHAR       | Feature identifier                              |
| `object_type`         | VARCHAR       | CityJSON type                                   |
| `geometry`            | BLOB          | WKB-encoded geometry for the requested LOD      |
| `geometry_properties` | JSON (VARCHAR)| Geometry metadata (type, semantics, material, texture) |
| *(attributes)*        | *(inferred)*  | Dynamic attribute columns                       |

Use with DuckDB Spatial:

```sql
LOAD spatial;
SELECT id, ST_GeomFromWKB(geometry) AS geom
FROM read_cityjsonseq('buildings.city.jsonl', lod => '2.2')
WHERE geometry IS NOT NULL;
```

### Metadata Columns

Both `cityjson_metadata` and `cityjsonseq_metadata` return:

| Column                | Type                                        | Description                      |
| --------------------- | ------------------------------------------- | -------------------------------- |
| `id`                  | INTEGER                                     | Row ID (always 1)               |
| `version`             | VARCHAR                                     | CityJSON version (e.g., `"2.0"`) |
| `identifier`          | VARCHAR                                     | Dataset identifier               |
| `title`               | VARCHAR                                     | Dataset title                    |
| `reference_date`      | DATE                                        | Reference date                   |
| `transform_scale`     | STRUCT(x DOUBLE, y DOUBLE, z DOUBLE)        | Coordinate transform scale       |
| `transform_translate` | STRUCT(x DOUBLE, y DOUBLE, z DOUBLE)        | Coordinate transform offset      |
| `geographical_extent` | STRUCT(min_x, min_y, min_z, max_x, max_y, max_z DOUBLE) | Bounding box          |
| `reference_system`    | STRUCT(base_url, authority, version, code VARCHAR) | CRS information            |
| `point_of_contact`    | STRUCT(contact_name, email_address, contact_type, role, phone, website VARCHAR, address STRUCT(...)) | Contact info |
| `city_objects_count`  | BIGINT                                      | Total number of CityObjects      |

```sql
-- Access nested struct fields
SELECT
    transform_scale.x AS scale_x,
    transform_translate.x AS translate_x,
    reference_system.authority AS crs_authority,
    reference_system.code AS crs_code
FROM cityjson_metadata('buildings.city.json');
```

## COPY TO (Writing CityJSON)

Write query results to CityJSON, CityJSONSeq, or FlatCityBuf files using the `COPY` statement.

### Basic Usage

```sql
-- Write to CityJSON (.city.json)
COPY (SELECT * FROM read_cityjson('input.city.json'))
TO 'output.city.json' (FORMAT cityjson);

-- Write to CityJSONSeq (.city.jsonl)
COPY (SELECT * FROM read_cityjsonseq('input.city.jsonl'))
TO 'output.city.jsonl' (FORMAT cityjsonseq);

-- Write to FlatCityBuf (.fcb) — requires -DCITYJSON_ENABLE_FCB=ON
COPY (SELECT * FROM read_cityjson('input.city.json'))
TO 'output.fcb' (FORMAT flatcitybuf);
```

### Options

| Option              | Type    | Description                                          |
| ------------------- | ------- | ---------------------------------------------------- |
| `version`           | VARCHAR | CityJSON version to write (default: `"2.0"`)        |
| `crs`               | VARCHAR | CRS identifier (e.g., `'https://www.opengis.net/def/crs/EPSG/0/7415'`) |
| `transform_scale`   | VARCHAR | Vertex quantisation scale as `'x,y,z'` (e.g., `'0.001,0.001,0.001'`) |
| `transform_translate` | VARCHAR | Vertex quantisation offset as `'x,y,z'` (e.g., `'0.0,0.0,0.0'`) |
| `metadata_query`    | VARCHAR | SQL query that returns metadata columns (`version`, `crs`, `transform_scale`, `transform_translate`) |

```sql
-- Write with explicit metadata
COPY (SELECT * FROM read_cityjson('input.city.json'))
TO 'output.city.json' (
    FORMAT cityjson,
    version '2.0',
    crs 'https://www.opengis.net/def/crs/EPSG/0/7415',
    transform_scale '0.001,0.001,0.001',
    transform_translate '84982.0,446857.0,0.0'
);

-- Carry metadata from the source file
COPY (SELECT * FROM read_cityjson('input.city.json'))
TO 'output.city.json' (
    FORMAT cityjson,
    metadata_query 'SELECT version, reference_system AS crs FROM cityjson_metadata(''input.city.json'')'
);
```

### Required Columns

The `COPY TO` statement requires these columns in the input query:

| Column        | Required | Description                        |
| ------------- | -------- | ---------------------------------- |
| `id`          | Yes      | CityObject identifier              |
| `feature_id`  | Yes      | Feature grouping key               |
| `object_type` | Yes      | CityJSON type                      |
| `children`    | No       | Child object IDs                   |
| `parents`     | No       | Parent object IDs                  |
| `geometry`    | No       | WKB geometry or geometry struct    |
| `geometry_properties` | No | Geometry metadata JSON       |

All other columns are written as CityJSON attributes.

### Round-Trip Example

```sql
-- Read, filter, and write back
COPY (
    SELECT * FROM read_cityjsonseq('city.jsonl')
    WHERE object_type = 'Building'
)
TO 'buildings_only.city.jsonl' (FORMAT cityjsonseq);
```

### CityJSON vs CityJSONSeq Output

| Format        | Extension     | Vertex Pool               | Structure                          |
| ------------- | ------------- | ------------------------- | ---------------------------------- |
| `cityjson`    | `.city.json`  | Single global vertex pool | One JSON document with all objects |
| `cityjsonseq` | `.city.jsonl` | Per-feature vertex pools  | One JSON object per line           |
| `flatcitybuf` | `.fcb`        | Per-feature vertex pools  | Cloud-optimized binary format      |

CityJSONSeq is preferred for large datasets — it supports streaming and lower memory usage. FlatCityBuf adds cloud-native features (spatial indexing, range requests) and requires `-DCITYJSON_ENABLE_FCB=ON` at build time.

## Remote File Support

Read files from HTTP, HTTPS, S3, and GCS URLs. The `httpfs` extension is auto-loaded when a remote URL is detected.

```sql
-- HTTPS
SELECT * FROM read_cityjsonseq('https://storage.googleapis.com/cityjson/delft.city.jsonl');

-- S3
SELECT * FROM read_cityjson('s3://my-bucket/buildings.city.json');

-- If httpfs is not installed, install it first:
INSTALL httpfs;
```

Supported URL schemes: `http://`, `https://`, `s3://`, `s3a://`, `s3n://`, `gcs://`, `gs://`, `r2://`, `hf://`.

## Common Patterns

### Create tables from CityJSON

```sql
CREATE TABLE meta AS SELECT * FROM cityjson_metadata('buildings.city.json');
CREATE TABLE buildings AS SELECT * FROM read_cityjson('buildings.city.json');

-- Join metadata with objects
SELECT b.*, m.version, m.reference_system.code AS epsg
FROM buildings b, meta m
WHERE b.object_type = 'Building';
```

### Filter and aggregate

```sql
-- Count objects by type
SELECT object_type, COUNT(*) AS cnt
FROM read_cityjsonseq('delft.city.jsonl')
GROUP BY object_type;

-- Find tall buildings
SELECT id, measuredHeight
FROM read_cityjson('buildings.city.json')
WHERE object_type = 'Building' AND measuredHeight > 30
ORDER BY measuredHeight DESC;
```

### Export subset to new file

```sql
COPY (
    SELECT * FROM read_cityjsonseq('delft.city.jsonl')
    WHERE object_type IN ('Building', 'BuildingPart')
)
TO 'delft_buildings.city.jsonl' (FORMAT cityjsonseq);
```

### Convert between formats

```sql
-- CityJSON → CityJSONSeq
COPY (SELECT * FROM read_cityjson('input.city.json'))
TO 'output.city.jsonl' (FORMAT cityjsonseq);

-- CityJSONSeq → FlatCityBuf (requires FCB support)
COPY (SELECT * FROM read_cityjsonseq('input.city.jsonl'))
TO 'output.fcb' (FORMAT flatcitybuf);

-- FlatCityBuf → CityJSON
COPY (SELECT * FROM read_flatcitybuf('input.fcb'))
TO 'output.city.json' (FORMAT cityjson);
```

## Building

### Prerequisites

- CMake 3.10+
- C++17 compatible compiler
- DuckDB source (git submodule)
- nlohmann/json library

### Build Steps

```sh
# Clone with submodules
git clone --recurse-submodules https://github.com/your-repo/duckdb-cityjson-extension.git
cd duckdb-cityjson-extension

# Build
GEN=ninja make

# Build with httpfs support (for remote files in the statically linked binary)
CORE_EXTENSIONS="httpfs" GEN=ninja make

# Incremental rebuild
cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb
```

### Optional: FlatCityBuf Support

To enable reading and writing FlatCityBuf (`.fcb`) files, build with:

```sh
EXT_FLAGS="-DCITYJSON_ENABLE_FCB=ON" GEN=ninja make
```

This downloads pre-built FlatCityBuf binaries from [GitHub releases](https://github.com/cityjson/flatcitybuf/releases) at configure time. Supported platforms: macOS (aarch64, x86_64), Linux (aarch64, x86_64), Windows (x86_64).

When enabled, the following additional functions are registered:
- `read_flatcitybuf(path)` — read `.fcb` files
- `flatcitybuf_metadata(path)` — read `.fcb` file metadata
- `COPY ... TO ... (FORMAT flatcitybuf)` — write `.fcb` files

### Running Tests

```sh
make test
```

SQL tests live in `test/sql/`.

## References

- [CityJSON specification (v2.0.1)](https://www.cityjson.org/specs/2.0.1/)
- [CityJSONSeq specification](https://www.cityjson.org/cityjsonseq/)
- [DuckDB documentation](https://duckdb.org/docs/)
- [DuckDB extension development](https://duckdb.org/docs/stable/dev/extensions)
