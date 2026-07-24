# DuckDB CityJSON Extension

A DuckDB extension for reading, querying, and writing [CityJSON](https://www.cityjson.org/) and [CityJSONSeq](https://www.cityjson.org/cityjsonseq/) files directly in SQL.

> ⚠️ **Experimental.** This library is under active development and should be considered experimental. Its API, output schema, and on-disk formats may change without notice, and bugs are expected — including ones that can affect data correctness. Do not rely on it for production workloads yet, and verify results against a trusted source before use. Please report issues you encounter.

## Features

- **Read CityJSON** (`.city.json`) and **CityJSONSeq** (`.city.jsonl`) files as tables
- **Write CityJSON / CityJSONSeq** files via `COPY TO`
- **Remote file support** — read from HTTP, HTTPS, S3, GCS URLs (requires `httpfs` extension)
- **Metadata functions** — inspect dataset version, CRS, transform, object counts
- **Automatic schema inference** — CityJSON attributes are mapped to DuckDB columns
- **CityParquet wide layout** — default mode emits one WKB geometry column per LOD plus a `bbox` extent column, ready for `COPY ... TO (FORMAT PARQUET)`
- **Per-LOD geometry** with WKB encoding for GIS/spatial compatibility
- **Filter pushdown** — equality filters on `id`, `feature_id`, and `object_type` are pushed into the scan
- **Streaming CityJSONSeq** — `.city.jsonl` files are read incrementally without loading the whole file into memory
- **FlatCityBuf** (`.fcb`) support — native C++ reader/writer, with real bbox and attribute-index query pushdown (optional, enabled by default)

## Quick Start

```sql
INSTALL cityjson FROM community;
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

-- Read a FlatCityBuf file
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

### `cityjson_geoparquet_geo(path)`

Returns a single `geo` VARCHAR: the [GeoParquet 1.1](https://geoparquet.org/)
`geo` metadata JSON for the dataset, ready to write into a Parquet footer so
GeoParquet-aware tools (GeoPandas, DuckDB `spatial`, GDAL/OGR) recognise the
geometry columns. DuckDB core writes the object-table Parquet; this supplies the
geospatial metadata it cannot infer from a plain `BLOB` column.

It declares **only GeoParquet-legal geometry columns** (CityParquet spec §13.3):
a `geometry_lod*` column qualifies only when every CityJSON geometry at that LoD
is a `MultiPoint` / `MultiLineString` / `MultiSurface` / `CompositeSurface` (WKB
types 1001–1007). A LoD containing any `Solid`-family geometry
(`PolyhedralSurface Z`) is **excluded** — declaring it would make the whole file
unreadable to strict GeoParquet readers. A dataset whose geometry is entirely
solid yields `NULL` (a valid CityParquet table that is simply not GeoParquet).
The CRS is resolved from the CityJSON `referenceSystem` to PROJJSON via an
embedded EPSG table; an unknown code is an error, and a dataset with no CRS gets
`"crs": null`.

`KV_METADATA` cannot contain a subquery, so pass the value via a variable:

```sql
SET VARIABLE geo = (SELECT geo FROM cityjson_geoparquet_geo('delft.city.jsonl'));

COPY (SELECT * FROM read_cityjsonseq('delft.city.jsonl'))
TO 'delft.parquet'
(FORMAT PARQUET, KV_METADATA {geo: getvariable('geo')});
```

The resulting `delft.parquet` opens as a GeoParquet file: its LoD0 footprint
column is read by GeoPandas/DuckDB `spatial` with the correct CRS, while the
`Solid` columns remain opaque blobs those readers ignore.

### `read_flatcitybuf(path [, lod => 'X.Y'] [, min_x => .., min_y => .., max_x => .., max_y => ..])` (optional)

Reads a [FlatCityBuf](https://github.com/cityjson/flatcitybuf) (`.fcb`) file via its native C++ reader. FlatCityBuf is a cloud-optimized binary format for CityJSON data, with an R-tree spatial index and per-column B+tree attribute indices.

```sql
SELECT * FROM read_flatcitybuf('buildings.fcb');

-- With LOD selection
SELECT id, object_type, ST_GeomFromWKB(geometry) AS geom
FROM read_flatcitybuf('buildings.fcb', lod => '2.2');

-- Bbox query — a real R-tree-level skip, not a post-filter: features outside
-- the box are never decoded. All four bounds must be given together.
SELECT id FROM read_flatcitybuf('buildings.fcb', min_x => 84000, min_y => 445000, max_x => 85000, max_y => 446000);

-- Attribute query — simple =,!=,>,>=,<,<= comparisons in a WHERE clause against a
-- column that was given a B+tree index at write time (see attr_index below) are
-- pushed down and answered via the index. WHERE on any other column, or with any
-- other operator, still returns correct results via normal (unpushed) filtering.
SELECT id FROM read_flatcitybuf('buildings.fcb') WHERE b3_h_dak_50p > 10;
```

**Parameters:** Same as `read_cityjson`, plus `min_x`/`min_y`/`max_x`/`max_y` (DOUBLE).

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

**`bbox` column** — a 3D extent STRUCT computed from the object's highest-LOD geometry, in world coordinates:

```
STRUCT(min_x DOUBLE, min_y DOUBLE, min_z DOUBLE, max_x DOUBLE, max_y DOUBLE, max_z DOUBLE)
```

**Geometry columns (CityParquet wide layout)** — one pair of columns per LOD found in the data, named after the normalized LOD (e.g. `geometry_lod2_2`, `geometry_properties_lod2_2`; a whole-number LOD like `2.0` normalizes to `geometry_lod2`):

| Column                          | Type           | Description                                             |
| ------------------------------- | -------------- | ------------------------------------------------------- |
| `geometry_lodX_Y`               | BLOB           | WKB-encoded geometry for that LOD (NULL if absent)      |
| `geometry_properties_lodX_Y`    | JSON (VARCHAR) | Geometry metadata in the CityParquet spec §8 form (below) |
| `material_lodX_Y`               | JSON (VARCHAR) | Per-surface material map for that LOD's geometry (§11.1); NULL if none |
| `texture_lodX_Y`                | JSON (VARCHAR) | Per-surface texture map for that LOD's geometry (§11.1); NULL if none |

This is the layout the CityParquet encoding formalises: each LOD becomes its own WKB column, and `COPY ... TO (FORMAT PARQUET)` yields a Parquet-encoded city model directly.

**`geometry_properties` shape (CityParquet spec §8).** The WKB geometry cannot
carry semantics or the solid's shell structure, so those live in
`geometry_properties` as a flattened, WKB-face-aligned JSON object:

| Key | Present when | Meaning |
| --- | --- | --- |
| `type` | always | CityJSON geometry type as a string (`"Solid"`, `"MultiSurface"`, …) |
| `shells` | solid-family geometry | Per-shell emitted-face counts. Flat for `Solid` (`[12]`); one array per solid for `MultiSolid`/`CompositeSolid` (`[[12],[8,4]]`). Recovers the shell partition the WKB flattens away. |
| `surfaces` | source has semantics | The CityJSON `surfaces` array, verbatim (order and content preserved, extended `+`-attributes inline) |
| `face_semantics` | source has semantics | A flat array with one entry per WKB face, in WKB face order — each the index of that face's surface in `surfaces`, or `null`. Replaces CityJSON's nested `semantics.values`. |
| `lod` | un-suffixed column only | Present only in per-LoD mode (below), where the column name carries no LoD |

Example (a `Solid` with per-surface semantics):

```json
{"type":"Solid",
 "shells":[6],
 "surfaces":[{"type":"GroundSurface"},{"type":"RoofSurface"},{"type":"WallSurface"},{"type":"WallSurface"}],
 "face_semantics":[0,1,2,2,2,3]}
```

`len(face_semantics)` always equals `sum(shells)` (the WKB face count). This is
also the metadata the [`duckdb-3d`](https://github.com/HideBa/duckdb-3d)
extension reads from `shells` to compute the volume of a solid with inner
shells. (Note: the old form — an integer `type` code, `cityjsonType`, and
scalar `shellCount`/`solidCount` with verbatim nested `semantics` — has been
replaced.)

**Appearance columns (`material_lodX_Y` / `texture_lodX_Y`, §11.1).** Each LoD's
geometry gets a paired `material_lod*` and `texture_lod*` column carrying the
CityJSON theme-shaped map verbatim, e.g.

```json
// material_lod2_2
{ "visual": { "values": [0, 0, 1, 1, …] } }
// texture_lod2_2
{ "visual": { "values": [[[texId, u0, v0, u1, v1, …]], …] } }
```

The cell is NULL where the geometry (or its appearance) is absent. Indices are
the source's own **feature-local** ids into its `appearance.materials` /
`appearance.textures` arrays — this extension passes the theme map through as-is.
It does **not** yet write dataset-global sidecar `materials.parquet` /
`textures.parquet` files or inline texture UV coordinates (the CityParquet
reference writer, `cityparquet-rs`, owns that normalisation). Consequently, on
`COPY … TO (FORMAT cityjson)` the material/texture maps are re-attached to their
geometry, but the appearance *definitions* are not regenerated, so exported
indices resolve only against the source's own appearance block.

### Per-LOD Mode (`lod => '...'`)

When `lod` is specified, the schema switches to:

| Column                | Type          | Description                                     |
| --------------------- | ------------- | ----------------------------------------------- |
| `id`                  | VARCHAR       | CityObject identifier                           |
| `feature_id`          | VARCHAR       | Feature identifier                              |
| `object_type`         | VARCHAR       | CityJSON type                                   |
| `geometry`            | BLOB          | WKB-encoded geometry for the requested LOD      |
| `geometry_properties` | JSON (VARCHAR)| Geometry metadata, spec §8 form (see above); carries `lod` here since the column name has no suffix |
| `bbox`                | STRUCT        | 3D extent of the geometry (`min_x..max_z DOUBLE`) |
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

-- Write to FlatCityBuf (.fcb)
COPY (SELECT * FROM read_cityjson('input.city.json'))
TO 'output.fcb' (FORMAT flatcitybuf);

-- Write to FlatCityBuf with a B+tree attribute index on two columns, so later
-- WHERE queries against read_flatcitybuf on those columns get pushed down
COPY (SELECT * FROM read_cityjson('input.city.json'))
TO 'output.fcb' (FORMAT flatcitybuf, attr_index 'b3_h_dak_50p,b3_dak_type', branching_factor 256);
```

### Options

| Option              | Type    | Description                                          |
| ------------------- | ------- | ---------------------------------------------------- |
| `version`           | VARCHAR | CityJSON version to write (default: `"2.0"`)        |
| `crs`               | VARCHAR | CRS identifier (e.g., `'https://www.opengis.net/def/crs/EPSG/0/7415'`) |
| `transform_scale`   | VARCHAR | Vertex quantisation scale as `'x,y,z'` (default: `'0.001,0.001,0.001'`, i.e. 1 mm) |
| `transform_translate` | VARCHAR | Vertex quantisation offset as `'x,y,z'` (default: `'0.0,0.0,0.0'`) |
| `metadata_query`    | VARCHAR | SQL query that returns metadata columns (`version`, `crs`, `transform_scale`, `transform_translate`) |
| `attr_index`        | VARCHAR | *(`flatcitybuf` only)* Comma-separated attribute column names to give a B+tree index, enabling `WHERE`-clause pushdown on `read_flatcitybuf` |
| `branching_factor`  | BIGINT  | *(`flatcitybuf` only)* B+tree branching factor applied to every column in `attr_index` (upstream default if omitted) |
| `index_node_size`   | BIGINT  | *(`flatcitybuf` only)* R-tree node size (upstream default if omitted) |

Vertices are quantised to integers against the transform before writing, so `transform_scale` sets the output precision. The default of `0.001` (1 mm) keeps round-trips lossless even for large projected coordinates; pass a smaller scale for finer precision, or carry the source transform via `metadata_query`.

Requesting `attr_index` on a column that never appears in any feature's attributes is not an error — there's simply nothing to index.

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
| `geometry_properties` | No | Geometry metadata — JSON text **or** a CityParquet spec §8 STRUCT (`STRUCT("type" VARCHAR, surfaces VARCHAR, face_semantics INTEGER[], shells INTEGER[][])`); either form reconstructs semantics/shells |

All other columns are written as CityJSON attributes. The wide CityParquet layout
(`geometry_lodX_Y` + `geometry_properties_lodX_Y` per LoD, as written by
`cityparquet-rs`) round-trips directly: `COPY (SELECT * FROM read_parquet('pkg.parquet'))
TO 'out.city.jsonl' (FORMAT cityjsonseq)` emits one multi-LoD CityObject per feature with
its semantics intact. A geometry column is accepted as WKB `BLOB` **or** DuckDB's
first-class `GEOMETRY` type (how a GeoParquet LoD0 footprint reads back), decoded through
the same CityParquet WKB subset (footprints, surfaces, solids).

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

CityJSONSeq is preferred for large datasets — it supports streaming and lower memory usage. FlatCityBuf adds cloud-native features (spatial indexing, attribute indexing, range requests) and is enabled by default at build time.

## Remote File Support

Read files from HTTP, HTTPS, S3, and GCS URLs. The `httpfs` extension is auto-loaded when a remote URL is detected — this applies to `read_flatcitybuf` too, which shares the same `httpfs`-backed transport (and therefore the same credentials/secrets/proxy configuration) as `read_cityjson`/`read_cityjsonseq`, rather than using a separate HTTP client.

```sql
-- HTTPS
SELECT * FROM read_cityjsonseq('https://storage.googleapis.com/cityjson/delft.city.jsonl');

-- S3
SELECT * FROM read_cityjson('s3://my-bucket/buildings.city.json');

-- If httpfs is not installed, install it first:
INSTALL httpfs;
```

Supported URL schemes: `http://`, `https://`, `s3://`, `s3a://`, `s3n://`, `gcs://`, `gs://`, `r2://`, `hf://`.

## Filter Pushdown

Equality filters on the predefined scalar columns `id`, `feature_id`, and `object_type` are pushed down into the scan, so non-matching CityObjects are skipped before they are materialised:

```sql
-- Pushed down: only 'Building' objects are decoded
SELECT id FROM read_cityjson('buildings.city.json') WHERE object_type = 'Building';

-- Pushed down: locate a single object by id
SELECT * FROM read_cityjson('buildings.city.json') WHERE id = 'building1';
```

Other predicates (ranges, filters on attribute columns, `IN`, etc.) still work — they are simply applied by DuckDB after the scan rather than pushed into the reader.

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

-- CityJSONSeq → FlatCityBuf
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

### FlatCityBuf Support

FlatCityBuf (`.fcb`) support is built on the native C++ [flatcitybuf](https://github.com/cityjson/flatcitybuf) library, vendored via a repo-owned vcpkg overlay port (`vcpkg_ports/flatcitybuf/`, plus a pinned `vcpkg_ports/flatbuffers/` — see that port's comments for why flatbuffers needs an exact version pin). It's portable C++ source, so there's no platform/architecture restriction and no separate download step: it builds like any other vcpkg dependency this extension already has (`nlohmann-json`, `openssl`).

It's enabled by default. To disable it, build with:

```sh
EXT_FLAGS="-DCITYJSON_ENABLE_FCB=OFF" GEN=ninja make
```

The following functions are registered:
- `read_flatcitybuf(path [, min_x, min_y, max_x, max_y])` — read `.fcb` files, with real bbox and attribute-index query pushdown
- `flatcitybuf_metadata(path)` — read `.fcb` file metadata
- `COPY ... TO ... (FORMAT flatcitybuf, [attr_index, branching_factor, index_node_size])` — write `.fcb` files

### Running Tests

```sh
make test
```

SQL tests live in `test/sql/`.

## References

- [CityJSON specification (v2.0.1)](https://www.cityjson.org/specs/2.0.1/)
- [CityJSONSeq specification](https://www.cityjson.org/cityjsonseq/)
- [DuckDB documentation](https://duckdb.org/docs/)
- [DuckDB extension development](https://duckdb.org/community_extensions/development)
