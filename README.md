# DuckDB CityJSON Extension

A DuckDB extension for reading and querying [CityJSON](https://www.cityjson.org/) files directly in DuckDB.

## Features

- **Read CityJSON files** (`.city.json` and `.cityjsonl` / CityJSON Sequences)
- **Automatic schema inference** from CityJSON attributes
- **Per-LOD geometry encoding** with WKB (Well-Known Binary) format for GIS compatibility
- **Multiple geometry LODs** (Levels of Detail) support
- **Semantic surface metadata** preservation

## Usage

### Basic Reading (Default Mode)

Read CityJSON files with geometry stored as structured JSON:

```sql
-- Load the extension
LOAD cityjson;

-- Read a CityJSON file
SELECT * FROM read_cityjson('path/to/file.city.json');

-- Query specific columns
SELECT id, object_type, measuredHeight
FROM read_cityjson('buildings.city.json')
WHERE object_type = 'Building';
```

### Per-LOD Reading with WKB Geometry (Recommended for GIS)

Use the `lod` parameter to get geometry encoded as WKB (BLOB), compatible with spatial extensions:

```sql
-- Read with LOD 2.2 geometry as WKB
SELECT id, object_type, geometry, geometry_properties
FROM read_cityjson('buildings.city.json', lod => '2.2');
```

This mode produces:

- **`geometry`** (BLOB): WKB-encoded geometry, compatible with PostGIS/Spatial extensions
- **`geometry_properties`** (VARCHAR): JSON with geometry metadata (type, LOD, semantics)

### Schema Comparison

| Mode                     | Geometry Column | Format                      | Use Case                         |
| ------------------------ | --------------- | --------------------------- | -------------------------------- |
| Default                  | `geom_lodX_Y`   | STRUCT with JSON boundaries | Full CityJSON preservation       |
| Per-LOD (`lod => '...'`) | `geometry`      | WKB BLOB                    | GIS analysis, spatial operations |

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

# Build (with ninja for faster builds)
GEN=ninja make
```

### Running Tests

```sh
make test
```

## Running the tests

Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:

```sh
make test
```

### Installing the deployed binaries

To install your extension binaries from S3, you will need to do two things. Firstly, DuckDB should be launched with the
`allow_unsigned_extensions` option set to true. How to set this will depend on the client you're using. Some examples:

CLI:

```shell
duckdb -unsigned
```

Python:

```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})
```

NodeJS:

```js
db = new duckdb.Database(":memory:", { allow_unsigned_extensions: "true" });
```

Secondly, you will need to set the repository endpoint in DuckDB to the HTTP url of your bucket + version of the extension
you want to install. To do this run the following SQL query in DuckDB:

```sql
SET custom_extension_repository='bucket.s3.eu-west-1.amazonaws.com/<your_extension_name>/latest';
```

Note that the `/latest` path will allow you to install the latest extension version available for your current version of
DuckDB. To specify a specific version, you can pass the version instead.

After running these steps, you can install and load your extension using the regular INSTALL/LOAD commands in DuckDB:

```sql
INSTALL cityjson
LOAD cityjson
```
