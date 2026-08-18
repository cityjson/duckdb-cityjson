# DuckDB CityJSON Extension

A DuckDB extension for reading, querying, and writing [CityJSON](https://www.cityjson.org/),
[CityJSONSeq](https://www.cityjson.org/cityjsonseq/) and
[FlatCityBuf](https://github.com/cityjson/flatcitybuf) files directly in SQL.

> ⚠️ **Experimental.** This library is under active development and should be considered experimental. Its API, output schema, and on-disk formats may change without notice, and bugs are expected — including ones that can affect data correctness. Do not rely on it for production workloads yet, and verify results against a trusted source before use. Please report issues you encounter.

## Features

- **Read** CityJSON (`.city.json`), CityJSONSeq (`.city.jsonl`) and FlatCityBuf (`.fcb`) as tables
- **Write** all three formats via `COPY … TO`
- **Remote files** — HTTP, HTTPS, S3, GCS URLs (`httpfs` is auto-loaded)
- **Automatic schema inference** — CityJSON attributes become typed DuckDB columns
- **CityParquet wide layout** — one WKB geometry column per LoD plus a `bbox` extent, ready for `COPY … TO (FORMAT PARQUET)`
- **GeoParquet interop** — footprint columns are read by GeoPandas, GDAL/OGR and DuckDB `spatial` with the correct CRS
- **Pushdown** — projection always; equality filters on `id` / `feature_id` / `object_type`; R-tree bbox and B+tree attribute queries on FlatCityBuf
- **Streaming CityJSONSeq** — read incrementally, without loading the whole file
- **CityParquet packages** — load a package directory as a schema and mutate it transactionally

## Quick start

```sql
INSTALL cityjson FROM community;
LOAD cityjson;

-- Read a remote CityJSONSeq file
SELECT COUNT(*) FROM read_cityjsonseq('https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl');
-- 2231
```

```sql
-- Attributes are typed columns; filter and aggregate as usual
SELECT object_type, COUNT(*) AS cnt
FROM read_cityjsonseq('https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl')
GROUP BY object_type ORDER BY cnt DESC;
-- BuildingPart  1116
-- Building      1115
```

```sql
-- Footprints are WKB MultiPolygon Z — hand them straight to DuckDB spatial
LOAD spatial;
SELECT id, ST_Area(ST_GeomFromWKB(geometry_lod0_0)) AS footprint_m2
FROM read_cityjsonseq('https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl')
WHERE geometry_lod0_0 IS NOT NULL
LIMIT 3;
-- NL.IMBAG.Pand.0503100000012869 |  7.21
-- NL.IMBAG.Pand.0503100000016459 | 10.34
-- NL.IMBAG.Pand.0503100000005156 | 99.25
```

```sql
-- Solid LoDs are PolyhedralSurface Z, which DuckDB spatial cannot parse —
-- use the extension's own extent function for those
SELECT id, cityjson_wkb_extent(geometry_lod2_2).max_z AS ridge_height
FROM read_cityjsonseq('https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl')
WHERE geometry_lod2_2 IS NOT NULL
LIMIT 3;
```

```sql
-- Convert between formats
COPY (SELECT * FROM read_cityjson('https://cityjson.open3d.city/cityjson/delft.city.json'))
TO 'delft.city.jsonl' (FORMAT cityjsonseq);
```

**→ [Full function reference with worked examples](docs/FUNCTIONS.md)**

## Functions at a glance

| Function | What it does |
| -------- | ------------ |
| `read_cityjson(path, …)` | Read `.city.json` — one row per CityObject |
| `read_cityjsonseq(path, …)` | Read `.city.jsonl`, streamed |
| `read_flatcitybuf(path, …)` | Read `.fcb`, with R-tree bbox and attribute-index pushdown |
| `cityjson_metadata(path)` | Dataset metadata — version, CRS, transform, counts |
| `cityjsonseq_metadata(path)` / `flatcitybuf_metadata(path)` | The same, for the other two formats |
| `COPY … TO (FORMAT cityjson\|cityjsonseq\|flatcitybuf)` | Write any of the three formats |
| `cityjson_geoparquet_geo(path)` | The `geo` + `city` Parquet footer keys for a CityParquet file |
| `cityjson_materials/textures/geometry_templates(path)` | Appearance sidecar tables, ids interned across the file |
| `cityjson_wkb_extent(blob)` | 3D extent of a WKB blob, solids included |
| `PRAGMA cityparquet_*` / `insert_*` | Load and transactionally mutate a CityParquet package |

All three read functions take `lod` and `sample_lines`. `read_cityjson` and
`read_cityjsonseq` additionally take `appearance` (`'local'` / `'sidecar'`) and
`geometry_encoding` (`'wkb'` / `'arrow-native'`); `read_flatcitybuf` instead
takes `min_x` / `min_y` / `max_x` / `max_y` for bbox pushdown.

## Output schema in brief

Every read produces, in order: the predefined columns `id`, `feature_id`,
`object_type`, `children`, `children_roles`, `parents`, `other`; one typed column
per CityJSON attribute; then **per LoD present in the data** a group of four; and
finally a `bbox` STRUCT.

| Column | Type |
| ------ | ---- |
| `geometry_lodX_Y` | BLOB (WKB) |
| `geometry_properties_lodX_Y` | `STRUCT("type" VARCHAR, surfaces VARCHAR, face_semantics INTEGER[], shells INTEGER[][])` |
| `material_lodX_Y` / `texture_lodX_Y` | JSON (VARCHAR) |

`geometry_properties` carries what WKB cannot: the CityJSON geometry type, the
semantic surfaces, a WKB-face-aligned `face_semantics` array, and the `shells`
partition a solid's flattened WKB loses. This table design — one object table
per CityGML module, WKB geometry plus a properties sidecar column per LoD — is
inspired by the [**CityParquet**](https://cityparquet.cityjson.org) encoding,
and `COPY … TO (FORMAT PARQUET)` yields a Parquet-encoded city model directly.

See [docs/FUNCTIONS.md](docs/FUNCTIONS.md#output-schema) for the full grammar.

## Building

```sh
# Clone with submodules
git clone --recurse-submodules https://github.com/cityjson/duckdb-cityjson.git
cd duckdb-cityjson

# Build
GEN=ninja make

# Incremental rebuild (include `unittest`, or `make test` runs a stale test binary)
cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb unittest

# Test
make test
```

Requires CMake 3.24+, a C++20 compiler, and the DuckDB submodule.

**FlatCityBuf** support is on by default, built from the
[flatcitybuf](https://github.com/cityjson/flatcitybuf) C++ library as a vcpkg
port. Disable with `EXT_FLAGS="-DCITYJSON_ENABLE_FCB=OFF" GEN=ninja make`, or for
local development build it into a gitignored prefix with `just vendor-fcb`.

**DuckDB-Wasm** artefacts are built by CI for `wasm_mvp` / `wasm_eh` /
`wasm_threads`. Reproduce the first locally with `just wasm-setup` then
`just wasm`; smoke-test it with `just test-wasm`.

SQL tests live in `test/sql/`. The wasm smoke test and the C++ harnesses under
`test/cpp/` are opt-in and are not run by `make test`.

## Documentation

- **[docs/FUNCTIONS.md](docs/FUNCTIONS.md)** — every function, with worked examples
- **[docs/DESIGN_DOC.md](docs/DESIGN_DOC.md)** — architecture overview
- **[docs/TRAPS.md](docs/TRAPS.md)** — implementation traps, per layer
- **[docs/UPDATING.md](docs/UPDATING.md)** — bumping the DuckDB target version
- **[CLAUDE.md](CLAUDE.md)** / **[AGENTS.md](AGENTS.md)** — how we work: build, test, review, style

## Related

The WKB geometry this extension emits also works well as input to the
[**duckdb-3d**](https://github.com/HideBa/duckdb-3d) extension for solid
validation, measurement, and processing.

## References

- [CityJSON specification (v2.0.1)](https://www.cityjson.org/specs/2.0.1/)
- [CityJSONSeq specification](https://www.cityjson.org/cityjsonseq/)
- [CityParquet specification](https://cityparquet.cityjson.org)
- [GeoParquet](https://geoparquet.org/)
- [DuckDB extension development](https://duckdb.org/community_extensions/development)
