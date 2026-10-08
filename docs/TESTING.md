# CityJSON extension — notebook SQL

The SQL cells of the DuckDB notebook `CityJSON extension`, in notebook order, run against
real data from where it is published. Headings keep the notebook's own cell numbers, so
the sequence has a gap where a cell covered a feature this extension no longer has.
Outputs shown are what the cell returns today; a cell without one returns a table too
wide to be worth reproducing. Run from the root of this repository, with `httpfs`,
`spatial` and `json` installed.

## Running it

`just test-notebook` runs this walkthrough as a test:

| File | Covers |
| ---- | ------ |
| `test/sql/cityjson_notebook_e2e.test` | Cells 2–3, 5, 7–27 |
| `test/sql/cityjson_notebook_geoparquet.test` | Cell 4, which needs `spatial` — a `require` for it would skip a whole file rather than one query |

Both are gated on `CITYJSON_NOTEBOOK_TEST` and stay out of `make test`. Every dataset is
read over HTTP from the URL it was published at, so nothing needs downloading first;
cells 26 and 27 read two small fixtures of this repository instead (`test/data/README.md`
says where they come from). Nothing in the test is pinned to a row count of a published
dataset: every assertion relates one count to another in the same run.

Two cells assert something different from what they say here, both deliberately:

- **Cell 4** compares with a bare `ST_Equals`, which does not hold. The two encodings
  apply the CityJSON `transform` at different points, so the decoded doubles differ in
  their last bits — `446014.454` against `446014.45399999997`. All 1115 LoD0 geometries
  differ bytewise and `ST_Equals` rejects 1059, while the largest area disagreement
  across Delft is 5.4e-9 m². The test snaps both sides to a micrometre grid, which is
  still far finer than any real geometric divergence.
- **Cells 21–22** cannot assert that every returned row intersects the query box,
  because the FlatCityBuf R-tree indexes *features* while the reader emits a row per
  *CityObject*: a building crossing the edge brings its parts with it, and on this box
  two lie outside by two and three metres. The test asserts the guarantee that does
  hold — every row belongs to a feature that intersects.

## 1 — Load the extension

```sql
LOAD 'build/release/extension/cityjson/cityjson.duckdb_extension';
```

## 2 — Read CityJSONSeq (Delft)

```sql
SELECT *
FROM read_cityjsonseq('https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl');
```

## 3 — Read CityJSON, filter by object type

```sql
SELECT *
FROM read_cityjson('https://storage.googleapis.com/cityjson/lod3_railway.city.json')
WHERE object_type IN ['Building', 'BuildingInstallation', 'BuildingPart'];
```

## 4 — Check LoD0 is valid GeoParquet

```sql
load spatial;
-- Check LoD0 is valid GeoParquet
WITH seq AS (
  SELECT id, ST_GeomFromWKB(geometry_lod0_0) AS geom
  FROM read_cityjsonseq('https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl')
),
doc AS (
  SELECT id, ST_GeomFromWKB(geometry_lod0_0) AS geom
  FROM read_cityjson('https://cityjson.open3d.city/cityjson/delft.city.json')
)
SELECT
  COALESCE(seq.id, doc.id)   AS id,
  seq.id IS NULL             AS missing_in_seq,
  doc.id IS NULL             AS missing_in_doc,
  ST_Area(seq.geom)          AS area_seq,
  ST_Area(doc.geom)          AS area_doc,
  abs(ST_Area(seq.geom) - ST_Area(doc.geom)) AS area_diff,
  ST_Equals(seq.geom, doc.geom)              AS geom_equal
FROM seq FULL JOIN doc USING (id)
WHERE seq.id IS NULL
   OR doc.id IS NULL
   OR NOT ST_Equals(seq.geom, doc.geom);
```

## 5 — Remote CityJSONSeq (Helsinki, textured)

```sql
SELECT *
FROM read_cityjsonseq('https://cityjson.open3d.city/cityjsonseq/Helsinki_tex.city.jsonl') limit 10;
```

## 7 — GeoParquet `geo` metadata

```sql
SELECT geo FROM cityjson_geoparquet_geo('https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl');
```

## 8 — Roundtrip testing

```sql
SET VARIABLE geo = (SELECT geo FROM cityjson_geoparquet_geo('https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl'));
COPY (SELECT * FROM read_cityjsonseq('https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl'))
  TO '/tmp/cp_test/delft_duckdb.parquet'
  (FORMAT PARQUET, KV_METADATA {geo: getvariable('geo')});
SELECT * FROM read_parquet('/tmp/cp_test/delft_duckdb.parquet');
```

## 9 — Material and texture

```sql
SELECT (SELECT count(*) FROM cityjson_materials('https://storage.googleapis.com/cityjson/lod3_railway.city.json'))          AS materials,
       (SELECT count(*) FROM cityjson_textures('https://storage.googleapis.com/cityjson/lod3_railway.city.json'))           AS textures,
       (SELECT count(*) FROM cityjson_implicit_geometries('https://storage.googleapis.com/cityjson/lod3_railway.city.json')) AS implicit_geometries;

SELECT * FROM cityjson_materials('https://storage.googleapis.com/cityjson/lod3_railway.city.json');
```

## 10 — Sidecar appearance at LoD3

```sql
SELECT count(*) AS rows, count(material_lod3_0) AS with_material
FROM read_cityjson('https://storage.googleapis.com/cityjson/lod3_railway.city.json', lod => '3', appearance := 'sidecar');
```

## 11 — Build a package schema from a CityJSON read

```sql
CREATE SCHEMA pkg;
CREATE TABLE pkg.building AS
SELECT * FROM read_cityjsonseq('https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl');

PRAGMA cityparquet_init('pkg');
SELECT table_name, role FROM pkg.__cityparquet ORDER BY 1;
```

```text
┌────────────┬─────────┐
│ table_name │  role   │
│  varchar   │ varchar │
├────────────┼─────────┤
│ building   │ object  │
└────────────┴─────────┘
```

## 12 — Validate the package

```sql
PRAGMA cityparquet_validate('pkg');
```

## 13 — Validation results

```sql
SELECT * FROM cityparquet_validation;
```

```text
┌────────────┬──────────┬────────────┬───────────┬─────────┐
│ check_name │ severity │ table_name │ object_id │ message │
│  varchar   │ varchar  │  varchar   │  varchar  │ varchar │
└────────────┴──────────┴────────────┴───────────┴─────────┘
                           0 rows
```

## 14 — Insert a 3DBAG tile into the package

The package holds Delft's 2231 objects; the tile adds 930 more.

```sql
PRAGMA insert_cityjson('pkg', 'https://data.3dbag.nl/v20250903/tiles/9/304/532/9-304-532.city.json.gz');
SELECT count(*) FROM pkg.building;
```

```text
┌──────────────┐
│ count_star() │
│    int64     │
├──────────────┤
│         3161 │
└──────────────┘
```

## 15 — Inspect the building table

```sql
SELECT * FROM pkg.building;
```

## 16 — Export CityParquet

The package was built from a reader's output by `CREATE TABLE`, so it states no CRS of its
own; `crs =>` gives it one.

```sql
SELECT * FROM cityparquet_write('pkg', '/tmp/cp_test/pkg_out', crs => 'EPSG:7415');
```

```text
┌──────────────────┬─────────┬───────┬─────────┐
│       file       │ action  │ rows  │  bytes  │
│     varchar      │ varchar │ int64 │  int64  │
├──────────────────┼─────────┼───────┼─────────┤
│ building.parquet │ written │  3161 │ 4931359 │
│ metadata.json    │ written │     0 │    7079 │
└──────────────────┴─────────┴───────┴─────────┘
```

## 17 — Read back the `geo` Parquet metadata

Only the LoD0 footprint is GeoParquet-legal; the solids are declared in `city` alone.

```sql
WITH meta AS (
  SELECT decode(value)::JSON AS geo
  FROM parquet_kv_metadata('/tmp/cp_test/pkg_out/building.parquet')
  WHERE decode(key) = 'geo'
)
SELECT
  geo ->> '$.version'         AS version,
  geo ->> '$.primary_column'  AS primary_column,
  json_keys(geo, '$.columns') AS geometry_columns
FROM meta;
```

```text
┌─────────┬─────────────────┬───────────────────┐
│ version │ primary_column  │ geometry_columns  │
│ varchar │     varchar     │     varchar[]     │
├─────────┼─────────────────┼───────────────────┤
│ 1.1.0   │ geometry_lod0_0 │ [geometry_lod0_0] │
└─────────┴─────────────────┴───────────────────┘
```

## 18 — Read back the `city` Parquet metadata

`city` is in each object table's own footer. Its `columns` is a list of entries, one per
geometry column, solids included.

```sql
WITH meta AS (
  SELECT decode(value)::JSON AS city
  FROM parquet_kv_metadata('/tmp/cp_test/pkg_out/building.parquet')
  WHERE decode(key) = 'city'
)
SELECT
  city ->> '$.version'                           AS version,
  city ->> '$.primary_column'                    AS primary_column,
  json_extract_string(city, '$.columns[*].name') AS columns,
  city ->> '$.crs.id.code'                       AS crs_code
FROM meta;
```

```text
┌─────────────┬─────────────────┬──────────────────────────────────────────────────────────────────────┬──────────┐
│   version   │ primary_column  │                               columns                                │ crs_code │
│   varchar   │     varchar     │                              varchar[]                               │ varchar  │
├─────────────┼─────────────────┼──────────────────────────────────────────────────────────────────────┼──────────┤
│ 0.1.0-draft │ geometry_lod2_2 │ [geometry_lod0_0, geometry_lod1_2, geometry_lod1_3, geometry_lod2_2] │ 7415     │
└─────────────┴─────────────────┴──────────────────────────────────────────────────────────────────────┴──────────┘
```

## 19 — Load a package directory back into a schema

```sql
CREATE SCHEMA delft;
PRAGMA cityparquet_read('/tmp/cp_test/pkg_out', 'delft');
SELECT count(*) FROM delft.building;
```

```text
┌──────────────┐
│ count_star() │
│    int64     │
├──────────────┤
│         3161 │
└──────────────┘
```

## 20 — Test FlatCityBuf

```sql
SELECT * FROM read_flatcitybuf('https://flatcitybuf.open3d.city/data/delft.fcb');
```

## 21 — FlatCityBuf: spatial + attribute filter

```sql
SELECT * FROM read_flatcitybuf('https://flatcitybuf.open3d.city/data/3dbag_all_index.fcb',
                              xmin := 84000, ymin := 446000, xmax := 85000, ymax := 447000)
WHERE b3_h_dak_50p >= 10 ORDER BY b3_h_dak_50p DESC LIMIT 10;
```

## 22 — FlatCityBuf: bbox only

```sql
SELECT * FROM read_flatcitybuf('https://flatcitybuf.open3d.city/data/3dbag_all_index.fcb',
                              xmin := 84000, ymin := 446000, xmax := 85000, ymax := 447000);
```

## 23 — Start a package from a CityJSON file

No table of your own: an empty schema, initialised, and the first insert creates every
module table the source needs and takes the source's CRS, so the write needs no `crs =>`.
Submit each statement separately — DuckDB expands every pragma in a script before running
any of it.

```sql
CREATE SCHEMA fresh;
PRAGMA cityparquet_init('fresh');
PRAGMA insert_cityjsonseq('fresh', 'https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl');
SELECT * FROM cityparquet_write('fresh', '/tmp/cp_test/fresh');

SELECT decode(value)::JSON ->> '$.crs.name' AS crs
FROM parquet_kv_metadata('/tmp/cp_test/fresh/building.parquet') WHERE decode(key) = 'city';
```

```text
┌──────────────────┬─────────┬───────┬─────────┐
│       file       │ action  │ rows  │  bytes  │
│     varchar      │ varchar │ int64 │  int64  │
├──────────────────┼─────────┼───────┼─────────┤
│ building.parquet │ written │  2231 │ 3741462 │
│ metadata.json    │ written │     0 │    6764 │
└──────────────────┴─────────┴───────┴─────────┘
┌──────────────────────────────────┐
│               crs                │
│             varchar              │
├──────────────────────────────────┤
│ Amersfoort / RD New + NAP height │
└──────────────────────────────────┘
```

## 24 — Hilbert order

`cityparquet_write` sorts whole features along a Hilbert curve by default. Written in
source order instead (`ordering => 'source'`), consecutive Delft Buildings lie a median
316 m apart; in Hilbert order, 10 m — which is what lets a spatial filter skip row groups.

```sql
SELECT count(*) FROM cityparquet_write('fresh', '/tmp/cp_test/fresh_source', ordering => 'source');

WITH r AS (
  SELECT 'hilbert' AS ordering, file_row_number AS n, (bbox.xmin + bbox.xmax) / 2 AS x, (bbox.ymin + bbox.ymax) / 2 AS y
  FROM read_parquet('/tmp/cp_test/fresh/building.parquet', file_row_number := true) WHERE object_type = 'Building'
  UNION ALL
  SELECT 'source', file_row_number, (bbox.xmin + bbox.xmax) / 2, (bbox.ymin + bbox.ymax) / 2
  FROM read_parquet('/tmp/cp_test/fresh_source/building.parquet', file_row_number := true) WHERE object_type = 'Building')
SELECT ordering, round(median(sqrt((x - px) ^ 2 + (y - py) ^ 2)), 1) AS median_step_m
FROM (SELECT *, lag(x) OVER w AS px, lag(y) OVER w AS py FROM r WINDOW w AS (PARTITION BY ordering ORDER BY n))
GROUP BY ordering ORDER BY ordering;
```

```text
┌──────────┬───────────────┐
│ ordering │ median_step_m │
│ varchar  │    double     │
├──────────┼───────────────┤
│ hilbert  │          10.3 │
│ source   │         315.7 │
└──────────┴───────────────┘
```

## 25 — Implicit geometry on lod3_railway

The railway's trees are `GeometryInstance`s of three templates: each reads into
`implicit_geometry` (template, reference point, matrix), and a COPY writes them back with
their `geometry-templates`, appearance included.

```sql
SELECT id, implicit_geometry.id AS template, ST_AsText(ST_GeomFromWKB(implicit_geometry.point)) AS point
FROM read_cityjson('https://storage.googleapis.com/cityjson/lod3_railway.city.json')
WHERE implicit_geometry IS NOT NULL ORDER BY id LIMIT 3;

COPY (SELECT * FROM read_cityjson('https://storage.googleapis.com/cityjson/lod3_railway.city.json')
      WHERE object_type IN ('SolitaryVegetationObject', 'CityObjectGroup'))
TO '/tmp/cp_test/vegetation.city.json' (FORMAT cityjson);

SELECT (SELECT count(*) FROM read_cityjson('/tmp/cp_test/vegetation.city.json') WHERE implicit_geometry IS NOT NULL) AS instances,
       (SELECT count(*) FROM cityjson_implicit_geometries('/tmp/cp_test/vegetation.city.json')) AS templates,
       (SELECT count(*) FROM (SELECT * FROM cityjson_implicit_geometries('https://storage.googleapis.com/cityjson/lod3_railway.city.json')
                              EXCEPT ALL
                              SELECT * FROM cityjson_implicit_geometries('/tmp/cp_test/vegetation.city.json'))) AS templates_changed;
```

```text
┌────────────────────────────┬──────────┬──────────────────────────────────────────────────────┐
│             id             │ template │                        point                         │
│          varchar           │  int64   │                       varchar                        │
├────────────────────────────┼──────────┼──────────────────────────────────────────────────────┤
│ GMLID_SO0107241_3793_12555 │        1 │ POINT Z (1.046 6.519 8.798)                          │
│ GMLID_SO0124800_3522_13577 │        0 │ POINT Z (0.7190000000000001 7.4399999999999995 9.1)  │
│ GMLID_SO015374_872_14131   │        0 │ POINT Z (0.6940000000000001 6.827999999999999 8.949) │
└────────────────────────────┴──────────┴──────────────────────────────────────────────────────┘
┌───────────┬───────────┬───────────────────┐
│ instances │ templates │ templates_changed │
│   int64   │   int64   │       int64       │
├───────────┼───────────┼───────────────────┤
│        15 │         3 │                 0 │
└───────────┴───────────┴───────────────────┘
```

## 26 — Address round trip

A 3D Helsinki building whose `address` was filled in with CityJSON's documented member
names (`test/data/address_location.city.jsonl`; the published Helsinki data spells them
`Country` / `Locality`, which do not map). The address reads into its own column, its
`location` a MultiPointZ, and COPY writes it back as the CityObject's `address` member.

```sql
SELECT a.street, a.house_number, a.city, a.country, ST_AsText(ST_GeomFromWKB(a.location)) AS location
FROM (SELECT unnest(address) AS a FROM read_cityjsonseq('test/data/address_location.city.jsonl'));

COPY (SELECT * FROM read_cityjsonseq('test/data/address_location.city.jsonl'))
TO '/tmp/cp_test/address.city.jsonl' (FORMAT cityjsonseq);

SELECT count(*) AS changed FROM (
  SELECT id, address FROM read_cityjsonseq('test/data/address_location.city.jsonl')
  EXCEPT ALL
  SELECT id, address FROM read_cityjsonseq('/tmp/cp_test/address.city.jsonl'));
```

```text
┌─────────────────┬──────────────┬──────────┬─────────┬──────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│     street      │ house_number │   city   │ country │                                                   location                                                   │
│     varchar     │   varchar    │ varchar  │ varchar │                                                   varchar                                                    │
├─────────────────┼──────────────┼──────────┼─────────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ Mannerheimintie │ 1            │ Helsinki │ Finland │ MULTIPOINT Z (25492829.152 6679442.0540000005 21.487000000000002, 25492827.173 6679443.5 21.487000000000002) │
│ NULL            │ NULL         │ Espoo    │ NULL    │ NULL                                                                                                         │
└─────────────────┴──────────────┴──────────┴─────────┴──────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
┌─────────┐
│ changed │
│  int64  │
├─────────┤
│       0 │
└─────────┘
```

## 27 — Append into a package cityparquet-rs wrote

`test/data/cityparquet_rs_delft` is cityparquet-rs's package of a Delft subset; the
appended file is one 3DBAG building and its part. The package's own rows keep their `bbox`
bit for bit — cityparquet-rs folds each object's declared extent into it, which
re-deriving from the geometry would narrow.

```sql
PRAGMA cityparquet_read('test/data/cityparquet_rs_delft', 'rsd');
PRAGMA insert_cityjsonseq('rsd', 'test/data/delft_append.city.jsonl');
SELECT * FROM cityparquet_write('rsd', '/tmp/cp_test/rs_appended');

SELECT count(*) FILTER (WHERE id LIKE '%-appended') AS appended,
       count(*) FILTER (WHERE s.bbox IS DISTINCT FROM o.bbox AND s.id IS NOT NULL) AS boxes_changed
FROM read_parquet('/tmp/cp_test/rs_appended/building.parquet') o
LEFT JOIN read_parquet('test/data/cityparquet_rs_delft/building.parquet') s USING (id);
```

```text
┌──────────────────┬─────────┬───────┬───────┐
│       file       │ action  │ rows  │ bytes │
│     varchar      │ varchar │ int64 │ int64 │
├──────────────────┼─────────┼───────┼───────┤
│ building.parquet │ written │    22 │ 72199 │
│ metadata.json    │ written │     0 │  7078 │
└──────────────────┴─────────┴───────┴───────┘
┌──────────┬───────────────┐
│ appended │ boxes_changed │
│  int64   │     int64     │
├──────────┼───────────────┤
│        2 │             0 │
└──────────┴───────────────┘
```
