# CityJSON extension — notebook SQL

Verbatim extraction of all 22 SQL cells from the DuckDB notebook `CityJSON extension`, in notebook order. Nothing in the notebook was modified.

> A few cells end with a stray `"` — that character is actually present in the cell source, so it is kept here.

## Running it

`just test-notebook` runs this walkthrough as a test:

| File | Covers |
| ---- | ------ |
| `test/sql/cityjson_notebook_e2e.test` | Cells 2–3 and 5–22 |
| `test/sql/cityjson_notebook_geoparquet.test` | Cell 4, which needs `spatial` — a `require` for it would skip a whole file rather than one query |

Both are gated on `CITYJSON_NOTEBOOK_TEST` and stay out of `make test`. The notebook's
local fixtures lived in a sibling repo of the parent workspace, which this repo cannot
reference; each is read over HTTP from the URL it was published at instead, so nothing
needs downloading first. `./data/delft` in cell 19 was generated rather than published,
so the harness reads back the package cell 16 writes — which makes those cells a
write-then-read round trip.

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
load './duckdb-cityjson/build/release/extension/cityjson/cityjson.duckdb_extension';
```

## 2 — Read CityJSONSeq (Delft)

```sql
SELECT *
FROM read_cityjsonseq('cityparquet-rs/tests/fixtures/delft.city.jsonl');"
```

## 3 — Read CityJSON, filter by object type

```sql
SELECT *
FROM read_cityjson('cityparquet-rs/tests/fixtures/lod3_railway.city.json') where object_type IN ['Building', 'BuildingInstallation', 'BuildingPart'];
```

## 4 — Check LoD0 is valid GeoParquet

```sql
load spatial;
-- Check LoD0 is valid GeoParquet
WITH seq AS (
  SELECT id, ST_GeomFromWKB(geometry_lod0_0) AS geom
  FROM read_cityjsonseq('cityparquet-rs/tests/fixtures/delft.city.jsonl')
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
-- SELECT * FROM read_cityjson('https://storage.googleapis.com/cityjson/delft.city.json') limit 10;
SELECT *
FROM read_cityjsonseq('https://cityjson.open3d.city/cityjsonseq/Helsinki_tex.city.jsonl') limit 10;
```

## 6 — Arrow-native column encoding

```sql
-- Arrow-native column
DESCRIBE SELECT * FROM read_cityjsonseq('cityparquet-rs/tests/fixtures/delft.city.jsonl',
                                        lod => '2.2', geometry_encoding := 'arrow-native');"
```

## 7 — GeoParquet `geo` metadata

```sql
SELECT geo FROM cityjson_geoparquet_geo('cityparquet-rs/tests/fixtures/delft.city.jsonl');"
```

## 8 — Roundtrip testing

```sql
-- Roundtip testing
SET VARIABLE geo = (SELECT geo FROM cityjson_geoparquet_geo('cityparquet-rs/tests/fixtures/delft.city.jsonl'));
COPY (SELECT * FROM read_cityjsonseq('cityparquet-rs/tests/fixtures/delft.city.jsonl'))
  TO '/tmp/cp_test/delft_duckdb.parquet'
  (FORMAT PARQUET, KV_METADATA {geo: getvariable('geo')});
SELECT* FROM read_parquet('/tmp/cp_test/delft_duckdb.parquet');"
```

## 9 — Material and texture

```sql
-- Material and texture
SELECT (SELECT count(*) FROM cityjson_materials('cityparquet-rs/tests/fixtures/lod3_railway.city.json'))          AS materials,
       (SELECT count(*) FROM cityjson_textures('cityparquet-rs/tests/fixtures/lod3_railway.city.json'))           AS textures,
       (SELECT count(*) FROM cityjson_geometry_templates('cityparquet-rs/tests/fixtures/lod3_railway.city.json')) AS templates;

SELECT * FROM cityjson_materials('cityparquet-rs/tests/fixtures/lod3_railway.city.json')
```

## 10 — Sidecar appearance at LoD3

```sql
SELECT count(*) AS rows, count(material_lod3_0) AS with_material
FROM read_cityjson('cityparquet-rs/tests/fixtures/lod3_railway.city.json', lod => '3', appearance := 'sidecar');"
```

## 11 — Mutation tests: build a package schema

```sql
-- Mutation tests
-- ROLLBACK;
CREATE SCHEMA pkg;
CREATE TABLE pkg.building AS
SELECT
  *
FROM
  read_cityjsonseq ('cityparquet-rs/tests/fixtures/delft.city.jsonl');


PRAGMA cityparquet_init('pkg');
SELECT table_name, role FROM pkg.__cityparquet ORDER BY 1;
```

## 12 — Validate the package

```sql
PRAGMA cityparquet_validate('pkg');
```

## 13 — Validation results

```sql
SELECT * FROM cityparquet_validation;
```

## 14 — Insert CityJSON into the package

```sql
PRAGMA insert_cityjson('pkg', './cityparquet-rs/bench/data/9-304-532.city.json');
```

## 15 — Inspect the building table

```sql
select * from pkg.building;
```

## 16 — Export CityParquet

```sql
-- export CityPParquet
SELECT * FROM cityparquet_write('pkg', '/tmp/cp_test/pkg_out', crs => 'EPSG:7415');
```

## 17 — Read back the `geo` Parquet metadata

```sql
WITH meta AS (
  SELECT decode(value)::JSON AS geo
  FROM parquet_kv_metadata('/tmp/cp_test/pkg_out/building.parquet')
  WHERE key::VARCHAR = 'geo'
)
SELECT
  geo ->> '$.version'         AS version,
  geo ->> '$.primary_column'  AS primary_column,
  json_keys(geo, '$.columns') AS geometry_columns,
  geo
FROM meta;
```

## 18 — Read back the `city` Parquet metadata

```sql
WITH meta AS (
  SELECT decode(value)::JSON AS city
  FROM parquet_kv_metadata('/tmp/cp_test/pkg_out')
  WHERE key::VARCHAR = 'city'
)
SELECT
  city ->> '$.version'         AS version,
  city ->> '$.primary_column'  AS primary_column,
  json_keys(city, '$.columns') AS columns,
  city
FROM meta;
```

## 19 — Automatically create table with cityparquet

```sql
-- Automatically create table with cityparquet
CREATE SCHEMA delft;
PRAGMA cityparquet_read('./data/delft', 'delft');
SELECT count(*) FROM delft.building;
```

## 20 — Test FlatCityBuf

```sql
-- Test FlatCityBuf----------
SELECT * FROM read_flatcitybuf('https://flatcitybuf.open3d.city/data/delft.fcb');
```

## 21 — FlatCityBuf: spatial + attribute filter

```sql
SELECT * FROM read_flatcitybuf('https://flatcitybuf.open3d.city/data/3dbag_all_index.fcb', xmin:=84000, ymin:=446000, xmax :=85000, ymax:=447000) where b3_h_dak_50p >= 10 order by b3_h_dak_50p desc limit 10;
```

## 22 — FlatCityBuf: bbox only

```sql
SELECT * FROM read_flatcitybuf('https://flatcitybuf.open3d.city/data/3dbag_all_index.fcb', xmin:=84000, ymin:=446000, xmax :=85000, ymax:=447000);
```