# Function reference

Every SQL function this extension registers, with a worked example for each.

The examples run against two public Delft datasets, so you can paste any of them
into a DuckDB shell and get the same numbers back:

| Dataset | URL |
| ------- | --- |
| CityJSONSeq | `https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl` |
| CityJSON | `https://cityjson.open3d.city/cityjson/delft.city.json` |

Both hold the same 2231 CityObjects — 1115 `Building` and 1116 `BuildingPart` —
in EPSG:7415 (RD New + NAP). Remote reads need `httpfs`, which is auto-loaded
when a URL is detected:

```sql
INSTALL cityjson FROM community;
LOAD cityjson;
```

> **Note on the Delft data's LoDs.** Geometry lives on the `BuildingPart` rows,
> not the `Building` rows: LoD 0.0, 1.2, 1.3 and 2.2 are all present, but a
> `Building` row's `geometry_lod2_2` is NULL. Examples below filter on
> `BuildingPart` (or on `geometry_lod2_2 IS NOT NULL`) for that reason — it is a
> property of this dataset's parent/child split, not of the extension.

## Contents

- [Reading](#reading) — `read_cityjson`, `read_cityjsonseq`, `read_flatcitybuf`
- [Metadata](#metadata) — `cityjson_metadata`, `cityjsonseq_metadata`, `flatcitybuf_metadata`
- [Writing](#writing) — `COPY … TO`
- [CityParquet footers](#cityparquet-footers) — `cityjson_geoparquet_geo`
- [Appearance sidecars](#appearance-sidecars) — `cityjson_materials`, `cityjson_textures`, `cityjson_geometry_templates`
- [Scalar helpers](#scalar-helpers) — `cityjson_wkb_extent`, `cityjson_appearance_ids`
- [CityParquet packages](#cityparquet-packages) — the `cityparquet_*` / `insert_*` pragmas
- [Output schema](#output-schema) — column grammar in detail

---

## Reading

### `read_cityjson(path, …)` / `read_cityjsonseq(path, …)`

One row per **CityObject**. `read_cityjson` takes a `.city.json` document with a
global vertex pool; `read_cityjsonseq` takes a `.city.jsonl` stream whose every
line after the header is a `CityJSONFeature` with its own local pool. Both
produce the same column grammar.

```sql
SELECT COUNT(*) FROM read_cityjsonseq('https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl');
-- 2231
```

```sql
SELECT object_type, COUNT(*) AS cnt
FROM read_cityjsonseq('https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl')
GROUP BY object_type ORDER BY cnt DESC;
-- BuildingPart   1116
-- Building       1115
```

Attributes become their own typed columns. The 3DBAG attributes on this dataset
infer as `DOUBLE`, `BIGINT`, `DATE`, `TIMESTAMP` and `BOOLEAN`:

```sql
SELECT id, b3_h_dak_max
FROM read_cityjsonseq('https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl')
WHERE object_type = 'Building'
ORDER BY b3_h_dak_max DESC LIMIT 3;
-- NL.IMBAG.Pand.0503100000030264   95.53
-- NL.IMBAG.Pand.0503100000032914   52.83
-- NL.IMBAG.Pand.0503100000031391   40.05
```

**Parameters** (shared by these two functions):

| Parameter | Type | Description |
| --------- | ---- | ----------- |
| `path` | VARCHAR | File path or URL |
| `lod` | VARCHAR | Restrict the schema to one LoD, e.g. `'2.2'` |
| `sample_lines` | BIGINT | Features sampled for schema inference (default 100) |
| `appearance` | VARCHAR | `'local'` (default) or `'sidecar'` — see [Appearance sidecars](#appearance-sidecars) |
| `geometry_encoding` | VARCHAR | `'wkb'` (default) or `'arrow-native'` — see below |

`read_flatcitybuf` shares only `lod` and `sample_lines`; it takes neither
`appearance` nor `geometry_encoding`, and adds the four bbox bounds instead.

#### `lod =>` — one LoD, same column grammar

```sql
SELECT id, cityjson_wkb_extent(geometry_lod2_2).max_z AS ridge_height
FROM read_cityjsonseq('https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl', lod => '2.2')
WHERE geometry_lod2_2 IS NOT NULL
LIMIT 3;
-- NL.IMBAG.Pand.0503100000012869-0 |  3.23
-- NL.IMBAG.Pand.0503100000016459-0 |  2.42
-- NL.IMBAG.Pand.0503100000005156-0 | 11.12
```

The LoD stays in the column name rather than collapsing to a bare `geometry`
column, which is what lets `COPY … TO cityjson` re-emit it at the right level.

> **Which LoDs DuckDB `spatial` can read.** `ST_GeomFromWKB` handles the
> footprint LoDs (`MultiPolygon Z`) but **throws `Unsupported geometry type in
> WKB` on any solid LoD**, which is encoded as `PolyhedralSurface Z`. Use
> [`cityjson_wkb_extent`](#cityjson_wkb_extentblob) for solids, and
> [`cityjson_wkb_geometry_type`](#cityjson_wkb_geometry_typeblob) to tell them
> apart. This is the same boundary that decides which columns `geo` may declare.

```sql
-- Footprints, however, go straight into spatial
LOAD spatial;
SELECT id, ST_Area(ST_GeomFromWKB(geometry_lod0_0)) AS footprint_m2
FROM read_cityjsonseq('https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl')
WHERE geometry_lod0_0 IS NOT NULL LIMIT 3;
-- NL.IMBAG.Pand.0503100000012869 |  7.21
-- NL.IMBAG.Pand.0503100000016459 | 10.34
-- NL.IMBAG.Pand.0503100000005156 | 99.25
```

#### `geometry_encoding => 'arrow-native'` (experimental)

Replaces the WKB `BLOB` with five nested LIST levels — solid → shell → face →
ring → vertex-pool index — plus a sibling `geometry_vertices_lod*` of
`STRUCT(x, y, z DOUBLE)[]` holding that row's pool:

```sql
SELECT geometry_lod2_2 FROM read_cityjsonseq('test/data/delft_subset.city.jsonl',
                                             geometry_encoding := 'arrow-native') LIMIT 1;
-- column type: integer[][][][][]
```

`geometry_properties_lod*` is unchanged and stays the **only** thing that says
whether a row is a `Solid` or a `MultiSurface` — the physical nesting is uniform
across both families, so never infer the CityJSON type from the shape.

### `read_flatcitybuf(path, …)`

Reads [FlatCityBuf](https://github.com/cityjson/flatcitybuf) (`.fcb`), a
cloud-optimised binary CityJSON encoding with an R-tree spatial index and
per-column B+tree attribute indices. Make one from the Delft data first:

```sql
COPY (SELECT * FROM read_cityjsonseq('https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl'))
TO 'delft.fcb' (FORMAT flatcitybuf);

SELECT COUNT(*) FROM read_flatcitybuf('delft.fcb');
-- 2231
```

**Bbox pushdown** is a real R-tree-level skip — features outside the box are
never decoded. All four bounds must be given together:

```sql
SELECT COUNT(*) FROM read_flatcitybuf('delft.fcb',
    min_x => 84900, min_y => 446200, max_x => 85200, max_y => 446500);
-- 58
```

**Attribute pushdown** answers `=`, `!=`, `>`, `>=`, `<`, `<=` from the B+tree,
for columns indexed at write time:

```sql
COPY (SELECT * FROM read_cityjsonseq('https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl'))
TO 'delft_indexed.fcb' (FORMAT flatcitybuf, attr_index 'b3_h_dak_max');

SELECT COUNT(*) FROM read_flatcitybuf('delft_indexed.fcb') WHERE b3_h_dak_max > 20;
-- 15
```

Any other column or operator still returns correct results — just via ordinary
post-scan filtering. Reads decode only what the query projects: a query that
touches no geometry column skips geometry conversion entirely.

---

## Metadata

`cityjson_metadata(path)`, `cityjsonseq_metadata(path)` and
`flatcitybuf_metadata(path)` each return a **single row** of dataset-level
metadata, with identical schemas.

```sql
SELECT version, city_objects_count,
       reference_system.authority AS auth, reference_system.code AS code
FROM cityjsonseq_metadata('https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl');
-- 2.0 | 2231 | EPSG | 7415
```

```sql
SELECT version, city_objects_count
FROM cityjson_metadata('https://cityjson.open3d.city/cityjson/delft.city.json');
-- 2.0 | 2231
```

| Column | Type |
| ------ | ---- |
| `id` | INTEGER (always 1) |
| `version` | VARCHAR |
| `identifier` / `title` | VARCHAR |
| `reference_date` | DATE |
| `transform_scale` / `transform_translate` | STRUCT(x, y, z DOUBLE) |
| `geographical_extent` | STRUCT(min_x … max_z DOUBLE) |
| `reference_system` | STRUCT(base_url, authority, version, code VARCHAR) |
| `point_of_contact` | STRUCT(contact_name, email_address, contact_type, role, phone, website VARCHAR, address STRUCT(…)) |
| `city_objects_count` | BIGINT |
| `features_count` | BIGINT |

**`city_objects_count` and `features_count` count different things**, and either
may be NULL when the source cannot report it cheaply. A CityJSONSeq line is one
*feature* and may carry several CityObjects — a `Building` plus its
`BuildingPart`s — so on 3DBAG data the two differ by roughly 2x:

```sql
SELECT city_objects_count, features_count
FROM flatcitybuf_metadata('https://flatcitybuf.open3d.city/data/delft.fcb');
-- NULL | 1115
```

A FlatCityBuf header carries `features_count` directly, so that is what is
reported. `city_objects_count` is NULL rather than a plausible wrong number: the
true count needs a full decode, which a metadata call should not pay for. Use
`SELECT COUNT(*) FROM read_flatcitybuf(…)` when you need it — that returns 2231
for this file, one row per CityObject. Whole-document CityJSON has no feature
concept, so its `features_count` is NULL.

---

## Writing

Three output formats, all from the same required column set.

```sql
-- CityJSONSeq (streaming, per-feature vertex pools) — preferred for large data
COPY (SELECT * FROM read_cityjsonseq('https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl'))
TO 'delft_out.city.jsonl' (FORMAT cityjsonseq);

-- CityJSON (one document, global vertex pool)
COPY (SELECT * FROM read_cityjson('https://cityjson.open3d.city/cityjson/delft.city.json'))
TO 'delft_out.city.json' (FORMAT cityjson);

-- FlatCityBuf (binary, spatially indexed)
COPY (SELECT * FROM read_cityjsonseq('https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl'))
TO 'delft_out.fcb' (FORMAT flatcitybuf);
```

Filtering on the way out is just a `WHERE`:

```sql
COPY (
    SELECT * FROM read_cityjsonseq('https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl')
    WHERE b3_h_dak_max > 20
)
TO 'delft_tall.city.jsonl' (FORMAT cityjsonseq);
```

### Options

| Option | Type | Description |
| ------ | ---- | ----------- |
| `version` | VARCHAR | CityJSON version to write (default `"2.0"`) |
| `crs` | VARCHAR | CRS identifier, e.g. `'https://www.opengis.net/def/crs/EPSG/0/7415'` |
| `transform_scale` | VARCHAR | Vertex quantisation scale `'x,y,z'` (default `'0.001,0.001,0.001'`) |
| `transform_translate` | VARCHAR | Quantisation offset `'x,y,z'` (default `'0.0,0.0,0.0'`) |
| `metadata_from` | VARCHAR | Path to read metadata and appearance definitions from, when the source is not discoverable from the query itself |
| `metadata_query` | VARCHAR | SQL whose result columns supply metadata. Recognised: `version`, `crs` (or `reference_system`, as a struct or a plain string), `transform_scale`, `transform_translate`, `title`, `identifier`, `reference_date` |
| `attr_index` | VARCHAR | *(flatcitybuf)* comma-separated columns to give a B+tree index |
| `branching_factor` | BIGINT | *(flatcitybuf)* B+tree branching factor |
| `index_node_size` | BIGINT | *(flatcitybuf)* R-tree node size |

Vertices are quantised to integers against the transform, so `transform_scale`
sets the output precision. The 1 mm default keeps round-trips lossless even for
large projected coordinates. Carry the source's own transform through instead:

```sql
COPY (SELECT * FROM read_cityjson('https://cityjson.open3d.city/cityjson/delft.city.json'))
TO 'delft_out.city.json' (
    FORMAT cityjson,
    metadata_query 'SELECT version, reference_system AS crs FROM cityjson_metadata(''https://cityjson.open3d.city/cityjson/delft.city.json'')'
);
```

**Metadata and appearance are inherited automatically.** When the `SELECT` names
exactly one reader, `COPY` recovers the source path from it and carries the
source's CRS, title, point of contact and geographical extent — and its material
and texture definitions — across without being asked:

```sql
COPY (SELECT * FROM read_cityjsonseq('https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl'))
TO 'delft_out.city.jsonl' (FORMAT cityjsonseq);

SELECT reference_system.code FROM cityjsonseq_metadata('delft_out.city.jsonl');
-- 7415
```

This needs the source to be **statically discoverable**. `COPY my_table TO …`, a
join across two sources, and a computed path are not; an ambiguous source is
refused rather than guessed at, because stamping the wrong CRS onto georeferenced
output is worse than stamping none. A warning is logged, and `metadata_from`
names the source explicitly:

```sql
COPY (SELECT * FROM my_table) TO 'out.city.jsonl'
  (FORMAT cityjsonseq, metadata_from 'delft.city.jsonl');
```

Precedence is `crs` / `metadata_query` > `metadata_from` > discovered source, so
an explicit option always wins.

Requesting `attr_index` on a column no feature carries is not an error — there is
simply nothing to index.

### Required columns

| Column | Required | Description |
| ------ | -------- | ----------- |
| `id` | Yes | CityObject identifier |
| `feature_id` | Yes | Feature grouping key |
| `object_type` | Yes | CityJSON type |
| `children` / `parents` | No | Hierarchy |
| `geometry` / `geometry_lod*` | No | WKB `BLOB` **or** DuckDB `GEOMETRY` |
| `geometry_properties*` | No | CityParquet STRUCT **or** JSON text |

Everything else is written as a CityJSON attribute.

**The wide CityParquet layout round-trips directly.** A Parquet object table goes
back to CityJSON with no intermediate step, one multi-LoD CityObject per feature:

```sql
COPY (SELECT * FROM read_parquet('out/building.parquet'))
TO 'roundtrip.city.jsonl' (FORMAT cityjsonseq);
```

```sql
SELECT COUNT(*) FILTER (WHERE geometry_lod0_0 IS NOT NULL) AS lod0,
       COUNT(*) FILTER (WHERE geometry_lod1_2 IS NOT NULL) AS lod12,
       COUNT(*) FILTER (WHERE geometry_lod2_2 IS NOT NULL) AS lod22
FROM read_cityjsonseq('roundtrip.city.jsonl');
-- 1115 | 1116 | 1116      ← every LoD preserved
```

Semantics survive the trip too — the property struct is rebuilt from the stored
columns, not re-derived from the geometry:

```sql
SELECT geometry_properties_lod2_2.type, geometry_properties_lod2_2.shells
FROM read_cityjsonseq('roundtrip.city.jsonl') WHERE geometry_lod2_2 IS NOT NULL LIMIT 1;
-- Solid | [[6]]
```

---

## CityParquet footers

### `cityjson_geoparquet_geo(path [, geometry_encoding =>])`

Returns one row of two VARCHARs — the Parquet footer keys DuckDB core cannot
infer from a plain `BLOB` column.

```sql
SELECT geo IS NOT NULL AS has_geo, city IS NOT NULL AS has_city
FROM cityjson_geoparquet_geo('https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl');
-- true | true
```

- **`city`** — the CityParquet `city` object. **Required on every CityParquet
  file**, so never NULL. Declares *every* `geometry_lod*` column, `Solid` family
  included, each with its `name`, physical `encoding`, `geometry_types`, `crs`,
  `edges` and an explicit `orientation_3d` (winding lives here; GeoParquet's
  planar `orientation` cannot express it).
- **`geo`** — [GeoParquet 1.1](https://geoparquet.org/) metadata, so GeoPandas,
  DuckDB `spatial` and GDAL/OGR recognise the geometry columns. Declares **only
  GeoParquet-legal** columns: a LoD qualifies only if every geometry there is a
  `MultiPoint` / `MultiLineString` / `MultiSurface` / `CompositeSurface`. A LoD
  containing any `Solid` is excluded — declaring it would make the whole file
  unreadable to strict GeoParquet readers. An all-solid dataset yields `NULL`: a
  valid CityParquet table that simply is not GeoParquet.

`crs` is **tri-state, exactly as in GeoParquet**: a PROJJSON object when known,
explicit `null` when the file holds CRS-bearing coordinates whose CRS is unknown
or unresolvable, and absent only for a file with no CRS-bearing coordinate at
all. It is never omitted (that would assert `OGC:CRS84` over projected national
coordinates) and never guessed.

`KV_METADATA` cannot contain a subquery, so pass the values via variables:

```sql
SET VARIABLE geo  = (SELECT geo  FROM cityjson_geoparquet_geo('https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl'));
SET VARIABLE city = (SELECT city FROM cityjson_geoparquet_geo('https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl'));

COPY (SELECT * FROM read_cityjsonseq('https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl'))
TO 'delft.parquet'
(FORMAT PARQUET, KV_METADATA {geo: getvariable('geo'), city: getvariable('city')});
```

`KV_METADATA` cannot *omit* a key — a NULL value writes the literal string
`"NULL"` — so an all-solid dataset must write `city` alone. For whole packages
use [`cityparquet_write`](#the-package-round-trip), which branches the footer
shape in C++ for exactly this reason.

---

## Appearance sidecars

CityJSON carries appearance as **feature-local indices** into per-feature arrays.
CityParquet requires **dataset-global sidecar ids** and **inlined texture UVs**:
once every feature's rows share one table, a feature-local index resolves to the
wrong definition — or to nothing.

> The Delft datasets used elsewhere in this document carry **no** appearance and
> **no** geometry templates, so every function in this section returns zero rows
> against them. The examples below therefore use a small fixture from this
> repository, `test/data/railway_appearance.city.jsonl`, which carries all three.

```sql
-- The sidecar tables, shaped as materials.parquet / textures.parquet
SELECT id, name, diffuseColor, transparency
FROM cityjson_materials('test/data/railway_appearance.city.jsonl');
-- 0 | UUID_e58d9d68-… | [0.496, 0.430, 0.297] | 0.0
-- 1 | UUID_f55b5612-… | [0.496, 0.430, 0.297] | 0.0
-- 2 | UUID_1c68ae93-… | [0.449, 0.449, 0.496] | 0.0
-- 3 | UUID_0794715b-… | [0.598, 0.598, 0.598] | 0.0

SELECT * FROM cityjson_textures('test/data/railway_appearance.city.jsonl');
SELECT * FROM cityjson_geometry_templates('test/data/railway_appearance.city.jsonl');
```

Object rows then reference those ids rather than feature-local ones:

```sql
SELECT id, material_lod3_0
FROM read_cityjsonseq('test/data/railway_appearance.city.jsonl', appearance := 'sidecar')
WHERE material_lod3_0 IS NOT NULL LIMIT 1;
-- GMLID_855011_330784_753 | {"visual":{"values":[2,2,2,2,2,2,3,3,3,…]}}
```

`appearance` accepts `'local'` (default, verbatim) or `'sidecar'`, on
`read_cityjson` and `read_cityjsonseq`. **`read_flatcitybuf` does not take it** —
sidecar normalisation is not available on the `.fcb` read path.

The template sidecar carries the same per-LoD column grammar as an object table
(`geometry_lod3_0`, `geometry_properties_lod3_0`, `material_lod3_0`,
`texture_lod3_0` for this fixture), alongside `id` and `name`.

**Definitions are interned, not read from the header.** CityJSONSeq does not keep
every definition in one place — the header carries some, each feature carries the
ones it uses under its *own* local indices, so a feature's material `0` is not in
general the header's material `0`. The sidecar is the interned union across the
whole file, matched by structural equality (CityJSON gives a material no identity
of its own). Header entries intern first, so their ids stay their ordinal
positions, which is what a plain CityJSON document yields.

**Geometry templates are in local coordinates**, exempt from the dataset
transform and the file CRS — an instance's `transformationMatrix` and reference
point place it into the world — so their WKB holds raw doubles. Each row
populates only its own LoD's columns, leaving the table sparse by construction.

**Texture UVs are inlined.** A source ring is `[texId, uvIdx, uvIdx, …]`; sidecar
mode emits `[texId, [u,v], [u,v], …]`. Both rewrites recurse to their leaves
rather than assuming a nesting depth, since a `Solid` nests one level deeper than
a `MultiSurface`.

---

## Scalar helpers

### `cityjson_wkb_extent(blob)`

3D extent of a WKB blob, **solid family included** — DuckDB `spatial` rejects
`PolyhedralSurface Z`, which is what every CityParquet solid LoD is.

```sql
SELECT cityjson_wkb_extent(geometry_lod2_2)
FROM read_cityjsonseq('https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl')
WHERE geometry_lod2_2 IS NOT NULL LIMIT 1;
--> STRUCT(min_x, min_y, min_z, max_x, max_y, max_z DOUBLE)
```

### `cityjson_appearance_ids(cell, kind)`

The sidecar ids an appearance cell references, as a list.

```sql
SELECT cityjson_appearance_ids(material_lod3_0, 'material') AS ids
FROM read_cityjsonseq('test/data/railway_appearance.city.jsonl', appearance := 'sidecar')
WHERE material_lod3_0 IS NOT NULL LIMIT 1;
-- [2, 3]
```

`kind` is `'material'` or `'texture'`. This is what `cityparquet_orphans` and
`cityparquet_vacuum` use to decide which sidecar rows are still reachable.

### `cityjson_wkb_geometry_type(blob)`

The WKB geometry type name, which is how you tell a GeoParquet-legal LoD from a
solid one without decoding:

```sql
SELECT DISTINCT cityjson_wkb_geometry_type(geometry_lod0_0)
FROM read_cityjsonseq('test/data/delft_subset.city.jsonl') WHERE geometry_lod0_0 IS NOT NULL;
-- MultiPolygon Z          ← GeoParquet-legal

SELECT DISTINCT cityjson_wkb_geometry_type(geometry_lod2_2)
FROM read_cityjsonseq('test/data/delft_subset.city.jsonl') WHERE geometry_lod2_2 IS NOT NULL;
-- PolyhedralSurface Z     ← solid; excluded from `geo`
```

### `cityjson_shift_appearance_ids(cell, kind, offset)`

Shifts every sidecar id in an appearance cell by a constant. This is the
renumbering primitive `cityparquet_merge` and `insert_cityjson` generate calls to
when folding one package's sidecar ids onto another's numbering; you rarely call
it directly.

### `cityparquet_city_field(city, field)`

Reads one field out of a `city` footer JSON string. Note that a footer which is
**absent** and one that explicitly declares `"crs": null` both come through as
SQL NULL — the package pragmas tell those apart by counting object-table footers
separately, because only the latter is a *stated* unknown.

---

## CityParquet packages

A CityParquet dataset is a *directory* of Parquet files — one object table per
CityGML module, plus optional `materials` / `textures` / `geometry_templates`
sidecars. Loading it into DuckDB gives you queryable tables; **mutating** it is
harder, because the package has relationships ordinary `INSERT` / `UPDATE` /
`DELETE` knows nothing about. Deleting a parent must cascade to its children, and
`feature_id`, `bbox` and the reciprocal `parents` / `children` / `children_roles`
arrays are derived state any structural edit invalidates.

These functions generate that SQL for you.

### The model: a package is a schema

A package becomes a DuckDB **schema** whose tables are named by the spec's file
basenames, plus a `__cityparquet` bookkeeping table. Object tables are
`building`, `bridge`, `tunnel`, `construction`, `transportation`, `vegetation`,
`relief`, `water_body`, `land_use`, `city_furniture`, `generics`; sidecars are
`materials`, `textures`, `geometry_templates`. **Naming is the whole binding** —
there is no registration state to keep in sync.

`__cityparquet` holds one row per package file (`table_name`, `file_name`,
`role`, `city`), where `city` is the file's recovered Parquet footer.

### Two ways to load a package — and why it matters

**Use `cityparquet_read` for an existing package directory.** It loads every file
*and* recovers each one's Parquet footer:

```sql
PRAGMA cityparquet_read('./data/delft', 'delft');
```

```sql
SELECT table_name, role, city IS NOT NULL AS has_footer FROM delft.__cityparquet;
-- building | object | true
```

**A hand-rolled `read_parquet` load is not equivalent.** It gives you the same
rows, but `read_parquet` returns only the data — the Parquet footer is discarded
and cannot be recovered afterwards:

```sql
CREATE SCHEMA delft;
```

```sql
CREATE TABLE delft.building AS SELECT * FROM read_parquet('./data/delft/building.parquet');
```

```sql
PRAGMA cityparquet_init('delft');
```

```sql
SELECT table_name, role, city IS NOT NULL AS has_footer FROM delft.__cityparquet;
-- building | object | false        ← no footer, so no CRS
```

`cityparquet_init` still registers the tables — that part works, and it is the
right call when you are **building** a package from scratch (as when the source
is a CityJSON read rather than an existing package). What it cannot do is invent
a footer that was thrown away. The consequences are concrete:

| | `cityparquet_read` | `read_parquet` + `cityparquet_init` |
| --- | --- | --- |
| Rows | ✅ | ✅ |
| `city` / `geo` footers | ✅ recovered | ❌ lost |
| Declared CRS | ✅ known | ❌ states nothing |
| CRS check on `insert_cityjson` / `cityparquet_merge` | enforced | skipped — a package that states nothing has nothing to check |
| `cityparquet_write` | reuses the package's CRS | needs `crs =>`, or writes an explicit `null` plus a warning |

So the plain load does not fail — it quietly drops to "CRS unknown", and the next
write says so out loud rather than guessing. Pass `crs =>` to `cityparquet_write`
to state it again, or use `cityparquet_read` and keep it throughout.

`cityparquet_init` is idempotent; re-run it after adding a table.

> **Submit these as separate statements.** DuckDB expands *every* pragma in a
> submitted script before running *any* of it, so a generator batched with the
> `CREATE SCHEMA` that precedes it sees a catalog without that schema and fails.

### Adding a CityJSON file

```sql
PRAGMA insert_cityjson('delft', 'tile.city.json');
--   also: insert_cityjsonseq, insert_flatcitybuf
--   named: create_tables = true, tables = ['building', …], lod = '2.2', sample_lines = 100
```

One call. Each object is routed to its **CityGML module** table — `Building` and
`BuildingPart` both to `building`, `Road` and `Square` both to `transportation` —
creating the module tables and sidecars the source needs, renumbering incoming
material / texture / template ids so they cannot collide with existing ones,
rewriting every reference to match, and re-deriving `feature_id`, the reciprocal
hierarchy and `bbox` afterwards.

PRAGMA named parameters use `=`, **not** `:=`:

```sql
PRAGMA insert_cityjson('delft', 'tile.city.json', create_tables = true);
```

Worth knowing:

- **Routing is total.** An object type belonging to no CityGML module is an
  error, not a silently skipped row. Extension types cannot be placed without
  their module declaration — read those with `read_cityjson` and insert yourself.
- **The file is opened twice** — once at plan time to learn its schema and object
  types, once by the generated read. The plan-time pass reads it *whole*, because
  a sample cannot tell you a rare type appears only in the tail.
- **Ids are identity.** An incoming id already in the destination refuses the
  entire insert.
- **The CRS must match**, and reprojection is never performed. The source's
  `metadata.referenceSystem` is resolved to PROJJSON first, so it is compared
  like with like. A package states **one** CRS for every row it holds, so an
  unknown on either side is refused rather than assumed; two unknowns are fine. A
  destination with no footer at all states nothing, so nothing is checked.

### Mutation

```sql
PRAGMA cityparquet_delete('delft', 'object_type = ''Building'' AND b3_h_dak_max > 20');
PRAGMA cityparquet_delete('delft', 'id = ''x''', cascade = false);
PRAGMA cityparquet_delete('delft', 'object_type = ''Road''', tables = ['transportation']);
```

```sql
UPDATE delft.building SET geometry_lod2_2 = … WHERE id = 'x';
PRAGMA cityparquet_reconcile('delft');
PRAGMA cityparquet_reconcile('delft', checks = ['bbox']);
```

There is deliberately **no `cityparquet_update`**. Attribute edits are ordinary
`UPDATE` and need no wrapper; only structural edits — geometry, hierarchy,
appearance — invalidate derived state, and `cityparquet_reconcile` re-derives
exactly that.

`cascade` walks `children` transitively, never `feature_id` equality: a predicate
may match a non-root object, and deleting a `BuildingPart` must not take out the
parent `Building` sharing its `feature_id`.

**Reconciling an already-correct package is a no-op**, `bbox` included. The
reader and `cityparquet_reconcile` agree on the specification's rule that `bbox`
is unioned across every stored LoD *and* across the object's descendants, so a
freshly-read package is already reconciled. `test/sql/cityparquet_reconcile.test`
asserts exactly that — zero changed rows.

### Inspection and housekeeping

```sql
PRAGMA cityparquet_validate('delft');
```

```sql
SELECT * FROM cityparquet_validation WHERE severity = 'error';
```

```sql
PRAGMA cityparquet_orphans('delft');
SELECT * FROM cityparquet_orphan_rows;

PRAGMA cityparquet_vacuum('delft');   -- delete unreferenced sidecar rows
```

`cityparquet_validate` reports `feature_id_null`, `feature_id_dangling`,
`parent_dangling`, `child_dangling`, `children_roles_misaligned` and
`id_duplicate`. Because a PRAGMA cannot be a subquery, both pragmas materialise
findings into a temp table you then select from — so results stay filterable.

### Merging packages

```sql
PRAGMA cityparquet_merge('delft', 'utrecht');
--   named: create_tables = true, tables = ['building', …]
```

Object ids must be unique across the **whole** destination package, not just the
target module — `parents`, `children` and `feature_id` all resolve by bare id
across files — and a collision refuses the entire merge rather than renaming
silently. The CRS rule is the one `insert_cityjson` applies, with both sides now
footers.

Sidecar ids are renumbered onto the destination's numbering and every incoming
reference shifted to match. The offset is `dst_max + 1 − src_min`, not
`dst_max + 1`: a source id may be negative, and adding `dst_max + 1` alone could
land back inside the occupied range. Schema evolution runs before any insert;
derived state is re-derived after.

### The package round trip

Write the schema back out as a package directory:

```sql
SELECT * FROM cityparquet_write('delft', 'out/', crs => 'EPSG:7415');
-- building.parquet | written | 2231 | 4142901
-- metadata.json    | written |    0 |    6721
```

It takes two named parameters: `crs` (below) and `source_format`, which records
the format the data originally came from into each file's `city` footer as
`source_format`.

…and load a package directory back into a fresh schema:

```sql
PRAGMA cityparquet_read('out/', 'loaded');
```

```sql
SELECT table_name, role FROM loaded.__cityparquet;
-- building | object

SELECT COUNT(*) FROM loaded.building;
-- 2231
```

`cityparquet_read` loads each package file into a table and recovers the Parquet
footer into `__cityparquet` — the one thing a hand-rolled `read_parquet` load
throws away. `cityparquet_write` regenerates each file's `city` and `geo` footers
from the data and writes a `metadata.json` STAC Item.

The written package opens as GeoParquet — note that its LoD0 column comes back as
DuckDB's first-class `GEOMETRY` type, so `ST_AsText` reads it directly and
`ST_GeomFromWKB` would fail:

```sql
LOAD spatial;
SELECT ST_AsText(geometry_lod0_0) FROM read_parquet('out/building.parquet')
WHERE geometry_lod0_0 IS NOT NULL LIMIT 1;
-- MULTIPOLYGON Z (((84593.249625 446461.355 0.475…)))
```

Three things worth knowing:

- **`crs =>` is how the CRS reaches the writer** when the package's footer does
  not carry one (as after a hand-rolled load). Omit it and the write still
  succeeds, with every file's `crs` an explicit `null` — the CRS unknown, said
  out loud — plus a warning, and a `metadata.json` declaring no Projection
  extension. A `crs =>` value that cannot be resolved is still an error: that is
  a bad argument, not an unknowable source CRS. Sidecars keep the key absent.
- **`geo` is recomputed, never carried.** GeoParquet legality flips both ways
  under mutation — inserting one `Solid` makes a clean column illegal, deleting
  the last makes it legal again. A stale `geo` declaring a now-solid column makes
  the *whole file* unreadable to Shapely, GeoPandas and DuckDB `spatial`.
- **It sees committed state.** Unlike the pragmas, `cityparquet_write` is a table
  function on an internal connection, because `KV_METADATA` cannot omit a key and
  the `geo`-or-no-`geo` decision depends on the data. Mutate, commit, then write.

`metadata.json` is the **dataset-level** view where the footers are per-file, so
every `city3d:*` field is a union or sum across the package. It carries the
Projection extension (`proj:projjson`, `proj:bbox`) and each asset's `file:size`,
but no per-asset row count. `geometry` stays null: STAC wants EPSG:4326 there and
a package's coordinates are not, so `proj:bbox` carries the real extent.

Atomicity is **per file at best**. A Parquet file that already exists is replaced
via DuckDB's own temp-file + rename, so it flips whole; `metadata.json` is
rewritten in place and is briefly incomplete. Either way the package as a whole
has a window during a write in which it is inconsistent, and concurrent readers
are unsupported. Where genuine cross-file atomicity matters, that is DuckLake's
job.

### Transactions

Each pragma **returns SQL text**, which DuckDB parses and executes in place of
the call. Atomicity is therefore DuckDB's own:

```sql
BEGIN;
PRAGMA cityparquet_delete('delft', 'object_type = ''Building''');
ROLLBACK;   -- undoes the whole cascade, survivor cleanup and re-derivation
```

### Seeing the SQL

Most mutating pragmas have a scalar twin returning the SQL they would run,
without running it. These seven exist:

```sql
SELECT cityparquet_delete_sql('delft', 'id = ''x''');
SELECT cityparquet_reconcile_sql('delft');
SELECT cityparquet_vacuum_sql('delft');
SELECT cityparquet_init_sql('delft');
SELECT cityparquet_validate_sql('delft');
SELECT cityparquet_merge_sql('delft', 'utrecht');
SELECT insert_cityjson_sql('delft', 'tile.city.json');
```

There is no `insert_cityjsonseq_sql` or `insert_flatcitybuf_sql`, and no
`cityparquet_read_sql` — inspect those by running them in a transaction you roll
back instead.

### Batching caveats

DuckDB expands every pragma in a submitted script *before* running any of it, so
each generator sees the catalog and data as they were **before the batch**. The
generated statements are idempotent, but two things a generator cannot do:

- **Preconditions only see pre-batch state.** Two inserts in one submission whose
  files share an object id will not catch each other; only the next
  `cityparquet_validate` will.
- **Cross-file derived state settles on the last reconcile**, covering the tables
  that generator knew about.

---

## Output schema

### Predefined columns

| Column | Type | Description |
| ------ | ---- | ----------- |
| `id` | VARCHAR | CityObject identifier |
| `feature_id` | VARCHAR | Root-family grouping key |
| `object_type` | VARCHAR | CityGML class name (`Building`, `Road`, …) |
| `children` | VARCHAR[] | Child CityObject ids |
| `children_roles` | VARCHAR[] | Roles, positionally aligned with `children` |
| `parents` | VARCHAR[] | Parent CityObject ids |
| `other` | JSON (VARCHAR) | Attributes not mapped to their own column |

Then **dynamic attribute columns** inferred from the data, then the per-LoD
geometry columns, and **last** a `bbox` STRUCT (`min_x … max_z DOUBLE`) in world
coordinates.

`bbox` is **unioned across every stored LoD and across the object's
descendants** — so a parent `Building` whose 3D detail lives on its
`BuildingPart` children still gets a full-height extent, not a flat one. (The
single exception is `lod =>` mode, which has only the one requested LoD to work
from.)

### Geometry columns (CityParquet wide layout)

One group per LoD found in the data, named after the normalised LoD. A suffix
always carries a minor, so `2.0` becomes `geometry_lod2_0`, never `geometry_lod2`:

| Column | Type | Description |
| ------ | ---- | ----------- |
| `geometry_lodX_Y` | BLOB | WKB geometry for that LoD (NULL if absent) |
| `geometry_properties_lodX_Y` | STRUCT | What WKB cannot carry (below) |
| `material_lodX_Y` | JSON (VARCHAR) | Per-surface material map; NULL if none |
| `texture_lodX_Y` | JSON (VARCHAR) | Per-surface texture map; NULL if none |

On the Delft data that yields `geometry_lod0_0`, `geometry_lod1_2`,
`geometry_lod1_3` and `geometry_lod2_2`, each with its three companions.

### `geometry_properties` — the part WKB cannot hold

WKB carries no semantics and no shell structure, so those live in a flattened,
WKB-face-aligned STRUCT whose fixed-shape parts a query engine reads without
parsing JSON:

```text
STRUCT("type" VARCHAR, surfaces VARCHAR, face_semantics INTEGER[], shells INTEGER[][])
```

| Field | Present when | Meaning |
| ----- | ------------ | ------- |
| `type` | always | CityJSON geometry type (`"Solid"`, `"MultiSurface"`, …) |
| `surfaces` | source has semantics | The CityJSON `surfaces` array verbatim as JSON text |
| `face_semantics` | source has semantics | One entry per WKB face, in WKB face order — the index of that face's surface in `surfaces`, or NULL |
| `shells` | solid-family geometry | Per-solid, then per-shell face counts — always two levels deep, so a lone `Solid` is `[[12, 4]]` |

There is no `lod` field: the level of detail rides the column name.

```sql
SELECT id, geometry_properties_lod2_2.type AS geom_type,
       geometry_properties_lod2_2.shells AS shells,
       len(geometry_properties_lod2_2.face_semantics) AS n_faces
FROM read_cityjsonseq('https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl')
WHERE object_type = 'BuildingPart' AND geometry_lod2_2 IS NOT NULL LIMIT 3;
-- NL.IMBAG.Pand.0503100000012869-0 | Solid | [[6]]  |  6
-- NL.IMBAG.Pand.0503100000016459-0 | Solid | [[6]]  |  6
-- NL.IMBAG.Pand.0503100000005156-0 | Solid | [[21]] | 21
```

Because `face_semantics` is a native `INTEGER[]`, surface-level analysis is a
positional filter a columnar engine can evaluate rather than a JSON parse:

```sql
SELECT id, len(list_filter(geometry_properties_lod2_2.face_semantics, lambda i: i = 1)) AS roof_faces
FROM read_cityjsonseq('https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl')
WHERE object_type = 'BuildingPart' AND geometry_lod2_2 IS NOT NULL LIMIT 3;
-- NL.IMBAG.Pand.0503100000012869-0 | 1
-- NL.IMBAG.Pand.0503100000016459-0 | 1
-- NL.IMBAG.Pand.0503100000005156-0 | 4
```

`len(face_semantics)` always equals the total of `shells` (the WKB face count).
This is also what [`duckdb-3d`](https://github.com/HideBa/duckdb-3d) reads from
`shells` to compute the volume of a solid with inner shells.

### Filter pushdown

Equality filters on `id`, `feature_id` and `object_type` are pushed into the
scan, so non-matching CityObjects are skipped before materialisation:

```sql
SELECT id FROM read_cityjsonseq('https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl')
WHERE object_type = 'Building';
```

Other predicates still work — DuckDB applies them after the scan. Projection
pushdown is always on: unprojected columns are never built.
