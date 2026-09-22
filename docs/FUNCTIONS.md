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
- [Mesh interchange](#mesh-interchange) — `read_obj`, `obj_materials`, `obj_textures`, `obj_metadata`, `COPY … TO (FORMAT obj)`, `COPY … TO (FORMAT gltf/glb)`
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

`read_flatcitybuf` shares only `lod` and `sample_lines`; it takes no
`appearance`, and adds the four bbox bounds instead.

#### `lod =>` — one LoD, same column grammar

```sql
SELECT id, cityjson_wkb_extent(geometry_lod2_2).zmax AS ridge_height
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
    xmin => 84900, ymin => 446200, xmax => 85200, ymax => 446500);
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

A `read_obj` source is discovered the same way: its `.mtl` materials and
textures, and the `vt` UV pool they reference, travel into the written
`appearance` block just as a CityJSON(Seq) source's would.

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

Everything else is written as a CityJSON attribute, with one exception: `bbox`,
`other`, `address` and `template` are **recognised but not round-tripped** — a
writer neither serialises their value into the output CityJSON nor declares them
as attribute columns. `bbox` is derived and recomputed on read, so writing it
back would be redundant; `other`, `address` and `template` are not written yet
(a future writer would reassemble `other`'s members onto the CityObject, and
`address`/`template` into their respective CityJSON members, rather than
flattening any of the three into `attributes`).

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

### `cityjson_geoparquet_geo(path)`

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

A geometry column that DuckDB has decoded to its native `GEOMETRY` type — which
`enable_geoparquet_conversion` does automatically for any column named in a
source file's `geo` footer key, whether or not the `spatial` extension is
installed or loaded — carries its own writer hook: `COPY ... TO ... (FORMAT
PARQUET)` over such a column stamps its own `geo` entry into `KV_METADATA`
alongside any `geo` key given explicitly above. Parquet allows duplicate keys
silently, so the file ends up with two, and a reader that picks the wrong one
gets a footer this COPY never intended. Convert the column back to WKB first
(`ST_AsWKB(col)`, in DuckDB core, no extension needed) before a `COPY` that also
supplies its own `geo` key.

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
-- GMLID_855011_330784_753 | {visual=[2, 2, 2, 2, 2, 2, 3, 3, 3, …]}
```

`appearance` accepts `'local'` (default) or `'sidecar'`, on `read_cityjson` and
`read_cityjsonseq`. Both modes emit the same typed `material_lod*` /
`texture_lod*` `MAP` cells (below); the mode decides only the **id space** —
feature-local in `'local'`, dataset-global sidecar ids in `'sidecar'` — and
texture UVs are inlined in both. **`read_flatcitybuf` does not take it** —
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

**Texture UVs are inlined.** A source ring is `[texId, uvIdx, uvIdx, …]`; every
`texture_lod*` cell replaces that with one `STRUCT(id BIGINT, uv DOUBLE[][])`
per ring — `id` the ring's (local or sidecar) texture id, `uv` one `[u, v]`
pair per ring vertex, and an untextured ring holds `{NULL, NULL}`. Both
`'local'` and `'sidecar'` mode build this same struct; only `id`'s numbering
differs. The builder walks the same per-type face nesting `face_semantics` does
(`FlattenPerFace`) — a fixed depth per geometry type, not a recursion to whatever
depth a geometry happens to nest.

### Reading appearance cells

Both columns are ordinary DuckDB `MAP`s, so `map['theme']` is the way in — it
returns the theme's per-WKB-face list, indexed by the same face order
`geometry_lod*`'s WKB emits:

```sql
SELECT material_lod2_2['visual'][3]
FROM read_cityjson('test/data/solid_material.city.json', lod := '2.2');
-- 1
```

A texture cell nests one level further — face, then ring — so index twice
before reaching the `STRUCT`'s fields:

```sql
SELECT texture_lod2_2['visual'][1][1].uv
FROM read_cityjson('test/data/solid_texture.city.json', lod := '2.2');
-- [[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0]]
```

`UNNEST` turns a theme's face list back into rows, one per WKB face:

```sql
SELECT id, UNNEST(material_lod3_0['visual']) AS face_material_id
FROM (
    SELECT id, material_lod3_0
    FROM read_cityjsonseq('test/data/railway_appearance.city.jsonl', appearance := 'sidecar')
    WHERE material_lod3_0 IS NOT NULL LIMIT 1
)
LIMIT 5;
-- GMLID_855011_330784_753 | 2
-- GMLID_855011_330784_753 | 2
-- GMLID_855011_330784_753 | 2
-- GMLID_855011_330784_753 | 2
-- GMLID_855011_330784_753 | 2
```

---

## Scalar helpers

### `cityjson_wkb_extent(blob)`

3D extent of a WKB blob, **solid family included** — DuckDB `spatial` rejects
`PolyhedralSurface Z`, which is what every CityParquet solid LoD is.

```sql
SELECT cityjson_wkb_extent(geometry_lod2_2)
FROM read_cityjsonseq('https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl')
WHERE geometry_lod2_2 IS NOT NULL LIMIT 1;
--> STRUCT(xmin, ymin, zmin, xmax, ymax, zmax DOUBLE)
```

### `cityjson_appearance_ids(cell)`

The sidecar ids an appearance cell references, as a list.

```sql
SELECT cityjson_appearance_ids(material_lod3_0) AS ids
FROM read_cityjsonseq('test/data/railway_appearance.city.jsonl', appearance := 'sidecar')
WHERE material_lod3_0 IS NOT NULL LIMIT 1;
-- [2, 3]
```

There is no separate `kind` argument: `cell`'s exact type — the material cell's
`MAP(VARCHAR, BIGINT[])` or the texture cell's `MAP(VARCHAR, STRUCT(id BIGINT,
uv DOUBLE[][])[][])` — is what the bind checks to tell the two apart, and
anything else is a bind error. This is what `cityparquet_orphans` and
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

### `cityjson_shift_appearance_ids(cell, offset)`

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

**Reconciling an already-correct package is a no-op for `feature_id`, hierarchy,
and, in general, `bbox`.** Both the reader and `cityparquet_reconcile` union a
row's own geometry across every stored LoD *and* across its descendants, so a
freshly-read package's structural columns are already reconciled. `bbox` is the
one exception: the reader additionally unions in the source's declared
per-object extent (never substituting it, since a declared extent is not
guaranteed to contain its geometry), but `cityparquet_reconcile` has no declared
extent to consult -- only stored geometry -- and must be able to shrink a stale
`bbox` after an in-place geometry `UPDATE`. So reconciling a package read from a
source with wider declared extents narrows those rows' `bbox`; the invariant
that holds universally is containment (reconcile's result is never wider than
the reader's), not identity. `test/sql/cityparquet_reconcile.test` asserts both:
zero changed rows for the structural columns, and zero containment violations
for `bbox`.

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

It takes three named parameters: `crs` (below); `source_format`, which records
the format the data originally came from into each file's `city` footer as
`source_format`; and `bloom` (default `true`), which writes Parquet bloom
filters on the object tables. DuckDB writes a filter only for a
dictionary-encoded column chunk, and its dictionary and bloom options apply to a
whole file, so each object table is written with 122 880-row row groups and a
dictionary cut-off of the same size under an 8 MiB dictionary-page cap: every
column chunk whose distinct values fit under that cap is dictionary-encoded and
carries a filter (FPP 0.01), placed after the last row group — `id` and
`feature_id` among them.

Which other columns qualify is conditional on that cap, not a fixed list. Over a
full row group the cap works out at roughly 68 bytes per row for a near-unique
column, so a high-cardinality string wider than that is written PLAIN and
carries no filter, while a compact WKB geometry or JSON column can stay under
the cap and carry one — in any row group, not only a short trailing one. In
practice the filtered set is wider than the string columns: on the delft package
(2 231 rows, one row group) 68 of 115 column chunks carry a filter, numeric and
temporal attributes, the `bbox` leaves, list elements and the geometry columns
included. That is an observation on the packages measured, not a guarantee about
any input. Sidecars carry no filter; `bloom => false` writes none at all. DuckDB
itself consults the filters for `=` and `IN` predicates, including those pushed
down from a join.

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

## Mesh interchange

### `read_obj(path, lod := …)`

Reads a Wavefront OBJ into the object-table schema. An OBJ carries no level of
detail, class, or CRS, so the caller supplies them: `lod` is required and names
the geometry columns; `object_type` (default `'Building'`) is every object's
class; the CRS lives on `obj_metadata` (below), because a row has nowhere to
carry it.

```sql
SELECT id, object_type, geometry_properties_lod2_2.type AS geom, geometry_properties_lod2_2.surfaces
FROM read_obj('test/data/obj/cube.obj', lod := '2.2');
-- cube | Building | Solid        | [{"type":"GroundSurface"},{"type":"WallSurface"},{"type":"RoofSurface"}]
-- slab | Building | MultiSurface | [{"type":"RoofSurface"}]
```

| Parameter | Type | Description |
| --- | --- | --- |
| `lod` | VARCHAR | **Required.** Normalised like every reader: `'2'` yields `geometry_lod2_0` |
| `object_type` | VARCHAR | CityGML class for every object (default `Building`) |
| `geometry_type` | VARCHAR | `'auto'` (default): `Solid` when every edge is shared by exactly two faces, else `MultiSurface`; or force `'Solid'` / `'MultiSurface'` |
| `appearance` | VARCHAR | `'local'` (default) or `'sidecar'`, as on the other readers |
| `sample_lines` | BIGINT | Accepted for uniformity; the whole file is parsed regardless |

Mapping:

- Each `o` is one object; its name is `id` and `feature_id`, taken as the rest of
  the line with surrounding whitespace trimmed (as `usemtl` and `newmtl` names
  are). A file without `o` is one object named after the file stem. A repeated
  `o` name resumes that object.
- All faces of an object form one geometry, one outer ring each, in file order
  and with the file's winding. OBJ cannot express holes.
- A `usemtl` name that is a CityJSON semantic surface type (`RoofSurface`,
  `WallSurface`, `GroundSurface`, …, or any `+`-prefixed extension name) is the
  face's surface; failing that, the `g` name under the same rule. `usemtl`/`g`
  state persists across `o`, as OBJ specifies.
- `.mtl` materials are the appearance definitions in `mtllib` order
  (`obj_materials`); a material with `map_Kd` is also a texture
  (`obj_textures`), and faces under it with `v/vt` indices get a `texture_lod*`
  ring whose `vt` index is resolved against the file's `vt` list and inlined as
  a `[u, v]` pair — the same for either `appearance` mode. A face with no UV
  carries an untextured ring (NULL `id`, NULL `uv`). A `usemtl` naming a
  material no `mtllib` declared leaves the face without one, and warns in
  `duckdb_logs` once per name.
- Positive and negative (relative) indices; `v` with a fourth component; `vn`
  ignored. A line ending in `\` continues on the next one. A `mtllib` naming
  several files loads every one of them, in order. A missing `.mtl`, or one that
  declares no material, is a warning in `duckdb_logs` and the geometry still
  reads.
- Coordinates are taken as written, Z-up, no axis swap — what cjio, 3dfier and
  geoflow write. A `# origin x y z` header comment, as the OBJ writer emits, is
  added back.

### `obj_materials(path)` / `obj_textures(path)`

The `.mtl` as `materials.parquet` / `textures.parquet` rows, same columns as
`cityjson_materials` / `cityjson_textures`. `Kd`→`diffuseColor`, `Ks`→`specularColor`,
`Ke`→`emissiveColor`, mean `Ka`→`ambientIntensity`, `1 − d` — or `Tr` as written, in a
block that has no `d` — →`transparency`, `Ns / 1000`→`shininess`; every other directive
(`illum`, `Ni`, `map_Ks`, `map_bump`, …) into `other`, key and rest of line verbatim.
`map_Kd`→`image_uri` with `image_type` from the extension, `wrapMode` `wrap`,
`textureType` `unknown`, `image_data` NULL; texture options before the file name
(`-s`, `-o`, …) are dropped.

A directive the block does not state is **NULL**, not a default: a material declaring
only `Kd` says nothing about its specular colour, and reports nothing.

```sql
SELECT id, name, diffuseColor, transparency, shininess, other
FROM obj_materials('test/data/obj/cube.obj');
-- 0 | GroundSurface | [0.3, 0.3, 0.3]   | NULL                | NULL   | NULL
-- 1 | brick         | [0.7, 0.3, 0.2]   | 0.30000000000000004 | 0.0007 | {"illum":"2","map_Ks":"spec.png"}
-- 2 | RoofSurface   | [0.9, 0.06, 0.09] | 0.3                 | NULL   | NULL

SELECT id, image_uri, image_type, wrapMode, textureType
FROM obj_textures('test/data/obj/cube.obj');
-- 0 | brick.png | PNG | wrap | unknown
```

### `obj_metadata(path [, crs := …])`

One row in `cityjson_metadata`'s shape: `geographical_extent` from every vertex,
`reference_system` from `crs` (`EPSG:7415`, the URN or the OGC URL; anything else
is an error), everything else NULL. `features_count` equals `city_objects_count`
— one `o` group is one feature.

```sql
SELECT reference_system, city_objects_count, features_count
FROM obj_metadata('test/data/obj/cube.obj', crs := 'EPSG:7415');
-- {'base_url': 'https://www.opengis.net/def/crs/', 'authority': EPSG, 'version': 0, 'code': 7415} | 2 | 2
```

This is where the CRS of an OBJ enters the stack: a `read_obj` source's
materials and textures already travel automatically as a `COPY` source (see
[Writing](#writing)), but its CRS exists nowhere in the file, so
`metadata_query` must supply it from `obj_metadata`:

```sql
COPY (SELECT * FROM read_obj('test/data/obj/cube.obj', lod := '2.2')) TO 'cube_out.city.jsonl'
(FORMAT cityjsonseq,
 metadata_query 'SELECT reference_system AS crs FROM obj_metadata(''test/data/obj/cube.obj'', crs := ''EPSG:7415'')');

SELECT reference_system.code FROM cityjsonseq_metadata('cube_out.city.jsonl');
-- 7415
```

### `COPY … TO 'x.obj' (FORMAT obj)`

Writes any relation the CityJSON writers accept (`id`, `feature_id`,
`object_type`, `geometry_lod*` and companions) as Wavefront OBJ, following what
cjio, 3dfier and geoflow write: one `o` per object, `g` per semantic surface,
`usemtl` per material, Z-up, no axis swap.

```sql
COPY (SELECT * FROM read_cityjson('test/data/holed_face.city.json', lod := '2.2'))
TO 'holed.obj' (FORMAT obj, lod '2.2');
```

```text
# Written by duckdb-cityjson
# crs https://www.opengis.net/def/crs/EPSG/0/7415
# origin 84500 446300 0
mtllib holed.mtl
o courtyard
v 0 0 10
v 4 0 10
v 4 4 10
v 0 4 10
v 1 1 10
v 1 3 10
v 3 3 10
v 3 1 10
v 0 0 0
v 4 0 0
g RoofSurface
usemtl RoofSurface
f 1 5 6
f 8 5 1
f 4 1 6
f 8 1 2
f 3 4 6
f 7 8 2
f 3 6 7
f 7 2 3
g WallSurface
usemtl WallSurface
f 9 10 2 1
```

The roof has a hole, which OBJ cannot express, so it is triangulated by earcut —
eight 3-vertex faces rather than one 8-vertex n-gon. (Which eight triangles
earcut picks is an implementation detail; the invariant is the count.)

Every `o` block opens with its own `g` and `usemtl` lines, even when the object
carries only one surface: OBJ lets `usemtl`/`g` persist across an `o` with
nothing to say (`read_obj`, above, relies on exactly that), so this writer
restates both on every object rather than counting on a reader to carry state
forward correctly. A face with no semantic surface is in group `default`:

```sql
COPY (SELECT * FROM read_cityjson('test/data/minimal.city.json', lod := '2.2'))
TO 'minimal.obj' (FORMAT obj);
```

```text
# Written by duckdb-cityjson
mtllib minimal.mtl
o building1
v 0 0 0
v 10 0 0
v 10 10 0
v 0 10 0
g default
usemtl Building
f 1 2 3 4
```

| Option | Default | Meaning |
| --- | --- | --- |
| `lod` | highest per object | Which `geometry_lodX_Y` to export; objects with nothing there are skipped (a warning, not an error — see below) |
| `origin` | `'auto'` | Subtract the extent's minimum corner (`'auto'`), nothing (`'none'`), or `'x,y,z'`. Recorded as `# origin x y z`, which `read_obj` adds back |
| `triangulate` | `false` | Hole-free faces stay n-gons; faces with holes are always triangulated (OBJ cannot express holes) |
| `precision` | `17` | Significant digits; 17 is the shortest text that round-trips |
| `materials_query`, `textures_query` | none | SQL returning `materials.parquet` / `textures.parquet`-shaped rows. **Their presence declares the cells to be sidecar-form.** Absent, refs are local-form and resolve against the discovered source or `metadata_from`. The missing-form refusal (below) only fires for a **discovered** source — `COPY my_table TO … (FORMAT obj)` never sees the `read_cityjson[seq](…, appearance := 'sidecar')` call that produced `my_table`, so the cells silently resolve as local-form instead of being refused |
| `metadata_from` | discovered | As for the CityJSON writers |

Three things the table cannot say in a cell:

- **`materials_query` and `textures_query` see committed state.** Each runs on a fresh
  connection, as `metadata_query` does, so a table created in an open transaction that
  has not been committed is not visible to them:

  ```sql
  BEGIN;
  CREATE TABLE mats AS SELECT 0 AS id, 'brick' AS name;
  COPY (SELECT * FROM read_obj('test/data/obj/cube.obj', lod := '2.2', appearance := 'sidecar'))
  TO 'tx.obj' (FORMAT obj, materials_query 'SELECT * FROM mats');
  -- Binder Error: materials_query failed: Catalog Error: Table with name mats does not exist!
  ```

- **An `o` name is one OBJ token.** Whitespace in an id is replaced by `_`, so an id
  carrying a space does not round-trip through `read_obj`.
- **One theme is written.** A material or texture cell may carry several themes
  (`{visual=…, winter=…}`); the mesh writers take the alphabetically first.

Remote output paths are not supported for mesh formats: the `.obj`, `.mtl` and any
copied images are written with local file streams, never through DuckDB's own
filesystem abstraction, so `COPY … TO 's3://…/x.obj'` fails opening the output
rather than reaching object storage:

```sql
COPY (SELECT * FROM read_cityjson('test/data/holed_face.city.json', lod := '2.2'))
TO 's3://nonexistent-bucket-cityjson-test/x.obj' (FORMAT obj);
-- Invalid Error: Failed to open output file: s3://nonexistent-bucket-cityjson-test/x.obj
```

A `.mtl` named after the OBJ is written beside it: `Kd`/`Ks`/`Ke`/`d`/`Ns` from the
CityJSON material, `map_Kd` when the face carries a texture whose bytes could be
found (`image_data`, else `image_uri` relative to the source), copied beside the OBJ
under its own basename. A textured face gets its own entry (`brick__tex0`) because
OBJ ties images to materials — `test/data/obj/cube.obj`'s `brick` material becomes
two `.mtl` entries once a texture is supplied for it:

```sql
CREATE TABLE tex_rows AS
    SELECT * FROM read_obj('test/data/obj/cube.obj', lod := '2.2', appearance := 'sidecar');
CREATE TABLE tex_defs AS
    SELECT id, image_uri, '\x89PNG\x0D\x0A\x1A\x0A'::BLOB AS image_data,
           image_type, wrapMode, textureType, borderColor, other
    FROM obj_textures('test/data/obj/cube.obj');

COPY (SELECT * FROM tex_rows) TO 'textured.obj'
(FORMAT obj, materials_query 'SELECT * FROM obj_materials(''test/data/obj/cube.obj'')',
             textures_query 'SELECT * FROM tex_defs');
```

```text
newmtl GroundSurface
Kd 0.3 0.3 0.3
d 1

newmtl brick__tex0
Kd 0.7 0.3 0.2
Ks 0.1 0.1 0.1
d 0.7
Ns 0.7
map_Kd brick.png

newmtl RoofSurface
Kd 0.9 0.06 0.09
d 0.7
```

The `.obj`'s faces carry `v/vt` pairs wherever a texture is resolved, plain `v`
otherwise (the first face is untextured, the next two are):

```text
f 1 2 3 4
f 1/1 4/2 5/3 6/4
f 4/1 3/2 7/3 5/4
…
```

Faces without a material are named after their semantic surface type, else their
class, with a fixed palette. `.mtl` entries are keyed by **identity**, not by the
rendered name string, so two distinct identities that would render to the same
label — two materials sharing a name, or a material and a default-coloured face
sharing one — get distinct entries, and the later one is suffixed: `_<id>` for a
colliding CityJSON material, `_default` for a colliding default-colour entry.
`duplicate_material_name.city.json` has two materials both named `brick`, ids `0`
and `1`, one on each face of a `MultiSurface`:

```sql
COPY (SELECT * FROM read_cityjson('test/data/duplicate_material_name.city.json', lod := '2.2'))
TO 'twins.obj' (FORMAT obj);
```

`twins.mtl`:

```text
# Written by duckdb-cityjson
newmtl brick
Kd 0.8 0.3 0.2
d 1

newmtl brick_1
Kd 0.2 0.3 0.8
d 1
```

and the faces of `twins.obj`:

```text
g default
usemtl brick
f 1 2 3 4
usemtl brick_1
f 5 6 7 8
```

A texture is part of an entry's identity only once its image has been found. A
face whose texture could not be loaded keeps its material colour and writes no
`map_Kd` — exactly what a face of the same material carrying no texture writes —
so the two share one entry rather than being written twice under two names.
`_default` is therefore reached only through a genuine name clash: a
default-coloured face whose label a material has already claimed, or a semantic
surface type that reads the same as an object class.

A material or texture cell is always **flat, one entry per WKB face**,
regardless of how deeply the source geometry nests — a `Solid`'s per-shell
`values` are resolved against its shells before the cell is built, so the mesh
writer indexes the cell directly by face position rather than walking shells:

```sql
SELECT material_lod2_2 FROM read_cityjson('test/data/solid_material.city.json', lod := '2.2');
-- {visual=[0, 1, 1, 1, 1, 2]}
```

Which **form** — local or sidecar — a cell is in is a separate question, and
cannot be told from the cell itself (a `MAP(VARCHAR, BIGINT[])` either way
regardless of form). So a source that was read with `appearance := 'sidecar'`
and no `*_query` is refused rather than guessed:

```sql
COPY (SELECT * FROM read_cityjsonseq('test/data/railway_appearance.city.jsonl', appearance := 'sidecar'))
TO 'refused.obj' (FORMAT obj, lod '3');
-- Binder Error: COPY TO obj: the source was read with appearance := 'sidecar', so its
-- material/texture cells hold sidecar ids; pass materials_query / textures_query
-- (e.g. materials_query 'SELECT * FROM cityjson_materials(''test/data/railway_appearance.city.jsonl'')')
-- so they can be resolved

COPY (SELECT * FROM read_cityjsonseq('test/data/railway_appearance.city.jsonl', appearance := 'sidecar'))
TO 'accepted.obj' (FORMAT obj, lod '3',
    materials_query 'SELECT * FROM cityjson_materials(''test/data/railway_appearance.city.jsonl'')');

SELECT line FROM (SELECT UNNEST(string_split(content, chr(10))) AS line FROM read_text('accepted.mtl'))
WHERE line LIKE 'newmtl%';
-- newmtl Bridge
-- newmtl UUID_1c68ae93-720e-4b72-a46e-326b65a3fd6b
-- newmtl UUID_0794715b-1334-4855-83d3-8e8270c11e78
```

A texture whose bytes cannot be found is not fatal — the face keeps its material
colour, no `map_Kd` is written, and a warning is logged. `railway_appearance.city.jsonl`
references two images that are not on disk beside the fixture, so this is a live
example rather than a contrived one:

```sql
COPY (SELECT * FROM read_cityjsonseq('test/data/railway_appearance.city.jsonl'))
TO 'railway.obj' (FORMAT obj, lod '3');
-- WARNING: cityjson: texture 2: could not read 'test/data/appearances/Bruecke-Schotter.jpg':
--          Failed to open file: test/data/appearances/Bruecke-Schotter.jpg; faces fall back to the material colour
-- WARNING: cityjson: texture 3: could not read 'test/data/appearances/Bruecke-Beton.jpg':
--          Failed to open file: test/data/appearances/Bruecke-Beton.jpg; faces fall back to the material colour

SELECT COUNT(*) FROM (SELECT UNNEST(string_split(content, chr(10))) AS line FROM read_text('railway.mtl'))
WHERE line LIKE 'map_Kd%';
-- 0
```

Objects the writer skips — no geometry, nothing at the requested `lod`, missing
`boundaries`, a degenerate ring, or the legacy `geom_lod*` STRUCT layout whose
`boundaries` are vertex indices rather than `[x,y,z]` coordinates — produce a
warning in `duckdb_logs`, not an error, so one malformed object does not fail the
whole COPY:

```sql
SET enable_logging = true;
SET logging_storage = 'memory';

COPY (SELECT * FROM read_cityjsonseq('test/data/delft_subset.city.jsonl'))
TO 'delft_lod12.obj' (FORMAT obj, lod '1.2');

SELECT message FROM duckdb_logs() WHERE message LIKE 'cityjson:%' LIMIT 1;
-- cityjson: object NL.IMBAG.Pand.0503100000012869: no geometry at lod '1.2'
```

### `COPY … TO 'x.glb' (FORMAT glb)` / `'x.gltf' (FORMAT gltf)`

glTF 2.0: one node and one mesh per object, the node named after the id, one
primitive per material or semantic-surface group actually used, float32
positions relative to `origin`, a root node whose matrix turns the Z-up data
into glTF's Y-up without touching the vertices — what cjio and py3dtiles do,
and what the 3D Tiles specification advises for a Z-up source.

```sql
COPY (SELECT * FROM read_cityjson('test/data/holed_face.city.json', lod := '2.2'))
TO 'holed.gltf' (FORMAT gltf, attributes true);
```

tinygltf pretty-prints the `.gltf` as JSON, so it can be inspected directly
(output below is from this command against the file just written):

```sh
python3 -c "
import json
d = json.load(open('holed.gltf'))
print(json.dumps(d['asset']))
print(json.dumps(d['scenes']))
print(json.dumps(d['nodes']))
print(len(d['meshes'][0]['primitives']))
print(json.dumps(d['materials']))
print(json.dumps(d['buffers']))
"
```

```text
{"extras": {"crs": "https://www.opengis.net/def/crs/EPSG/0/7415", "origin": [84500.0, 446300.0, 0.0], "units": "m", "up": "z"}, "generator": "duckdb-cityjson", "version": "2.0"}
[{"nodes": [0]}]
[{"children": [1], "matrix": [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, -1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0], "name": "root"}, {"extras": {"lod": "2.2", "name": "ring", "object_type": "Building"}, "mesh": 0, "name": "courtyard"}]
2
[{"doubleSided": true, "name": "RoofSurface", "pbrMetallicRoughness": {"baseColorFactor": [0.9, 0.06, 0.09, 1.0], "metallicFactor": 0.0}}, {"doubleSided": true, "name": "WallSurface", "pbrMetallicRoughness": {"baseColorFactor": [0.8, 0.8, 0.8, 1.0], "metallicFactor": 0.0}}]
[{"byteLength": 204, "uri": "holed.bin"}]
```

The scene has one root, the root's matrix is the Z-up-to-Y-up rotation, its
one child is the `courtyard` object's node — the node's own `name` is the
object id, `courtyard`; the `name` inside its `extras` is instead that
object's own `name` *attribute*, `"ring"`, carried there because `attributes
true` puts every attribute column on the node, and this fixture happens to
have one called `name`. The roof's hole (which glTF cannot express any more
than OBJ can) is triangulated into the same two primitives — one per semantic
surface — that the `obj` writer's own example above produces from the same
fixture. `roughnessFactor`, `alphaMode` and a `[1,1,1,1]` `baseColorFactor`
never appear above: tinygltf omits a value equal to its schema default
(`roughnessFactor 1`, `alphaMode OPAQUE`, an opaque white `baseColorFactor`)
rather than writing it out.

Against a source with no CRS, `asset.extras.crs` is **absent**, not `null` —
tinygltf drops a `null`-typed `extras` member entirely, which is what "absent"
should mean for a value nothing states:

```sql
COPY (SELECT * FROM read_cityjson('test/data/minimal.city.json', lod := '2.2'))
TO 'minimal.gltf' (FORMAT gltf);
```

```sh
python3 -c "import json; print(json.dumps(json.load(open('minimal.gltf'))['asset']))"
```

```text
{"extras": {"origin": [0.0, 0.0, 0.0], "units": "m", "up": "z"}, "generator": "duckdb-cityjson", "version": "2.0"}
```

| Option | Default | Meaning |
| --- | --- | --- |
| `lod` | highest per object | As for `obj` |
| `origin` | `'auto'` | As for `obj`, but there is no `# origin` header to read back from — `asset.extras.origin` is the record, in double precision. `origin 'none'` writes projected-magnitude coordinates straight into float32, where one ULP at 1e5 is 8 mm; subtracting an origin leaves magnitudes around 1e3, where it is 0.1 mm |
| `materials_query`, `textures_query` | none | As for `obj` |
| `metadata_from` | discovered | As for `obj` |
| `attributes` | `false` | Every attribute column of the row, with `object_type` and `lod`, into the node's `extras` |

`asset.extras` always carries `{"origin", "up": "z", "units": "m"}`, plus
`"crs"` when the source states one, so a 3D Tiles packager can put the origin
into a tile `transform` in double precision rather than the float32 the mesh
itself is written in.

How an attribute reaches `extras` depends on which of the two ways it arrived
by. An attribute **column** whose value is not a plain scalar comes
through as a **string**: a `LIST` or `STRUCT` column is rendered as DuckDB's
own display text, not as JSON. An entry of the **`other`** column, in
contrast, is parsed JSON by the time the writer sees it — the sink parses that
cell and merges its members in as attributes — so it keeps whatever shape it
had, nested objects and mixed-type arrays included:

```sql
COPY (SELECT * REPLACE ('{"nested": {"a": [1, 2.5, "s"]}, "empty_list": [], "empty_obj": {}, "nil": null}' AS other),
             ['a', 'b']::VARCHAR[] AS tags
      FROM read_cityjson('test/data/minimal.city.json', lod := '2.2'))
TO 'tagged.gltf' (FORMAT gltf, attributes true);
```

```sh
python3 -c "import json; print(json.dumps(json.load(open('tagged.gltf'))['nodes'][1]['extras']))"
```

```text
{"empty_list": null, "empty_obj": null, "function": "residential", "lod": "2.2", "measuredHeight": 15.5, "nested": {"a": [1, 2.5, "s"]}, "object_type": "Building", "tags": "[a, b]", "yearOfConstruction": 2020}
```

`"tags": "[a, b]"` is the column path: it is not valid JSON inside a JSON
document, and a consumer that wants a real array back has to parse it itself.
`"nested"` is the `other` path, and arrives as the object it is.

Two shapes tinygltf will not carry through: an **empty** array or object
becomes `null` (`"empty_list"`, `"empty_obj"` above), and a member whose value
is JSON `null` is **dropped** entirely (`"nil"` above is absent, and so is
`asset.extras.crs` for a source with no CRS). The two stay distinguishable —
an empty container is a member valued `null`, a `null` is no member at all —
but neither arrives as what the source said.

Materials map `diffuseColor` to `baseColorFactor` (alpha `1 − transparency`,
`alphaMode` `BLEND` once transparency is non-zero, else the omitted default
`OPAQUE`), `metallicFactor 0`, an implied `roughnessFactor 1`, `doubleSided
true`. A glTF material is keyed by **identity** — the CityJSON material (or,
absent one, the semantic surface or object class) plus whether it is textured
— never by the rendered `name`, and unlike a `.mtl` entry a glTF material name
need not be unique: `duplicate_material_name.city.json` (`obj`'s own example
above, two materials both named `brick`) gets two glTF materials named
`brick` too, with **no** `_1` suffix, because nothing here needs one:

```sql
COPY (SELECT * FROM read_cityjson('test/data/duplicate_material_name.city.json', lod := '2.2'))
TO 'twins.gltf' (FORMAT gltf);
```

```sh
python3 -c "
import json
d = json.load(open('twins.gltf'))
for m in d['materials']:
    print(m['name'], m['pbrMetallicRoughness']['baseColorFactor'])
"
```

```text
brick [0.8, 0.3, 0.2, 1.0]
brick [0.2, 0.3, 0.8, 1.0]
```

A material's transparency maps to alpha and `alphaMode`: `solid_material.city.json`
carries no transparent material of its own, so `materials_query` overrides one
material's `transparency` to demonstrate it (`red` alone gets `alphaMode
BLEND`; the schema default `OPAQUE` stays omitted for the other two):

```sql
COPY (SELECT * FROM read_cityjson('test/data/solid_material.city.json', lod := '2.2'))
TO 'solid_mat.gltf'
(FORMAT gltf,
 materials_query 'SELECT id, name, ambientIntensity, diffuseColor, specularColor, emissiveColor,
   CASE WHEN name = ''red'' THEN 0.4 ELSE transparency END AS transparency,
   shininess, isSmooth, other FROM cityjson_materials(''test/data/solid_material.city.json'')');
```

```sh
python3 -c "
import json
d = json.load(open('solid_mat.gltf'))
for m in d['materials']:
    print(m['name'], m['pbrMetallicRoughness']['baseColorFactor'], m.get('alphaMode'))
"
```

```text
red [1.0, 0.0, 0.0, 0.6] BLEND
grey [0.5, 0.5, 0.5, 1.0] None
dark [0.1, 0.1, 0.1, 1.0] None
```

Textures use `TEXCOORD_0` with the V axis flipped to glTF's top-left origin.
A face is emitted **per corner** when it is textured — a vertex shared
between two faces, or between two ring positions of the same face, is
duplicated wherever its UV differs, since the UV is a per-corner attribute
glTF has no way to share the way it shares `POSITION`; an untextured face
shares vertices with the rest of its primitive by vertex index, same as `obj`
shares them by `v` index:

```sql
CREATE TABLE tex_rows AS SELECT * FROM read_obj('test/data/obj/cube.obj', lod := '2.2', appearance := 'sidecar');
CREATE TABLE tex_defs AS
SELECT id, image_uri, '\x89PNG\x0D\x0A\x1A\x0A'::BLOB AS image_data, image_type, wrapMode, textureType, borderColor, other
FROM obj_textures('test/data/obj/cube.obj');

COPY (SELECT * FROM tex_rows) TO 'cube_tex.gltf'
(FORMAT gltf, materials_query 'SELECT * FROM obj_materials(''test/data/obj/cube.obj'')', textures_query 'SELECT * FROM tex_defs');
```

```sh
python3 -c "
import json
d = json.load(open('cube_tex.gltf'))
for mesh in d['meshes']:
    for prim in mesh['primitives']:
        mat = d['materials'][prim['material']]
        print(mat['name'], 'textured=', 'TEXCOORD_0' in prim['attributes'],
              'vertices=', d['accessors'][prim['attributes']['POSITION']]['count'],
              'indices=', d['accessors'][prim['indices']]['count'])
"
```

```text
GroundSurface textured= False vertices= 4 indices= 6
brick textured= True vertices= 16 indices= 24
RoofSurface textured= False vertices= 4 indices= 6
RoofSurface textured= False vertices= 4 indices= 6
```

(`cube.obj` reads as two objects, `cube` and `slab` — see `read_obj` above —
so `RoofSurface` appears twice, once per mesh. `brick` covers four quad faces
of `cube`, each contributing four corners of its own: 4 × 4 = 16 vertices, no
sharing across faces, against 4 × 6 = 24 indices for their two triangles
apiece.)

GLB embeds an image as a `bufferView` alongside its `mimeType`; `.gltf` writes
it to a file beside the output and records a `uri` instead — glTF allows only
one of the two per image, and tinygltf writes whichever applies:

```sql
COPY (SELECT * FROM tex_rows) TO 'cube.glb'
(FORMAT glb, materials_query 'SELECT * FROM obj_materials(''test/data/obj/cube.obj'')', textures_query 'SELECT * FROM tex_defs');
```

The `.glb` is binary, so it is unpacked before inspection: a 12-byte file
header (magic, version, total length), then chunks, each with its own 8-byte
header (length, type) — the first chunk is always `JSON`:

```sh
python3 -c "
import json, struct
data = open('cube.glb', 'rb').read()
off = 12
chunks = {}
while off < len(data):
    clen, ctype = struct.unpack('<I4s', data[off:off+8])
    chunks[ctype] = data[off+8:off+8+clen]
    off += 8 + clen
j = json.loads(chunks[b'JSON'])
print(j['images'])
print(json.dumps(j['materials'][1]))
"
```

```text
[{'bufferView': 0, 'mimeType': 'image/png'}]
{"alphaMode": "BLEND", "doubleSided": true, "name": "brick", "pbrMetallicRoughness": {"baseColorFactor": [0.7, 0.3, 0.2, 0.7], "baseColorTexture": {"index": 0}, "metallicFactor": 0.0}}
```

A texture whose bytes cannot be read is skipped with a warning and the
material keeps its colour, exactly as for `obj`. A texture whose bytes were
read but whose **file cannot be written** beside a `.gltf` is not: tinygltf
has no way to report a partial success, so the image writer's failure fails
the whole write and the COPY errors with `Failed writing glTF output`. `obj`
warns and carries on there, leaving the face without its `map_Kd`. A texture whose image type
cannot be told — no declared `image_type`, no recognisable file extension on
its `image_uri`, and its bytes carry neither the PNG nor the JPEG magic number
— is skipped the same way, because a GLB image with an empty `mimeType` is
invalid:

```sql
CREATE TABLE tex_defs2 AS
SELECT id, 'brick'::VARCHAR AS image_uri, '\xDE\xAD\xBE\xEF'::BLOB AS image_data,
       NULL::VARCHAR AS image_type, wrapMode, textureType, borderColor, other
FROM obj_textures('test/data/obj/cube.obj');

COPY (SELECT * FROM tex_rows) TO 'cube_unknown.glb'
(FORMAT glb, materials_query 'SELECT * FROM obj_materials(''test/data/obj/cube.obj'')', textures_query 'SELECT * FROM tex_defs2');
-- WARNING: cityjson: texture 0: unknown image type
```

Dropped faces (a ring with no plane normal to triangulate against) and
skipped objects (no geometry, nothing at the requested `lod`, …) produce
`duckdb_logs` warnings, exactly as for `obj`. `railway_appearance.city.jsonl`
exercises both an unreadable texture and a face with no plane normal in one
real file:

```sql
SET enable_logging = true;
SET logging_storage = 'memory';

COPY (SELECT * FROM read_cityjsonseq('test/data/railway_appearance.city.jsonl'))
TO 'railway.glb' (FORMAT glb, lod '3');

SELECT message FROM duckdb_logs() WHERE message LIKE 'cityjson:%';
-- cityjson: texture 2: could not read 'test/data/appearances/Bruecke-Schotter.jpg': Failed to open file: test/data/appearances/Bruecke-Schotter.jpg; faces fall back to the material colour
-- cityjson: texture 3: could not read 'test/data/appearances/Bruecke-Beton.jpg': Failed to open file: test/data/appearances/Bruecke-Beton.jpg; faces fall back to the material colour
-- cityjson: object GMLID_855011_330784_753: face 67 has no plane normal and could not be triangulated, skipped
```

Remote output paths are not supported for mesh formats, `gltf`/`glb` included:
tinygltf writes through local file streams, never through DuckDB's own
filesystem abstraction, so `COPY … TO 's3://…/x.glb'` fails opening the output
rather than reaching object storage:

```sql
COPY (SELECT * FROM read_cityjson('test/data/holed_face.city.json', lod := '2.2'))
TO 's3://nonexistent-bucket-cityjson-test/x.glb' (FORMAT glb);
-- Invalid Error: Failed writing glTF output: s3://nonexistent-bucket-cityjson-test/x.glb
```

The appearance-form refusal (a source read with `appearance := 'sidecar'` and
no `*_query`) applies to every mesh format, `gltf`/`glb` included — see `obj`
above for the message and the fix.

No normals are written — viewers compute flat ones, which is what a city
model, with no curved surfaces, wants. `EXT_mesh_features` /
`EXT_structural_metadata` are not written; node names and `extras` carry the
identity and the attributes instead.

A GLB's chunk headers are 32-bit, so a GLB cannot exceed 4 GiB. A write whose
geometry and embedded images would take it past that is refused rather than
written with wrapped chunk lengths and no error — either up front, from the
buffer size before anything is serialised, or after writing, from the file's
actual size, whichever catches it first — and no file is left behind either
way. Write `FORMAT gltf`, whose buffer is a separate file the limit does not
apply to, or split the export with a lower `lod` or a `WHERE` clause.

Under duckdb-wasm, prefer GLB: a `.gltf` needs its `.bin` and any images
placed beside it, which the browser cannot do for a file it only hands to the
user as a download.

`just test-gltf-validate` (opt-in; needs `node`/`npx` for `gltf-validator`,
and network to fetch a remote fixture and run it through httpfs) and `just
test-obj-cjio` (opt-in; needs `cjio` and network) cross-check the writers
against independent tools.

---

## Output schema

### Predefined columns

Reserved columns appear in the order below, before every attribute column
(CityParquet spec, "Reserved columns"):

| Column | Type | Description |
| ------ | ---- | ----------- |
| `id` | VARCHAR | CityObject identifier |
| `feature_id` | VARCHAR | Root-family grouping key |
| `object_type` | VARCHAR | CityGML class name (`Building`, `Road`, …) |
| `parents` | VARCHAR[] | Parent CityObject ids |
| `children` | VARCHAR[] | Child CityObject ids |
| `children_roles` | VARCHAR[] | Roles, positionally aligned with `children` |
| `address` | STRUCT[] | Reserved; always NULL — no reader parses source addresses yet |
| `bbox` | STRUCT (`xmin … zmax DOUBLE`) | 3D extent in world coordinates (below) |
| *(the per-LoD geometry group, below)* | | |
| `template` | STRUCT(`id BIGINT, point BLOB, transformationMatrix DOUBLE[]`) | Reserved; always NULL — no reader parses geometry-template instances yet |
| `other` | JSON (VARCHAR) | Source members not mapped to a reserved or attribute column |

Then **every attribute column** inferred from the data, last.

`bbox` is **unioned across every stored LoD and across the object's
descendants** — so a parent `Building` whose 3D detail lives on its
`BuildingPart` children still gets a full-height extent, not a flat one. (The
single exception is `lod =>` mode, which has only the one requested LoD to work
from.)

A query selecting a reserved column **by name** is unaffected by this order; one
selecting by ordinal position (`SELECT #4`, or a `SELECT *` a caller then indexes
into) must be updated.

**A source CityObject's own `geographicalExtent`** has no dedicated column and is
never carried in `other`: on scan it is unioned into `bbox` alongside the extent
computed from the object's own geometry, so the two extents merge into the one
stored value rather than travelling separately. All three `COPY … TO` formats
(`cityjson`, `cityjsonseq`, `flatcitybuf`) rebuild `geographicalExtent` from `bbox`
alone on the way out. A CityObject with no `bbox` (e.g. one carrying no geometry and
no source extent) writes no `geographicalExtent` member at all.

### Geometry columns (CityParquet wide layout)

`bbox` (above) leads the geometry group; then one group per LoD found in the
data, named after the normalised LoD. A suffix always carries a minor, so `2.0`
becomes `geometry_lod2_0`, never `geometry_lod2`:

| Column | Type | Description |
| ------ | ---- | ----------- |
| `geometry_lodX_Y` | BLOB | WKB geometry for that LoD (NULL if absent) |
| `geometry_properties_lodX_Y` | STRUCT | What WKB cannot carry (below) |
| `material_lodX_Y` | `MAP(VARCHAR, BIGINT[])` | Theme → one sidecar id (or NULL) per WKB face; NULL if no material |
| `texture_lodX_Y` | `MAP(VARCHAR, STRUCT(id BIGINT, uv DOUBLE[][])[][])` | Theme → per WKB face → per ring → the id and its `[u, v]` pairs; NULL if no texture |

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
