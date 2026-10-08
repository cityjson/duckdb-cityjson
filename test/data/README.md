## cityparquet_rs_minimal/

A CityParquet package written by the reference implementation (cityparquet-rs, monorepo
commit f9e0374) from `lod3_railway.city.json`: `cityparquet convert lod3_railway.city.json
-o OUT --lod0`. It exists so the package-layer tests read files this extension did not
produce: a reader and writer that agree on a wrong encoding pass every assertion made
against each other.

Trimmed to the `building`, `bridge`, `vegetation` and `generics` object tables and all
three sidecars (`materials`, `textures`, `implicit_geometries`) -- the full conversion
also writes `city_furniture`, `relief`, `transportation`, `tunnel` and `water_body` --
with `metadata.json`'s `assets` cut to match; nothing else is edited. `generics` is kept
because it holds the CityObjectGroup the vegetation objects belong to. `--lod0` gives
`building` a GeoParquet-legal `geometry_lod0_0`, which DuckDB reads as `GEOMETRY`. The
`other` and `surfaces` columns carry the Parquet JSON logical type; the
`material_lod*` / `texture_lod*` columns are the specification's typed MAPs, flat per WKB
face, which is what `cityparquet_rs_appearance.test` pins. `cityparquet validate` reports
no error and no warning on it.

## address_location.city.jsonl

One real 3D Helsinki building (a CityJSONSeq header and one feature, copied from
cityparquet-rs's `crates/core/tests/data/`) whose `address` member was filled in by
hand, because the published Helsinki data spells its address members `Country` /
`Locality` / `ThoroughfareName` and so exercises none of the recognised vocabulary. The
first address carries every recognised member and a `location` MultiPoint over vertices
0 and 4; the second only `locality`, plus a differently cased `Locality` and an `id`,
neither of which maps. `cityjson_address.test` reads and writes it.

## railway_vegetation.city.json

`lod3_railway.city.json` (the CityGML LoD3 railway sample, as cityparquet-rs fetches it
into `tests/fixtures/`) cut down to the objects with implicit geometry: its 15
SolitaryVegetationObjects, each one `GeometryInstance` of one of the three geometry
templates, and the CityObjectGroup whose children 14 of them are. The header,
`transform`, `metadata`, `geometry-templates` and the appearance `materials` and
`textures` are unchanged; `vertices` keeps only the 15 reference points and
`vertices-texture` only the UVs the templates use, both re-indexed in first-use order
(the templates' texture rings re-pointed to match). `cityjson_implicit_geometry.test`
reads it.

## degenerate_rings.city.jsonl + degenerate_rings_appearance.city.jsonl

Real objects with rings cut short by hand, for the readers' drop of rings that cannot
form a closed WKB ring (`cityjson_degenerate_rings.test`); no published dataset at hand
carries one. `degenerate_rings.city.jsonl` is `delft_subset.city.jsonl`'s header and
first feature, with face 1 of the BuildingPart's LoD 2.2 `Solid` (a `WallSurface`) cut
to its first two vertices. `degenerate_rings_appearance.city.jsonl` is
`railway_appearance.city.jsonl` with the textured Bridge's surface 0 exterior ring, the
materialled CityFurniture's surface 1 exterior ring and its surface 46 interior ring
(16 vertices) each cut to their first two vertices. Everything else is unchanged.

## cityparquet_rs_delft/

`delft_subset.city.jsonl` as cityparquet-rs (monorepo commit f9e0374) writes it:
`cityparquet convert delft_subset.city.jsonl -o OUT --lod0 --ordering hilbert`,
unedited. The destination of `cityparquet_append.test`'s Delft append -- a package this
extension did not write, in the shape the database benchmark appends into.

## railway_append.city.jsonl

Appearance-bearing and implicit-geometry objects of `lod3_railway.city.json` to append to
`cityparquet_rs_minimal/`, every id (and every `parents` / `children` entry) suffixed
`-appended` so none collides: `railway_appearance.city.jsonl`'s textured Bridge and
materialled CityFurniture, unchanged otherwise, then two features holding the 15
implicit-geometry objects and their CityObjectGroup, each with the reference points it
uses as its own `vertices`. The header is `railway_appearance.city.jsonl`'s, with
`lod3_railway`'s own `geometry-templates` and appearance `materials` / `textures`, and a
`vertices-texture` cut to the UVs the templates use (re-indexed in first-use order, the
templates' texture rings re-pointed to match).

## textured_holes.city.json + degenerate_rings_texture.city.json

`lod3_railway.city.json` cut down to one Building, `GMLID_BUI130363_1235_6047`: 19
textured `MultiSurface` surfaces, surface 0 an exterior ring with one hole and surface 4
one with two. Its vertices and the UVs it uses are re-indexed in first-use order, the
appearance `materials` and `textures` arrays kept whole. `degenerate_rings_texture` is
the same object with surface 0's hole and surface 4's exterior ring cut to their first
two vertices, for `cityjson_degenerate_rings.test`'s ring-level texture check.

## footprint_outlines.city.jsonl

`delft_subset.city.jsonl`'s header and first two features, with each Building's LoD0
footprint (a `MultiSurface`) turned into its outlines -- every surface's exterior ring,
closed, as one `MultiLineString` -- and its semantics dropped; the BuildingParts are
unchanged. No published fixture at hand carries a `MultiLineString`, the one CityJSON
geometry type whose CityGML CM name (`MultiCurve`) differs; `cityjson_multicurve.test`
reads it.

## delft_append.city.jsonl

One 3DBAG feature -- a Building and its BuildingPart, ids suffixed `-appended` --
as a CityJSONSeq with a header declaring EPSG:7415 and no appearance. It is the
append input the CityParquet monorepo's database benchmark derives from the 3DBAG
corpus for its `append-object` scenario, copied unchanged, so it was not produced
by this extension. Its CRS matches `delft_subset.city.jsonl`'s and none of its ids
occurs there, so it appends cleanly to a package built from that file.

## obj/

Hand-written Wavefront OBJ fixtures for `read_obj`. `cube.obj` + `cube.mtl` is a closed
unit cube (one `o`, three `g`/`usemtl` semantic groups, one textured plain material,
one face with negative indices, one `v` with a `w` component) followed by an open slab
that inherits the cube's last `g`/`usemtl` state -- OBJ state persists across `o`, and
the fixture pins that. `open_roof.obj` has no `o` line and no materials.
`continuation.obj` ends a `v` line with a backslash, which continues it on the next
line -- one triangle, not two vertices and a stray coordinate. The expected values in
the tests are derived by hand from these files, not from the reader.

## holed_face.city.json

A hand-written LoD 2.2 `MultiSurface` for the `COPY ... (FORMAT obj)` tests: one roof
face carrying an inner ring (wound opposite the outer, as CityJSON requires) plus a
plain wall face, over twelve vertices of which two are duplicates the wall shares with
the roof. Every expectation in `copy_obj.test` -- ten `v` lines, the eight roof
triangles, `f 9 10 2 1` for the wall, `# origin 84500 446300 0` -- is derived by hand
from these coordinates.

## solid_material.city.json

A hand-written unit-cube `Solid` (one shell, six faces) with `RoofSurface` /
`WallSurface` / `GroundSurface` semantics and three materials of distinct
`diffuseColor`. It exists so `copy_obj.test` can pin that a Solid's material cell is
honoured in both shapes it reaches the mesh writers in: the reader's CityJSON-NESTED
`values` (one list per shell) and the spec's FLAT `values` (one id per WKB face), the
latter substituted with a SQL `REPLACE` so no reader produced it.

## null_lead_texture.city.json

A hand-written LoD 2.2 `MultiSurface` of two disjoint unit squares whose texture
`values` are `[null, [[0, 0, 1, 2, 3]]]`: the first face carries no texture, the second
a full ring of UV indices. It exists so `copy_obj.test` can pin that a cell's shape is
measured from the first entry that carries something -- measuring from the leading
`null` classifies the cell as a shape it is not, and the geometry loses its appearance
without a warning. The image it names (`brick.png`) is deliberately not on disk: the
`vt` and `f v/vt` lines are what the test is about, not `map_Kd`.

## solid_null_first_face.city.json

A hand-written unit-cube `Solid` (one shell, six faces) whose texture `values` is
`[[[null], [[0, 0, 1, 2, 3]], [[null]], [[null]], [[null]], [[null]]]]`: the first face
carries no texture -- spelled `[null]`, not the bare `null` `null_lead_texture.city.json`
uses -- the second a full ring of UV indices, and the remaining four are `[[null]]`. It
exists so `copy_obj.test` can pin that "carries nothing" is recursive: an all-null
substructure reads the same as a bare `null` at every level `FirstMeasurable` measures
from, not only the outermost. The image it names (`brick.png`) is deliberately not on
disk.

## duplicate_material_name.city.json

A hand-written LoD 2.2 `MultiSurface` of two disjoint unit squares, one material each,
where both materials are named `brick` and differ only in `diffuseColor`. It exists so
`copy_obj.test` can pin the `_<id>` suffix: a `.mtl` entry is keyed by identity, not by
the name it renders to, so the second material gets `newmtl brick_1` rather than
overwriting the first.

## solid_texture.city.json

The same unit-cube `Solid` as `solid_material.city.json`, textured instead: one PNG
texture and a four-pair `vertices-texture` pool, with every one of the six faces
carrying the same UV ring. It exists so `copy_obj.test` can pin that a *texture* cell
is honoured in both shapes it reaches the mesh writers in -- the reader's per-shell
`values` and the spec's per-WKB-face one, which for a texture nest one level deeper
than a material's. The image it names is deliberately not on disk: the `vt` and
`f v/vt` lines are the point, not `map_Kd`.

## quad_texture.city.json + quad_texture.png

One textured `MultiSurface` quad whose four `vertices-texture` pairs
(`[0.1, 0.2]`, `[0.9, 0.2]`, `[0.9, 0.7]`, `[0.1, 0.7]`) are **not** symmetric under
`v -> 1 - v`. Every other textured fixture's UV pool is the unit square, which that
flip maps onto itself, so none of them can tell a glTF writer that flips the V axis
from one that does not. `copy_gltf.test` reads the `TEXCOORD_0` bytes out of the
`.bin` and compares them against the float32 pair it expects. `quad_texture.png` is a
real 1x1 PNG and is on disk beside the fixture, because the flip is only written for
a face whose texture image could actually be loaded.

## helsinki_tex_uv_count.city.jsonl

Copied unchanged from cityparquet-rs's `crates/core/tests/data/`: the header line and
three features (lines 16875, 16509 and 43164) of the real Helsinki textured CityJSONSeq
(`Helsinki_tex.city.jsonl`, City of Helsinki, CC BY 4.0), each with one textured LoD2
roof ring of four UVs. `BID_087ea02f…`'s ring `[0, 2, 4, 6, 0]` is explicitly closed, so
its four UVs are one per vertex; `BID_e47016b2…`'s `[0, 4, 4, 6, 1]` repeats a vertex in
place and `BID_ce1e6c36…`'s `[0, 4, 6, 8, 1]` has five distinct vertices, so both are
one UV short. `cityjson_texture_closed_ring.test` reads it, and
`cityparquet_json_attribute.test` inserts it for its `Integrate_LoD[1]` objects.
