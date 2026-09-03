## cityparquet_rs_minimal/

A CityParquet package written by the reference implementation (cityparquet-rs) from
`lod3_railway.city.json`. It exists so the package-layer tests read a file this
extension did not produce: a reader and writer that agree on a wrong encoding pass
every assertion made against each other.

Trimmed to `building.parquet` + `bridge.parquet` + `metadata.json` (the full
conversion produces twelve object/sidecar tables); `metadata.json`'s `assets` map is
hand-edited to match. The point of the fixture is the schema and footer conventions
`building.parquet` carries -- notably the reserved `address` and `template` columns --
not the row count or the full module set. It also carries an `other_attributes`
column, an ordinary attribute of that foreign file; nothing in the current format
reserves that name (the format's escape hatch is the single `other` column).

## obj/

Hand-written Wavefront OBJ fixtures for `read_obj`. `cube.obj` + `cube.mtl` is a closed
unit cube (one `o`, three `g`/`usemtl` semantic groups, one textured plain material,
one face with negative indices, one `v` with a `w` component) followed by an open slab
that inherits the cube's last `g`/`usemtl` state -- OBJ state persists across `o`, and
the fixture pins that. `open_roof.obj` has no `o` line and no materials. `continuation.obj`
ends a line with a backslash, which the reader refuses. The expected values in the tests
are derived by hand from these files, not from the reader.

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
