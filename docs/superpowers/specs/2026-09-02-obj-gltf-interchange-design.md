# OBJ and glTF interchange — design

Date: 2026-09-02. Status: approved design, awaiting implementation plan.

## Goal

Make CityParquet tables and CityJSON reads exportable to the two mesh formats
the rest of the 3D world consumes — Wavefront OBJ and glTF 2.0 — and make OBJ
importable into the CityParquet object-table schema, all as SQL:

```sql
-- OBJ in
SELECT * FROM read_obj('campus.obj', lod := '2.2', object_type := 'Building');

-- OBJ / glTF / GLB out
COPY (SELECT * FROM loaded.building) TO 'campus.obj' (FORMAT obj, materials_query 'SELECT * FROM loaded.materials');
COPY (SELECT * FROM read_cityjsonseq('delft.city.jsonl')) TO 'delft.glb' (FORMAT glb, attributes true);
```

Everything lives in this extension (`duckdb-cityjson`). `duckdb-3d` is not
touched: it builds at C++17, has no file I/O, its kernel may not know CityJSON,
and its triangulator ear-clips every ring of a face independently, which fills
holes instead of cutting them.

## Dependencies

Three new vcpkg ports, all present at the pinned baseline `84bab45d`:

| Port | Version | Licence | Role |
| --- | --- | --- | --- |
| `tinyobjloader` (feature `double`) | 2.0.0rc13 | MIT | OBJ parsing via its callback API |
| `tinygltf` | 2.9.7, **pinned with a manifest `overrides` entry** | MIT | glTF / GLB serialisation; uses the `nlohmann-json` already linked |
| `earcut-hpp` | 2.2.4 | ISC | Hole-aware polygon triangulation |

Why these and not others is recorded in the research this design came from:
no baseline library writes OBJ with double precision, per-surface `usemtl` and
`o`/`g` nesting, so the OBJ writer is hand-written; `cgltf` cannot write
unknown per-object extensions and `fastgltf` has typed extensions only, which
would block the 3D Tiles metadata extensions later; tinygltf writes `extras`
and extension JSON verbatim. tinygltf's C++ header is deprecated upstream in
favour of a C11 rewrite, hence the pin: a baseline bump must not silently move
the port to an incompatible API.

Compile-time configuration for tinygltf: `TINYGLTF_NO_STB_IMAGE`,
`TINYGLTF_NO_STB_IMAGE_WRITE`, `TINYGLTF_NO_EXTERNAL_IMAGE`,
`TINYGLTF_NO_INCLUDE_JSON` (we supply nlohmann). Images are never decoded:
they enter the model as raw bytes in a bufferView, which tinygltf serialises
as-is (verified in 2.9.7's `UpdateImageObject`: an image with a bufferView and
no URI is left untouched).

## Part 1 — reading OBJ

### Surface

```sql
read_obj(path, lod := '2.2'
              [, object_type := 'Building']
              [, geometry_type := 'auto' | 'Solid' | 'MultiSurface']
              [, appearance := 'local' | 'sidecar']
              [, sample_lines := 100])
obj_materials(path)                 -- materials.parquet-shaped rows from the .mtl
obj_textures(path)                  -- textures.parquet-shaped rows from map_Kd
obj_metadata(path [, crs := 'EPSG:7415'])
```

`read_obj` produces the same output schema as `read_cityjson`: the reserved
columns, `bbox`, one `geometry_lodX_Y` / `geometry_properties_lodX_Y` /
`material_lodX_Y` / `texture_lodX_Y` group for the one LoD given, and no
attribute columns (OBJ carries none).

`lod` is required: an OBJ carries no level of detail, and the value names the
columns. It is normalised exactly as the other readers normalise it (`'2'`
becomes `lod2_0`).

`appearance` has the same meaning and the same default (`'local'`) as on the
other readers, for consistency; `'sidecar'` is what a `cityparquet_write` of
the result needs, exactly as it does for a CityJSON read. In local form the
texture UV pool is the file's `vt` list.

### Where the CRS goes

`read_obj` does **not** take `crs`. A row has nowhere to carry a CRS, so a
`crs` parameter on the reader would be observable nowhere — a silent no-op,
which this repository treats as a defect. The argument lands on
`obj_metadata(path, crs := …)`, which returns one row shaped like
`cityjson_metadata`: `version` NULL, `reference_system` resolved from the
argument through the existing PROJJSON table (NULL when omitted),
`geographical_extent` computed from the vertices, `transform` NULL. That row
is what `COPY … (metadata_query …)` and `cityparquet_write(…, crs => …)`
already consume, so the CRS reaches every writer the way it does from any
other source.

### Mapping rules

- **Objects.** Each `o` line starts a city object; its name is the `id` and
  the `feature_id`. A file with no `o` line is one object whose id is the
  file's stem. Every object gets `object_type` from the parameter. No parents,
  children or attributes.
- **Geometry.** All faces of an object form one geometry at the given LoD.
  `geometry_type := 'auto'` (default) yields `Solid` with one exterior shell
  when the face set is closed — every undirected edge is used by exactly two
  faces — and `MultiSurface` otherwise. `'Solid'` and `'MultiSurface'` force
  the type; forcing `Solid` on an open mesh is allowed (val3dity does the
  same: "a solid will be formed by only its exterior shell"). Faces are
  written as one outer ring each — OBJ cannot express holes — in file order,
  with the file's own winding. WKB types follow the existing encoder
  (`PolyhedralSurface Z` for a solid, `MultiPolygon Z` for a multi-surface).
- **Semantics.** A `usemtl` name that is a CityJSON semantic surface type
  (`RoofSurface`, `WallSurface`, `GroundSurface`, `ClosureSurface`,
  `OuterCeilingSurface`, `OuterFloorSurface`, `Window`, `Door`,
  `InteriorWallSurface`, `CeilingSurface`, `FloorSurface`, `WaterSurface`,
  `WaterGroundSurface`, `TrafficArea`, `AuxiliaryTrafficArea`, `+…`
  extension names) becomes that surface; the face's `face_semantics` entry
  points at it and `surfaces` holds one `{"type": …}` per distinct name.
  Any other `usemtl` name is a material, not a surface. When the `usemtl`
  name is not a surface type, the innermost `g` name is used under the same rule. Faces
  that match neither get NULL semantics. This is the Obj2CityGML convention
  (surface types from group and material names) and the inverse of what the
  writer below emits, so a round trip preserves semantics.
- **Materials.** Each `newmtl` in the referenced `.mtl` files becomes one
  materials row: `id` = ordinal across the mtl files in `mtllib` order,
  `name`, `diffuseColor` from `Kd`, `specularColor` from `Ks`,
  `emissiveColor` from `Ke`, `ambientIntensity` from the mean of `Ka`,
  `transparency` = `1 - d` (or `Tr`), `shininess` from `Ns` scaled to
  `[0,1]` by `/1000` (the range MTL uses); everything else into `other`.
  A face whose `usemtl` is a semantic-surface name **and** a defined material
  gets both. `material_lodX_Y` carries one theme, `"visual"`, in
  CityJSON `values` form.
- **Textures.** A material with `map_Kd` yields one textures row: `id` =
  ordinal among textured materials, `image_uri` = the `map_Kd` path as
  written, `image_type` = upper-cased extension (`"JPG"`, `"PNG"`),
  `wrapMode` `"wrap"`, `textureType` `"unknown"`. Faces with `v/vt` indices
  under such a material get a `texture_lodX_Y` entry, theme `"visual"`,
  ring form `[texId, uv…]` — indices into the `vt` pool in local mode,
  inlined `[u, v]` pairs in sidecar mode. Faces without `vt` get `[null]`.
- **Indices.** 1-based positive and negative (relative) indices are both
  honoured. `v` with a fourth `w` component ignores it; `v` with six values
  keeps the first three (vertex colours are dropped). `vn` is ignored — the
  normal of a polygon is implied by its ring. Backslash line continuation is
  **not** supported, a limitation of the parser library; the reader says so
  in its error when it meets a line ending in `\`.
- **Coordinates.** Doubles as parsed (the `double` feature). No axis swap
  and no translation: an OBJ produced by cjio, 3dfier or geoflow is Z-up in
  world coordinates already, and one that is not is the user's to fix with
  `ST_3DTransform` afterwards. An `# origin x y z` comment written by our own
  exporter (Part 2) **is** honoured: the offset is added back so a
  round trip is exact.
- **`bbox`** is computed as for every other reader.

### Implementation

`OBJReader` is a `CityJSONReader` subclass. It parses once, lazily, into
`CityJSONFeature` records — one per object, with the object's own vertex
pool and `vertices-texture` pool — and serves `ReadMetadata`,
`ReadAllChunks`, `ReadNFeatures`, `Columns` from that cache, as
`LocalCityJSONReader` does. `read_obj` then binds through
`BindCityJSONReadRaw` and scans through `CityJSONScan`, unchanged, which is
what gives it WKB encoding, the per-LoD column pair, `bbox`, sidecar
normalisation and filter pushdown for free. The reader kind is recorded so
`FindCopySourceRef` can name it.

All bytes come through DuckDB's `FileSystem`, never `std::ifstream`: the OBJ
text and each `.mtl` named in `mtllib` (resolved against the OBJ's
directory). Image files are not read: `obj_textures` leaves `image_data` NULL
and reports `image_uri` as written, exactly as the CityJSON readers do.
tinyobjloader's `LoadObjWithCallback` takes an `std::istream`, so the text is
wrapped in an `std::istringstream`; a `MaterialReader` subclass hands it the
already-loaded MTL text so it never opens a file itself. This is what keeps
`read_obj('s3://…/x.obj')` working.

The parse uses the callback API, not `LoadObj`: the mainline API collapses
`o` and `g` into a single name and would lose the object id.

## Part 2 — writing OBJ and glTF

### Surface

```sql
COPY (…) TO 'out.obj'  (FORMAT obj  [, lod '2.2'] [, origin 'auto'|'none'|'x,y,z'] [, triangulate false]
                                    [, precision 17] [, materials_query '…'] [, textures_query '…'] [, metadata_from '…']);
COPY (…) TO 'out.gltf' (FORMAT gltf [, lod …] [, origin …] [, attributes false] [, materials_query …] [, textures_query …] [, metadata_from …]);
COPY (…) TO 'out.glb'  (FORMAT glb  … same as gltf …);
```

The three formats register as three `CopyFunction`s sharing the existing
`CityJSONCopyToBind`, `InitGlobal`, `InitLocal`, `Sink` and `Combine`. The
sink already turns each row back into a CityJSON object with coordinates
inlined, `semantics` re-nested from `face_semantics`, and the
`material_lod*` / `texture_lod*` cells re-attached; that is exactly the input
a mesh writer needs. Only `Finalize` dispatches. The two booleans `is_seq` /
`is_fcb` on `CityJSONCopyBindData` become `enum class CopyFormat { CityJSON,
CityJSONSeq, FlatCityBuf, Obj, Gltf, Glb }`; every existing use site is
updated.

Options common to the three:

| Option | Default | Meaning |
| --- | --- | --- |
| `lod` | highest LoD present per object | Which `geometry_lodX_Y` column to export. An object with nothing at that LoD is skipped. |
| `origin` | `'auto'` | `'auto'`: subtract the minimum corner of the exported extent. `'none'`: world coordinates. `'x,y,z'`: subtract that point. |
| `materials_query` | none | SQL returning materials.parquet-shaped rows. Its presence declares the cells to be in sidecar form (see *Resolving appearance*). |
| `textures_query` | none | SQL returning textures.parquet-shaped rows; same rule. |
| `metadata_from` | discovered source | As for the CityJSON writers. Supplies local-form appearance definitions and the CRS. |

OBJ-only: `triangulate` (default `false`: hole-free faces stay n-gons),
`precision` (significant digits, default 17 = shortest round-trip via
`std::to_chars`). glTF-only: `attributes` (default `false`: put each row's
attribute columns into the node's `extras`).

### The mesh model

A shared `MeshModel` is built from the collected objects once per COPY,
independent of the output format:

```
object  { id, object_type, lod, attributes(json), faces[] }
face    { rings[] (outer first), surface (index into surfaces or -1), material (id or -1), uv[] per ring or empty }
vertex pool: doubles, deduplicated per object by exact coordinate match
```

`shells` structure is not preserved — neither format has it; a solid's faces
are flattened in WKB order, which is also the order `face_semantics` uses.

Faces with inner rings are triangulated with earcut after projection onto
the plane of their Newell normal, with the vertex coordinates shifted to the
ring's first vertex before projecting (the same precaution duckdb-3d's
triangulator takes: at RD magnitudes the signed-area products of absolute
coordinates drown the answer). Every face is triangulated for glTF; for OBJ
only holed faces are, unless `triangulate true`. Triangles keep the parent
face's surface, material and UVs (UVs are interpolated by vertex index, not
recomputed, since earcut returns indices into the input ring vertices).

### Resolving appearance

`material_lod*` cells are shape-identical in local form and sidecar form —
`{"visual": {"values": [2, 2, …]}}` either way — and only texture cells
differ (`[texId, 5, 6, 7]` versus `[texId, [u, v], …]`). The writer must
therefore be told, not guess; guessing wrong recolours every face silently.
The rule:

1. `materials_query` / `textures_query` present ⇒ the cells are sidecar
   form. Ids resolve against the query results. Any source appearance is
   ignored. A `texture_lod*` cell that still carries integer UV references is
   an error.
2. Neither present ⇒ the cells are local form and resolve against the
   discovered source (or `metadata_from`), using the `appearance` blocks the
   bind already loads: header materials/textures and, for CityJSONSeq, each
   feature's own `vertices-texture` pool. This is the CityJSON writer's
   existing path.
3. A discovered source whose read had `appearance := 'sidecar'` and no
   `*_query` option is **refused** at bind with a message naming the two
   options. To know this, `CopySourceRef` gains one field,
   `sidecar_appearance`, which `FindCopySourceRef` fills from the reader
   call's named parameter. (For a whole-document CityJSON source the ids
   happen to coincide, since header entries intern first; the refusal is
   still correct, because the writer cannot tell that source from a Seq one
   whose ids do not.)
4. No appearance anywhere ⇒ default colours (below).

Texture bytes come from `image_data` when non-null, else from `image_uri`
resolved against the source file's directory, read through `FileSystem`;
a texture whose bytes cannot be read is reported as a warning and its faces
fall back to the material's colour.

Default colours when a face has no material: by semantic surface type
(`RoofSurface` red `(0.9, 0.06, 0.09)`, `WallSurface` light grey, `GroundSurface`
dark grey, `Window`/`Door` blue-grey, water blue, vegetation green — the
Up3date palette), else by `object_type` (cjio's palette: Building terracotta,
Road grey, WaterBody cyan, vegetation green, LandUse yellow, Bridge purple,
Tunnel black, TINRelief brown, GenericCityObject pink), else mid grey.

### OBJ output

Follows what cjio, 3dfier and geoflow write, so the file opens the way
3DBAG's users already expect:

```
# Written by duckdb-cityjson
# crs EPSG:7415
# origin 84500 446300 0
mtllib out.mtl
o NL.IMBAG.Pand.0503100000012869-0
v 93.2496 161.355 0.475
…
g RoofSurface
usemtl RoofSurface
f 1 2 3 4
g WallSurface
usemtl mat_3
f 5 6 7 8
```

- One `o` per object, one vertex block per object (indices absolute,
  1-based, never negative), `g <surface type>` whenever the surface changes,
  `usemtl <name>` whenever the material changes: the material name when the
  face has a resolved material, else the surface type when it has one, else
  `<object_type>` — so a materials-free 3DBAG export colours by surface like
  geoflow's and a cjio-style export colours by class like 3dfier's.
- Z-up, no axis swap. `origin` subtracted and recorded in the `# origin`
  comment; `# crs` written when known. Both are comments, so every consumer
  ignores them and `read_obj` honours the origin.
- `vt` lines and `f v/vt` form only for faces that carry UVs; no `vn`.
- `out.mtl` beside the file: one `newmtl` per material actually referenced,
  `Kd` from `diffuseColor`, `Ks` from `specularColor`, `Ke` from
  `emissiveColor`, `d` = `1 - transparency`, `Ns` = `shininess * 1000`,
  `map_Kd <image>` for textured materials. Default-colour surfaces get
  `newmtl RoofSurface` etc. Texture images are copied beside the OBJ under
  their original basename (de-duplicated by `id`). A material whose name
  collides with another entry's has `_<id>` appended.

### glTF / GLB output

- **Scene graph.** One root node with the Z-up-to-Y-up matrix
  `[1,0,0,0, 0,0,-1,0, 0,1,0,0, 0,0,0,1]` (cjio, py3dtiles, and what the 3D
  Tiles 1.1 spec advises for Z-up sources); vertex data untouched. One child
  node per object, `name` = id, one mesh with one primitive per material.
- **Buffers.** Positions `VEC3` float32 relative to `origin`, with the
  mandatory `min`/`max`; indices `UNSIGNED_SHORT` when the mesh has fewer
  than 65 535 vertices, else `UNSIGNED_INT`; `TEXCOORD_0` float32 with the V
  axis flipped (glTF's UV origin is top-left, CityJSON's is bottom-left);
  no normals (viewers compute flat normals, which is what a city model wants).
  Triangles CCW as exported by the CityJSON winding; `doubleSided: true`
  so a source with mixed winding still renders.
- **Materials.** `pbrMetallicRoughness.baseColorFactor` = `diffuseColor` +
  `1 - transparency`, `metallicFactor 0`, `roughnessFactor 1`,
  `alphaMode BLEND` when transparency > 0, `emissiveFactor` from
  `emissiveColor`, `name` from the material name. Textured materials add
  `baseColorTexture` with a sampler whose wrap follows `wrapMode`.
- **Images.** GLB: raw bytes in a bufferView, `mimeType` from `image_type`.
  `.gltf`: written as separate files beside the output, `uri` relative.
  A `.gltf` also writes `out.bin`.
- **Metadata.** `asset.generator = "duckdb-cityjson"`, `asset.extras =
  {"crs": …, "origin": [x, y, z], "up": "z", "units": "m"}` — the pg2b3dm
  precedent, so a 3D Tiles packager can put the origin into a tile
  `transform` in double precision. With `attributes true`, each node's
  `extras` carries `object_type`, `lod` and every attribute column of the
  row as JSON. `EXT_mesh_features` and `EXT_structural_metadata` are a
  documented follow-up; tinygltf writes extension JSON verbatim, so they
  need no library change.
- **Precision.** float32 is glTF's rule; the origin subtraction is what
  keeps a 3DBAG export at millimetre precision (ULP at 1e5 is 8 mm, at
  `|origin|`-relative magnitudes of 1e3 it is 0.1 mm).

### Sidecar files and DuckDB's temp-rename

`Finalize` writes the main file to `gstate.temp_file_path`, which DuckDB
renames to the final path afterwards. The `.mtl`, `.bin` and copied images
are not part of that dance: they are named from the **final**
`bind_data.file_path` (same directory, same stem) and written directly, and
`mtllib` / `buffers[].uri` / `images[].uri` reference the final basenames.
They are written through `std::ofstream`, as the existing writers are,
which is what duckdb-wasm's MEMFS also supports. `GLB` is the mode that
works everywhere and is the documented recommendation under wasm.

## Testing

Fixtures this extension did not produce come first (CLAUDE.md: a
writer-then-reader test is a circular oracle):

- `test/data/obj/cube.obj` + `cube.mtl`: hand-written, `o` twice, `g`
  and `usemtl` per semantic surface, one negative-index face, one face with
  `v/vt` and `map_Kd`, a comment-only line, a `v` with `w`.
- `test/data/obj/open_roof.obj`: an open mesh, for `geometry_type := 'auto'`.
- `test/data/obj/holed_face.city.json`: a CityJSON object with a face that
  has an inner ring, for the exporter.

Suites (`test/sql/`):

- `read_obj.test`: schema (`DESCRIBE`), object count, `geometry_properties`
  type/surfaces/face_semantics, WKB type via `cityjson_wkb_geometry_type` and
  extent via `cityjson_wkb_extent`, materials and textures rows, sidecar-mode UV
  inlining, relative indices, `lod` normalisation, and every documented
  error (`lod` missing, bad `geometry_type`, backslash continuation).
- `obj_metadata.test`: extent equals the vertex bbox; `crs` resolved and
  omitted.
- `copy_obj.test`: `read_text` assertions on the OBJ and MTL — one `o` per
  object, `g`/`usemtl` sequence, `# origin`, vertex precision (an RD
  coordinate survives with `precision 17`), `triangulate true` emits only
  3-vertex faces, holed face is triangulated even with `triangulate false`,
  `origin 'none'`. Round trip `read_cityjson → COPY obj → read_obj` compared
  by `EXCEPT ALL` on ids, surface types and face counts (this one *is*
  circular and is labelled as such; the parity it proves is that the two
  sides agree on the conventions above, the fixture tests prove the
  conventions are the real ones).
- `copy_gltf.test`: `read_blob` + byte inspection of the GLB header (magic,
  version 2, total length, JSON chunk type), JSON chunk parsed with DuckDB's
  `json` extension when present: node count = object count, names, root
  matrix, accessor `min`/`max`, `asset.extras.origin`, `attributes true`
  puts the attribute into `nodes[i].extras`, image bufferView present, `.gltf`
  writes `.bin` beside. Sidecar-mode refusal without `*_query`; package export
  with `materials_query`.
- Kernel-level C++ test (`test/cpp/`, opt-in like the others): earcut wrapper
  on a square with a square hole gives 8 triangles of the expected total area.

Opt-in (`justfile`, not `make test`): `just test-gltf-validate` exports the
Delft tile to GLB and runs the Khronos `gltf-validator` (npx) expecting zero
errors; `just test-obj-cjio` compares vertex sets with cjio's `export obj` of
the same file.

`docs/FUNCTIONS.md` gains a *Mesh interchange* section with executed
examples; `docs/DESIGN_DOC.md` gains the mesh model and the appearance
resolution rule; `docs/TRAPS.md` gains the local/sidecar ambiguity and the
temp-rename sidecar trap.

## Out of scope

3D Tiles tilesets and b3dm, mesh compression (meshoptimizer / Draco /
`KHR_mesh_quantization`), reading glTF, geometry templates on either side,
CRS reprojection on export (that is `ST_3DTransform`'s job upstream of the
COPY), `EXT_mesh_features` / `EXT_structural_metadata` (follow-up), OBJ
free-form curves, and vertex colours.
