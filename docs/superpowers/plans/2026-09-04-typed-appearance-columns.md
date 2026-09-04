# Typed Appearance Columns — duckdb-cityjson (phase 3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the extension read, write, shift, validate and export the per-LoD appearance columns `material_lodX_Y` / `texture_lodX_Y` as the specification's typed `MAP` columns, flat per WKB face, in both `appearance := 'local'` and `'sidecar'` modes.

**Architecture:** One new kernel, `appearance_flatten.{hpp,cpp}`, turns a CityJSON geometry's `material` / `texture` map into flat per-WKB-face cells using the same face walk `geometry_properties.cpp` uses for `face_semantics`; one new `appearance_cell.{hpp,cpp}` converts those cells to and from DuckDB `Value`s (the scan, the template sidecar, the id functions and the COPY sink all go through it). The scan emits typed cells in both modes — the mode decides only the id space (feature-local vs dataset-global sidecar ids); UVs are inlined in both. The COPY sink re-nests the flat cells from `shells` (reusing `RenestValues`) and re-interns UV pairs into a per-feature `vertices-texture` pool for the CityJSON family; the mesh writers consume the flat cells directly and the depth classifier goes. `cityjson_appearance_ids` / `cityjson_shift_appearance_ids` become MAP-typed overloads, and the SQL that `insert_cityjson`, `cityparquet_merge` and vacuum generate follows.

**Tech Stack:** C++20 DuckDB extension (this repo), nlohmann json, sqllogictest; the Rust CLI (`lib/cityparquet-rs`) only to regenerate the foreign fixture.

**Spec:** `documents/docs/03-specification/04-appearance-templates.mdx` (section *material / texture columns*, invariants and nullability), `01-dataset-package.mdx` (round trip), and the design note `ai/design-notes/specs/2026-09-04-typed-appearance-columns-design.md` (paths relative to the monorepo root `/data2/hideba/cityparquet`). Read them first; the specification is the binding authority.

## Global Constraints

- The DuckDB types, exactly: `material_lodX_Y` is `MAP(VARCHAR, BIGINT[])`; `texture_lodX_Y` is `MAP(VARCHAR, STRUCT(id BIGINT, uv DOUBLE[][])[][])`. Written to Parquet they are the standard MAP groups (`key_value`/`key`/`value`) and read back as the same DuckDB types; the Rust writer's fixture (`test/data/cityparquet_rs_minimal`) is the foreign oracle for that.
- Cell rules (spec): map keys are themes (`""` for the unnamed theme), unique, never empty; a map value is never NULL; `len(material[theme])` equals the WKB face count; `len(texture[theme])` equals the WKB face count; each face entry has one struct per ring in `PolygonZ` ring order; a ring struct is `{id, uv}` with `id` and `uv` NULL together; `uv` has one `[u, v]` per source ring index (the WKB encoder appends the closing point, so that is the WKB point count − 1); a whole-geometry `{"value": n}` is expanded per face; CityJSON's null shorthand (a null shell, a null face, a ring `[null]`) expands to NULL entries / bare rings; a theme whose entries are all null stays present; the cell is NULL only when the geometry carries no material (or texture) at all. The face order is `wkb_encoder.cpp`'s: document order, shells flattened outer-first, no dropping.
- Both `appearance` modes emit this shape. `'local'` (default) keeps the source's feature-local index values as ids and inlines UVs from the feature's own (or the header's) `vertices-texture` pool; `'sidecar'` resolves ids through `AppearanceIndex` as today. A UV index outside the pool, or a texture id that does not resolve, makes that ring bare (`{NULL, NULL}`) — never a NULL pair inside a `uv` list.
- **TDD, strictly** (this repo's `CLAUDE.md`): the failing sqllogictest first, run it and see it fail for the expected reason, then the implementation. `just rebuild` before every `make test` (`make test` does not rebuild).
- Gate per task: `just rebuild && make test` — green except failures the report lists and attributes to a later task; the first fully green suite is Task 5's. The pre-commit hook (clang-format 11.0.1 + clang-tidy, blocking) must run on every commit; never `--no-verify`, never `SKIP_TIDY`.
- Commit in THIS repository (`lib/duckdb-cityjson`, branch `develop`), conventional prefixes (`feat!:`, `refactor:`, `test:`, `docs:`), British English, the trailer `Claude-Session: https://claude.ai/code/session_018spx3RYGD6ZRYC8xaznCLf` as the last line. A breaking change says so with `!` and describes the migration.
- Document the present: no "used to be JSON", no "previously nested". `other` / `surfaces` stay JSON text and their mentions stay.
- Prefer fixtures this extension did not produce for the read side (`cityparquet_rs_minimal`); prefer file-level assertions where row-level ones cannot see.
- Do not touch `lib/duckdb-3d`, `lib/cityparquet-rs` or `documents/` from this plan except where a task names a file there.

---

## File map

| File | Responsibility |
| --- | --- |
| `test/data/cityparquet_rs_minimal/*` + `test/data/README.md` | The foreign fixture, regenerated by the Rust writer (Task 1) |
| `src/include/cityjson/types.hpp`, `src/cityjson/column_types.cpp` | `ColumnType::MaterialMap` / `TextureMap` and their `LogicalType`s (Task 2) |
| `src/cityjson/lod_table.cpp`, `src/cityjson/city_object_utils.cpp`, `src/cityjson/appearance_table_function.cpp` | the per-LoD quartet uses the new kinds/types (Task 2) |
| `src/include/cityjson/appearance_flatten.hpp`, `src/cityjson/appearance_flatten.cpp` (new) | `MaterialCell`, `TextureRing`, `TextureCell`, `FlattenMaterialMap`, `FlattenTextureMap`, the shared per-face walk (Task 2) |
| `src/include/cityjson/appearance_cell.hpp`, `src/cityjson/appearance_cell.cpp` (new) | `MaterialCellValue`, `TextureCellValue` (cell → `Value`), `MaterialCellFromValue`, `TextureCellFromValue` (`Value` → cell), `CellToFlatJson` (Task 2) |
| `src/cityjson/scan_function.cpp`, `src/cityjson/geometry_properties.cpp` | the scan writes cells; the face walk is shared (Task 2) |
| `src/cityjson/appearance_normalise.{cpp,hpp}` | `NormaliseMaterialMap` / `NormaliseTextureMap` deleted; `AppearanceIndex` stays (Task 2) |
| `src/cityjson/cityparquet_appearance.cpp` | MAP-typed `cityjson_appearance_ids` / `cityjson_shift_appearance_ids` (Task 3) |
| `src/cityjson/cityparquet_validate.cpp`, `cityparquet_insert.cpp`, `cityparquet_merge.cpp` | generated SQL drops the `kind` argument (Task 3) |
| `src/cityjson/copy_function.cpp`, `src/cityjson/cityjson_writer.cpp` (or wherever `WriteCityJSONSeq` attaches a feature's appearance block) | re-nesting and UV re-interning on the CityJSON family (Task 4) |
| `src/cityjson/mesh_model.cpp`, `src/cityjson/appearance_source.cpp` | flat cells only; classifier deleted; `UV()` takes pairs only (Task 4) |
| `test/sql/*.test` | every appearance assertion moves to the typed shape (Tasks 2–5) |
| `docs/FUNCTIONS.md`, `docs/DESIGN_DOC.md`, `docs/TRAPS.md`, `documents/docs/07-tutorials/03-duckdb-cityjson.mdx`, `documents/docs/06-resources/02-software.mdx` | the present shape (Task 6) |

---

### Task 1: Regenerate the foreign fixture and pin its types

**Files:**
- Replace: `test/data/cityparquet_rs_minimal/building.parquet`, `bridge.parquet`, `metadata.json`
- Modify: `test/data/README.md` (the `cityparquet_rs_minimal/` paragraph)
- Create: `test/sql/cityparquet_rs_appearance.test`

**Interfaces:**
- Produces: a package whose `material_lod3_0` / `texture_lod3_0` are Parquet MAPs written by the reference implementation — the read-side oracle for every later task.

- [ ] **Step 1: Write the failing test**

`test/sql/cityparquet_rs_appearance.test`:

```
# name: test/sql/cityparquet_rs_appearance.test
# description: The reference writer's typed appearance columns read back as MAPs
# group: [sql]

require cityjson

# The package was written by cityparquet-rs, not by this extension (test/data/README.md).
statement ok
PRAGMA cityparquet_read('test/data/cityparquet_rs_minimal', 'rs');

query TT
SELECT typeof(material_lod3_0), typeof(texture_lod3_0) FROM rs.building LIMIT 1;
----
MAP(VARCHAR, BIGINT[])	MAP(VARCHAR, STRUCT(id BIGINT, uv DOUBLE[][])[][])

# One material id per WKB face: the list length equals the face count of the WKB.
query I
SELECT COUNT(*) FROM rs.building
WHERE material_lod3_0 IS NOT NULL
  AND len(map_values(material_lod3_0)[1]) <> cityjson_wkb_face_count(geometry_lod3_0);
----
0
```

If no `cityjson_wkb_face_count` (or equivalent WKB face-count scalar) exists in this extension, replace the last block with a comparison against `len(geometry_properties_lod3_0.face_semantics)` for rows where that is not NULL, and say so in the report.

- [ ] **Step 2: Run it and watch it fail**

Run: `just rebuild && ./build/release/test/unittest test/sql/cityparquet_rs_appearance.test`
Expected: the `typeof` block fails with `VARCHAR VARCHAR` (the committed fixture predates the typed columns).

- [ ] **Step 3: Regenerate the fixture**

```bash
cd /data2/hideba/cityparquet/lib/cityparquet-rs
cargo run -q -p cityparquet-cli -- convert tests/fixtures/lod3_railway.city.json --output /tmp/claude-1020/-data2-hideba-cityparquet/60ef30a0-0572-40dd-8e68-d18a43dbd3c1/scratchpad/rs_minimal_full --overwrite
cd /data2/hideba/cityparquet/lib/duckdb-cityjson
cp /tmp/claude-1020/-data2-hideba-cityparquet/60ef30a0-0572-40dd-8e68-d18a43dbd3c1/scratchpad/rs_minimal_full/building.parquet test/data/cityparquet_rs_minimal/
cp /tmp/claude-1020/-data2-hideba-cityparquet/60ef30a0-0572-40dd-8e68-d18a43dbd3c1/scratchpad/rs_minimal_full/bridge.parquet test/data/cityparquet_rs_minimal/
```

Then `metadata.json`: take the regenerated one and apply the same hand trim the committed one carries — `git diff` the committed `metadata.json` against the regenerated full one to see exactly which `assets` entries were removed (only `building.parquet` and `bridge.parquet` remain) and reproduce that; keep every `properties` value the regenerated file carries. Do not hand-edit anything else.

- [ ] **Step 4: Run the new test and the three package tests**

Run: `./build/release/test/unittest test/sql/cityparquet_rs_appearance.test test/sql/cityparquet_geoparquet_roundtrip.test test/sql/cityparquet_merge.test test/sql/cityparquet_footer_attributes.test`
Expected: the new test passes; the three existing ones still pass (they assert nothing about appearance; `cityparquet_merge.test`'s `nested_src` merge must still succeed — if its merge now fails at `WidenedType` because the destination's appearance columns are VARCHAR while the fixture's are MAP, note the failure and attribute it to Task 3, which changes the shift function the merge calls).

- [ ] **Step 5: Update `test/data/README.md`**

Add one sentence to the `cityparquet_rs_minimal/` paragraph: "Its `material_lod*` / `texture_lod*` columns are the specification's typed MAPs, flat per WKB face, which is what `cityparquet_rs_appearance.test` pins."

- [ ] **Step 6: Commit**

```bash
git add test/data/cityparquet_rs_minimal test/data/README.md test/sql/cityparquet_rs_appearance.test
git commit -m "test: regenerate the cityparquet-rs fixture with typed appearance columns

The reference writer now emits material_lod* / texture_lod* as MAPs,
flat per WKB face; the fixture carries them and a test pins their types.

Claude-Session: https://claude.ai/code/session_018spx3RYGD6ZRYC8xaznCLf"
```

---

### Task 2: The scan emits typed cells in both modes

**Files:**
- Modify: `src/include/cityjson/types.hpp` (replace `AppearanceJson` with `MaterialMap` and `TextureMap`), `src/cityjson/column_types.cpp` (`ToString` → `"MAP"`, `ToLogicalTypeId` → `MAP`, `ToDuckDBType` builds the two types, `IsComplex`), `src/cityjson/lod_table.cpp:109-110`, `src/cityjson/city_object_utils.cpp:204-205`, `src/cityjson/appearance_table_function.cpp:300-309` (template columns typed) and `:191-207` (template cells written through the new cell code)
- Create: `src/include/cityjson/appearance_flatten.hpp`, `src/cityjson/appearance_flatten.cpp`
- Create: `src/include/cityjson/appearance_cell.hpp`, `src/cityjson/appearance_cell.cpp`
- Modify: `src/cityjson/geometry_properties.cpp` (expose the per-face walk), `src/cityjson/scan_function.cpp:73-104`, `src/cityjson/appearance_normalise.{hpp,cpp}` (delete `NormaliseMaterialMap`, `NormaliseTextureMap`, `RemapMaterialValues`, `RemapTextureValues`, `IsRing`, `RewriteThemes`; keep `AppearanceIndex`, `MaterialKey`, `TextureKey`), `CMakeLists.txt` (new sources)
- Tests: `test/sql/cityjson_appearance.test`, `test/sql/cityjson_appearance_sidecar.test`, `test/sql/cityparquet_column_order.test`, `test/sql/cityjson_bbox.test`, `test/sql/cityjson_bind_data_copy.test`, `test/sql/cityjson_notebook_e2e.test`, `test/sql/cityjson_seq_multipass.test` — whichever of these assert on cell text or `typeof`

**Interfaces:**

```cpp
// appearance_flatten.hpp
namespace duckdb::cityjson {
struct MaterialCell { std::vector<std::pair<std::string, std::vector<std::optional<int64_t>>>> themes; };
struct TextureRing  { std::optional<int64_t> id; std::vector<std::array<double, 2>> uv; /* empty iff !id */ };
struct TextureCell  { std::vector<std::pair<std::string, std::vector<std::vector<TextureRing>>>> themes; };

//! One entry per WKB face in wkb_encoder order: the `values` leaf for that face
//! (a material id, or a texture face's ring array), or null. Expands CityJSON's
//! null shorthand at every level. Shared with geometry_properties.cpp.
json FlattenPerFace(const Geometry &geometry, const json &values);
//! The ring count of every face, in the same order.
std::vector<size_t> RingCountsPerFace(const Geometry &geometry);
//! The vertex count of every ring of every face (source indices; the WKB point count minus one).
std::vector<std::vector<size_t>> RingVertexCountsPerFace(const Geometry &geometry);

using IdResolver = std::function<int64_t(int64_t)>;   // returns -1 when unresolvable
MaterialCell FlattenMaterialMap(const Geometry &geometry, const json &material_map, const IdResolver &resolve);
TextureCell  FlattenTextureMap(const Geometry &geometry, const json &texture_map, const IdResolver &resolve,
                               const std::vector<std::array<double, 2>> &uv_pool);
}

// appearance_cell.hpp
namespace duckdb::cityjson {
LogicalType MaterialCellType();   // MAP(VARCHAR, BIGINT[])
LogicalType TextureCellType();    // MAP(VARCHAR, STRUCT(id BIGINT, uv DOUBLE[][])[][])
Value MaterialCellValue(const MaterialCell &cell);   // Value::MAP(...)
Value TextureCellValue(const TextureCell &cell);
MaterialCell MaterialCellFromValue(const Value &value);   // throws InvalidInputException on a malformed value
TextureCell  TextureCellFromValue(const Value &value);
//! `{"<theme>": {"values": [...]}}` — material: ids/null per face; texture: per face a ring array,
//! each ring `[id, [u, v], ...]` or `[null]`. What the COPY sink and the mesh model consume.
json MaterialCellToFlatJson(const MaterialCell &cell);
json TextureCellToFlatJson(const TextureCell &cell);
}
```

`ColumnTypeUtils::ToDuckDBType(ColumnType::MaterialMap)` returns `MaterialCellType()`, `TextureMap` → `TextureCellType()`.

- [ ] **Step 1: Write the failing tests**

Replace the `typeof` block of `test/sql/cityjson_appearance.test` with

```
query TT
SELECT typeof(material_lod3_0), typeof(texture_lod3_0)
FROM read_cityjsonseq('test/data/railway_appearance.city.jsonl') LIMIT 1;
----
MAP(VARCHAR, BIGINT[])	MAP(VARCHAR, STRUCT(id BIGINT, uv DOUBLE[][])[][])
```

and its `LIKE '%"visual"%'` blocks with `map_contains(material_lod3_0, 'visual')` / `map_contains(texture_lod3_0, 'visual')` (if `map_contains` is unavailable in the pinned DuckDB, use `'visual' IN map_keys(...)`). Add these blocks to the same file:

```
# One material id per WKB face, in the 'visual' theme.
query I
SELECT len(material_lod3_0['visual'])
FROM read_cityjsonseq('test/data/railway_appearance.city.jsonl')
WHERE id = 'GMLID_855011_330784_753';
----
<the WKB face count of that object — derive it once with len(geometry_properties_lod3_0.face_semantics) or from the fixture's boundaries, and write the literal here>

# A whole-geometry material (`{"value": n}` in the source) is expanded to one id per face.
query I
SELECT list_distinct(material_lod2_2['visual']) FROM read_cityjson('test/data/solid_material.city.json', lod := '2.2');
----
[0, 1, 2]

# The reader's cell is flat: a Solid's material is one id per WKB face, not one list per shell.
query I
SELECT material_lod2_2['visual'] FROM read_cityjson('test/data/solid_material.city.json', lod := '2.2');
----
[0, 1, 1, 1, 1, 2]

# A texture cell nests per face, then per ring: six faces, one ring each, four UV pairs per ring,
# UVs inlined from the feature's vertices-texture pool even in local mode.
query III
SELECT len(texture_lod2_2['visual']), len(texture_lod2_2['visual'][1]), len(texture_lod2_2['visual'][1][1].uv)
FROM read_cityjson('test/data/solid_texture.city.json', lod := '2.2');
----
6	1	4

# A face with no texture is one bare ring per ring: id and uv NULL together.
query II
SELECT texture_lod2_2['visual'][1][1].id IS NULL, texture_lod2_2['visual'][1][1].uv IS NULL
FROM read_cityjson('test/data/null_lead_texture.city.json', lod := '2.2');
----
true	true
```

(`solid_material.city.json`'s reader cell is `[[0,1,1,1,1,2]]` today per `copy_obj.test`; `null_lead_texture`'s first face is a bare `null`.) In `test/sql/cityjson_appearance_sidecar.test`, re-pin the UV inlining with typed accessors instead of `LIKE`:

```
# Sidecar mode inlines UVs; the first ring of the textured object's first face reads the
# pool's [2.3283218, 0.8466667] under texture id 2 (interned after the header's pair).
query II
SELECT texture_lod3_0['visual'][1][1].id, texture_lod3_0['visual'][1][1].uv[1]
FROM read_cityjsonseq('test/data/railway_appearance.city.jsonl', appearance := 'sidecar')
WHERE texture_lod3_0 IS NOT NULL;
----
2	[2.3283218, 0.8466667]

# Local mode carries the same shape with the feature-local id (0) and the same inlined pair.
query II
SELECT texture_lod3_0['visual'][1][1].id, texture_lod3_0['visual'][1][1].uv[1]
FROM read_cityjsonseq('test/data/railway_appearance.city.jsonl')
WHERE texture_lod3_0 IS NOT NULL;
----
0	[2.3283218, 0.8466667]
```

and the template cell `{"":{"values":[0,0]}}` becomes `{'': [0, 0]}` (DuckDB's MAP rendering — run it once and paste the exact rendering). Delete the local-mode `NOT LIKE '%,[%.%]%'` block (local mode inlines too now).

- [ ] **Step 2: Run and watch them fail**

Run: `just rebuild && ./build/release/test/unittest test/sql/cityjson_appearance.test test/sql/cityjson_appearance_sidecar.test`
Expected: the `typeof` block fails with `VARCHAR VARCHAR`; the accessor blocks fail to bind on a VARCHAR.

- [ ] **Step 3: Implement**

1. `appearance_flatten.cpp`: `FlattenPerFace` is `FlattenFaceSemantics`'s walk generalised — move that walk here (keeping `FlattenShellValues`'s semantics: a missing/short/null values level yields nulls sized by `boundaries`) and make `geometry_properties.cpp` call `FlattenPerFace` for `face_semantics`, so semantics and appearance share ONE face order. `FlattenMaterialMap`: per theme, `values` → `FlattenPerFace` → each entry `resolve(id)` (`< 0` or non-integer → null); `value` → `n_faces` copies of `resolve(value)`; a theme with neither key → skipped with nothing (do not throw: the reader is lenient on source data, and the theme carries nothing). `FlattenTextureMap`: per theme, `FlattenPerFace` gives per-face ring arrays (or null); for face `i` with `RingVertexCountsPerFace(geometry)[i]` ring sizes: a null face → one bare ring per ring; an array → ring `r` from entry `r` (missing → bare; `[null]` → bare; `[t, uvIdx…]` → `id = resolve(t)` (unresolvable → bare ring), `uv` = the first `vertex_count` indices looked up in `uv_pool` (an index outside the pool, a non-integer, or fewer indices than vertices → the ring is bare and the cell is still written); surplus indices beyond the vertex count are dropped.
2. `appearance_cell.cpp`: `Value::MAP(LogicalType::VARCHAR, <value type>, keys, values)`; a texture ring struct `Value::STRUCT({{"id", ..}, {"uv", ..}})` with `Value(LogicalType::BIGINT)` / `Value(LogicalType::LIST(LogicalType::LIST(LogicalType::DOUBLE)))` for the nulls; `MaterialCellFromValue` / `TextureCellFromValue` walk `MapValue::GetChildren` / `ListValue::GetChildren` / `StructValue::GetChildren` and throw `InvalidInputException` on any shape violation (a NULL map value, a NULL face or ring, `id`/`uv` disagreeing on nullness, a pair without two values).
3. `scan_function.cpp`: the `AppearanceJson` branch becomes two branches (`MaterialMap` / `TextureMap`). Resolver: sidecar mode `[&](int64_t local) { return index.ResolveMaterial(feature.id, local); }`; local mode identity. UV pool: the feature's own, else the header's, else empty — in BOTH modes. Write with `wrappers[col_idx]...` — use `output.data[col_idx].SetValue(output_row, MaterialCellValue(cell))` (the same `SetValue` route the template sidecar uses; appearance cells are sparse and this path is not the hot one — say so in a comment) or a null. A geometry with no material → NULL cell; a map that flattens to zero themes → NULL cell.
4. `appearance_table_function.cpp`: template columns typed via `ColumnTypeUtils::ToDuckDBType(ColumnType::MaterialMap|TextureMap)`; the scan writes `MaterialCellValue(FlattenMaterialMap(geometry, *geometry.material, header resolver))` etc. with the header UV pool.
5. `column_types.cpp` / `types.hpp` / `lod_table.cpp` / `city_object_utils.cpp`: the enum split and the two types. Grep for every `AppearanceJson` use (`vector_writer.hpp:203` comment, `IsComplex`, docs strings) and update each.
6. Delete the dead normaliser functions; keep `AppearanceIndex`.

- [ ] **Step 4: Run the suite, record the red**

Run: `just rebuild && make test 2>&1 | tail -60`
Expected: the two edited files pass; remaining failures are in files that call `cityjson_appearance_ids` / `cityjson_shift_appearance_ids` on the columns (Task 3), COPY round trips and mesh COPYs (Task 4), insert/merge/vacuum (Task 5). List each failing file with its first failing assertion and the owning task in the report. Fix any other failure now.

- [ ] **Step 5: Commit**

```bash
git add src test CMakeLists.txt
git commit -m "feat!: emit the appearance columns as typed MAPs, flat per WKB face

material_lod* is MAP(VARCHAR, BIGINT[]) and texture_lod* is
MAP(VARCHAR, STRUCT(id BIGINT, uv DOUBLE[][])[][]) in both appearance
modes; the mode decides the id space only. Cells are flattened with the
walk face_semantics uses, a whole-geometry value is expanded per face and
UVs are inlined from the feature's pool. Migration: read the cells with
map/list/struct accessors instead of JSON text.

Claude-Session: https://claude.ai/code/session_018spx3RYGD6ZRYC8xaznCLf"
```

---

### Task 3: MAP-typed id functions, and the SQL that calls them

**Files:**
- Modify: `src/cityjson/cityparquet_appearance.cpp` (+ `.hpp`): `cityjson_appearance_ids(cell) -> BIGINT[]` with two overloads (material MAP, texture MAP); `cityjson_shift_appearance_ids(cell, offset BIGINT)` with two overloads returning the input's type. The `kind` argument is gone — the type says which. Implemented over `MaterialCellFromValue` / `TextureCellFromValue` and `MaterialCellValue` / `TextureCellValue` (row-at-a-time `Value`s, like today's JSON parse).
- Modify: `src/cityjson/cityparquet_validate.cpp:88-95`, `src/cityjson/cityparquet_insert.cpp:431-441, 458-477`, `src/cityjson/cityparquet_merge.cpp:219-234, 245-274`: drop the `'material'`/`'texture'` literal from the generated calls (`cityjson_appearance_ids(col)`, `cityjson_shift_appearance_ids(col, offset)`).
- Tests: `test/sql/cityparquet_vacuum.test:7-46`, `test/sql/cityparquet_merge.test:11-41` (unit tests on literals), plus the column-level uses in `cityjson_appearance_sidecar.test:88-106`, `cityjson_appearance_roundtrip.test:63-65`, `cityjson_seq_multipass.test`, `cityparquet_insert.test`, `cityparquet_merge.test`, `cityparquet_vacuum.test`.

**Interfaces:**
- Consumes: Task 2's `appearance_cell.hpp`.
- Produces: the two overloaded scalar functions; `Value`-typed MAP literals in tests use DuckDB syntax `MAP {'visual': [0, 0, 1, 1, NULL, 2]}` and `MAP {'visual': [[{'id': 0, 'uv': [[0.0, 0.0], [1.0, 0.0], [1.0, 1.0]]}], [{'id': 5, 'uv': [[0.5, 0.25], [0.5, 0.5], [0.25, 0.5]]}]]}`.

- [ ] **Step 1: Rewrite the unit tests first**

`test/sql/cityparquet_vacuum.test` (the `cityjson_appearance_ids` section) becomes:

```
# material: one id per face, deduplicated, nulls ignored.
query T
SELECT cityjson_appearance_ids(MAP {'visual': [0, 0, 1, 1, NULL, 2]}::MAP(VARCHAR, BIGINT[]));
----
[0, 1, 2]

# Every theme contributes; the outer key set is dynamic.
query T
SELECT cityjson_appearance_ids(MAP {'a': [1, 1], 'b': [2, NULL]}::MAP(VARCHAR, BIGINT[]));
----
[1, 2]

# texture: per face, per ring, only the struct's id is a reference; a bare ring contributes nothing.
query T
SELECT cityjson_appearance_ids(MAP {'visual': [
  [{'id': 0, 'uv': [[0.0, 0.0], [1.0, 0.0], [1.0, 1.0]]}],
  [{'id': 5, 'uv': [[0.5, 0.25], [0.5, 0.5], [0.25, 0.5]]}, {'id': NULL, 'uv': NULL}]
]}::MAP(VARCHAR, STRUCT(id BIGINT, uv DOUBLE[][])[][]));
----
[0, 5]

query I
SELECT cityjson_appearance_ids(NULL::MAP(VARCHAR, BIGINT[])) IS NULL;
----
true
```

and `test/sql/cityparquet_merge.test`'s shift section:

```
query T
SELECT cityjson_shift_appearance_ids(MAP {'visual': [0, 1, NULL]}::MAP(VARCHAR, BIGINT[]), 10);
----
{visual=[10, 11, NULL]}

# Only a ring's id moves; the [u, v] pairs stay exactly as they are.
query T
SELECT cityjson_shift_appearance_ids(MAP {'visual': [[{'id': 0, 'uv': [[0.5, 0.25], [0.5, 0.5], [0.25, 0.5]]}]]}::MAP(VARCHAR, STRUCT(id BIGINT, uv DOUBLE[][])[][]), 4)['visual'][1][1];
----
{'id': 4, 'uv': [[0.5, 0.25], [0.5, 0.5], [0.25, 0.5]]}
```

Run each once after implementing and paste DuckDB's exact MAP/STRUCT rendering into the expected lines. Delete the `'{"visual":{"value":7}}'` and `kind must be` blocks (no `value` form, no `kind`).

- [ ] **Step 2: Run and watch them fail** — `just rebuild && ./build/release/test/unittest test/sql/cityparquet_vacuum.test test/sql/cityparquet_merge.test` — expected: binder errors (no MAP overload).

- [ ] **Step 3: Implement**, update the three SQL generators, then update the column-level test uses listed in Files (drop the kind argument; where a test compared shifted JSON text with `LIKE '%2%'`, assert the typed value, e.g. `material_lod2_2['visual'] = [2, 2]`).

- [ ] **Step 4: Run the suite, record the red** — `just rebuild && make test 2>&1 | tail -60` — expected: insert/merge/vacuum now green; remaining failures are COPY-side (Task 4) — list them.

- [ ] **Step 5: Commit**

```bash
git add src test
git commit -m "feat!: cityjson_appearance_ids and cityjson_shift_appearance_ids take the typed cells

The MAP type says whether a cell is material or texture, so the kind
argument is gone; insert, merge and vacuum generate the new calls.

Claude-Session: https://claude.ai/code/session_018spx3RYGD6ZRYC8xaznCLf"
```

---

### Task 4: COPY — re-nest for CityJSON, consume flat for meshes

**Files:**
- Modify: `src/cityjson/copy_function.cpp`: `apply_appearance` (lines ~1210–1236) reads the MAP `Value` through `MaterialCellFromValue` / `TextureCellFromValue`, converts with `*CellToFlatJson`, and (CityJSON / CityJSONSeq / FlatCityBuf formats only) re-nests each theme's `values` with `RenestValues(type, flat, shells)` (the `shells` and `type` come from the row's matching `geometry_properties_*` struct, already parsed by `apply_properties`; textures re-nest at the face level with each face's ring array as one unit) and rewrites texture rings from `[id, [u, v], …]` to `[id, uvIdx, …]` against a per-feature UV interner (bitwise `[u, v]` key → index, first-use order) whose pool becomes that output feature's `vertices-texture`. Locate where `CityJSONWriter::WriteCityJSONSeq` / `WriteCityJSON` attach `source_appearance_by_feature[feature]` and `source_appearance_header`, and replace the verbatim `vertices-texture` with the re-interned pool (materials / textures arrays stay verbatim: local ids line up with them). The un-suffixed legacy `geom_lod*` STRUCT path is untouched.
- Modify: `src/cityjson/mesh_model.cpp`: delete `ValuesDepth`, `ClassifyValues`, `ValuesShape`, `FirstMeasurable`, `CarriesNothing`, the `Nested` walk in `ForEachFace`, and the `value` broadcast branch; `BuildMeshModel` indexes the flat `values` by face position only (the `Flat` branch that exists today). `src/cityjson/appearance_source.cpp`: `UV()` accepts an inline `[u, v]` pair only; an integer is an `InvalidInputException` naming the feature in both forms (the local-form pool lookup and `uv_pool_by_feature_` / `header_uv_pool_` go). `TRAPS.md`'s "two shapes" paragraph goes in Task 6.
- Tests: `test/sql/copy_obj.test` (the nested-vs-flat `REPLACE` blocks: keep ONE assertion per fixture that the reader's own cell colours the faces; delete the hand-substituted alternate shape and its prose, since the reader now produces the spec shape), `test/sql/copy_mesh_appearance.test`, `test/sql/copy_gltf.test`, `test/sql/cityjson_appearance_roundtrip.test`, `test/sql/cityjson_appearance.test` (the COPY round-trip blocks), `test/sql/read_obj.test` if it pins a cell.

**Interfaces:**
- Consumes: `appearance_cell.hpp`, `RenestValues`.

- [ ] **Step 1: Write the failing round-trip test**

Add to `test/sql/cityjson_appearance_roundtrip.test` (the `json` extension is not linked in this build, so the file-level assertions are regexps over the text, as `copy_gltf.test` does):

```
# A Solid's material comes back nested per shell, and a texture's UV pairs come back as
# indices into a rebuilt vertices-texture pool -- the file, not the row, is the oracle.
statement ok
COPY (SELECT * FROM read_cityjson('test/data/solid_texture.city.json', lod := '2.2'))
TO '__TEST_DIR__/solid_texture_rt.city.json' (FORMAT cityjson);

# One shell: the material values are a list of one list.
query I
SELECT regexp_matches(content, '"material":\{"visual":\{"values":\[\[[0-9,]+\]\]\}\}')
FROM read_text('__TEST_DIR__/solid_texture_rt.city.json');
----
true

# A ring is an id followed by four integer UV indices, never [u, v] pairs, and the pool exists.
query II
SELECT regexp_matches(content, '\[0,[0-9]+,[0-9]+,[0-9]+,[0-9]+\]'),
       regexp_matches(content, '"vertices-texture":\[\[')
FROM read_text('__TEST_DIR__/solid_texture_rt.city.json');
----
true	true

# Reading the written file back yields the same cells as the source: value-equal round trip.
query I
SELECT COUNT(*) FROM (
  SELECT material_lod2_2['visual'] AS m, texture_lod2_2['visual'][1][1].uv AS uv
  FROM read_cityjson('__TEST_DIR__/solid_texture_rt.city.json', lod := '2.2')
  EXCEPT ALL
  SELECT material_lod2_2['visual'], texture_lod2_2['visual'][1][1].uv
  FROM read_cityjson('test/data/solid_texture.city.json', lod := '2.2')
);
----
0
```

(`solid_texture.city.json` carries a texture only; if it has no material, drop the material regexp there and put it on a `solid_material.city.json` round trip instead.)

- [ ] **Step 2: Run and watch it fail** — `just rebuild && ./build/release/test/unittest test/sql/cityjson_appearance_roundtrip.test` — expected: today's `apply_appearance` calls `ParseJson` on a MAP's `ToString()` and swallows the error, so the written file has no texture; the regexps fail.

- [ ] **Step 3: Implement** as described in Files; then update the listed tests. For `copy_obj.test`, keep the assertion that a Solid's material colours faces `0,1,1,1,1,2` and the textured Solid's `vt`/`f v/vt` output, driven by the reader's own cell.

- [ ] **Step 4: Run the whole suite** — `just rebuild && make test` — expected: green except anything you attribute to Task 5 (insert/merge on the regenerated fixture, if Task 3 left something). Include the summary line.

- [ ] **Step 5: Commit**

```bash
git add src test
git commit -m "feat: COPY re-nests the flat appearance cells and re-interns UV pools

The CityJSON family gets CityJSON's per-shell nesting and a rebuilt
vertices-texture pool per feature; the mesh writers read the flat cells
by face position, and the nesting classifier is gone.

Claude-Session: https://claude.ai/code/session_018spx3RYGD6ZRYC8xaznCLf"
```

---

### Task 5: The first fully green suite, and the opt-in harnesses

**Files:**
- Read/modify: any test still red after Task 4; `test/cpp/` harnesses if they compile against a changed signature (`run_obj_parser_tests.sh`, `run_face_triangulation_tests.sh` — they do not touch appearance, confirm and say so).

- [ ] **Step 1: Run everything**

```bash
just rebuild && make test
test/cpp/run_obj_parser_tests.sh
test/cpp/run_face_triangulation_tests.sh
just test-obj-cjio     # if cjio is installed; otherwise say it was skipped
just test-gltf-validate  # if the validator is installed; otherwise say it was skipped
```

Expected: `make test` fully green; record the assertion/test-case counts. Fix whatever is left (each fix with its failing-first evidence in the report).

- [ ] **Step 2: Commit** (only if something changed)

```bash
git add src test
git commit -m "test: the appearance suite is green on the typed cells

Claude-Session: https://claude.ai/code/session_018spx3RYGD6ZRYC8xaznCLf"
```

---

### Task 6: Documentation

**Files:**
- Modify: `docs/FUNCTIONS.md` — the schema table rows (~1606–1607): `MAP(VARCHAR, BIGINT[])` "theme → one sidecar id (or NULL) per WKB face" / `MAP(VARCHAR, STRUCT(id BIGINT, uv DOUBLE[][])[][])` "theme → per WKB face → per ring → the id and its [u, v] pairs"; the "Appearance sidecars" section (~409–467): the example cell in the new rendering, the `appearance` option text ("both modes emit the typed cells; the mode decides the id space; UVs are inlined in both"), the "Texture UVs are inlined" paragraph; the `cityjson_appearance_ids` / `cityjson_shift_appearance_ids` docs (~483–495): new signatures, no `kind`; the Solid worked example (~1161). Add a short "Reading appearance cells" example with the accessor idioms (`material_lod2_2['visual'][3]`, `texture_lod2_2['visual'][1][1].uv`, `UNNEST`).
- Modify: `docs/DESIGN_DOC.md` §7 (the cell shape, both modes) and §9 (delete the dual-shape paragraph; the mesh writers consume flat cells by face position; the CityJSON sink re-nests from `shells` and re-interns UV pools).
- Modify: `docs/TRAPS.md` — delete "A material/texture cell also comes in two shapes"; rewrite "Local and sidecar material cells are indistinguishable" (still true: only the id space differs, so the mesh COPY is still told the form via the `*_query` options); add: "The scan writes appearance cells through `Value::MAP` + `SetValue`, not the flat-vector writers — fine for sparse appearance, wrong for a hot column."
- Modify (monorepo): `documents/docs/07-tutorials/03-duckdb-cityjson.mdx` column table rows (`material_lodX_Y` | `MAP(VARCHAR, BIGINT[])` | …; `texture_lodX_Y` | `MAP(VARCHAR, STRUCT(id BIGINT, uv DOUBLE[][])[][])` | …) and any sentence there saying the cells are JSON text; `documents/docs/06-resources/02-software.mdx` — remove the "Known deviations" sentence about JSON appearance cells added in phase 1.
- `CLAUDE.md` / `AGENTS.md` (byte-identical): only if a workflow sentence mentions the JSON cells.

- [ ] **Step 1: Sweep**

```bash
grep -rn "JSON (VARCHAR)\|values\":\|nested, per-shell\|two \*shapes\*\|two shapes\|verbatim" docs/FUNCTIONS.md docs/DESIGN_DOC.md docs/TRAPS.md CLAUDE.md | grep -i "material\|texture\|appearance"
```

Read each hit in context and rewrite the ones that describe the old cells.

- [ ] **Step 2: Gates** — `just rebuild && make test` (docs only, but the tidy hook still runs) and, from the monorepo root, `just docs-build`.

- [ ] **Step 3: Commit** (extension repo, then the two `documents/` files are committed in the monorepo by the controller with the submodule pointer bump)

```bash
git add docs CLAUDE.md AGENTS.md
git commit -m "docs: describe the typed appearance cells

Claude-Session: https://claude.ai/code/session_018spx3RYGD6ZRYC8xaznCLf"
```

---

## Not in this plan

- The monorepo side: bumping the `lib/duckdb-cityjson` pointer, `just mcp-corpus`, root `just check`, committing the two `documents/` edits, pushing both repositories — the controller does these after the final review.
- Any change under `lib/cityparquet-rs` (phase 2, done) or `lib/duckdb-3d`.
