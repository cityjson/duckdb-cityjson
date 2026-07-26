# CityParquet Appearance Normalisation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make this extension able to emit a spec-conformant CityParquet appearance layer — dataset-global sidecar ids and inlined texture UVs — so `insert_cityjson` becomes possible.

**Architecture:** Parse the CityJSON `appearance` object (currently not parsed at all) into typed structs; expose the definitions as `cityjson_materials(path)` / `cityjson_textures(path)` table functions shaped like the spec's sidecar tables; and add `appearance := 'sidecar'` to the readers, which rewrites each geometry's feature-local material/texture indices into dataset-global ids and replaces texture UV *indices* with the actual `[u, v]` pairs.

**Tech Stack:** C++17, DuckDB extension API, nlohmann::json, SQL logic tests.

## Scope

This is **plan 2 of 3**. It delivers appearance normalisation only. Package I/O (`cityparquet_read` / `cityparquet_write`) and `insert_cityjson` / `cityparquet_merge` follow in plan 3, and both depend on this.

Design: `docs/superpowers/specs/2026-07-25-cityparquet-mutation-functions-design.md`.
Plan 1 (shipped): `docs/superpowers/plans/2026-07-25-cityparquet-mutation-core.md`.

## Global Constraints

- Everything in plan 1's "Corrections discovered during implementation" section still applies — read it. In particular: no `JSON` type, no `json_extract`, `StringUtil::Join` takes `duckdb::vector`, and the two ODR link traps.
- Build: `cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb unittest`
- Test one file: `./build/release/test/unittest --test-dir . "test/sql/<name>.test"`
- Every new `.cpp` goes in `EXTENSION_SOURCES` in `CMakeLists.txt`.
- **`appearance := 'local'` stays the default.** The existing behaviour and the `COPY TO cityjson` round trip must not change; every current test must keep passing untouched.

## Where appearance actually lives

Verified against `test/data/railway_appearance.city.jsonl`:

- **CityJSON** (`.city.json`): one top-level `appearance` object with `materials`, `textures`, `vertices-texture`. Everything is document-global.
- **CityJSONSeq** (`.city.jsonl`): the **header line** carries `appearance.materials` and `appearance.textures` — the shared *definitions*. **Each feature line** carries its own `appearance`, whose `vertices-texture` is that feature's local UV pool (the fixture's line 2 has 115 of them and an empty `materials` array).

So material and texture **definitions** are dataset-global in both formats, while **UV pools are per-feature** in Seq. That asymmetry is the whole reason UVs must be inlined: a stored UV index would be meaningless once features are merged into one table.

A feature *may* also carry its own non-empty `materials` / `textures`. Task 2 handles the common case (header-only) and **rejects** the other with a clear error rather than silently mis-numbering; Task 6 lifts that restriction.

## Sidecar id assignment

`id` = the ordinal position in the source's `appearance.materials` / `appearance.textures` array, 0-based. This is deterministic, requires no interning, and matches what `cityparquet-rs` must also do for packages from the two implementations to merge. It is stated normatively in the design doc.

---

### Task 1: Parse the `appearance` object

**Files:**
- Modify: `src/include/cityjson/cityjson_types.hpp` — add `Material`, `Texture`, `Appearance`; add `std::optional<Appearance> appearance` to `CityJSON` and to `CityJSONFeature`
- Modify: `src/cityjson/cityjson_types.cpp` — `FromJson` / `ToJson` for each
- Test: `test/sql/cityjson_appearance_sidecar.test` (first assertions only; grows through Tasks 2–5)

**Interfaces:**
- Consumes: nothing.
- Produces:
  ```cpp
  struct Material {
      std::optional<std::string> name;
      std::optional<double> ambient_intensity;
      std::optional<std::vector<double>> diffuse_color;   // 3 values in [0,1]
      std::optional<std::vector<double>> specular_color;
      std::optional<std::vector<double>> emissive_color;
      std::optional<double> transparency;
      std::optional<double> shininess;
      std::optional<bool> is_smooth;
      json other;                                          // unmapped members
      static Material FromJson(const json &obj);
  };
  struct Texture {
      std::optional<std::string> image_uri;      // CityJSON "image"
      std::optional<std::string> image_type;     // CityJSON "type", verbatim ("PNG")
      std::optional<std::string> wrap_mode;
      std::optional<std::string> texture_type;
      std::optional<std::vector<double>> border_color;  // 4 values
      json other;
      static Texture FromJson(const json &obj);
  };
  struct Appearance {
      std::vector<Material> materials;
      std::vector<Texture> textures;
      std::vector<std::array<double, 2>> vertices_texture;  // "vertices-texture"
      static Appearance FromJson(const json &obj);
  };
  ```
  `CityJSON::appearance` and `CityJSONFeature::appearance`, both `std::optional<Appearance>`.

Note the CityJSON→spec name changes: `image` → `image_uri`, `type` → `image_type`. `image_data` (the spec's optional embedded bytes) has no CityJSON source and is always null here.

- [ ] **Step 1: Write the failing test**

Create `test/sql/cityjson_appearance_sidecar.test`:

```
# name: test/sql/cityjson_appearance_sidecar.test
# description: Dataset-global sidecar ids and inlined texture UVs
# group: [sql]

require cityjson

# The two materials in the CityJSONSeq header become two sidecar rows, ids
# assigned by ordinal position in appearance.materials.
query IT
SELECT id, name FROM cityjson_materials('test/data/railway_appearance.city.jsonl') ORDER BY id;
----
0	UUID_e58d9d68-2e8c-4b81-bec1-1892e9d71c81
1	UUID_f55b5612-d5f9-4ef8-842d-ca4849d90b59

# Colour columns are typed lists of three values in [0,1], not JSON.
query T
SELECT typeof(diffuseColor) FROM cityjson_materials('test/data/railway_appearance.city.jsonl') LIMIT 1;
----
DOUBLE[]

query I
SELECT COUNT(*) FROM cityjson_materials('test/data/railway_appearance.city.jsonl')
WHERE len(diffuseColor) = 3 AND len(specularColor) = 3 AND len(emissiveColor) = 3;
----
2
```

- [ ] **Step 2: Run it and confirm it fails** — `Catalog Error: cityjson_materials does not exist`.

```bash
./build/release/test/unittest --test-dir . "test/sql/cityjson_appearance_sidecar.test"
```

- [ ] **Step 3: Add the structs to `cityjson_types.hpp`** exactly as in Interfaces above, placed before `struct CityJSON`.

- [ ] **Step 4: Implement `FromJson` in `cityjson_types.cpp`.** For each struct, read the known members into typed fields and put every unrecognised member into `other`. Guard the colour arrays: read them only when the value is an array of numbers, leave `nullopt` otherwise — a malformed colour must not abort the whole read.

- [ ] **Step 5: Parse it in the readers.** In `CityJSON::FromJson`, populate `appearance` from the `"appearance"` member. In the CityJSONSeq feature parser, do the same per feature.

- [ ] **Step 6: Build.** The test still fails (no table function yet) — that is expected; Task 2 closes it.

```bash
cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb unittest
```

- [ ] **Step 7: Commit**

```bash
git add src/include/cityjson/cityjson_types.hpp src/cityjson/cityjson_types.cpp test/sql/cityjson_appearance_sidecar.test
git commit -m "feat(appearance): parse the CityJSON appearance object

Material, texture and UV-pool definitions were not parsed at all -- only
the per-geometry theme maps were carried through. Nothing can emit a
conformant sidecar without them."
```

---

### Task 2: `cityjson_materials` and `cityjson_textures`

**Files:**
- Create: `src/include/cityjson/appearance_table_function.hpp`
- Create: `src/cityjson/appearance_table_function.cpp`
- Modify: `CMakeLists.txt`, `src/cityjson_extension.cpp`
- Test: `test/sql/cityjson_appearance_sidecar.test` (the Task 1 assertions now pass)

**Interfaces:**
- Consumes: `Appearance`, `Material`, `Texture` (Task 1); `OpenAnyCityJSONFile` (`reader_factory.cpp`).
- Produces:
  - `cityjson_materials(path)` → `id BIGINT, name VARCHAR, ambientIntensity DOUBLE, diffuseColor DOUBLE[], specularColor DOUBLE[], emissiveColor DOUBLE[], transparency DOUBLE, shininess DOUBLE, isSmooth BOOLEAN, other VARCHAR`
  - `cityjson_textures(path)` → `id BIGINT, image_uri VARCHAR, image_data BLOB, image_type VARCHAR, wrapMode VARCHAR, textureType VARCHAR, borderColor DOUBLE[], other VARCHAR`
  - `void RegisterAppearanceTableFunctions(ExtensionLoader &loader);`

Column names and order follow the spec's sidecar tables exactly, including its mixed casing (`ambientIntensity`, `wrapMode`) — those are the spec's names, not a style choice.

- [ ] **Step 1: The failing test is already written** (Task 1, Step 1). Re-run it to confirm the same failure.

- [ ] **Step 2: Implement the two table functions.** Both open the file via `OpenAnyCityJSONFile`, read the *header* appearance (`reader->ReadMetadata()`), and emit one row per array entry with `id` = ordinal.

  **Reject the unsupported case explicitly.** If any *feature* carries a non-empty `materials` or `textures` array, throw:
  ```
  cityjson_materials: per-feature appearance definitions are not yet supported
  (feature '<id>' declares its own materials); only the dataset header's
  appearance is read. See Task 6.
  ```
  Silently ignoring them would mis-number every id downstream, which is worse than refusing.

- [ ] **Step 3: Register**, build, run the test until green.

- [ ] **Step 4: Run the full suite** — `./build/release/test/unittest --test-dir . "test/sql/*"` — and commit.

---

### Task 3: `appearance := 'sidecar'` — global material ids

**Files:**
- Modify: `src/include/cityjson/reader.hpp` (`CityJSONReadOptions`), `src/cityjson/bind_function.cpp` (parse the option), `src/cityjson/table_function_registration.cpp` (declare it), `src/cityjson/scan_function.cpp` or `city_object_utils.cpp` (apply it)
- Test: `test/sql/cityjson_appearance_sidecar.test`

**Interfaces:**
- Consumes: Task 1's structs.
- Produces: `read_cityjson(path, appearance := 'sidecar')`, likewise on `read_cityjsonseq` and `read_flatcitybuf`. Values: `'local'` (default, unchanged) and `'sidecar'`.

Because material definitions are dataset-global in both formats, a material index in a geometry's theme map **already equals** its sidecar id. So for materials this mode is close to a no-op — assert that rather than assume it, because it is what makes the whole scheme coherent.

- [ ] **Step 1: Write the failing test** — append to `test/sql/cityjson_appearance_sidecar.test`:

```
# Every material id a geometry references resolves against the sidecar.
query I
SELECT COUNT(*) FROM (
  SELECT UNNEST(cityjson_appearance_ids(material_lod3_0, 'material')) AS ref
  FROM read_cityjsonseq('test/data/railway_appearance.city.jsonl', appearance := 'sidecar')
  WHERE material_lod3_0 IS NOT NULL
) r
WHERE r.ref NOT IN (SELECT id FROM cityjson_materials('test/data/railway_appearance.city.jsonl'));
----
0

# The default is unchanged.
query I
SELECT COUNT(*) FROM read_cityjsonseq('test/data/railway_appearance.city.jsonl')
WHERE material_lod3_0 IS NOT NULL;
----
1

statement error
SELECT * FROM read_cityjsonseq('test/data/railway_appearance.city.jsonl', appearance := 'nonsense');
----
appearance must be 'local' or 'sidecar'
```

- [ ] **Step 2–4:** run red, add the option (validating its value in the bind), build, run green, run the full suite, commit.

---

### Task 4: Inline texture UVs

The substantive half. A texture theme map's ring is `[texId, uvIdx, uvIdx, …]` where each `uvIdx` indexes that **feature's** `vertices-texture`. The spec requires `[texId, [u,v], [u,v], …]`, because the UV pool is per-feature and cannot survive being merged into one table.

**Files:**
- Create: `src/include/cityjson/appearance_normalise.hpp`
- Create: `src/cityjson/appearance_normalise.cpp`
- Modify: `CMakeLists.txt`; the scan path from Task 3
- Test: `test/sql/cityjson_appearance_sidecar.test`

**Interfaces:**
- Consumes: `Appearance::vertices_texture` (Task 1).
- Produces:
  ```cpp
  //! Rewrite a texture theme map in place, replacing every UV *index* with the
  //! [u, v] pair it names in `uv_pool`. An index outside the pool is an error.
  //! A ring of a single null (an untextured ring) is left as-is.
  json InlineTextureUVs(const json &texture_map, const std::vector<std::array<double, 2>> &uv_pool);
  ```
  Recurse to the ring exactly as `cityjson_appearance_ids` does — the nesting depth varies with geometry type (a Solid nests one deeper than a MultiSurface), so the ring must be *recognised* (the innermost array, whose elements are scalars) rather than assumed at a fixed depth.

- [ ] **Step 1: Write the failing test:**

```
# Texture UVs are inlined: each ring becomes [texId, [u,v], [u,v], ...], so a
# ring's second element is a two-element list rather than an integer index.
query T
SELECT typeof(cityjson_appearance_ids(texture_lod3_0, 'texture'))
FROM read_cityjsonseq('test/data/railway_appearance.city.jsonl', appearance := 'sidecar')
WHERE texture_lod3_0 IS NOT NULL;
----
BIGINT[]

query I
SELECT COUNT(*) FROM read_cityjsonseq('test/data/railway_appearance.city.jsonl', appearance := 'sidecar')
WHERE texture_lod3_0 IS NOT NULL
  AND texture_lod3_0 LIKE '%[[%';
----
1

# In local mode the UV entries stay bare integer indices.
query I
SELECT COUNT(*) FROM read_cityjsonseq('test/data/railway_appearance.city.jsonl')
WHERE texture_lod3_0 IS NOT NULL AND texture_lod3_0 NOT LIKE '%[[%';
----
1
```

- [ ] **Step 2–5:** run red; implement `InlineTextureUVs`; wire it into the sidecar path; build; green; full suite; commit.

---

### Task 5: `cityjson_geometry_templates`

**Files:**
- Modify: `cityjson_types.hpp/cpp` — parse `geometry-templates` (`templates`, `vertices-templates`)
- Modify: `src/cityjson/appearance_table_function.cpp` — add the function
- Test: `test/sql/cityjson_appearance_sidecar.test`

**Interfaces:**
- Produces: `cityjson_geometry_templates(path)` → `id BIGINT, name VARCHAR, geometry_lod* BLOB, geometry_properties_lod* STRUCT(...), material_lod* VARCHAR, texture_lod* VARCHAR`.

**`id` is `BIGINT`** and `name` holds the source identifier — the spec was amended on 2026-07-26 (see `docs/CITYPARQUET_SPEC_QUESTIONS.md` §2). Template geometry is in **local** coordinates and exempt from the file CRS, so it is encoded from `vertices-templates` with no dataset transform applied.

No fixture in `test/data/` currently has geometry templates. **Add one** — a minimal `.city.json` with one template and one object instantiating it — rather than leaving the function untested.

- [ ] Steps follow the same red/green/commit shape as Tasks 1–4.

---

### Task 6: Per-feature appearance definitions

Lifts Task 2's restriction: a CityJSONSeq feature carrying its own non-empty `materials` / `textures` must be interned into the dataset-global set, with each feature's local indices remapped to the interned ids.

Deliberately last: it needs a fixture that does not exist yet, the header-only case covers the data actually to hand, and Task 2's hard error means nothing silently mis-numbers in the meantime.

- [ ] **Step 1:** construct a fixture with two features declaring overlapping-but-distinct materials.
- [ ] **Step 2:** intern by structural equality of the definition, assigning ids in first-seen order (header entries first, keeping their ordinals stable).
- [ ] **Step 3:** remap each feature's local indices through its own mapping.
- [ ] **Step 4:** remove the Task 2 error; assert the previously-rejected input now normalises.

---

### Task 7: Documentation

- [ ] Update `README.md` (an "Appearance normalisation" subsection), `CLAUDE.md` and `AGENTS.md` (the function tables, kept in sync).
- [ ] Note in `docs/CITYPARQUET_SPEC_QUESTIONS.md` that the ordinal id rule must match `cityparquet-rs`, since packages from the two implementations will otherwise fail to merge cleanly.
