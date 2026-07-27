# CityParquet Merge and Package I/O Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `insert_cityjson('ams', 'tile.city.json')` — one call, transactional, consistent — plus the package round trip (`cityparquet_read` / `cityparquet_write`) that lets a mutated package be written back with correct footers and STAC Item.

**Architecture:** Everything is a SQL-generating `PragmaFunction`, as in plans 1 and 2. `cityparquet_merge` is the core; `insert_cityjson` is that same generator with a CityJSON front end that stages the source first. The write side is also a pragma: `KV_METADATA {city: getvariable('…')}` is accepted, so footer JSON can be computed by earlier statements in the generated script rather than needing an internal connection.

**Tech Stack:** C++17, DuckDB extension API, nlohmann::json, SQL logic tests.

## Status

**All seven tasks are done.** 854 assertions across 32 test files pass.

Shipped: the supporting scalars, `cityparquet_merge`, `insert_cityjson` (with
`insert_cityjsonseq`, `insert_flatcitybuf` and `insert_cityjson_sql`),
`cityparquet_read`, `cityparquet_write` (data files, `city`/`geo` footers, and a
`metadata.json` STAC Item carrying the aggregate `city3d:*` inventory plus the
Projection, File and Table extension fields), and the documentation.

Two open spec questions came out of Task 6 and are recorded in
`docs/CITYPARQUET_SPEC_QUESTIONS.md`: which STAC extension "Statistics" means, and
whether `city3d:lods` is defined in the normalised LoD spelling (the only one a written
package can recover).

### What Task 3 turned up

The first attempt at `insert_cityjson` was reverted because it re-derived the incoming
column list instead of asking the reader. The fix was to extract `InferCityJSONColumns`
and `InspectCityJSONSource` as the single source of truth — but pulling on that thread
exposed four defects, three of them already shipped:

1. **The CityJSONSeq reader is a one-shot stream** (`fix(reader): a CityJSONSeq reader
   must answer more than one whole-file question`). Two shipped paths asked it twice and
   silently got nothing the second time: `read_cityjson` on a `.city.jsonl` returned a
   schema with **no geometry columns at all**, and `appearance := 'sidecar'` interned
   from the header alone, so every feature-local index fell through to the identity
   mapping and pointed at the wrong definition. `ReadAllChunks` and `ReadNFeatures` now
   rewind; `ReadNextFeature` deliberately does not.
2. **`cityparquet_merge` did not evolve sidecar schemas.** `geometry_templates` carries
   per-LoD columns, so merging two packages whose templates use different LoDs failed to
   bind.
3. **Template appearance references were not shifted on merge**, so the sidecar rows
   moved while their references stayed put.
4. **Reconcile assumed too much**: a table the script itself creates was invisible to it
   (pre-batch catalog), and its bbox phase assumed every object table has a `bbox`
   column, which a source with only template geometry does not.

## Scope and order

**Plan 3 of 3.** Ordered to deliver `insert_cityjson` first, because it is the headline
request and does **not** depend on the write side. Package I/O follows.

Plan 1 (shipped): `docs/superpowers/plans/2026-07-25-cityparquet-mutation-core.md`
Plan 2 (shipped): `docs/superpowers/plans/2026-07-26-cityparquet-appearance-normalisation.md`
Design: `docs/superpowers/specs/2026-07-25-cityparquet-mutation-functions-design.md`

## Global Constraints

- **Read plan 1's "Corrections discovered during implementation" first.** All eleven still
  apply: no `JSON` type, no `json_extract`, no subqueries in lambdas, PRAGMA named
  parameters use `=`, `StringUtil::Join` takes `duckdb::vector`, the two ODR link traps,
  and pragma expansion happening before execution.
- Build: `cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb unittest`
- Test: `./build/release/test/unittest --test-dir . "test/sql/<name>.test"`
- New `.cpp` files go in `EXTENSION_SOURCES`.
- **Batched pragmas see pre-batch state.** Every generated `ALTER` uses `IF NOT EXISTS`;
  anything depending on row data is computed by the generated SQL, never at plan time.

## Mechanism notes

1. **`parquet_kv_metadata(path)` reads the footer back**, so `cityparquet_read` can
   populate `__cityparquet.city` in pure generated SQL. It stays a pragma.
2. **`KV_METADATA` accepts `getvariable()`** — verified — which solves computing a footer
   *value* in generated SQL.
3. **But it cannot omit a *key*.** A `NULL` variable writes the literal string `"NULL"`
   as the value, not an absent key:
   ```sql
   SET VARIABLE g = NULL;
   COPY (SELECT 1 AS a) TO 'f.parquet' (FORMAT PARQUET, KV_METADATA {geo: getvariable('g')});
   -- parquet_kv_metadata then reports  geo | NULL  as a written value
   ```
   This is decisive for the write side. The specification requires a table whose geometry
   is entirely solid to write **no `geo` key at all** — and solid-only is the normal 3DBAG
   shape, not an edge case. Legality is data-dependent, so it cannot be decided at plan
   time, and SQL cannot branch the shape of a `COPY` statement.

   **Therefore `cityparquet_write` is a table function, not a pragma.** It uses an
   internal connection, computes each footer in C++, and emits a `COPY` carrying only the
   keys that belong. The cost is that it sees **committed** state: mutate, commit, then
   write. That is the design the original spec proposed, and this is why.

---

### Task 1: `cityjson_shift_appearance_ids` and `cityjson_wkb_geometry_type`

Two small scalars the later tasks need.

**Files:**
- Modify: `src/cityjson/cityparquet_appearance.cpp` (shift), `src/cityjson/wkb_extent.cpp` (type) — or a new file if either grows
- Test: `test/sql/cityparquet_merge.test`

**Interfaces:**
- `cityjson_shift_appearance_ids(cell VARCHAR, kind VARCHAR, offset BIGINT) -> VARCHAR` — add
  `offset` to every material/texture id in an appearance cell, leaving UV pairs alone.
  Reuse the recursion in `cityparquet_appearance.cpp`: for `'material'` every integer
  leaf is an id; for `'texture'` only each ring's first element is.
- `cityjson_wkb_geometry_type(geom BLOB) -> VARCHAR` — the WKB type name with the `" Z"`
  suffix (`'MultiPolygon Z'`, `'PolyhedralSurface Z'`, …), for the footer's
  `geometry_types`. Reads the type code from the WKB header; no full decode needed.

- [ ] Red/green/commit as usual. Assert that shifting a texture cell moves the ids but
      **not** the `[u,v]` pairs — the obvious bug here is shifting coordinates.

---

### Task 2: `cityparquet_merge` — the core

**Files:**
- Create: `src/include/cityjson/cityparquet_merge.hpp`, `src/cityjson/cityparquet_merge.cpp`
- Modify: `CMakeLists.txt`, `src/cityjson_extension.cpp`
- Test: `test/sql/cityparquet_merge.test`

**Interfaces:**
- Consumes: `ObjectTablesInSchema`, `SidecarTablesInSchema`, `QualifiedName`,
  `GeometryLodColumns`, `AppearanceLodColumns` (plan 1, `cityparquet_package.hpp`);
  `BuildReconcileSQL` (plan 1); Task 1's scalars.
- Produces:
  - `PRAGMA cityparquet_merge(dst VARCHAR, src VARCHAR [, on_conflict =] [, crs =] [, create_tables =] [, tables =])`
  - `cityparquet_merge_sql(dst, src) -> VARCHAR`
  - `std::string BuildMergeSQL(ClientContext &, const std::string &dst, const std::string &src, const MergeOptions &);`

The ordered algorithm, from the design doc's "Insert / merge" section. **The order is
normative.**

**Phase 0 — plan time, catalog only.** Diff `src`'s columns against `dst`'s. Produces the
`ALTER`/`CREATE` statements and the routing map. Nothing depending on row *data* may be
decided here.

**Phase 1 — preconditions, before any mutation.** Each emits a `SELECT error(...)` guard:
1. **id uniqueness across the whole destination package**, not just the target module —
   `parents`/`children`/`feature_id` resolve by bare id across files, so a `Road`
   colliding with a `Building` id is still a collision. Rejects the entire merge.
2. **CRS equality** from `__cityparquet.city`. One CRS per file, no per-row escape hatch.
3. **Dangling parents** in the incoming batch resolving against neither `src` nor `dst`.

**Phase 2 — schema evolution.** Must precede any `INSERT`.
- Add each missing `geometry_lod*` **together with its three companions**
  (`geometry_properties_lod*`, `material_lod*`, `texture_lod*`) — the spec requires all
  four to exist whenever the geometry column does.
- Add new attribute columns; widen existing ones per the promotion lattice
  (`BIGINT`→`DOUBLE`, else `VARCHAR`).
- `CREATE TABLE IF NOT EXISTS` for missing modules **and insert their `__cityparquet` row
  in the same statement group**, carrying `city` from an existing sibling. A module table
  without its bookkeeping row cannot be written with valid footer metadata.

**Phase 3 — sidecar merge with id remap.** The offset depends on row **data**, so it is
computed in the generated SQL, never at plan time:

```sql
CREATE OR REPLACE TEMP TABLE __cp_mat_off AS
  SELECT (SELECT coalesce(max(id), -1) FROM dst.materials) + 1
       - (SELECT coalesce(min(id),  0) FROM src.materials) AS off;
```

`dst_max + 1 − src_min`, **not** `dst_max + 1`: a source id may be negative, and adding
`dst_max + 1` alone can then land back inside the destination's occupied range.

Then insert the shifted sidecar rows, and rewrite every reference in the staged object
rows through `cityjson_shift_appearance_ids` and `template.id`.

**Phase 4 — routed inserts** by `object_type` → module.

**Phase 5 — derived state.** `BuildReconcileSQL(context, dst, {})` — hierarchy, then
`feature_id`, then `bbox`. Do not reimplement.

- [ ] **Step 1:** write `test/sql/cityparquet_merge.test` covering: two packages merge;
      sidecar ids do not collide and references still resolve; a colliding object id
      rejects the whole merge; a new LoD in `src` adds all four columns to `dst`; a new
      attribute column appears; `BIGINT`→`DOUBLE` widening; the result validates clean;
      `ROLLBACK` undoes everything.
- [ ] **Steps 2–6:** red, implement, build, green, full suite, commit.

---

### Task 3: `insert_cityjson`

**Files:**
- Modify: `src/cityjson/cityparquet_merge.cpp` (or a sibling), `src/cityjson_extension.cpp`
- Test: `test/sql/cityparquet_insert.test`

**Interfaces:**
- `PRAGMA insert_cityjson(schema, path [, lod =] [, sample_lines =] [, on_conflict =] [, crs =] [, create_tables =] [, tables =])`
- Siblings `insert_cityjsonseq`, `insert_flatcitybuf`, mirroring the reader trio.
- `insert_cityjson_sql(schema, path) -> VARCHAR`

One call, not two. The generator opens the file at plan time and runs the **same schema
inference the reader's bind uses** (`InferSchema` → `LODTableUtils::InferLODTables`,
`src/cityjson/bind_function.cpp`), so it knows the incoming columns without executing
anything. It emits: staging `CREATE TEMP TABLE`s from
`read_cityjson(path, appearance := 'sidecar')` plus `cityjson_materials` /
`cityjson_textures` / `cityjson_geometry_templates`, then the Task 2 merge script against
that staging schema, then `DROP`s.

**Two honest costs**, both worth stating in the docs: the file is opened twice (plan-time
inference, then execution), and routing needs the *complete* `object_type` set rather
than a sample, or a rare type in the file's tail lands nowhere.

- [ ] Red/green/commit. Assert the headline case end to end: insert a `.city.json` into a
      loaded package, validate clean, appearance references resolve against the merged
      sidecars.

---

### Task 4: `cityparquet_read`

**Files:** `src/cityjson/cityparquet_package.cpp`; test `test/sql/cityparquet_io.test`

- `PRAGMA cityparquet_read(dir, schema [, overwrite =])` → generates `CREATE SCHEMA IF NOT
  EXISTS`, one `CREATE TABLE … AS SELECT * FROM read_parquet(…)` per file found, and
  populates `__cityparquet` **including `city`**, read back via `parquet_kv_metadata`.

The file list comes from a `FileSystem` glob at plan time — no SQL needed, and no data
read. Pure generation.

---

### Task 5: `cityparquet_write` — data files and footers

**Files:** `src/cityjson/cityparquet_write.cpp`; test `test/sql/cityparquet_io.test`

- `PRAGMA cityparquet_write(schema, dir [, crs =] [, version =] [, source_format =])`
- Scalar `cityparquet_city_json(...)` / `cityparquet_geo_json(...)` assembling the footer
  objects from facts the generated SQL computes.

Per table the script: computes the geometry types actually present (via
`cityjson_wkb_geometry_type`) and the column extent into variables; assembles `city` and
`geo` with the scalars; then `COPY <table> TO '<dir>/<file>' (FORMAT PARQUET,
KV_METADATA {city: getvariable(…), geo: getvariable(…)})`.

Normative details that are easy to get wrong:

- **`geo` is recomputed, never carried.** GeoParquet legality flips in *both* directions
  under mutation: inserting one `Solid` makes a previously-clean column illegal to
  declare, deleting the last `Solid` makes it newly legal. A stale `geo` declaring a
  column that now holds `PolyhedralSurfaceZ` makes the **whole file** unreadable to
  Shapely, GeoPandas and DuckDB spatial. A table whose geometry is entirely solid must
  write **no** `geo` key at all.
- **`city` carries every field the writer does not recompute** — `version`, `crs`,
  `source_format`, `source_version`, `extensions`, `appearance_defaults`, `other` — from
  `__cityparquet`. This is not an allow-list: `source_version` and `other` hold
  non-derivable provenance, so omitting them makes an unmodified read/write cycle lossy.
- **A module table with no rows is not written**, per "no file for a module with no rows".
- **`crs` is required.** A hand-rolled load leaves `__cityparquet.city` NULL, so
  `cityparquet_write` must demand `crs` explicitly and **fail** rather than write a file
  with none — an absent `city.crs` on a file carrying CRS-bearing coordinates is a
  conversion error, never a silent omission.

---

### Task 6: `metadata.json` STAC Item

**Files:** `src/cityjson/cityparquet_write.cpp`; test `test/sql/cityparquet_io.test`

Aggregate `city3d:*` across every file (`city3d:lods` is the union), the `assets` map as
the package's file inventory, and the File/Projection/Statistics extension fields, which
depend on final bytes and so are computed last.

`collection.json` is **not** touched — it describes a dataset spanning multiple packages,
and a single package's write has no business reaching outside its own directory.

**On atomicity, state it plainly in the docs:** per-file only. `PhysicalCopyToFile`
already gives temp-file + rename per file. Cross-file atomicity does **not** exist and
ordering does not rescue it — CityParquet mandates stable basenames, so a write
overwrites in place and the old `metadata.json` already points at the files being
replaced. Concurrent readers during a write are unsupported. This is a permanent property
of a bare directory package, and where it matters, DuckLake is the right tool.

---

### Task 7: Documentation

- [ ] README section for the package round trip and `insert_cityjson`.
- [ ] CLAUDE.md / AGENTS.md function tables, kept in sync.
- [ ] Update `docs/CITYPARQUET_SPEC_QUESTIONS.md` and the parent repo's
      `docs/CROSS_MODULE_FOLLOWUP.md` if anything new surfaces.
