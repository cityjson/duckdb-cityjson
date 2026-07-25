# CityParquet mutation functions — design

**Status:** design approved, not yet implemented
**Date:** 2026-07-25
**Scope:** `duckdb-cityjson` — new SQL functions for transactional, consistency-preserving
insert / delete / update of a CityParquet dataset held in DuckDB.

## Problem

A [CityParquet dataset](https://github.com/cityjson/cityparquet) is a *directory* of Parquet
files, not one file: one object table per CityGML module (`building.parquet`,
`transportation.parquet`, …), optional `materials.parquet` / `textures.parquet` /
`geometry_templates.parquet` sidecars, a package-level `metadata.json` STAC Item, and a
per-file Parquet footer carrying `city` and `geo` JSON metadata objects.

Because it is Parquet, a user can load it into DuckDB and treat it as a set of tables. What
they cannot easily do is *mutate* it. The package has internal relationships that ordinary
`INSERT` / `UPDATE` / `DELETE` does not know about:

- deleting a parent city object must cascade to its children;
- inserting a feature that carries materials / textures / geometry templates must also insert
  the corresponding sidecar rows and remap their references;
- `feature_id`, `bbox`, and the reciprocal `parents` / `children` / `children_roles` arrays are
  derived state that any structural edit invalidates;
- the footer `city` / `geo` objects and the STAC Item must be regenerated from the mutated
  data before the package is written back.

Expressing all of that by hand is possible but produces long, error-prone SQL. This design adds
functions that generate it.

## Non-goals

- A general-purpose transactional table format. DuckLake already occupies that space, and the
  sibling `citylake` project uses it.
- Concurrency control between multiple writers.
- Writing packages to object storage (S3) transactionally.

## The model

**A package is a DuckDB schema.** Table names are exactly the spec's file basenames, so there
is no registration state to keep in sync and no session-scoped binding to go stale:

| Role | Tables |
|---|---|
| Object tables | `building`, `bridge`, `tunnel`, `construction`, `transportation`, `vegetation`, `relief`, `water_body`, `land_use`, `city_furniture`, `generics`, plus any extension module |
| Sidecars | `materials`, `textures`, `geometry_templates` |
| Bookkeeping | `__cityparquet` |

`__cityparquet` holds one row per package file — `table_name`, `file_name`, `role`, and the
`city` footer JSON as loaded. It is what lets the write side regenerate `city.version`,
`city.crs`, `city.source_format`, `city.extensions` and `city.appearance_defaults` faithfully
rather than inferring them. It is an ordinary table, so it participates in transactions like
everything else.

A user who loads the package by hand (`CREATE TABLE building AS SELECT * FROM
read_parquet('building.parquet')`, one per file) gets everything except `__cityparquet`;
`PRAGMA cityparquet_read` populates it, and `PRAGMA cityparquet_init` creates it for a
hand-rolled load.

## Mechanism: pragmas that expand to SQL

DuckDB's `PragmaFunction::PragmaCall(name, pragma_query_t, args)` registers a pragma whose
implementation **returns a SQL string**. DuckDB parses that string and executes the resulting
statements *in place of* the pragma
(`duckdb/src/planner/statement_preprocessor.cpp:107-122`). `IMPORT DATABASE` is built this
way.

Crucially, `GetTransactionHandling` (`statement_preprocessor.cpp:80-86`) wraps the expansion in
a transaction when the caller is not already in one, and joins the caller's transaction when
they are. So:

```sql
PRAGMA cityparquet_delete('ams', $$id = 'NL.IMBAG.Pand.001'$$);
-- runs atomically

BEGIN;
PRAGMA insert_cityjson('ams', 'tile_a.city.json');
PRAGMA insert_cityjson('ams', 'tile_b.city.json');
COMMIT;
-- both inserts in one transaction
```

Atomicity, isolation and rollback are DuckDB's own, not ours. The extension only generates
text. This is why the pragma route is preferred over a table function that performs side
effects: a table function executing DML would need an internal connection and therefore a
*separate* transaction.

Pragmas support named parameters (`duckdb/src/parser/transform/statement/transform_pragma.cpp:26-33`),
so `PRAGMA insert_cityjson('ams', 'f.city.json', lod := '2.2')` parses.

### Constraint: expansion happens before execution

`StatementPreprocessor::Preprocess` expands every pragma in a submitted script *before* any of
it runs. A pragma generator therefore cannot observe tables created by earlier statements in
the same submission. This design avoids the problem entirely by having each generator derive
what it needs at plan time in C++ (file inspection, catalog lookup) rather than by querying.

## Function surface

### Insert from a source format

```sql
PRAGMA insert_cityjson('ams', 'new_tile.city.json');
PRAGMA insert_cityjsonseq('ams', 'new_tile.city.jsonl');
PRAGMA insert_flatcitybuf('ams', 'new_tile.fcb');
```

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| *schema* (positional 1) | VARCHAR | — | Destination package schema |
| *path* (positional 2) | VARCHAR | — | Source file path or URL |
| `lod` | VARCHAR | all | Restrict to one LoD |
| `sample_lines` | BIGINT | 100 | Schema-inference sample, as on `read_cityjson` |
| `on_conflict` | VARCHAR | `'error'` | Only `'error'` in v1 |
| `crs` | VARCHAR | `'strict'` | Only `'strict'` in v1 — reject cross-CRS input |
| `create_tables` | BOOLEAN | `true` | Create module tables the package lacks |
| `tables` | VARCHAR[] | all | Restrict which module tables may receive rows |

The generator opens the file at plan time and runs the same schema inference the reader's bind
uses (`InferSchema` → `LODTableUtils::InferLODTables`, `src/cityjson/bind_function.cpp:45`),
plus one full pass for the complete `object_type` set. No SQL is executed to obtain either.

Emitted script, abbreviated:

```sql
CREATE TEMP TABLE __cp_stg AS
  SELECT * FROM read_cityjson('new_tile.city.json', appearance := 'sidecar');
CREATE TEMP TABLE __cp_stg_materials AS SELECT * FROM cityjson_materials('new_tile.city.json');

-- preconditions
SELECT error('insert_cityjson: duplicate id ' || id) FROM __cp_stg
  WHERE id IN (SELECT id FROM ams.building UNION ALL SELECT id FROM ams.generics ...);

-- schema evolution
ALTER TABLE ams.building ADD COLUMN geometry_lod1_2 BLOB;
ALTER TABLE ams.building ADD COLUMN geometry_properties_lod1_2 STRUCT(...);
ALTER TABLE ams.building ADD COLUMN material_lod1_2 JSON;
ALTER TABLE ams.building ADD COLUMN texture_lod1_2 JSON;
ALTER TABLE ams.building ALTER COLUMN bouwjaar SET DATA TYPE DOUBLE;

-- sidecar merge, routed inserts, derived-state fixups
INSERT INTO ams.materials SELECT id + <offset>, … FROM __cp_stg_materials;
INSERT INTO ams.building  SELECT … FROM __cp_stg WHERE object_type IN ('Building','BuildingPart',…);
INSERT INTO ams.transportation SELECT … FROM __cp_stg WHERE object_type IN ('Road','Railway',…);
…
DROP TABLE __cp_stg; DROP TABLE __cp_stg_materials;
```

### Package-to-package merge

```sql
PRAGMA cityparquet_merge('ams', 'utrecht');
```

The primitive for combining tiles. `insert_cityjson` is this same generator with a CityJSON
front end: stage, then merge. Named parameters `on_conflict`, `crs`, `create_tables`, `tables`
as above.

### Delete

```sql
PRAGMA cityparquet_delete('ams', $$object_type = 'Building' AND b3_h_dak_max > 20$$);
```

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| *schema* (positional 1) | VARCHAR | — | Package schema |
| *predicate* (positional 2) | VARCHAR | — | SQL predicate, evaluated against each object table |
| `cascade` | BOOLEAN | `true` | Delete the transitive `children` closure |
| `tables` | VARCHAR[] | all | Restrict which object tables the *predicate* is evaluated against |

`tables` scopes only the predicate, never the cascade. The `children` closure and the
survivor-reference cleanup are always package-wide; restricting them would leave orphaned
children and dangling references in the tables that were excluded, which is precisely the
failure this function exists to prevent.

### Reconcile after raw edits

```sql
UPDATE ams.building SET geometry_lod2_2 = … WHERE id = 'x';
PRAGMA cityparquet_reconcile('ams');
```

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| *schema* (positional 1) | VARCHAR | — | Package schema |
| `checks` | VARCHAR[] | all | Subset of `['feature_id','hierarchy','bbox']` |

There is deliberately **no `cityparquet_update`**. Attribute edits are ordinary `UPDATE` and
need no wrapper. Only structural edits — geometry, `parents` / `children`, appearance —
invalidate derived state, and `cityparquet_reconcile` re-derives exactly that. Wrapping
`UPDATE` would require intercepting arbitrary SQL, for which DuckDB offers no hook.

### Inspection and housekeeping

```sql
SELECT * FROM cityparquet_validate('ams');  -- (check, severity, table_name, object_id, message)
SELECT * FROM cityparquet_orphans('ams');   -- (table_name, id, reason)
PRAGMA cityparquet_vacuum('ams');           -- delete orphaned sidecar rows
```

`cityparquet_validate` and `cityparquet_orphans` are read-only table functions.

### Package I/O

```sql
PRAGMA cityparquet_init('ams');                       -- create __cityparquet for a manual load
PRAGMA cityparquet_read('amsterdam/', 'ams');         -- CREATE TABLE … read_parquet(…) per file,
                                                      -- and populate __cityparquet from
                                                      -- parquet_kv_metadata()
SELECT * FROM cityparquet_write('ams', 'amsterdam/'); -- (file, action, rows, bytes)
```

`cityparquet_init` assigns each existing table its role by name convention and leaves the
`city` column `NULL`, because a hand-rolled load has discarded the footer the values would come
from. `cityparquet_write` therefore requires `crs` (and accepts `version`, `source_format`) as
named parameters when any `__cityparquet.city` is `NULL`, and fails rather than writing a file
with no CRS — the spec makes an absent `city.crs` on a file carrying CRS-bearing coordinates a
conversion error, never a silent omission. `cityparquet_read` has no such gap: it populates
`city` from `parquet_kv_metadata()`.

`cityparquet_read` is a pragma (pure SQL generation; the file list comes from a `FileSystem`
glob at plan time). `cityparquet_write` is a table function, because it must compute
data-dependent footer metadata and Parquet's `KV_METADATA` copy option is a parse-time literal
(`CopyInfo::options` is `case_insensitive_map_t<vector<Value>>`), so it cannot be filled in by
a generated statement.

### SQL-text twins

Every mutation pragma has a scalar-function twin that returns the SQL it would run, instead of
running it:

```sql
SELECT insert_cityjson_sql('ams', 'new_tile.city.json');
SELECT cityparquet_delete_sql('ams', $$id = 'x'$$);
SELECT cityparquet_merge_sql('ams', 'utrecht');
SELECT cityparquet_reconcile_sql('ams');
```

Same generator, no extra logic. This is the debugging affordance and the paper's evidence: the
generated text *is* the hand-written SQL each one-line pragma replaces.

## Prerequisite: appearance normalisation

`read_cityjson` currently emits `material_lod*` / `texture_lod*` with the source's
**feature-local** indices, and does not produce sidecar rows at all (README:262-267,
`test/sql/cityjson_appearance.test`). The spec requires dataset-global sidecar `id` values that
resolve by matching the sidecar's `id` column, with texture UVs inlined as `[u,v]` pairs. Until
that gap is closed, no function here can produce or extend a conformant package.

Proposed surface:

```sql
read_cityjson(path, appearance := 'sidecar')   -- global ids, UVs inlined
cityjson_materials(path)                       -- → materials.parquet rows
cityjson_textures(path)                        -- → textures.parquet rows
cityjson_geometry_templates(path)              -- → geometry_templates.parquet rows
```

Ids are assigned by ordinal position in the source's `appearance.materials` /
`appearance.textures` arrays, so the four functions agree without sharing state. The default
stays `appearance := 'local'`, leaving existing behaviour and the `COPY TO cityjson` round trip
untouched. The same option is added to `read_cityjsonseq` and `read_flatcitybuf`.

## Consistency algorithms

### Insert / merge

Ordered; each phase depends on the previous having completed.

**Phase 0 — plan time (C++, no SQL).** Infer the source schema; take a full pass for the
complete `object_type` set; diff against the destination catalog. Produces the `ALTER` /
`CREATE` statements, the routing map, and the sidecar id offsets.

**Phase 1 — stage** into temp tables.

**Phase 2 — preconditions.** Fail fast, before any mutation:

1. **id uniqueness across the whole package**, not just the target module — `parents`,
   `children` and `feature_id` resolve by bare `id` across files, so a `Road` colliding with a
   `Building` id is still a collision. A collision rejects the entire insert.
2. **CRS equality.** `city.crs` is one CRS per file with no per-row escape hatch, so a mismatch
   is a hard error rather than a silent write.
3. **Dangling parents** in the incoming batch resolving against neither staged nor existing
   rows.

**Phase 3 — schema evolution.** Must precede any `INSERT`.

- Add missing `geometry_lod*` columns **together with their three companions**
  (`geometry_properties_lod*`, `material_lod*`, `texture_lod*`) — the spec requires all four to
  exist whenever the geometry column does.
- Add new attribute columns.
- Widen existing attribute columns per the promotion lattice (`BIGINT` → `DOUBLE`; otherwise
  `VARCHAR`, or `JSON` for structured values).
- Create missing module tables when `create_tables := true`.

**Phase 4 — sidecar merge with id remap.** `materials.id` and `textures.id` are `BIGINT`, so
offsetting by the destination's `max(id) + 1` is deterministic and collision-free. Then rewrite
every reference: the ids embedded in `material_lod*` / `texture_lod*` JSON *values*, and
`template.id`. The spec states these are values, not row positions, and "MUST NOT be
interpreted as a row position".

**Phase 5 — routed inserts** into the evolved tables, by `object_type` → module.

**Phase 6 — derived state**, strictly in this order:

1. `feature_id` — resolve each row's root parent over staged ∪ existing rows.
2. Reciprocal hierarchy — a new subtree attaching under an *existing* parent must be appended
   to that parent's `children` and `children_roles`, positionally aligned.
3. `bbox` — depends on the corrected hierarchy, so it cannot run earlier.

**Phase 7 — drop staging tables.**

### Delete

1. Resolve the matched row set from the predicate, across every in-scope object table.
2. **Cascade via `children`, transitively — not via `feature_id =`.** A predicate may match a
   non-root object; deleting a `BuildingPart` must not take out the parent `Building` that
   shares its `feature_id`.
3. Delete the matched rows.
4. **Strip dangling references from survivors**, keeping `children_roles` positionally aligned
   to `children`:

   ```sql
   UPDATE ams.generics SET
     children = list_transform(kept, x -> x[1]),
     children_roles = list_transform(kept, x -> x[2])
   FROM (
     SELECT id, list_filter(list_zip(children, children_roles),
                            x -> x[1] NOT IN (SELECT id FROM __cp_deleted)) AS kept
     FROM ams.generics
   ) f WHERE ams.generics.id = f.id;
   ```

   Each stripped reference is reported.
5. **`bbox` bottom-up** from surviving ancestors. Because `bbox` is unioned across every stored
   LoD *and* across descendants, this is a recursive CTE over the hierarchy, and it crosses
   module tables — a `CityObjectGroup` in `generics` may have members in `building`. An
   ancestor whose subtree is now empty falls back to its own geometry, or `NULL` if it has
   none.

Orphaned sidecar rows are **not** handled here. Whether a material is orphaned is a
whole-package set difference, so it belongs in `cityparquet_vacuum`.

### Reconcile

Phase 6 standalone, over the whole package.

## Transaction boundaries

**In-database mutation is genuinely transactional.** The pragma expansion is wrapped in a
transaction, or joins the caller's. Atomicity, isolation and rollback are DuckDB's.

**Writing the package to disk is not.** Per-file atomicity is real and already available:
`PhysicalCopyToFile` hands a copy function a `tmp_`-prefixed path and calls `MoveTmpFile` after
finalize (`duckdb/src/execution/operator/persistent/physical_copy_to_file.cpp:493-505`), which
is how the existing `COPY TO cityjson` is already safe. `cityparquet_write` reuses that
discipline per file.

**Cross-file atomicity does not exist.** No POSIX or S3 primitive provides it. The mitigation is
ordering only: write and rename every data file, then rename `metadata.json` last. A reader that
consults `metadata.json` before trusting file contents sees either the whole old generation or
the whole new one. A reader that opens `building.parquet` directly can observe a torn package
during the commit window. This is the manifest-as-commit-point convention from Iceberg and
Delta without a catalog behind it, and it is a **permanent limitation of a bare directory
package**, not a defect to fix later — genuine cross-file atomicity is what DuckLake is for.

`cityparquet_write` runs on an internal connection and therefore sees **committed** state:
mutate, commit, then write.

## Metadata regeneration on write

`cityparquet_write` recomputes, per file:

- `city.columns` — the actual WKB `geometry_types` now present, `orientation_3d`, per-column
  `bbox`;
- `city.primary_column` — which may have to change if the previous primary's column emptied;
- `city.attributes` — the attribute-column list;
- `city.version`, `crs`, `source_format`, `extensions`, `appearance_defaults` — carried from
  `__cityparquet`;
- `geo` — **recomputed from the mutated data, never carried over**.

The `geo` recomputation is not optional bookkeeping. GeoParquet legality flips in both
directions under mutation: inserting a single `Solid` into a previously-clean column makes that
column illegal to declare, and deleting the last `Solid` makes it newly legal. The spec is
explicit that a wrongly declared column makes the *whole file* unreadable to Shapely, GeoPandas
and DuckDB spatial — so a stale `geo` is data-corruption-adjacent, not merely untidy. A table
whose geometry becomes entirely solid must drop its `geo` key altogether.

Then `metadata.json`: the aggregate `city3d:*` fields (union across every current file), the
`assets` map (sidecar files appear and disappear as their tables gain and lose rows), and the
File / Projection / Statistics extension fields, which depend on the final bytes and so must be
computed last. A module table with no rows is not written, per the spec's "no file for a module
with no rows".

`collection.json` is **not** touched — it describes a dataset spanning multiple packages, and a
single package's write has no business reaching outside its own directory.

## Explicitly cut

- **`ATTACH … (TYPE CITYPARQUET)` as a real catalog.** This is the elegant surface, and it
  requires a `StorageExtension`, a `Catalog`, a `TableCatalogEntry` with working physical
  storage, and a `TransactionManager` — the scope DuckLake, `postgres_scanner` and
  `sqlite_scanner` each occupy as standing engineering efforts. The pragma approach obtains the
  same transactionality by delegating to DuckDB.
- **S3 writes.** Reads over `httpfs` are unaffected. Writing needs its own design — no rename,
  no cross-file swap, no lock primitive — and must not gate local mutation.
- **Multi-writer concurrency control.** Documented as "one writer at a time"; no optimistic
  concurrency, no retry.
- **CRS reprojection on insert.** Rejected instead; reprojection needs a projection library and
  is an unrelated feature.
- **`cityparquet_update`**, automatic pruning of now-empty columns, `collection.json`
  regeneration, entity resolution on merge, and Hilbert re-sorting / row-group re-tuning.
- **Multiple geometries per `(object, LoD)` and multiple template instances per object.** Both
  are open specification questions; a mutation API must not get ahead of the spec. The existing
  "keep first, drop the rest" behaviour is inherited.

## Risks and open items

**`geometry_templates.id` is `VARCHAR` while `materials.id` / `textures.id` are `BIGINT`.** The
specification lists this as an open question and calls it "an inconsistency rather than a
decision". A `BIGINT` id remaps by offsetting; a `VARCHAR` id needs prefixing, which mangles a
possibly meaningful identifier. Implementing merge forces this to be resolved, and the
resolution belongs upstream in the spec, not in this extension.

**Appearance normalisation is substantial new work** and is a hard prerequisite. It also makes
this extension a second owner of logic the README currently delegates to `cityparquet-rs`;
the two implementations must agree on ordinal id assignment or packages produced by one will
not merge cleanly with the other's.

**`city3d:*` STAC aggregation requires a full scan** of every table on write. Acceptable at
research scale; worth noting as a cost that grows with package size.

**Double file read on insert** — once at plan time for schema inference and the `object_type`
set, once at execution by the reader. Sampling would avoid the second pass but is unsound for
routing: a rare object type in the file's tail would land nowhere. `geoparquet_table_function.cpp`
already takes the same trade-off (`ReadAllChunks` rather than sampling) for the same reason.

## Build order

1. Appearance normalisation — `appearance := 'sidecar'`, `cityjson_materials`,
   `cityjson_textures`, `cityjson_geometry_templates`.
2. Package I/O — `cityparquet_init`, `cityparquet_read`, `cityparquet_write`, including footer
   and STAC regeneration.
3. `cityparquet_validate`, `cityparquet_orphans`, `cityparquet_vacuum` — read-only and
   independently testable, and they are the oracle the mutation tests assert against.
4. `cityparquet_reconcile` — Phase 6 alone.
5. `cityparquet_delete`.
6. `cityparquet_merge`, then `insert_cityjson` / `insert_cityjsonseq` / `insert_flatcitybuf` on
   top of it.
7. The `_sql()` twins, alongside each generator as it lands.

Steps 3–7 are all pure SQL generation and are testable end to end from `test/sql/*.test` by
asserting on the generated text and on the resulting table state.
