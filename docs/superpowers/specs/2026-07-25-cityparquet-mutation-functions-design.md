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
it runs. Two consequences, and the second is easy to get wrong:

1. A generator cannot observe tables created by earlier statements in the same submission. This
   design sidesteps that by deriving what it needs at plan time in C++ — file inspection and
   catalog lookup — rather than by querying.
2. **A generator's view of the destination is also pre-batch.** In the two-insert example
   above, the second pragma is expanded against the schema and data as they were *before* the
   first insert ran. Any decision that depends on destination state must therefore be either
   idempotent or deferred into the generated SQL:
   - **Schema evolution is made idempotent** — every generated `ALTER TABLE … ADD COLUMN` uses
     `IF NOT EXISTS`, and type widening is emitted only as a widening, so re-emitting it is a
     no-op.
   - **Sidecar id offsets are deferred**, because they depend on row data rather than the
     catalog (see Phase 4).

   With those two rules, batching several mutation pragmas in one submission is safe. Without
   them it silently corrupts, which is why they are normative here rather than an optimisation.

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

**Predicate scoping across heterogeneous tables.** Object tables do not share a column set — a
predicate naming `b3_h_dak_max` cannot bind against `transportation`, which has no such column.
The generator therefore parses the predicate at plan time (`Parser::ParseExpressionList`, then
walking for `ColumnRefExpression`) and applies it only to those object tables that carry every
column it references. Tables that do not are skipped, and the set actually searched is
reported. This makes the natural invocation work without the user having to spell out `tables`
themselves, which is the point of the wrapper.

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

## Prerequisite: a WKB extent scalar

`bbox` recomputation must derive an extent from a `geometry_lod*` cell. Generated SQL has
nothing to do that with today: the extension exposes no WKB-to-extent function, and DuckDB
spatial cannot help — it rejects exactly the encodings CityParquet relies on, raising
`Unsupported geometry type in WKB` on `PolyhedralSurfaceZ`, which is what every solid LoD is.
Without this, `cityparquet_reconcile` cannot service its headline case (a raw
`UPDATE … SET geometry_lod2_2 = …`) for solids, which is most of the interesting data.

```sql
cityjson_wkb_extent(geom BLOB) -> STRUCT(xmin DOUBLE, ymin DOUBLE, zmin DOUBLE,
                                         xmax DOUBLE, ymax DOUBLE, zmax DOUBLE)
```

A scalar function over the existing `wkb_decoder.cpp`, returning `NULL` for a `NULL` input. It
handles the whole WKB vocabulary CityParquet writes, solid family included, which is precisely
why it cannot be delegated to `spatial`.

## Consistency algorithms

### Insert / merge

Ordered; each phase depends on the previous having completed.

**Phase 0 — plan time (C++, no SQL).** Infer the source schema; take a full pass for the
complete `object_type` set; diff against the destination *catalog*. Produces the `ALTER` /
`CREATE` statements and the routing map — all catalog-derivable. Nothing that depends on row
**data** may be decided here; such decisions are emitted as SQL instead.

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
- Create missing module tables when `create_tables := true`, **and insert the corresponding
  `__cityparquet` row in the same statement group**, carrying the package's `city` values from
  any existing sibling row. A module table without its bookkeeping row cannot be written with
  valid footer metadata.

Every `ADD COLUMN` is emitted with `IF NOT EXISTS` so that batching several pragmas in one
submission cannot produce a duplicate-column error.

**Phase 4 — sidecar merge with id remap.** `materials.id` and `textures.id` are `BIGINT`. The
offset depends on row **data**, not the catalog, so it is computed in the generated SQL after
staging, never at plan time:

```sql
CREATE TEMP TABLE __cp_mat_off AS
  SELECT (SELECT coalesce(max(id), -1) FROM ams.materials) + 1
       - (SELECT coalesce(min(id),  0) FROM __cp_stg_materials) AS off;
```

Offsetting by `dst_max + 1 − src_min` rather than `dst_max + 1` is what makes this correct for
arbitrary `BIGINT` ids: a source id may be negative, in which case adding `dst_max + 1` alone
can land back inside the destination's occupied range. Subtracting the source minimum maps the
incoming range to start immediately after the destination's maximum, whatever its sign.

Then rewrite every reference through the mapping: the ids embedded in `material_lod*` /
`texture_lod*` JSON *values*, and `template.id`. The spec states these are values, not row
positions, and "MUST NOT be interpreted as a row position".

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
5. **Recompute `feature_id`** over the survivors. This is not optional bookkeeping under
   `cascade := false`: deleting a root leaves its descendants alive, and once the deleted parent
   has been stripped from their `parents` they *are* roots — but they still carry a
   `feature_id` pointing at an object that no longer exists. Re-deriving the root-parent chain
   is the only thing that repairs the family.
6. **`bbox` bottom-up** from surviving ancestors. Because `bbox` is unioned across every stored
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

**Cross-file atomicity is not provided at all, and ordering does not rescue it.** No POSIX or
S3 primitive gives a multi-file swap. It is tempting to reach for the manifest-last convention
from Iceberg and Delta — rename every data file, rename `metadata.json` last — but that
convention does **not** transfer here, and claiming it would be wrong. Iceberg works because its
data files are immutable and generation-scoped: a new snapshot writes *new* paths, so the old
manifest keeps resolving to an intact old generation for as long as anyone needs it. CityParquet
mandates stable basenames (`building.parquet`), so a write **overwrites in place**. The old
`metadata.json` therefore already points at the very files being replaced, and a reader holding
it can still observe a new `building.parquet` beside an old `transportation.parquet`. Renaming
the manifest last buys nothing.

What is honestly on offer is: **per-file atomicity, and no more.** Each file individually flips
whole; the package as a whole has a window during which it is inconsistent. Concurrent readers
during a write are unsupported. Making this genuinely atomic would need either
generation-scoped filenames or a directory-level swap, both of which conflict with the spec's
directory layout — which is exactly the boundary where DuckLake, with a real catalog, is the
right tool instead.

`cityparquet_write` runs on an internal connection and therefore sees **committed** state:
mutate, commit, then write.

## Metadata regeneration on write

`cityparquet_write` recomputes, per file:

- `city.columns` — the actual WKB `geometry_types` now present, `orientation_3d`, per-column
  `bbox`;
- `city.primary_column` — which may have to change if the previous primary's column emptied;
- `city.attributes` — the attribute-column list;
- `city.version`, `crs`, `source_format`, `source_version`, `extensions`,
  `appearance_defaults` and `other` — carried verbatim from `__cityparquet`. The carried set is
  "every `city` field the writer does not recompute", not an enumerated allow-list:
  `source_version` and `other` hold non-derivable provenance and producer metadata, so dropping
  them would make even an unmodified read/write cycle lossy;
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

0. `cityjson_wkb_extent` — small, self-contained, and every later `bbox` step depends on it.
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
