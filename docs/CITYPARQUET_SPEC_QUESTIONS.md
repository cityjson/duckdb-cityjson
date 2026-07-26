# CityParquet specification questions raised by this implementation

Divergences and contradictions found while implementing the CityParquet mutation
functions in this extension. Each needs a decision in the specification
(`documents/docs/03-specification/`), not merely in this repository — several also
affect `cityparquet-rs`, the Rust reference implementation.

---

## 1. `bbox` struct field names — **resolved: the spec moves**

**Status:** decided 2026-07-25. The specification will be amended; this extension does
not change.

The specification writes the bounding-box struct as
`STRUCT<xmin, ymin, zmin, xmax, ymax, zmax DOUBLE>`
([object table schema](https://github.com/cityjson/cityparquet)). This extension has
always emitted `min_x, min_y, min_z, max_x, max_y, max_z`
(`src/cityjson/column_types.cpp:127-132`), and now `cityjson_wkb_extent` returns the
same shape so a recomputed extent assigns straight into `bbox`.

**Decision:** amend the specification to `min_x … max_z`.

**Still to do:** update the specification text, and check whether `cityparquet-rs`
emits the spec's names — if it does, it must move too, or the two reference
implementations will disagree about a reserved column.

---

## 2. Sidecar id types — **resolved: templates become BIGINT**

**Status:** decided 2026-07-25.

`materials.id` and `textures.id` are `BIGINT`; `geometry_templates.id` is `VARCHAR`.
The specification already lists this as open and calls it "an inconsistency rather than
a decision".

Merging two packages forces the question: a `BIGINT` id remaps by offsetting
(`dst_max + 1 − src_min`), which is deterministic and collision-free, whereas a
`VARCHAR` id can only be disambiguated by prefixing, which mangles a possibly
meaningful identifier and makes the remap logic differ per sidecar.

**Decision:** make `geometry_templates.id` a `BIGINT` too, so all three sidecars remap
identically. A source template's own string identifier must then be preserved
elsewhere (a `name` column, or `other`) rather than serving as the id.

**Still to do:** amend the specification; update `cityparquet-rs`; this affects plan 2
(`cityparquet_merge`).

---

## 3. `bbox` and descendants — **open, and a genuine contradiction in the spec**

**Status:** open. Discovered 2026-07-26 while implementing `cityparquet_reconcile`.

The specification contradicts itself about whether a parent's `bbox` includes its
descendants' geometry.

- **The normative text says it does.** The metadata page, explaining why GeoParquet's
  `covering` is not written: *"CityParquet's `bbox` is deliberately neither per-column
  nor null-aligned: it is one column per row, unioned across every stored LoD **and**
  across the object's descendants."*
- **The worked example says it does not.** The object-table-schema page shows a real
  3DBAG `Building` with `bbox` `{111970.858, 443555.458, 45.648, 111989.694,
  443588.204, 45.648}` — flat, `zmin == zmax`, the LoD0 footprint — beside its child
  `BuildingPart` with `{…, 53.290, …, 59.142}`. The parent's box does not contain the
  child's z-range. The page's own prose even highlights this: *"here the `Building`
  carries the LoD0 footprint (flat bbox, `zmin == zmax`) while the 3D solids live on
  the `BuildingPart`."*

Both cannot be right. A parent whose bbox excludes its children is useless for the
hierarchy-aware spatial pruning the metadata page justifies `bbox` by.

**Current behaviour, which is split:**

- `read_cityjson` / `read_cityjsonseq` compute each row's `bbox` from **that row's own
  geometry only** — matching the worked example.
- `cityparquet_reconcile` unions **across descendants** — matching the normative text.

So reconciling a freshly-read package legitimately changes every non-leaf row's `bbox`.
`test/sql/cityparquet_reconcile.test` asserts this explicitly rather than papering over
it.

**Options:**

1. **Union across descendants** (keep the normative text; fix the reader and the worked
   example). Makes `bbox` a real pruning aid on parents; costs a hierarchy walk at read
   time, which the reader does not currently do.
2. **Own geometry only** (keep the example; fix the metadata text). Cheap and local, but
   then a query pruning on a `Building`'s `bbox` silently misses buildings whose
   geometry lives on their parts — which is the normal 3DBAG shape, so this would be a
   correctness trap.
3. **Both, in separate columns.** Rejected as scope creep; the format has one `bbox`.

**Recommendation:** option 1. The normative text states the intent, the pruning argument
depends on it, and the worked example looks like it was transcribed from the current
reader's output rather than derived from the rule.

**Impact if option 2 is chosen instead:** `cityparquet_reconcile`'s bbox phase and its
test must change, and `cityparquet_delete` inherits the change.
