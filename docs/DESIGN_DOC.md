# Architecture

**Purpose.** An orientation map: what the major pieces are, which way they
depend on each other, and why the design is shaped this way. It deliberately
carries **no signatures, member lists, or code**. Those live in the headers under
`src/include/cityjson/`, which are the authority and cannot drift from the
implementation the way a prose copy of them would.

For *what the functions do*, see [FUNCTIONS.md](FUNCTIONS.md). For the normative
encoding this extension implements, see the CityParquet specification in the
parent workspace's `documents/`.

---

## 1. What this is

A C++20 DuckDB extension registering SQL functions that read, write, and mutate
3D city models. It has two jobs that pull in different directions:

1. **Be a faithful CityJSON I/O path** — every CityObject, attribute, semantic
   surface and appearance reference survives a round trip.
2. **Be an executable prototype of the CityParquet encoding** — a columnar,
   analytics-shaped layout where geometry is WKB in per-LoD columns and a query
   engine can filter without parsing JSON.

Most of the design tension in this codebase comes from serving both at once:
CityJSON is a nested document format, CityParquet is a flat columnar one, and
the mapping between them is lossy in exactly one direction unless carefully
compensated for. `geometry_properties` is that compensation.

## 2. Layers

Dependencies point **downward**, with one acknowledged exception noted below.

```
┌──────────────────────────────────────────────────────────────────┐
│  Registration        cityjson_extension.cpp                      │
│                      table_function_registration.cpp             │
├──────────────────────────────────────────────────────────────────┤
│  SQL surface                                                     │
│    scan functions    bind → init_global → init_local → scan      │
│    copy sink         COPY … TO (cityjson|cityjsonseq|flatcitybuf)│
│    package pragmas   cityparquet_* / insert_*   (generate SQL)   │
│    scalar / table    metadata, geoparquet_geo, appearance, wkb   │
├──────────────────────────────────────────────────────────────────┤
│  Readers             CityJSONReader interface + implementations  │
│                      reader_factory (extension, then sniffing)   │
├──────────────────────────────────────────────────────────────────┤
│  Schema & encoding                                               │
│    column inference  column_types, city_object_utils, lod_table  │
│    geometry out      wkb_encoder, arrow_native_encoder           │
│    geometry in       wkb_decoder, geometry_properties            │
│    appearance        appearance_normalise                        │
│    vector writing    vector_writer                               │
├──────────────────────────────────────────────────────────────────┤
│  Core model          cityjson_types (CityJSON, CityObject, …)    │
│                      error, json_utils, crs_projjson              │
└──────────────────────────────────────────────────────────────────┘
```

**Readers sit above schema &amp; encoding, not below it.** A reader reports the
columns it will produce, so the interface itself depends on the column types, and
every concrete reader uses the shared attribute/geometry inference. Nothing in
the schema layer includes a reader header.

**The one upward edge:** the core geometry type normalises its own LoD string,
which lives in the schema layer. It is the single place the core model reaches
upward, and it is worth knowing about before assuming the layering is total.

## 3. Core data model

`cityjson_types.hpp` holds the in-memory shape of a CityJSON document. Everything
above it speaks these types; nothing below them exists.

```
CityJSON  ─── the document / stream header
  ├── Transform            scale + translate applied to integer vertices
  ├── Metadata             title, identifier, reference date, extent, CRS…
  │     ├── PointOfContact
  │     └── GeographicalExtent
  └── vertices             global pool (whole-document reads only)

CityJSONFeature  ─── one CityJSONSeq line
  ├── vertices             pool local to this feature
  └── CityObject           one or more, keyed by id
        ├── attributes     free-form, drives schema inference
        ├── Geometry       one per LoD present
        │     ├── boundaries   nested index arrays into the vertex pool
        │     ├── semantics    surfaces + values
        │     └── material / texture   theme-shaped maps
        ├── children / parents / children_roles
        ├── GeographicalExtent
        └── feature_id     root-family id

CityJSONFeatureChunk  ─── features + the chunk ranges a scan walks
```

**The vertex-pool rule is the one thing to internalise.** Geometry boundaries are
*indices*, and which pool they index depends on the format: a CityJSONSeq feature
carries its own, a whole CityJSON document shares one global pool. Resolving the
wrong pool produces geometry that is silently, plausibly wrong. Every encoder
takes the pool explicitly rather than reaching for a default.

## 4. Readers

`CityJSONReader` is a small abstract interface — identify yourself, read
metadata, read all chunks, read N features for sampling, report columns, count
objects. `reader_factory` picks an implementation by file extension, falling back
to content sniffing for an ambiguous `.json` or an unknown extension.

The **generic** read path goes through that factory and never names a concrete
reader. Format-specific functions deliberately do the opposite: the FlatCityBuf
table function and the CityJSONSeq metadata function each construct their one
reader directly, because for them the format is not in question.

| Implementation | Format | Loading |
| -------------- | ------ | ------- |
| `LocalCityJSONReader` | `.city.json` | whole document into memory |
| `LocalCityJSONSeqReader` | `.city.jsonl` | incremental, line by line |
| `FlatCityBufReader` | `.fcb` | random access via range reads |

Remote files are not a fourth reader. They go through DuckDB's own FileSystem —
including `httpfs` for HTTP/S3/GCS — so there is **one** HTTP stack and one
credentials/secrets/proxy story across every format. FlatCityBuf's range reads
are adapted onto that same FileSystem rather than using the upstream library's
own HTTP client.

**Streaming is a property of the reader, not of the scan.** The CityJSONSeq
reader is a forward cursor: its "read the next feature" operation deliberately
does *not* rewind, because that is the streaming scan's position. The bulk
operations rewind so they can be asked more than once.

## 5. The scan path

DuckDB's table function lifecycle — **bind → init global → init local → scan**,
called repeatedly — with the responsibilities split like this:

- **Bind** resolves parameters, opens a reader, reads metadata, and infers the
  column schema by sampling. Its output is immutable for the query's lifetime and
  shared across threads.
- **Init global** creates shared execution state. For most formats this is just a
  chunk cursor. FlatCityBuf is the exception: its projection is only known here,
  not at bind, so it does its one real read at this point and the bind's chunk
  storage stays empty.
- **Init local** holds the per-thread projection.
- **Scan** walks CityObjects and writes them into DuckDB vectors, honouring the
  projection and any pushed-down equality filters, until the source is exhausted.

**Schema inference has exactly one entry point.** Anything that needs to know
which columns a read will produce — most importantly the package pragmas, which
must generate SQL naming those columns *before* the read runs — calls the same
inference the bind calls. Re-deriving it from its ingredients produced a
different answer, and the generated SQL then named a column the staged relation
did not have.

## 6. Geometry: two encodings, one property struct

A CityJSON geometry is nested index arrays. Two physical encodings exist:

- **WKB** (default) — a `BLOB` per LoD. Universally readable; solids become
  `PolyhedralSurface Z`. Rings are closed, source winding preserved.
- **Arrow-native** (experimental) — five nested LIST levels (solid → shell → face
  → ring → index) plus a sibling vertex-pool column. Rings are *not* closed.

The encoding is chosen by rewriting the finished column list at a single point,
so neither of the two places that derive geometry columns — the wide layout and
the single-LoD layout — needs to know encodings exist.

**`geometry_properties` is invariant across encodings, and it is load-bearing.**
Neither physical form can express the CityJSON geometry *type* — the nesting is
uniform across `Solid` and `MultiSurface` — nor semantic surfaces, nor a solid's
shell partition. So a companion struct carries the type, the surfaces, a
WKB-face-aligned semantics array, and the per-shell face counts. Two consequences
worth stating plainly:

- **Never infer the CityJSON type from the geometry's shape.** The property
  struct is the only truth.
- The face-aligned semantics array is a native list, not JSON, precisely so
  surface-level analysis is a columnar filter rather than a parse.

## 7. Appearance

Materials and textures are kept **separate from geometry**, following OBJ /
COLLADA / glTF precedent, and exist in two modes:

- **local** (default) — the source's own feature-local indices, passed through
  verbatim.
- **sidecar** — dataset-global ids into interned material/texture tables, with
  texture UVs inlined.

Sidecar mode exists because CityParquet needs it: once every feature's rows share
one table, a feature-local index resolves to the wrong definition. Interning is
by **structural equality**, because CityJSON gives a material no identity of its
own, and header-declared entries intern first so their ids keep their ordinal
positions.

Geometry templates sit slightly outside the model: they are in local, unplaced
coordinates, exempt from the dataset transform and the file CRS, and an instance's
own matrix places them. That exemption is why templates are the one thing in a
package that legitimately declares no CRS.

## 8. The package layer

A CityParquet package is a **directory of Parquet files**; loaded into DuckDB it
becomes a **schema** whose table names are the spec's file basenames, plus one
bookkeeping table recovered from the Parquet footers. Naming is the entire
binding — there is no registration state to keep in sync.

**The central design decision: these functions generate SQL rather than execute
it.** They are DuckDB pragmas whose return value is SQL text, which DuckDB then
parses and runs in place of the pragma, inside the caller's transaction. So
atomicity, rollback and constraint enforcement are DuckDB's, not ours — a package
mutation is undone by `ROLLBACK` like any other statement. Most mutating pragmas
have a scalar twin returning the same text without running it, which makes the
layer inspectable and testable as strings. (The coverage is not total: of the
three insert pragmas only the CityJSON one has a twin.)

The cost is that generation happens *before* execution, for the whole submitted
script: a generator's view of the catalog and data is pre-batch. Everything
generated is therefore written to be idempotent, and anything genuinely
destination-dependent is deferred into the generated SQL rather than decided
while generating it.

**One function breaks the rule, for a reason worth remembering.** The package
writer must sometimes emit a Parquet footer key and sometimes *omit* it — the
spec forbids declaring GeoParquet metadata for a solid-only table — and SQL
cannot branch the shape of a `COPY`, nor can DuckDB's key-value metadata option
omit a key. So the writer is a table function assembling metadata in C++, at the
cost of running on an internal connection and seeing only committed state.

Derived state is the other organising idea here. `feature_id`, `bbox`, and the
reciprocal hierarchy arrays are *computed*, not authored, so any structural edit
invalidates them and a single reconcile operation re-derives exactly those. This
is why there is deliberately no update wrapper: attribute edits are ordinary SQL
and invalidate nothing.

## 9. Writing

One shared sink serves all three output formats, so format-independent concerns —
reassembling CityObjects from rows, the CityGML-to-CityJSON type mapping,
quantising vertices against the transform — are implemented once. The formats
differ only in how they lay out what the sink produces: one document with a
global vertex pool, one line per feature with local pools, or a binary file with
indices.

Vertices are quantised to integers against the transform on the way out, so the
transform's scale *is* the output precision. The default is chosen so round trips
stay lossless for large projected national coordinates.

## 10. CRS handling

CRSs are compared as **PROJJSON**, never as strings: a Parquet footer holds
PROJJSON while a CityJSON source holds a reference-system URL, and comparing them
raw made every insert into a known-CRS package a bogus mismatch. The source is
resolved and re-serialised so both sides are canonical text.

The stored CRS is **tri-state**, following GeoParquet's convention: an object
means known, explicit null means "there are CRS-bearing coordinates here whose
CRS we cannot state", and absent means the default geographic CRS — so the key
may be omitted only by a file with no CRS-bearing coordinate at all. The
distinction matters because omitting the key on projected national coordinates
silently asserts something false. A package states one CRS for every row it
holds, so an unknown on either side of an insert or merge is refused rather than
assumed. Reprojection is never performed.

## 11. Invariants

Things that hold across the whole codebase, and that a change should not quietly
break:

1. **One row per CityObject**, in every read path, at every projection.
2. **Every projected column's value is identical** regardless of which internal
   path produced it — full or selective decode, streaming or materialised.
3. **The LoD lives in the column name**, never in a value. That is what keeps it
   recoverable on export.
4. **The geometry property struct is the only source of CityJSON geometry type.**
5. **Source vertex order and ring winding are preserved** on both encode and
   decode.
6. **Derived state is re-derivable.** Nothing that can be recomputed from
   geometry and hierarchy is treated as authored data.

## 12. Where the detail lives

| Question | Look at |
| -------- | ------- |
| What does this function do, with examples? | [FUNCTIONS.md](FUNCTIONS.md) |
| What are the exact types and signatures? | `src/include/cityjson/*.hpp` |
| What are the known traps in this layer? | [CLAUDE.md](../CLAUDE.md) / [AGENTS.md](../AGENTS.md) |
| What is the normative encoding? | the parent workspace's `documents/` |
| How is behaviour pinned? | `test/sql/*.test`, plus opt-in harnesses in `test/cpp/` |

## References

- [CityJSON specification](https://www.cityjson.org/specs/)
- [CityJSONSeq specification](https://www.cityjson.org/cityjsonseq/)
- [GeoParquet](https://geoparquet.org/)
- [DuckDB extension development](https://duckdb.org/docs/stable/dev/extensions)
