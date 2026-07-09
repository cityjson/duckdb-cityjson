# Review Fix Guide

This note turns the architectural and performance review into concrete work
items for `duckdb-cityjson`. It focuses on the current `feat/bbox-column` branch
against `origin/main`.

## Summary

Fix these before merging:

1. ✅ **Fixed.** Default-mode `COPY TO cityjson` drops geometry.
2. ✅ **Fixed.** Streaming `read_cityjsonseq` advertises `~0 rows` to DuckDB.
3. ✅ **Fixed.** `read_cityjsonseq` silently returns zero rows for regular CityJSON input.

Then address the follow-up performance items:

1. ⬜ Avoid repeated per-row geometry work for WKB, properties, and bbox.
2. ⬜ Keep pushed-down filters parallel where possible.
3. ⬜ Tighten reader responsibilities and format-specific contracts.

> **Status (correctness fixes 1–3 complete).** The three merge blockers above are
> implemented and covered by tests; the full SQL suite passes (272 assertions, 9
> files). Each fixed section below opens with an implementation note. The follow-up
> performance items (4–6) remain open.

## 1. Default COPY drops geometry

Priority: high. ✅ **Fixed** in `copy_function.cpp` / `copy_function.hpp`.

Resolution: `DetectColumnRole` now recognises the wide CityParquet columns
(`geometry_lod*`, `geometry_properties_lod*`) alongside the legacy `geometry` /
`geom_lod*` layout, and routes `bbox` to a dedicated `Bbox` role so it is no longer
written back as a stringified attribute. The sink pairs each geometry column with its
own per-LOD properties column (`geometry_lod2_2` → `geometry_properties_lod2_2`) via a
name-keyed map, falling back to the single legacy properties column for `geom_lod*`,
and applies the properties to the matching geometry rather than always `geometries[0]`.
Covered by the geometry round-trip assertions in `test/sql/cityjson_copy.test`.

Current behaviour:

- Default reads now emit CityParquet-style columns named `geometry_lod2_2`,
  `geometry_properties_lod2_2`, and `bbox`.
- `COPY TO cityjson` still recognises only `geometry` and the old `geom_lod*`
  pattern.
- A default read -> copy -> read round trip silently writes objects with empty
  geometry.

Confirmed with:

```sh
build/release/duckdb -unsigned -c "LOAD 'build/release/extension/cityjson/cityjson.duckdb_extension'; COPY (SELECT * FROM read_cityjson('test/data/minimal.city.json')) TO '/tmp/cityjson-review-rt.city.json' (FORMAT cityjson); SELECT COUNT(*) FILTER (WHERE column_name = 'geometry_lod2_2') FROM (DESCRIBE SELECT * FROM read_cityjson('/tmp/cityjson-review-rt.city.json'));"
```

Expected result after the fix: `1`.

Current result: `0`.

Where to fix:

- `src/cityjson/copy_function.cpp`
- `src/include/cityjson/copy_function.hpp` if the role model needs to support
  multiple geometry-property columns.
- `test/sql/cityjson_copy.test`

Implementation plan:

1. Update `DetectColumnRole` so it recognises:
   - `geometry`
   - `geometry_lod*`
   - old `geom_lod*`, if backward compatibility is still useful
   - `geometry_properties`
   - `geometry_properties_lod*`
2. Stop storing only one `geometry_properties_col`. The default wide layout can
   have one properties column per LOD.
3. Pair geometry columns and properties columns by LOD:
   - `geometry` pairs with `geometry_properties`
   - `geometry_lod2_2` pairs with `geometry_properties_lod2_2`
   - `geometry_lod2` pairs with `geometry_properties_lod2`
4. When decoding each WKB BLOB, derive the LOD from the geometry column name.
5. Apply the matching properties JSON to the same geometry, not always to
   `geometries[0]`.
6. Add a round-trip assertion that checks geometry survives, not only row count
   and attributes.

Suggested helper shape:

```cpp
static bool IsWideGeometryColumn(const std::string &name);
static bool IsWideGeometryPropertiesColumn(const std::string &name);
static std::optional<std::string> ExtractLODFromWideColumn(const std::string &name);
```

Suggested bind data change:

```cpp
std::vector<idx_t> geometry_cols;
std::unordered_map<std::string, idx_t> geometry_properties_by_lod;
idx_t geometry_properties_col = DConstants::INVALID_INDEX; // keep for per-LOD mode
```

Suggested test:

```sql
COPY (SELECT * FROM read_cityjson('test/data/minimal.city.json'))
TO '__TEST_DIR__/rt_geom.city.json' (FORMAT cityjson);

SELECT COUNT(*) FILTER (WHERE column_name = 'geometry_lod2_2')
FROM (DESCRIBE SELECT * FROM read_cityjson('__TEST_DIR__/rt_geom.city.json'));
----
1

SELECT COUNT(*) FROM read_cityjson('__TEST_DIR__/rt_geom.city.json')
WHERE geometry_lod2_2 IS NOT NULL;
----
1
```

## 2. Streaming cardinality is wrong

Priority: medium-high. ✅ **Fixed** in `optional_callbacks.cpp`.

Resolution: `CityJSONCardinality` returns `nullptr` (unknown) for streaming scans
instead of deriving zero from the empty `chunks`, and `CityJSONProgress` returns
`-1.0` (unknown) for streaming rather than reporting an immediate 100%. The streaming
`EXPLAIN` now shows DuckDB's default `~1 row` estimate instead of the misleading
`~0 rows`.

Current behaviour:

- `read_cityjsonseq` now binds in streaming mode.
- Streaming bind data intentionally leaves `bind_data.chunks` empty.
- `CityJSONCardinality` still estimates from `bind_data.chunks`.
- DuckDB sees the scan as approximately zero rows.

Confirmed with:

```sh
build/release/duckdb -unsigned -c "LOAD 'build/release/extension/cityjson/cityjson.duckdb_extension'; EXPLAIN SELECT * FROM read_cityjsonseq('test/data/sample.city.jsonl');"
```

Current plan includes:

```text
~0 rows
```

Where to fix:

- `src/cityjson/optional_callbacks.cpp`
- `src/include/cityjson/table_function.hpp`
- `src/cityjson/bind_function.cpp`
- `src/cityjson/metadata_table_function.cpp` may provide useful count logic to
  reuse.

Recommended fix:

Do not report an exact max cardinality of zero for streaming scans.

Prefer this conservative first step:

```cpp
unique_ptr<NodeStatistics> CityJSONCardinality(ClientContext &context,
                                               const FunctionData *bind_data_p) {
    auto &bind_data = bind_data_p->Cast<CityJSONBindData>();

    if (bind_data.streaming) {
        return nullptr; // unknown cardinality
    }

    const size_t total = bind_data.chunks.TotalCityObjectCount();
    auto stats = make_uniq<NodeStatistics>();
    stats->has_estimated_cardinality = true;
    stats->estimated_cardinality = total;
    stats->has_max_cardinality = true;
    stats->max_cardinality = total;
    return stats;
}
```

If exact estimates matter later, add an optional streaming count cache:

- `std::optional<size_t> estimated_cardinality;`
- populate it only when a cheap metadata/count path is available.
- avoid a full second pass over remote CityJSONSeq files during bind.

Suggested test:

```sql
EXPLAIN SELECT * FROM read_cityjsonseq('test/data/sample.city.jsonl');
```

Assert manually for now that the plan no longer advertises `~0 rows`. If the
SQL test framework cannot match explain output robustly, add a small C++ or
scripted regression test.

Also update progress:

- `CityJSONProgress` currently uses `bind_data.chunks.TotalCityObjectCount()`.
- For streaming scans, return an unknown or coarse progress value rather than
  deriving from empty chunks.

## 3. `read_cityjsonseq` accepts regular CityJSON and returns zero rows

Priority: medium-high. ✅ **Fixed** in `reader_factory.cpp`, `reader.hpp`,
`bind_function.cpp`, `local_cityjsonseq_reader.cpp`.

Resolution: added a sequence-only factory `OpenCityJSONSeqFile` that always constructs
a `LocalCityJSONSeqReader`, and `CityJSONSeqBind` now uses it instead of the
auto-detecting `OpenAnyCityJSONFile`. `LocalCityJSONSeqReader::ReadMetadata` now rejects
non-sequence first lines two ways: an unparsable bare `{` (pretty-printed CityJSON) and
a parsed CityJSON object carrying a non-empty `CityObjects` (minified CityJSON). Both
surface a clear format error instead of an empty scan. Covered by two `statement error`
cases in `test/sql/cityjsonseq.test` (multi-line and minified fixtures).

Current behaviour:

- `CityJSONSeqBind` calls `OpenAnyCityJSONFile`.
- That factory can return `LocalCityJSONReader` for a `.city.json` file.
- The scan path is marked `streaming=true`.
- `StreamingScan` calls `ReadNextFeature()`, whose base implementation returns
  `nullopt`.
- The query succeeds with zero rows instead of reporting a format error.

Confirmed with:

```sh
build/release/duckdb -unsigned -c "LOAD 'build/release/extension/cityjson/cityjson.duckdb_extension'; SELECT COUNT(*) FROM read_cityjsonseq('test/data/minimal.city.json');"
```

Current result: `0`.

Where to fix:

- `src/cityjson/bind_function.cpp`
- `src/cityjson/reader_factory.cpp`
- `src/include/cityjson/reader.hpp`

Recommended fix:

Make the public functions enforce their contracts:

- `read_cityjson`: may auto-detect CityJSON or CityJSONSeq.
- `read_cityjsonseq`: must construct `LocalCityJSONSeqReader` directly or call a
  dedicated factory that only accepts sequence input.
- `cityjsonseq_metadata`: already does this correctly; copy that approach.
- `LocalCityJSONSeqReader::ReadMetadata()` should reject a first-line CityJSON
  object that contains non-empty `CityObjects`; an empty `CityObjects` object is
  acceptable because this writer emits one in CityJSONSeq headers.
- During bind, detect the regular-CityJSON single-line case. If schema sampling
  reads no `CityJSONFeature` lines and the header had non-empty `CityObjects`,
  raise a format error instead of exposing an empty scan.

Suggested implementation:

```cpp
std::unique_ptr<CityJSONReader> OpenCityJSONSeqFile(ClientContext &context,
                                                    const std::string &file_name,
                                                    size_t sample_lines) {
    return std::make_unique<LocalCityJSONSeqReader>(context, file_name, sample_lines);
}
```

Then in `CityJSONSeqBind`:

```cpp
reader = OpenCityJSONSeqFile(context, file_name, options.sample_lines);
```

The sequence reader should reject non-sequence content before scan starts. Do
not rely on `ReadNextFeature()` returning `nullopt`; that is what currently
turns regular CityJSON input into a successful empty result.

Suggested test:

```sql
statement error
SELECT * FROM read_cityjsonseq('test/data/minimal.city.json');
----
First line must be CityJSON metadata
```

Use the exact error text produced by the final implementation.

## 4. Reduce repeated geometry work

Priority: medium.

Current behaviour:

- `WriteCityObjectRow` resolves geometries per projected geometry/properties
  column.
- WKB encoding allocates a fresh `std::vector<uint8_t>` for every row and LOD.
- `bbox` walks geometry boundaries separately.
- `geometry_properties` serialises JSON separately.

Where to improve:

- `src/cityjson/scan_function.cpp`
- `src/cityjson/city_object_utils.cpp`
- `src/cityjson/vector_writer.cpp`

Recommended approach:

Create a small row-local cache inside `WriteCityObjectRow`:

```cpp
struct RowGeometryCache {
    std::unordered_map<std::string, Geometry> geometry_by_lod;
    std::unordered_map<std::string, std::vector<uint8_t>> wkb_by_lod;
    std::unordered_map<std::string, json> properties_by_lod;
    std::optional<GeographicalExtent> bbox;
};
```

Keep it local to one output row so there is no lifetime or concurrency problem.

Do not compute anything unless the column is projected. Projection pushdown is
already available in `projected_cols`; use it as the source of truth.

Suggested checks:

- `SELECT id FROM read_cityjson(...);` should not encode WKB or compute bbox.
- `SELECT geometry_lod2_2 FROM read_cityjson(...);` should not compute
  geometry properties or bbox.
- `SELECT *` should still produce all geometry columns and bbox.

This may need instrumentation or a benchmark rather than a SQL assertion.

## 5. Keep scalar filter pushdown parallel where possible

Priority: medium.

Current behaviour:

- Equality filters on `id`, `feature_id`, and `object_type` are consumed.
- When any pushed filter exists, `MaxThreads()` returns `1`.
- For already materialised CityJSON reads, this can turn a broad filter such as
  `object_type = 'Building'` into a single-threaded scan even though chunks are
  already available.

Where to improve:

- `src/cityjson/global_state.cpp`
- `src/cityjson/scan_function.cpp`
- `src/cityjson/bind_function.cpp`

Recommended approach:

- Keep streaming scans single-threaded for now.
- Keep materialised scans parallel even with equality filters.
- Let each batch scan its assigned range and emit only matching rows.
- It is acceptable for filtered batches to return fewer than
  `STANDARD_VECTOR_SIZE` rows.

Be careful:

- DuckDB table functions may be called repeatedly until exhaustion. Returning a
  short chunk is fine, but returning an empty chunk usually signals completion.
- If a batch contains no matches, the scan should continue to later batches
  before returning an empty output.

Suggested shape:

```cpp
while (output_row == 0) {
    size_t batch_index = global_state.batch_index.fetch_add(1);
    if (batch_index >= active_plan.BatchCount()) {
        output.SetCardinality(0);
        return;
    }
    // scan this batch, applying MatchesFilters
}
```

Suggested tests:

- Existing filter tests should still pass.
- Add a multi-batch fixture if possible, with early batches containing no
  matches and later batches containing matches.

## 6. Clean up reader contracts

Priority: medium.

The current reader abstraction still mixes too many responsibilities:

- file opening
- remote/local IO
- metadata parsing
- schema sampling
- full materialisation
- streaming feature reads
- counting

Where to improve:

- `src/include/cityjson/reader.hpp`
- `src/cityjson/local_cityjson_reader.cpp`
- `src/cityjson/local_cityjsonseq_reader.cpp`
- `src/cityjson/reader_factory.cpp`

Recommended split:

- `CityJSONSource`: opens local or remote file handles.
- `CityJSONRecordCursor`: yields metadata and feature records.
- `CityJSONParser`: converts JSON records to typed structs.
- `CityJSONSchemaInferer`: samples records and returns columns.
- `CityJSONScanPlanner`: maps output batches to source positions.

Do not do this as a large rewrite before the three correctness fixes. The
short-term goal is to make the existing contracts explicit:

- generic factory for `read_cityjson`
- sequence-only factory for `read_cityjsonseq`
- unknown cardinality for streaming unless explicitly counted

## Verification checklist

Before merging, run:

```sh
make test
```

Also run these targeted checks:

```sh
build/release/duckdb -unsigned -c "LOAD 'build/release/extension/cityjson/cityjson.duckdb_extension'; COPY (SELECT * FROM read_cityjson('test/data/minimal.city.json')) TO '/tmp/cityjson-review-rt.city.json' (FORMAT cityjson); SELECT COUNT(*) FILTER (WHERE column_name = 'geometry_lod2_2') FROM (DESCRIBE SELECT * FROM read_cityjson('/tmp/cityjson-review-rt.city.json'));"
```

```sh
build/release/duckdb -unsigned -c "LOAD 'build/release/extension/cityjson/cityjson.duckdb_extension'; EXPLAIN SELECT * FROM read_cityjsonseq('test/data/sample.city.jsonl');"
```

```sh
build/release/duckdb -unsigned -c "LOAD 'build/release/extension/cityjson/cityjson.duckdb_extension'; SELECT COUNT(*) FROM read_cityjsonseq('test/data/minimal.city.json');"
```

Expected outcomes after fixes:

- The geometry round-trip query returns `1`.
- The `EXPLAIN` output no longer says `~0 rows` for `read_cityjsonseq`.
- The regular CityJSON input to `read_cityjsonseq` raises a clear binder or
  format error.

## Suggested commit order

1. Fix default COPY geometry role detection and add round-trip geometry tests.
2. Fix streaming cardinality/progress and add an explain/statistics regression.
3. Enforce `read_cityjsonseq` sequence-only reader construction.
4. Optimise row-local geometry computation.
5. Revisit parallel filtered materialised scans.
6. Refactor reader contracts once behaviour is stable.
