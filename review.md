# Code Review Notes

This review focuses on the current CityJSON extension code under `src/cityjson`
and `src/include/cityjson`, especially module responsibility, IO performance,
pushdown opportunities, and naming/API consistency.

## Highest Priority Issues

### Bind data equality ignores schema-changing parameters

`CityJSONBindData::Equals` currently compares only `file_name`.

Relevant file:

- `src/cityjson/bind_data.cpp`

Why it matters:

- `lod` and `use_wkb_encoding` change both the output schema and the row values.
- DuckDB may use bind data equality in planning/caching paths, so treating
  `read_cityjson(path)` and `read_cityjson(path, lod => '2.2')` as equal is risky.

Improvement tips:

- Include all bind-time options that affect schema or results in `Equals`.
- At minimum compare `file_name`, `target_lod`, and `use_wkb_encoding`.
- Add a SQL test that queries the same file with and without `lod` in one script
  and verifies different schemas.

### `sample_lines` is registered but not used

`sample_lines` is documented and registered as a named parameter, but bind only
parses `lod`. Some reader construction paths hard-code `100`.

Relevant files:

- `src/cityjson/table_function_registration.cpp`
- `src/cityjson/bind_function.cpp`
- `src/cityjson/flatcitybuf_table_function.cpp`
- `README.md`

Why it matters:

- The public API promises user-controlled schema sampling.
- Users cannot trade schema quality against bind-time cost.
- Tests will not catch regressions in schema inference depth.

Improvement tips:

- Add a small bind options struct, for example `CityJSONReadOptions`.
- Parse `sample_lines` once in shared bind helper code.
- Pass it into reader factories instead of relying on `DEFAULT_SAMPLE_LINES`.
- Validate it: reject negative values and define behavior for `0`.
- Add a SQL test with an attribute that appears only after the first sample row.

## Module Responsibility

### Reader classes do too much

The `CityJSONReader` implementations currently cover:

- file opening
- remote/local content loading
- format-specific parsing
- metadata extraction
- schema inference
- chunk creation
- full data materialization
- partial read helpers such as `ReadNFeatures`

Relevant files:

- `src/include/cityjson/reader.hpp`
- `src/cityjson/local_cityjson_reader.cpp`
- `src/cityjson/local_cityjsonseq_reader.cpp`
- `src/cityjson/flatcitybuf_reader.cpp`

Why it matters:

- The design makes streaming and pushdown difficult.
- Schema inference and scan execution are coupled to full materialization.
- Format-specific IO behavior leaks into bind functions.

Improvement tips:

- Split the reader layer into smaller roles:
  - `CityJSONSource`: opens local/remote file handles or byte streams.
  - `CityJSONRecordCursor`: yields metadata and feature records incrementally.
  - `CityJSONParser`: converts JSON records into `CityJSON`/`CityJSONFeature`.
  - `CityJSONSchemaInferer`: samples records and returns columns.
  - `CityJSONScanPlanner`: owns chunk offsets and scan scheduling.
- Keep the current `CityJSONReader` interface as an adapter temporarily if that
  helps avoid a large refactor.

### Bind logic is duplicated across formats

`read_cityjson`, `read_cityjsonseq`, and `read_flatcitybuf` repeat the same
logic for option parsing, metadata loading, full data loading, LOD schema
inference, and output type population.

Relevant files:

- `src/cityjson/bind_function.cpp`
- `src/cityjson/flatcitybuf_table_function.cpp`

Why it matters:

- Parameter bugs are easy to fix in one path and miss in another.
- New options such as filter pushdown or `sample_lines` have to be threaded
  through multiple nearly identical implementations.

Improvement tips:

- Extract a shared helper such as `BindCityJSONRead`.
- Let the caller provide:
  - function name for error messages
  - reader factory
  - format-specific constraints
- Return a fully populated `CityJSONBindData`.

### Vector writing mixes extraction, conversion, and serialization

`CityJSONScan` creates JSON values for projected columns, then `vector_writer`
serializes or parses those JSON values again.

Relevant files:

- `src/cityjson/scan_function.cpp`
- `src/cityjson/vector_writer.cpp`
- `src/cityjson/city_object_utils.cpp`

Why it matters:

- Geometry values are converted to JSON and then parsed back into `Geometry`.
- The writer becomes responsible for data interpretation, not just DuckDB vector
  writes.
- This adds avoidable CPU and allocation overhead on geometry-heavy data.

Improvement tips:

- Add direct writer helpers for common source types:
  - `WriteCityObjectField`
  - `WriteGeometryStruct`
  - `WriteGeometryWKB`
  - `WriteAttributeValue`
- Keep JSON serialization only for actual JSON/VARCHAR output columns.
- Make `vector_writer` responsible for DuckDB vector mechanics, not object
  extraction policy.

## IO And Performance

### Full data is read during bind

Bind currently calls `ReadAllChunks()` for normal scans. This means the complete
file is read and parsed before DuckDB begins execution.

Relevant files:

- `src/cityjson/bind_function.cpp`
- `src/cityjson/flatcitybuf_table_function.cpp`

Why it matters:

- Projection pushdown cannot reduce IO because the expensive read already
  happened.
- Parallel scanning is mostly scheduling over already materialized data.
- Large CityJSONSeq and remote files will pay high memory and latency costs.

Improvement tips:

- Store scan metadata and reader configuration in bind data, not all records.
- Move feature reading to scan-time local/global state.
- For CityJSONSeq, stream records line by line during execution.
- For regular CityJSON, full parse may still be necessary, but avoid parsing it
  multiple times and consider a lazy iterator over `CityObjects`.
- Keep full materialization as a fallback path if needed.

### Remote and DuckDB FileSystem reads load the entire file into memory

`OpenAnyCityJSONFile(ClientContext&, ...)` calls `ReadFileContent()` before it
even knows whether the file is CityJSON or CityJSONSeq.

Relevant files:

- `src/cityjson/reader_factory.cpp`
- `src/cityjson/json_utils.cpp`

Why it matters:

- CityJSONSeq loses its natural streaming advantage.
- Metadata-only and sampling queries still download/read the full object.
- Very large remote files can become memory-bound.

Improvement tips:

- Prefer DuckDB `FileHandle` or stream-style APIs over whole-file strings.
- Detect format from extension first where reliable.
- For `.jsonl`, read only the first line for metadata and then stream features.
- For remote files, avoid requiring `GetFileSize()` plus one full read whenever
  possible.

### Chunk scheduling repeatedly scans from the beginning

For every output batch, `CityJSONScan` starts at the first chunk and walks
feature counts until it finds the batch start position.

Relevant file:

- `src/cityjson/scan_function.cpp`

Why it matters:

- Later batches pay repeated prefix-scan cost.
- The cost grows with number of chunks/features.
- Parallel execution can amplify the repeated work.

Improvement tips:

- During bind or global init, build an index of row ranges:
  - chunk index
  - feature index
  - starting city object row
  - ending city object row
- Let `batch_index` map directly to a precomputed scan range.
- Alternatively, make `CityJSONFeatureChunk::chunks` represent exact output-row
  ranges instead of feature ranges.

### Metadata table reads all data to count objects

`cityjson_metadata` and `cityjsonseq_metadata` read all chunks just to populate
`city_objects_count`, and silently return `0` if counting fails.

Relevant file:

- `src/cityjson/metadata_table_function.cpp`

Why it matters:

- Metadata queries are expected to be cheap.
- Returning `0` on error makes count failures indistinguishable from empty data.
- This path repeats expensive scan parsing outside the main scan.

Improvement tips:

- Return `NULL` or expose an error if the count cannot be computed.
- Add a named parameter such as `count_objects => true/false` if exact counts are
  expensive.
- For CityJSONSeq, count objects with a lightweight streaming pass without
  retaining all features.
- For FlatCityBuf, continue using metadata count where available.

### Projection pushdown is only partially effective

The table functions enable `projection_pushdown`, and scan respects projected
columns when writing output. But all source records and all schema information
are already materialized before scan.

Relevant files:

- `src/cityjson/table_function_registration.cpp`
- `src/cityjson/scan_function.cpp`
- `src/cityjson/bind_function.cpp`

Improvement tips:

- Use projection information earlier in global/local init.
- In `lod` mode, only encode WKB if `geometry` is projected.
- Avoid computing `geometry_properties` unless projected.
- Consider a scan-time column plan that marks required source fields:
  - ids only
  - attributes only
  - hierarchy columns
  - geometry metadata
  - full geometry/WKB

### Filter pushdown opportunities

Filter pushdown is not implemented. Some filters are good candidates.

Good first targets:

- `id = ...`
- `feature_id = ...`
- `object_type = ...`
- `geometry IS NOT NULL` in `lod` mode
- bbox filters for FlatCityBuf, where the format has spatial index/extent data

Improvement tips:

- Start with simple equality filters on predefined scalar columns.
- Store accepted filters in bind or global state.
- Keep non-pushable filters in DuckDB by not consuming them.
- For FlatCityBuf, map bbox predicates to native selection APIs if available.

## Naming, Syntax, And API Consistency

### LOD formatting is inconsistent

Numeric LOD values are converted with `std::to_string(double)`, which can
produce strings like `2.000000`. Geometry column parsing expects a stricter
`geom_lodX_Y` pattern.

Relevant files:

- `src/cityjson/cityjson_types.cpp`
- `src/cityjson/column_types.cpp`
- `src/cityjson/lod_table.cpp`

Improvement tips:

- Create one canonical LOD utility module.
- Normalize all LODs at parse time.
- Support common CityJSON LOD spellings consistently:
  - `"2"`
  - `"2.0"`
  - `2`
  - `2.0`
- Use the same formatter for column names, LOD table names, and user parameter
  matching.

### `geographic` and `geographical` are mixed

The code has both `geographic_extent` and `geographical_extent` naming.

Relevant files:

- `src/include/cityjson/cityjson_types.hpp`
- `src/cityjson/metadata_table.cpp`
- `src/cityjson/column_types.cpp`

Improvement tips:

- Use CityJSON spec names at JSON boundaries.
- Use one internal C++ naming convention everywhere else.
- If SQL output column names are already public API, keep them stable and only
  normalize internal names.

### `VectorWrapper` advertises type safety but does not enforce it

`VectorWrapper` stores a `VectorType`, but `AsFlatMut`, `AsListMut`, and
`AsStructMut` all return the same raw pointer without checking the stored type.

Relevant file:

- `src/cityjson/vector_writer.cpp`

Improvement tips:

- Either remove `VectorWrapper` and pass `Vector&` directly, or enforce checks.
- If kept, make incorrect accessor usage throw an internal error.
- Update comments to match actual behavior.

### Comments include implementation-task labels

Several comments use labels such as `Task 22`, `Task 23`, and so on.

Relevant file:

- `src/cityjson/vector_writer.cpp`

Improvement tips:

- Replace task labels with domain-oriented section comments.
- Keep comments focused on why a block exists or what invariant it maintains.

### `return std::move(result)` for `unique_ptr`

Several functions return local `unique_ptr` values with `std::move`.

Relevant files:

- `src/cityjson/bind_data.cpp`
- `src/cityjson/bind_function.cpp`
- `src/cityjson/metadata_table_function.cpp`
- `src/cityjson/flatcitybuf_table_function.cpp`

Improvement tips:

- Prefer `return result;` for local return values.
- This is mostly style, but removing unnecessary moves improves consistency with
  modern C++ expectations.

## Suggested Refactor Order

1. Fix correctness/API issues first:
   - bind-data equality
   - `sample_lines` parsing and tests
   - LOD normalization

2. Extract shared bind options and bind helper:
   - one parser for named parameters
   - one path for LOD schema selection
   - one path for output names/types

3. Introduce a scan planning layer:
   - precompute row/chunk offsets
   - remove repeated prefix scans
   - make projected column requirements explicit

4. Make CityJSONSeq streaming real:
   - avoid whole-file `std::string` reads
   - read metadata and samples independently
   - scan records during execution

5. Simplify vector writing:
   - direct writers from typed CityJSON objects
   - JSON serialization only when the output column is JSON/VARCHAR

6. Add filter pushdown incrementally:
   - start with simple scalar filters
   - then `geometry IS NOT NULL`
   - then FlatCityBuf bbox/spatial pushdown

## Useful Tests To Add

- `sample_lines` affects schema inference.
- Same file queried with and without `lod` produces different schema and values.
- Numeric and string LOD values normalize to the same result.
- Projection of only `id` and `object_type` does not compute WKB in `lod` mode.
- Metadata count failure is not silently reported as zero.
- CityJSONSeq remote/local paths do not require full materialization for
  metadata-only access once streaming is introduced.

---

## Progress Update

Work is being done on branch `develop`. Each item below was fixed/addressed in a
separate commit and the full SQL test suite was run after every commit.
Current state: **all tests pass** (213 assertions in 7 test cases).

### Done

- [x] **Bind data equality ignores schema-changing parameters**
  - `CityJSONBindData::Equals` now compares `file_name`, `target_lod`, and
    `use_wkb_encoding`.
  - Added SQL test verifying different schemas with/without `lod`.
  - Commit: `225692c`

- [x] **`sample_lines` is registered but not used**
  - Added `CityJSONReadOptions` and `ParseCityJSONReadOptions`.
  - `sample_lines` is parsed once, validated (non-negative), and propagated to
    all reader factories.
  - Added SQL tests for late-attributes and rejection of negative values.
  - Commit: `d92ff22`

- [x] **LOD formatting is inconsistent**
  - Added canonical `LODTableUtils::NormalizeLOD` for strings and doubles.
  - Geometry LODs are normalized at parse time; column matching supports
    `geom_lod2` as well as `geom_lod2_2`.
  - Added SQL tests for numeric LODs.
  - Commit: `1b5cd00`

- [x] **`geographic` and `geographical` are mixed**
  - Renamed `Metadata::geographic_extent` → `geographical_extent` everywhere.
  - Commit: `3ca6350`

- [x] **Bind logic is duplicated across formats**
  - Extracted shared `BindCityJSONRead` helper used by CityJSON, CityJSONSeq,
    and FlatCityBuf binds.
  - Commit: `ba3ee07`

- [x] **Chunk scheduling repeatedly scans from the beginning**
  - Added `CityJSONScanPlan` / `CityJSONScanPosition` and
    `CityJSONFeatureChunk::BuildScanPlan`.
  - Scan now maps `batch_index` directly to a precomputed source position.
  - Commit: `75c67ee`

- [x] **Metadata table reads all data to count objects**
  - Added `CityJSONReader::CountCityObjects()` with a streaming override for
    `LocalCityJSONSeqReader`.
  - Metadata functions now use `CountCityObjects()` and propagate errors
    instead of silently returning `0`.
  - Commit: `e591b60`

- [x] **Full data is read during bind (CityJSONSeq)**
  - `read_cityjsonseq` now sets `streaming=true`; full materialization is
    deferred to `CityJSONInitGlobal`.
  - Commit: `e591b60`

- [x] **`VectorWrapper` advertises type safety but does not enforce it**
  - `AsFlatMut`, `AsListMut`, and `AsStructMut` now check the stored type and
    throw on mismatch.
  - Added a type-agnostic `SetNull` method for NULL handling.
  - Commit: `49f4907`

- [x] **Comments include implementation-task labels**
  - Replaced `Task 22/23/...` labels with domain-oriented section comments.
  - Commit: `49f4907`

- [x] **`return std::move(result)` for `unique_ptr`**
  - Removed unnecessary `std::move` returns across bind, init, and copy
    functions.
  - Commit: `9ef08f8`

- [x] **Vector writing mixes extraction, conversion, and serialization (partial)**
  - Added direct `WriteGeometry(Vector*, const Geometry&, size_t)` overload.
  - `CityJSONScan` now writes geometry struct columns directly from
    `Geometry` objects instead of serializing to JSON and parsing back.
  - Commit: `8146cad`

### Partially done / next up

- [ ] **Make CityJSONSeq streaming real**
  - Metadata-only queries are now cheap, and bind no longer materializes
    CityJSONSeq data. True per-batch scan-time streaming still needs a reader
    iterator and scan-loop rewrite.

- [ ] **Remote and DuckDB FileSystem reads load the entire file into memory**
  - `CountCityObjects()` streams from the pre-loaded string for CityJSONSeq
    metadata, but `OpenAnyCityJSONFile(ClientContext&, ...)` still uses
    `ReadFileContent()` for format detection. A streaming `FileHandle`-based
    path is still needed.

- [ ] **Reader classes do too much**
  - `BindCityJSONRead` reduces duplication, but the `CityJSONReader` classes
    still mix IO, parsing, schema inference, and materialization. Splitting
    into `CityJSONSource` / `CityJSONRecordCursor` / etc. is still open.

- [ ] **Projection pushdown is only partially effective**
  - Projected columns are respected when writing output, but source records are
    fully materialized and WKB/geometry-properties are computed regardless of
    projection. Needs a scan-time column plan.

- [ ] **Filter pushdown opportunities**
  - Not implemented. Recommended first step: equality filters on `id`,
    `feature_id`, and `object_type` via `pushdown_complex_filter`, storing
    accepted filters in bind/global state and skipping rows in the scan loop.

### Recommended restart point

The next chunk of work is **filter pushdown** (step 6 of the suggested refactor
order). It requires:
1. Setting `func.filter_pushdown = true` and registering a
   `pushdown_complex_filter` callback.
2. Consuming simple equality expressions on `id`/`feature_id`/`object_type`.
3. Rewriting the scan loop to skip non-matching rows across batch boundaries.

Alternatively, if IO/streaming is higher priority, continue with a
`FileHandle`-based streaming CityJSONSeq reader before tackling filters.

