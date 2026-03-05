# Development Plan: Remote Reader, FlatCityBuf, and COPY TO

## Context

The DuckDB CityJSON extension currently only supports local file reading via `std::ifstream`. Three new capabilities are needed:

1. **Remote file support** — read CityJSON/CityJSONSeq from HTTP, S3, GCS URLs
2. **FlatCityBuf reader** — read `.fcb` files (cloud-optimized binary CityJSON format)
3. **COPY TO** — write query results to `.city.json` / `.city.jsonl` files

Implementation order: Task 1 → Task 2 → Task 3 (each builds on prior work).

---

## Task 1: Remote File Reader (DuckDB FileSystem API)

### Approach

Replace `std::ifstream` with DuckDB's `FileSystem` API. Read entire file content into a `std::string` buffer, then parse from the buffer. This fits the current "load all in bind" pattern.

### Files to Modify

| File | Change |
|------|--------|
| `src/include/cityjson/json_utils.hpp` | Add `ReadFileContent(ClientContext&, const string&)` |
| `src/cityjson/json_utils.cpp` | Implement using `FileSystem::GetFileSystem(context).OpenFile()` |
| `src/include/cityjson/reader.hpp` | Add content-based constructors; update `OpenAnyCityJSONFile` signature to accept `ClientContext&` |
| `src/cityjson/reader_factory.cpp` | Accept `ClientContext&`, read content via `ReadFileContent()`, pass to constructors |
| `src/cityjson/local_cityjson_reader.cpp` | Add constructor from string content; parse from buffer instead of ifstream |
| `src/cityjson/local_cityjsonseq_reader.cpp` | Add constructor from string content; use `std::istringstream` instead of `std::ifstream` |
| `src/cityjson/bind_function.cpp` | Pass `context` to `OpenAnyCityJSONFile()` |
| `src/cityjson/metadata_table_function.cpp` | Pass `context` to factory |

### Implementation Details

1. **`ReadFileContent()`**: Use `FileSystem::GetFileSystem(context)` → `fs.OpenFile(path, FILE_FLAGS_READ)` → `handle->GetFileSize()` → `handle->Read(buffer, size)` → return string
2. **Reader constructors**: Add `(const string& name, string content, size_t sample_lines)` overloads that store content and parse from it
3. **Factory**: `OpenAnyCityJSONFile(context, file_name)` calls `ReadFileContent()` and passes content to reader constructors
4. **CityJSONSeq line reading**: Replace `std::ifstream` with `std::istringstream(content_)` — all `std::getline` calls work identically

### Verification

```bash
cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb
./build/release/duckdb -c "INSTALL httpfs; LOAD httpfs; SELECT COUNT(*) FROM read_cityjson('https://3d.bk.tudelft.nl/opendata/cityjson/2.0/DenHaag_01.city.json');"
# Existing local tests must still pass
make test
```

---

## Task 2: FlatCityBuf Reader

### Approach

Add `FlatCityBufReader` implementing `CityJSONReader`. FCB features return as CityJSONFeature JSON strings — reuse existing `CityJSONFeature::FromJson()`. Link `libfcb_cpp.a` via CMake FetchContent from GitHub releases.

### Files to Create

| File | Purpose |
|------|---------|
| `src/include/cityjson/flatcitybuf_reader.hpp` | `FlatCityBufReader` class declaration |
| `src/cityjson/flatcitybuf_reader.cpp` | FCB reader implementation |
| `src/include/cityjson/flatcitybuf_table_function.hpp` | Registration declarations |
| `src/cityjson/flatcitybuf_table_function.cpp` | `read_flatcitybuf` bind/registration |

### Files to Modify

| File | Change |
|------|--------|
| `src/cityjson/reader_factory.cpp` | Add `.fcb` extension detection (guarded by `#ifdef CITYJSON_HAS_FCB`) |
| `src/cityjson_extension.cpp` | Register `read_flatcitybuf` and `flatcitybuf_metadata` functions |
| `CMakeLists.txt` | FetchContent for libfcb_cpp.a; add new source files; define `CITYJSON_HAS_FCB` |

### Implementation Details

1. **`FlatCityBufReader`**: Opens FCB file, reads metadata via `fcb_reader_metadata()`, iterates features via `fcb_reader_select_all()` or `fcb_reader_select_bbox(bbox)`. Parses each JSON string with `CityJSONFeature::FromJson()`.
2. **Table function**: `read_flatcitybuf(path, [bbox, sample_lines, lod])`. Reuses `CityJSONScan` for the scan phase since data is in the same `CityJSONFeatureChunk` format.
3. **Build**: CMake `FetchContent_Declare` downloads pre-built `libfcb_cpp.a` + headers from `github.com/cityjson/flatcitybuf/releases`. Guarded by `CITYJSON_HAS_FCB` compile definition.

### FCB C++ API (key functions)

```cpp
fcb_reader_open(path)           // Open FCB file
fcb_reader_metadata()           // Get version, feature count, CRS, transform
fcb_reader_select_all()         // Iterate all features
fcb_reader_select_bbox(bbox)    // Spatial query
// Each feature returns: { id: string, json: string (CityJSONFeature JSON) }
```

### Verification

```bash
# Convert test data to FCB first
fcb ser -i test/data/sample.city.jsonl -o test/data/sample.fcb
cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb
./build/release/duckdb -c "SELECT COUNT(*) FROM read_flatcitybuf('test/data/sample.fcb');"
./build/release/duckdb -c "SELECT * FROM flatcitybuf_metadata('test/data/sample.fcb');"
make test
```

---

## Task 3: COPY TO Statement

### Approach

Register `CopyFunction` for formats `cityjson` and `cityjsonseq`. Metadata is provided via `metadata_query` option that executes a SQL query (e.g., from `cityjson_metadata()`). The sink phase accumulates CityObjects grouped by `feature_id`, and the finalize phase writes the output file.

### Files to Create

| File | Purpose |
|------|---------|
| `src/include/cityjson/copy_function.hpp` | Copy function bind data, global/local state |
| `src/cityjson/copy_function.cpp` | All COPY TO callbacks (options, bind, init, sink, combine, finalize) |
| `src/include/cityjson/cityjson_writer.hpp` | Writer utilities declaration |
| `src/cityjson/cityjson_writer.cpp` | CityJSON/CityJSONSeq file writing + vertex quantisation |

### Files to Modify

| File | Change |
|------|--------|
| `src/cityjson_extension.cpp` | Register COPY functions |
| `CMakeLists.txt` | Add new source files |

### COPY Function Callbacks

1. **`copy_options`**: Define options — `version`, `crs`, `metadata_query`, `transform_scale`, `transform_translate`
2. **`copy_to_bind`**:
   - If `metadata_query` is set: execute it, extract version/transform/CRS from result
   - Map input columns to roles: `id`, `feature_id`, `object_type`, `children`, `parents`, `children_roles`, `geometry`/`geom_lod*`, `geometry_properties`, and remaining = attributes
   - Validate mandatory columns exist (`id`, `feature_id`, `object_type`)
3. **`copy_to_sink`**: For each row in DataChunk:
   - Extract `feature_id`, `id`, `object_type`
   - Build `CityObject` with type, attributes (non-reserved columns), geometry, hierarchy
   - Reconstruct semantic surfaces from `geometry_properties` column
   - Buffer locally
4. **`copy_to_combine`**: Lock global state mutex, merge local buffer into `feature_objects` map grouped by `feature_id`
5. **`copy_to_finalize`**:
   - For `.city.json`: Build single JSON object with all CityObjects, global vertex pool (quantised), write file
   - For `.city.jsonl`: Write metadata line, then one `CityJSONFeature` JSON per line with per-feature vertex pool

### Vertex Quantisation

When `transform` is available:
```
compressed_vertex[i] = round((real_coord[i] - translate[i]) / scale[i])
```
- Collect all unique compressed vertices into a pool
- Replace boundary coordinate values with vertex indices into the pool
- For `.city.jsonl`: per-feature vertex pool
- For `.city.json`: global vertex pool

### Semantic Surface Reconstruction

The `geometry_properties` column contains JSON with semantics/material/texture. During COPY TO:
1. Parse `geometry_properties` JSON
2. Attach `semantics`, `material`, `texture` back to the `Geometry` object
3. Write as part of the geometry in the output

### Column Role Detection

| Column Name Pattern | Role |
|---|---|
| `id` | CityObject ID (key in `CityObjects` map) |
| `feature_id` | Feature grouping key |
| `object_type` | CityObject `type` field |
| `children` | CityObject `children` array |
| `parents` | CityObject `parents` array |
| `children_roles` | CityObject `children_roles` array |
| `geom_lod*` or `geometry` | Geometry (LOD-specific or single) |
| `geometry_properties` | Semantics/material/texture JSON |
| `other` | Extension fields |
| Everything else | `attributes` map entries |

### SQL Usage

```sql
-- Using metadata_query to get metadata from an existing file
COPY (SELECT * FROM read_cityjson('input.city.json'))
TO 'output.city.json'
(FORMAT cityjson,
 metadata_query 'SELECT * FROM cityjson_metadata(''input.city.json'')');

-- CityJSONSeq output
COPY (SELECT * FROM read_cityjsonseq('input.city.jsonl'))
TO 'output.city.jsonl'
(FORMAT cityjsonseq,
 metadata_query 'SELECT * FROM cityjsonseq_metadata(''input.city.jsonl'')');

-- Explicit metadata values also supported
COPY (SELECT * FROM my_buildings)
TO 'output.city.json'
(FORMAT cityjson,
 version '2.0',
 transform_scale '0.001,0.001,0.001',
 transform_translate '84000,446000,0',
 crs 'urn:ogc:def:crs:EPSG::7415');
```

### Verification

```bash
cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb

# Round-trip test: read → COPY TO → read again → compare
./build/release/duckdb -c "
  COPY (SELECT * FROM read_cityjson('test/data/minimal.city.json'))
  TO '/tmp/test_output.city.json'
  (FORMAT cityjson, metadata_query 'SELECT * FROM cityjson_metadata(''test/data/minimal.city.json'')');
  SELECT COUNT(*) FROM read_cityjson('/tmp/test_output.city.json');
"

# CityJSONSeq round-trip
./build/release/duckdb -c "
  COPY (SELECT * FROM read_cityjsonseq('test/data/sample.city.jsonl'))
  TO '/tmp/test_output.city.jsonl'
  (FORMAT cityjsonseq, metadata_query 'SELECT * FROM cityjsonseq_metadata(''test/data/sample.city.jsonl'')');
  SELECT COUNT(*) FROM read_cityjsonseq('/tmp/test_output.city.jsonl');
"

make test
```

---

## Summary of All New/Modified Files

### New Files (8)
- `src/include/cityjson/flatcitybuf_reader.hpp`
- `src/cityjson/flatcitybuf_reader.cpp`
- `src/include/cityjson/flatcitybuf_table_function.hpp`
- `src/cityjson/flatcitybuf_table_function.cpp`
- `src/include/cityjson/copy_function.hpp`
- `src/cityjson/copy_function.cpp`
- `src/include/cityjson/cityjson_writer.hpp`
- `src/cityjson/cityjson_writer.cpp`

### Modified Files (10)
- `src/include/cityjson/json_utils.hpp`
- `src/cityjson/json_utils.cpp`
- `src/include/cityjson/reader.hpp`
- `src/cityjson/reader_factory.cpp`
- `src/cityjson/local_cityjson_reader.cpp`
- `src/cityjson/local_cityjsonseq_reader.cpp`
- `src/cityjson/bind_function.cpp`
- `src/cityjson/metadata_table_function.cpp`
- `src/cityjson_extension.cpp`
- `CMakeLists.txt`

---

## Risk Areas

1. **Task 1 — Memory doubling**: Reading the entire file into a string before parsing doubles peak memory. Acceptable since bind already loads all chunks.
2. **Task 2 — FCB library platform support**: Pre-built binaries must match target OS/arch. FetchContent download may fail in CI without internet. Mitigated by `#ifdef CITYJSON_HAS_FCB` guard.
3. **Task 2 — FCB API stability**: CXX bridge API may change across flatcitybuf versions. All FCB calls are isolated in `FlatCityBufReader`.
4. **Task 3 — Vertex quantisation precision**: Integer rounding introduces sub-millimeter error. Validate with round-trip epsilon tests.
5. **Task 3 — CityJSON spec compliance**: Output must be valid CityJSON 2.0. Validate with `cjval` tool in tests.
