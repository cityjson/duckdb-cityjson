# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

The **DuckDB CityJSON Extension** provides convenient functions for working with CityJSON data in DuckDB. CityJSON is a JSON-based encoding for storing 3D city models, and this extension enables efficient reading, writing, and querying of CityJSON datasets.

**Key characteristics:**
- Built using CMake and Make
- Uses VCPKG for dependency management
- Integrates with DuckDB's build system
- Extension can be built as both a static library and a loadable module

## Planned Functions

### Reading Functions

- **`read_cityjson(filename)`**: Read CityJSON files into DuckDB tables, mapping CityJSON properties to columns
- **`read_cityjsonseq(filename)`**: Read CityJSON Text Sequences (line-delimited CityJSON) into DuckDB
- **`read_cityparquet(filename)`**: Read CityParquet files (Parquet encoding of CityJSON) using DuckDB's Parquet extension

### Writing Functions

- **`write_cityjson(table)`**: Export DuckDB tables to CityJSON format (used with `COPY` statement)
- **`write_cityparquet(table)`**: Export DuckDB tables to CityParquet format with validation of mandatory fields

### Metadata Functions

- **`cityjson_metadata(filename)`**: Extract CityJSON metadata including coordinate reference system, transform parameters, and dataset information

### Future Functions

Additional convenience functions for CityJSON data manipulation will be added based on usage patterns and requirements.

## Build Commands

### Initial Build
```bash
make
```
This builds:
- `./build/release/duckdb` - DuckDB shell with extension pre-loaded
- `./build/release/test/unittest` - Test runner with extension linked
- `./build/release/extension/<extension_name>/<extension_name>.duckdb_extension` - Loadable binary

### Fast Incremental Builds
If ccache and ninja are installed:
```bash
GEN=ninja make
```

### Debug Build
```bash
make debug
```

### Build Types
Other available build types: `release`, `reldebug`, `relassert`

## Testing

### Run All SQL Tests
```bash
make test
```

### Run Debug Tests
```bash
make test_debug
```

Tests are located in `test/sql/*.test` and follow the [SQLLogicTest](https://duckdb.org/dev/sqllogictest/intro.html) format.

## Development Workflow

### Extension Structure
- **Header**: [src/include/quack_extension.hpp](src/include/quack_extension.hpp) - Extension class definition
- **Implementation**: [src/quack_extension.cpp](src/quack_extension.cpp) - Function implementations and registration
- **Configuration**: [CMakeLists.txt](CMakeLists.txt) and [extension_config.cmake](extension_config.cmake)
- **Tests**: [test/sql/](test/sql/) directory

### Adding New Scalar Functions
Register functions in the `LoadInternal()` function in [src/quack_extension.cpp](src/quack_extension.cpp):
```cpp
auto my_function = ScalarFunction("function_name", {LogicalType::INPUT_TYPE},
                                  LogicalType::OUTPUT_TYPE, MyFunctionImpl);
loader.RegisterFunction(my_function);
```

### Dependency Management
Dependencies are managed via VCPKG:
1. Add dependency to [vcpkg.json](vcpkg.json)
2. Use `find_package()` in [CMakeLists.txt](CMakeLists.txt)
3. Link with `target_link_libraries()`

## Extension Architecture

### Core Components
1. **Extension Class** (`QuackExtension`): Inherits from `duckdb::Extension`, implements `Load()`, `Name()`, and `Version()` methods
2. **Function Registration**: Functions are registered via `ExtensionLoader` in the `Load()` method
3. **C Entry Point**: `extern "C"` function provides loadable extension entry point

### Function Execution Pattern
Functions use DuckDB's executor framework:
- `DataChunk` for input columns
- `Vector` for result output
- `UnaryExecutor::Execute()` for processing rows
- `StringVector::AddString()` for string results

## Running the Extension

### With Built Shell
```bash
./build/release/duckdb
```
The extension is automatically loaded in this shell.

### Loading Loadable Extension
```sql
LOAD '/path/to/extension.duckdb_extension';
```
Requires DuckDB to be started with `-unsigned` flag or `allow_unsigned_extensions=true`.

## Important Files

- [Makefile](Makefile) - Build orchestration (wraps extension-ci-tools makefile)
- [CMakeLists.txt](CMakeLists.txt) - Extension-specific CMake configuration
- [extension_config.cmake](extension_config.cmake) - Tells DuckDB which extension to load
- [vcpkg.json](vcpkg.json) - Dependency specification
- [test/sql/*.test](test/sql/) - SQLLogicTest test files

## DuckDB Submodule

The `duckdb/` directory is a git submodule pointing to the DuckDB source. Clone with `--recurse-submodules` to populate it.

## Notes

- Extension binaries only work with the specific DuckDB version they were built against
- The extension must be rebuilt when updating the DuckDB submodule version
- Use `extension-ci-tools` for CI/CD pipelines (included as submodule)
