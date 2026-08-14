# justfile — shortcuts for building, testing, and running CI checks locally.
# Run `just` with no arguments to list all recipes. Requires: just, cmake, ninja,
# python3, and clang-format/clang-tidy 11.x for the format/tidy recipes.
#
# The CI pipeline (.github/workflows/MainDistributionPipeline.yml) runs two things
# via duckdb/extension-ci-tools: (1) build + test, (2) format-check + tidy-check.
# The recipes below mirror those so you can reproduce CI before pushing.

# Build a fast release binary. Optional: pass CORE_EXTENSIONS="httpfs" etc. via env.
build_targets := "cityjson_extension cityjson_loadable_extension duckdb unittest"

# List available recipes (default).
default:
    @just --list

# Full release build (configures the project the first time; slow on a clean tree).
build:
    GEN=ninja make release

# Full debug build.
debug:
    GEN=ninja make debug

# Fast incremental rebuild of the extension, duckdb CLI, and test binary.
# Use this in the edit-build-test loop after `just build` has configured the tree.
rebuild:
    cmake --build build/release --target {{build_targets}}

# Build with FlatCityBuf (.fcb) support enabled.
build-fcb:
    EXT_FLAGS="-DCITYJSON_ENABLE_FCB=ON" GEN=ninja make release

# Build flatbuffers + flatcitybuf (tag cpp-v0.8.1) into .vendor/prefix.
# The loadable extension is a shared object, so both static libs need PIC.
# Point CMake at the result with -Dflatcitybuf_DIR / -Dflatbuffers_DIR (or export
# CMAKE_PREFIX_PATH="$(pwd)/.vendor/prefix"); test/cpp/run_encoder_tests.sh wants
# FCB_PREFIX="$(pwd)/.vendor/prefix".
vendor-fcb:
    #!/usr/bin/env bash
    set -euo pipefail
    PREFIX="$(pwd)/.vendor/prefix"
    SRC="$(pwd)/.vendor/src"
    mkdir -p "$SRC"
    if [ ! -d "$SRC/flatbuffers" ]; then
        git clone --depth 1 --branch v25.9.23 https://github.com/google/flatbuffers "$SRC/flatbuffers"
    fi
    cmake -S "$SRC/flatbuffers" -B "$SRC/flatbuffers/build" -DCMAKE_BUILD_TYPE=Release \
        -DFLATBUFFERS_BUILD_TESTS=OFF -DFLATBUFFERS_BUILD_FLATC=OFF -DFLATBUFFERS_BUILD_FLATHASH=OFF \
        -DFLATBUFFERS_BUILD_FLATLIB=ON -DFLATBUFFERS_BUILD_SHAREDLIB=OFF \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_INSTALL_PREFIX="$PREFIX"
    cmake --build "$SRC/flatbuffers/build" -j && cmake --install "$SRC/flatbuffers/build"
    if [ ! -d "$SRC/flatcitybuf" ]; then
        git clone --depth 1 --branch cpp-v0.8.1 https://github.com/cityjson/flatcitybuf "$SRC/flatcitybuf"
    fi
    cmake -S "$SRC/flatcitybuf/src/cpp" -B "$SRC/flatcitybuf/build" -DCMAKE_BUILD_TYPE=Release \
        -DFCB_WITH_JSON=ON -DFCB_WITH_CURL=OFF -DFCB_BUILD_TESTS=OFF -DFCB_BUILD_EXAMPLES=OFF \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_PREFIX_PATH="$PREFIX" -DCMAKE_INSTALL_PREFIX="$PREFIX"
    cmake --build "$SRC/flatcitybuf/build" -j && cmake --install "$SRC/flatcitybuf/build"

# Run the full SQL test suite (assumes a build exists; run `just rebuild` first).
test:
    ./build/release/test/unittest "test/sql/*"

# Rebuild then run the full suite — the common inner-loop command.
t: rebuild test

# Run a single test file, e.g. `just test-file test/sql/cityjson_delft_e2e.test`.
test-file FILE:
    ./build/release/test/unittest "{{FILE}}"

# Run the extension's canonical test target the same way CI does (builds if needed).
test-ci:
    make test

# clang-format check — matches the CI "Format Check" job (scans src incl. cjseq submodule).
format-check:
    make format-check

# Apply clang-format fixes in place (src + test).
format-fix:
    make format-fix

# clang-tidy check — matches the CI "Tidy Check" job (needs a vcpkg toolchain to configure).
tidy:
    make tidy-check

# Reproduce the CI gates locally: format check, then build, then test.
ci: format-check build test

# Remove all build artifacts.
clean:
    make clean
