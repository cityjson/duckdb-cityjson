#!/usr/bin/env bash
# Opt-in kernel test for the Wavefront OBJ/MTL parser. No DuckDB include path and no
# vcpkg include path: the kernel is pure (no DuckDB types, no third-party headers), and
# this script is what proves that stays true. `error.cpp` comes along because the parser
# throws CityJSONError, which is itself DuckDB-free.
set -euo pipefail
cd "$(dirname "$0")/../.."
OUT="build/obj_parser_test"
mkdir -p build
c++ -std=c++20 -O1 -Isrc/include \
    test/cpp/test_obj_parser.cpp src/cityjson/obj_parser.cpp src/cityjson/error.cpp -o "$OUT"
"$OUT"
