#!/usr/bin/env bash
# Opt-in kernel test for the face triangulator. Needs a configured tree
# (build/release/vcpkg_installed) for mapbox/earcut.hpp and nlohmann/json.hpp.
set -euo pipefail
cd "$(dirname "$0")/../.."
INC="$(find build/release/vcpkg_installed -maxdepth 2 -type d -name include | head -1)"
OUT="build/face_triangulation_test"
c++ -std=c++20 -O1 -Isrc/include -I"$INC" -Iduckdb/src/include \
    test/cpp/test_face_triangulation.cpp src/cityjson/face_triangulation.cpp -o "$OUT"
"$OUT"
