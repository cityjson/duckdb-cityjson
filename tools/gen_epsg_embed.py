#!/usr/bin/env python3
"""Regenerate src/cityjson/epsg_projjson_data.cpp from the vendored gz asset.

The asset (src/assets/epsg_projjson.json.gz) is a gzip-compressed JSON object
mapping EPSG code (decimal-string key) to its PROJJSON, sourced from
cityparquet-rs (crates/cityparquet-schema/assets, generated offline via pyproj).
Embedding it as a byte array keeps `make` hermetic — no network, no pyproj.

Run from the repo root:  python3 tools/gen_epsg_embed.py
"""
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GZ = os.path.join(ROOT, "src/assets/epsg_projjson.json.gz")
OUT = os.path.join(ROOT, "src/cityjson/epsg_projjson_data.cpp")


def main() -> None:
    data = open(GZ, "rb").read()
    rows = []
    row = []
    for b in data:
        row.append(str(b))
        if len(row) == 40:
            rows.append(",".join(row) + ",")
            row = []
    if row:
        rows.append(",".join(row) + ",")
    body = "\n".join(rows)
    text = (
        "// Generated from src/assets/epsg_projjson.json.gz by tools/gen_epsg_embed.py — do not edit.\n"
        "// Vendored EPSG->PROJJSON table (gzip-compressed), sourced from cityparquet-rs\n"
        "// (crates/cityparquet-schema/assets), generated offline via pyproj.\n"
        '#include "cityjson/epsg_projjson_data.hpp"\n'
        "namespace duckdb {\nnamespace cityjson {\n"
        f"const unsigned int EPSG_PROJJSON_GZ_LEN = {len(data)}u;\n"
        "const unsigned char EPSG_PROJJSON_GZ[] = {\n"
        f"{body}\n"
        "};\n"
        "} // namespace cityjson\n} // namespace duckdb\n"
    )
    open(OUT, "w").write(text)
    print(f"generated {OUT} ({len(text)} bytes) from {len(data)} gz bytes")


if __name__ == "__main__":
    main()
