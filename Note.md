How I change.

## Current structure and issue

- it encodes all geomerty of lod as different geometry columns such as `geometry_lod0`, `geometry_lod1`, `geometry_lod2`, `geometry_lod3`, `geometry_lod4`.
- geomerty columns are json string of geometry
- We encodes different city object into one table.

## How it should be

- considering the real world scenario, we don't use different LOD at the same time. Maybe it not neccessary to encode all LOD into one table. We can create different table for different LOD. and even after export that, we can keep as different parquet files.
