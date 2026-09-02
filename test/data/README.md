## cityparquet_rs_minimal/

A CityParquet package written by the reference implementation (cityparquet-rs) from
`lod3_railway.city.json`. It exists so the package-layer tests read a file this
extension did not produce: a reader and writer that agree on a wrong encoding pass
every assertion made against each other.

Trimmed to `building.parquet` + `bridge.parquet` + `metadata.json` (the full
conversion produces twelve object/sidecar tables); `metadata.json`'s `assets` map is
hand-edited to match. The point of the fixture is the schema and footer conventions
`building.parquet` carries -- notably the reserved `address` and `template` columns --
not the row count or the full module set. It also carries an `other_attributes`
column, an ordinary attribute of that foreign file; nothing in the current format
reserves that name (the format's escape hatch is the single `other` column).

## obj/

Hand-written Wavefront OBJ fixtures for `read_obj`. `cube.obj` + `cube.mtl` is a closed
unit cube (one `o`, three `g`/`usemtl` semantic groups, one textured plain material,
one face with negative indices, one `v` with a `w` component) followed by an open slab
that inherits the cube's last `g`/`usemtl` state -- OBJ state persists across `o`, and
the fixture pins that. `open_roof.obj` has no `o` line and no materials. `continuation.obj`
ends a line with a backslash, which the reader refuses. The expected values in the tests
are derived by hand from these files, not from the reader.
