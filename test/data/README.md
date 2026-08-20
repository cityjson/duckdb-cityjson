## cityparquet_rs_minimal/

A CityParquet package written by the reference implementation (cityparquet-rs) from
`lod3_railway.city.json`. It exists so the package-layer tests read a file this
extension did not produce: a reader and writer that agree on a wrong encoding pass
every assertion made against each other.

Trimmed to `building.parquet` + `bridge.parquet` + `metadata.json` (the full
conversion produces eleven object/sidecar tables); `metadata.json`'s `assets` map is
hand-edited to match. The point of the fixture is the schema and footer conventions
`building.parquet` carries -- notably `address`, `template` and `other_attributes`
columns -- not the row count or the full module set.
