#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {
namespace cityjson {

/**
 * Registers the CityParquet appearance sidecar readers:
 *
 *   cityjson_materials(path) -> materials.parquet rows
 *   cityjson_textures(path)  -> textures.parquet rows
 *
 * Column names and order follow the specification's sidecar tables exactly, including
 * its mixed casing (`ambientIntensity`, `wrapMode`, `borderColor`) — those are the
 * spec's names, not a style choice, and a package is read by matching them.
 *
 * `id` is the entry's ordinal position in the source's `appearance.materials` /
 * `appearance.textures` array. That is deterministic, needs no interning, and is the
 * rule `cityparquet-rs` must also follow for packages produced by the two
 * implementations to merge.
 */
void RegisterAppearanceTableFunctions(ExtensionLoader &loader);

} // namespace cityjson
} // namespace duckdb
