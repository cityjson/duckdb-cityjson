#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {
namespace cityjson {

/**
 * Registers the two appearance-cell scalar functions:
 *
 *   cityjson_appearance_ids(cell) -> BIGINT[]
 *   cityjson_shift_appearance_ids(cell, offset BIGINT) -> the cell's own type
 *
 * `cell` is a `material_lod*` cell -- MAP(VARCHAR, BIGINT[]) -- or a `texture_lod*`
 * one -- MAP(VARCHAR, STRUCT(id BIGINT, uv DOUBLE[][])[][]). The type says which, so
 * there is no `kind` argument; anything else is refused at bind. The accepted types
 * are derived from ColumnType::MaterialMap / ColumnType::TextureMap, so neither can
 * drift from the column it exists to read, and a VARCHAR attribute that merely looks
 * like an appearance column (`material_lodging`) is rejected rather than parsed.
 *
 * The first extracts the sidecar ids a cell references, across every theme,
 * deduplicated and ascending -- which is how vacuum finds sidecar rows nothing
 * references. The second adds `offset` to each of those ids and leaves everything
 * else, the texture `uv` pairs included, exactly as it was -- which is how insert and
 * merge renumber an incoming package's references onto the destination's id space.
 */
void RegisterAppearanceIdsFunction(ExtensionLoader &loader);

} // namespace cityjson
} // namespace duckdb
