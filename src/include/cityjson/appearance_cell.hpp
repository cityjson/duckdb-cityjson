#pragma once

#include "cityjson/appearance_flatten.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {
namespace cityjson {

/**
 * The DuckDB representation of a flattened appearance cell.
 *
 * The specification types both columns (04-appearance-templates.mdx): a theme is an
 * open key set, which is what a MAP expresses, and what a theme holds has a fixed
 * shape, so the map's value is typed rather than JSON text.
 */

//! MAP(VARCHAR, BIGINT[]) -- theme -> one material id (or NULL) per WKB face.
LogicalType MaterialCellType();

//! MAP(VARCHAR, STRUCT(id BIGINT, uv DOUBLE[][])[][]) -- theme -> per WKB face ->
//! per ring of that face.
LogicalType TextureCellType();

//! Build the cell value. Every child type is derived from the column type above, so
//! the value's type matches the vector's exactly.
Value MaterialCellValue(const MaterialCell &cell);
Value TextureCellValue(const TextureCell &cell);

//! Read a cell back. Throws InvalidInputException when the value violates the
//! specification's invariants -- a NULL map value, a NULL face or ring, `id` and `uv`
//! disagreeing on nullness, or a `[u, v]` entry that does not hold two values.
MaterialCell MaterialCellFromValue(const Value &value);
TextureCell TextureCellFromValue(const Value &value);

//! `{"<theme>": {"values": [...]}}` -- material: ids/null per face; texture: per face
//! a ring array, each ring `[id, [u, v], ...]` or `[null]`. The flat JSON form the
//! COPY sink and the mesh model consume.
json MaterialCellToFlatJson(const MaterialCell &cell);
json TextureCellToFlatJson(const TextureCell &cell);

} // namespace cityjson
} // namespace duckdb
