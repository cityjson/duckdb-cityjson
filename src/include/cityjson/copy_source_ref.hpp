#pragma once

#include "cityjson/reader.hpp"
#include "duckdb/parser/query_node.hpp"

#include <optional>
#include <string>

namespace duckdb {
namespace cityjson {

//! A CityJSON source discovered inside a COPY TO's SELECT statement.
struct CopySourceRef {
	std::string path;
	//! Which factory opened (or should reopen) this source. Recorded from the read_*
	//! call that named the path, so COPY reopens with the same reader the query used
	//! rather than re-detecting the format from a name that may not describe it.
	ReaderKind kind = ReaderKind::Auto;
	//! The reader call was `appearance := 'sidecar'`: its material/texture cells hold
	//! dataset-global ids, not indices into the file's own blocks.
	bool sidecar_appearance = false;
};

//! Walk a parsed query node for exactly one read_cityjson / read_cityjsonseq /
//! read_flatcitybuf / read_obj call and return the file it names.
//!
//! COPY binds a relation, not a file, so anything the source carries at FILE level
//! -- its metadata header (the CRS above all) and its appearance definitions -- is
//! out of reach of the rows alone. Recovering the path is what puts it back in
//! reach. CopyInfo::select_statement survives to our bind: bind_copy.cpp takes a
//! Copy() of it rather than moving it, then hands the whole CopyInfo to
//! CopyFunctionBindInput.
//!
//! Returns nullopt when there is no such call, or more than one. An ambiguous
//! source must never be guessed at: stamping the wrong CRS onto georeferenced
//! output is a worse failure than stamping none, and the caller has an explicit
//! `metadata_from` option for the cases this cannot resolve.
std::optional<CopySourceRef> FindCopySourceRef(const QueryNode &node);

} // namespace cityjson
} // namespace duckdb
