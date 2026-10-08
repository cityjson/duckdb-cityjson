#pragma once

#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"

#include <string>
#include <vector>

namespace duckdb {
namespace cityjson {

//! StringUtil::Join takes duckdb::vector, which std::vector does not convert to.
std::string Join(const std::vector<std::string> &parts, const std::string &separator);

//! Optionally-quoted identifier (KeywordHelper::WriteOptionallyQuoted).
std::string Quoted(const std::string &name);

//! One catalog column, as the SQL generators consume it.
struct ColumnInfo {
	std::string name;
	LogicalType type;
};

//! Catalog columns of schema.table, in order. Non-templated Catalog::GetEntry --
//! the templated form ODR-uses TableCatalogEntry::Name (see cityparquet_package.cpp).
std::vector<ColumnInfo> TableColumns(ClientContext &context, const std::string &schema, const std::string &table);

//! Case-insensitive lookup; nullptr when absent.
const ColumnInfo *FindColumn(const std::vector<ColumnInfo> &columns, const std::string &name);

//! True for a geometry_properties struct type carrying its `surfaces` JSON text field.
bool HasSurfacesField(const LogicalType &type);

//! `implicit_geometry` with its `id` shifted by `offset_expr`, onto the destination's
//! implicit_geometries numbering; a NULL cell stays NULL.
std::string ShiftedImplicitGeometry(const std::string &offset_expr);

//! `type` with every JSON alias removed, nested ones included: JSON and VARCHAR hold
//! the same text, and compare equal through this.
LogicalType WithoutJsonAlias(const LogicalType &type);

//! The promotion lattice (spec 02-object-table-schema.mdx, "Attribute types and
//! promotion"), applied across the data a package holds after an insert or merge:
//! BIGINT -> DOUBLE is a safe widening; anything else scalar that disagrees falls back
//! to VARCHAR. INVALID means the destination already accommodates the source -- a
//! DOUBLE takes a BIGINT, a VARCHAR any scalar. A caller that widens says so with
//! LogWidening: the column's type changes for the rows already there. `function` and `column_name` name the caller and the
//! column being evolved, for the exception below.
//!
//! A nested type (STRUCT/LIST/MAP) on either side is refused outright rather than
//! stringified: reserved structural columns (`bbox`, `children`, `children_roles`, a
//! `geometry_properties_lod*`, ...) disagreeing in shape between two packages means
//! they disagree about the package's own schema, not that one of them needs
//! coercing to text. Silently rewriting such a column to VARCHAR destroys it --
//! `bbox.xmin` stops binding, and cityparquet_write can no longer emit the STRUCT
//! the spec requires -- so this throws a BinderException instead.
//!
//! A geometry column is refused for the same reason: GEOMETRY and BLOB are two
//! encodings of one WKB value, never text. A caller that can reconcile the pair
//! (insert_cityjson converts staged WKB to the destination's GEOMETRY) does so before
//! asking.
LogicalType WidenedType(const LogicalType &destination, const LogicalType &source, const std::string &function,
                        const std::string &column_name);

//! The warning a caller logs when it widens `column_name` of `table`.
void LogWidening(ClientContext &context, const std::string &function, const std::string &table,
                 const std::string &column_name, const LogicalType &from, const LogicalType &incoming,
                 const LogicalType &to);

//! A column reference for a WKB-consuming SQL expression, converting DuckDB-native
//! GEOMETRY to WKB BLOB via ST_AsWKB. Any column named in a package's `geo` footer is
//! promoted to native GEOMETRY on a plain `read_parquet` (enable_geoparquet_conversion,
//! on by default and gated only on the `parquet` extension being loaded, not on
//! `spatial` -- so this promotion happens whether or not `spatial` is installed).
//! ST_AsWKB rather than a `::BLOB` cast: the cast is registered by `spatial` only once
//! it is loaded, whereas ST_AsWKB/ST_AsBinary ship in DuckDB core and need no extension
//! at all. Shared by cityparquet_write's CopySourceList and cityparquet_reconcile's
//! BboxPhase -- every site that hands a geometry column to a BLOB-only function.
std::string GeometryColumnRef(const std::string &quoted_name, const LogicalType &type);

// --- the one-CRS-per-package precondition, shared by insert_cityjson and merge -------
//
// A package states ONE CRS for every row it holds (spec 05-metadata.mdx: `city.crs` is
// "the one CRS every geometry column and the bbox column share", with no per-row escape
// hatch), and neither function reprojects. Both therefore refuse incoming rows whose CRS
// cannot be shown to be the destination's.
//
// Two traps live in reading a package's declared CRS, and both of them bit:
//
//  - **Only OBJECT-table footers declare a CRS.** A sidecar's footer legitimately carries
//    no `crs` key at all, so a DISTINCT spanning every footer answers a perfectly ordinary
//    package with two rows -- and a scalar subquery rejects that with "More than one row
//    returned by a subquery", nothing to do with CRSs.
//  - **Absent and null are not the same destination state.** `cityparquet_city_field`
//    maps both to SQL NULL, so the count of object footers is what distinguishes a
//    package that *declares* its CRS unknown from one whose footer is missing entirely
//    (a hand-rolled load), which declares nothing and is not checked at all.

//! Scalar subquery yielding the one CRS a package's object tables declare, as canonical
//! PROJJSON text, or SQL NULL when none of them declares one. Aggregated rather than
//! DISTINCTed so it cannot return two rows; OneCrsPerPackageSQL is what reports a package
//! whose footers disagree.
std::string DeclaredCrsExpr(const std::string &schema);

//! Scalar subquery: does this package state a CRS at all? False for a hand-rolled load,
//! whose footers are NULL -- that states nothing, as opposed to stating "unknown". A
//! footer counts when it carries `version` (every footer a writer produces does) or a
//! CRS: the bookkeeping `city` of a package that has not been written yet may hold no
//! more than its `extensions` declaration, which says nothing about the CRS.
std::string CrsStatedExpr(const std::string &schema);

//! Refuse a package whose own object-table footers declare more than one CRS, by name --
//! otherwise DeclaredCrsExpr silently picks one. `label` names the side for the message
//! ("destination" / "source").
std::string OneCrsPerPackageSQL(const std::string &function, const std::string &schema, const std::string &label);

//! The wording that differs between the precondition's two callers.
struct CrsCheckWording {
	//! Opens every message: "insert_cityjson" / "cityparquet_merge".
	std::string function;
	//! Names the incoming side: "the source" / "the source package".
	std::string source_noun;
	//! What to do when the incoming side has no CRS this writer can resolve.
	std::string source_unknown_hint;
	//! What to do when the destination package declares its own CRS unknown.
	std::string destination_unknown_hint;
};

//! The precondition itself, over the tri-state `crs`: known vs known compares, known vs
//! unknown is refused in both directions (an unknown cannot be shown to be the package's
//! one CRS, and asserting it would be the guess the specification forbids), unknown vs
//! unknown passes, and a side that states nothing is not checked.
std::string CrsPreconditionSQL(const CrsCheckWording &wording, const std::string &destination_crs_expr,
                               const std::string &destination_stated_expr, const std::string &source_crs_expr,
                               const std::string &source_stated_expr);

} // namespace cityjson
} // namespace duckdb
