#include "cityjson/vector_writer.hpp"
#include "cityjson/temporal_parser.hpp"
#include "cityjson/cityjson_types.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {
namespace cityjson {

// ============================================================
// VectorWrapper - type-safe DuckDB vector wrapper
// ============================================================

VectorWrapper::VectorWrapper(VectorType type, Vector *vector) : type_(type), vector_(vector) {
}

Vector *VectorWrapper::AsFlatMut() {
	if (type_ != VectorType::Flat) {
		throw CityJSONError::Other("VectorWrapper: expected Flat vector, got " +
		                           std::to_string(static_cast<int>(type_)));
	}
	return vector_;
}

Vector *VectorWrapper::AsListMut() {
	if (type_ != VectorType::List) {
		throw CityJSONError::Other("VectorWrapper: expected List vector, got " +
		                           std::to_string(static_cast<int>(type_)));
	}
	return vector_;
}

Vector *VectorWrapper::AsStructMut() {
	if (type_ != VectorType::Struct) {
		throw CityJSONError::Other("VectorWrapper: expected Struct vector, got " +
		                           std::to_string(static_cast<int>(type_)));
	}
	return vector_;
}

// ============================================================
// CreateVectors - wrap projected output vectors
// ============================================================

std::vector<VectorWrapper> CreateVectors(DataChunk &output, const std::vector<Column> &columns,
                                         const std::vector<idx_t> &projected_column_ids) {
	std::vector<VectorWrapper> wrappers;

	for (idx_t i = 0; i < projected_column_ids.size(); i++) {
		idx_t col_idx = projected_column_ids[i];
		const Column &col = columns[col_idx];
		Vector &vec = output.data[i];

		// Determine vector type based on column type
		VectorType vec_type;

		if (col.kind == ColumnType::VarcharArray || col.kind == ColumnType::GeometryArrowNative ||
		    col.kind == ColumnType::GeometryVerticesArrowNative) {
			vec_type = VectorType::List;
		} else if (col.kind == ColumnType::Geometry || col.kind == ColumnType::GeographicalExtent ||
		           col.kind == ColumnType::GeometryPropertiesStruct) {
			vec_type = VectorType::Struct;
		} else {
			// All primitives and Json (stored as VARCHAR) are Flat -- including
			// AppearanceJson, which stays JSON text unlike GeometryPropertiesStruct.
			vec_type = VectorType::Flat;
		}

		wrappers.emplace_back(vec_type, &vec);
	}

	return wrappers;
}

// ============================================================
// Primitive Writers
// ============================================================

// Template for numeric types
template <typename T>
void WritePrimitive(Vector *vec, size_t row, const T &value) {
	auto data = FlatVector::GetData<T>(*vec);
	data[row] = value;
}

// Specialization for VARCHAR. By const reference, like the primary template: a
// specialization has to match its signature, and std::string is the one
// instantiation here whose copy is worth avoiding.
template <>
void WritePrimitive<std::string>(Vector *vec, size_t row, const std::string &value) {
	FlatVector::GetData<string_t>(*vec)[row] = StringVector::AddString(*vec, value);
}

// Explicit instantiations for common types
template void WritePrimitive<bool>(Vector *vec, size_t row, const bool &value);
template void WritePrimitive<int32_t>(Vector *vec, size_t row, const int32_t &value);
template void WritePrimitive<int64_t>(Vector *vec, size_t row, const int64_t &value);
template void WritePrimitive<double>(Vector *vec, size_t row, const double &value);

// ============================================================
// WriteVarcharArray
// ============================================================

void WriteVarcharArray(Vector *list_vec, const json &value, size_t row) {
	if (!value.is_array()) {
		FlatVector::SetNull(*list_vec, row, true);
		return;
	}

	// Get list data and child vector
	auto list_data = FlatVector::GetData<list_entry_t>(*list_vec);
	auto &child_vec = ListVector::GetEntry(*list_vec);
	auto list_size = ListVector::GetListSize(*list_vec);

	// Set list entry metadata
	list_data[row].offset = list_size;
	list_data[row].length = value.size();

	// Reserve space in the list
	ListVector::Reserve(*list_vec, list_size + value.size());

	// Write each array element
	for (size_t i = 0; i < value.size(); i++) {
		const auto &elem = value[i];
		if (elem.is_string()) {
			std::string str = elem.get<std::string>();
			FlatVector::GetData<string_t>(child_vec)[list_size + i] = StringVector::AddString(child_vec, str);
		} else {
			// Non-string element - set NULL
			FlatVector::SetNull(child_vec, list_size + i, true);
		}
	}

	// Update list size
	ListVector::SetListSize(*list_vec, list_size + value.size());
}

// ============================================================
// WriteGeometry
// ============================================================

void WriteGeometry(Vector *struct_vec, const Geometry &geom, size_t row) {
	// Get child vectors
	// STRUCT(lod VARCHAR, type VARCHAR, boundaries VARCHAR,
	//        semantics VARCHAR, material VARCHAR, texture VARCHAR)
	auto &children = StructVector::GetEntries(*struct_vec);

	// Write lod (index 0)
	WritePrimitive(children[0].get(), row, geom.lod);

	// Write type (index 1)
	WritePrimitive(children[1].get(), row, geom.type);

	// Write boundaries as JSON string (index 2)
	WritePrimitive(children[2].get(), row, geom.boundaries.dump());

	// Write semantics as JSON string (index 3) - nullable
	if (geom.semantics.has_value()) {
		WritePrimitive(children[3].get(), row, geom.semantics->dump());
	} else {
		FlatVector::SetNull(*children[3], row, true);
	}

	// Write material as JSON string (index 4) - nullable
	if (geom.material.has_value()) {
		WritePrimitive(children[4].get(), row, geom.material->dump());
	} else {
		FlatVector::SetNull(*children[4], row, true);
	}

	// Write texture as JSON string (index 5) - nullable
	if (geom.texture.has_value()) {
		WritePrimitive(children[5].get(), row, geom.texture->dump());
	} else {
		FlatVector::SetNull(*children[5], row, true);
	}
}

void WriteGeometry(Vector *struct_vec, const json &value, size_t row) {
	if (!value.is_object()) {
		FlatVector::SetNull(*struct_vec, row, true);
		return;
	}

	// Parse geometry
	Geometry geom;
	try {
		geom = Geometry::FromJson(value);
	} catch (const CityJSONError &) {
		FlatVector::SetNull(*struct_vec, row, true);
		return;
	}

	WriteGeometry(struct_vec, geom, row);
}

// ============================================================
// WriteGeographicalExtent
// ============================================================

void WriteGeographicalExtent(Vector *struct_vec, const json &value, size_t row) {
	if (!value.is_array() || value.size() != 6) {
		FlatVector::SetNull(*struct_vec, row, true);
		return;
	}

	// Parse geographical extent
	GeographicalExtent extent;
	try {
		extent = GeographicalExtent::FromJson(value);
	} catch (const CityJSONError &) {
		FlatVector::SetNull(*struct_vec, row, true);
		return;
	}

	// Get child vectors
	// STRUCT(min_x DOUBLE, min_y DOUBLE, min_z DOUBLE,
	//        max_x DOUBLE, max_y DOUBLE, max_z DOUBLE)
	auto &children = StructVector::GetEntries(*struct_vec);

	// Write all 6 fields
	WritePrimitive(children[0].get(), row, extent.min_x);
	WritePrimitive(children[1].get(), row, extent.min_y);
	WritePrimitive(children[2].get(), row, extent.min_z);
	WritePrimitive(children[3].get(), row, extent.max_x);
	WritePrimitive(children[4].get(), row, extent.max_y);
	WritePrimitive(children[5].get(), row, extent.max_z);
}

// ============================================================
// WriteToVector
// ============================================================

void WriteToVector(const Column &col, const json &value, VectorWrapper &wrapper, size_t row) {
	// Handle NULL values
	if (value.is_null()) {
		wrapper.SetNull(row);
		return;
	}

	// Dispatch based on column type
	switch (col.kind) {
	case ColumnType::Boolean: {
		if (value.is_boolean()) {
			WritePrimitive(wrapper.AsFlatMut(), row, value.get<bool>());
		} else if (value.is_number()) {
			WritePrimitive(wrapper.AsFlatMut(), row, value.get<int64_t>() != 0);
		} else if (value.is_string()) {
			auto s = value.get<std::string>();
			WritePrimitive(wrapper.AsFlatMut(), row, s == "true" || s == "1");
		} else {
			FlatVector::SetNull(*wrapper.AsFlatMut(), row, true);
		}
		break;
	}

	case ColumnType::BigInt: {
		if (value.is_number_integer()) {
			WritePrimitive(wrapper.AsFlatMut(), row, value.get<int64_t>());
		} else if (value.is_number_float()) {
			WritePrimitive(wrapper.AsFlatMut(), row, static_cast<int64_t>(value.get<double>()));
		} else if (value.is_string()) {
			try {
				WritePrimitive(wrapper.AsFlatMut(), row, std::stoll(value.get<std::string>()));
			} catch (...) {
				FlatVector::SetNull(*wrapper.AsFlatMut(), row, true);
			}
		} else {
			FlatVector::SetNull(*wrapper.AsFlatMut(), row, true);
		}
		break;
	}

	case ColumnType::Double: {
		if (value.is_number()) {
			WritePrimitive(wrapper.AsFlatMut(), row, value.get<double>());
		} else if (value.is_string()) {
			try {
				WritePrimitive(wrapper.AsFlatMut(), row, std::stod(value.get<std::string>()));
			} catch (...) {
				FlatVector::SetNull(*wrapper.AsFlatMut(), row, true);
			}
		} else {
			FlatVector::SetNull(*wrapper.AsFlatMut(), row, true);
		}
		break;
	}

	case ColumnType::Varchar: {
		if (value.is_string()) {
			WritePrimitive(wrapper.AsFlatMut(), row, value.get<std::string>());
		} else {
			// Non-string value in a VARCHAR column: convert to string representation
			// This handles mixed-type attributes where schema inference chose VARCHAR
			WritePrimitive(wrapper.AsFlatMut(), row, value.dump());
		}
		break;
	}

	case ColumnType::Timestamp: {
		if (!value.is_string()) {
			throw CityJSONError::ColumnTypeMismatch("TIMESTAMP", value.dump());
		}
		int64_t timestamp_micros = ParseTimestampString(value.get<std::string>());
		WritePrimitive(wrapper.AsFlatMut(), row, timestamp_micros);
		break;
	}

	case ColumnType::Date: {
		if (!value.is_string()) {
			throw CityJSONError::ColumnTypeMismatch("DATE", value.dump());
		}
		int32_t date_days = ParseDateString(value.get<std::string>());
		WritePrimitive(wrapper.AsFlatMut(), row, date_days);
		break;
	}

	case ColumnType::Time: {
		if (!value.is_string()) {
			throw CityJSONError::ColumnTypeMismatch("TIME", value.dump());
		}
		int64_t time_micros = ParseTimeString(value.get<std::string>());
		WritePrimitive(wrapper.AsFlatMut(), row, time_micros);
		break;
	}

	case ColumnType::Json: {
		// Serialize JSON to string
		WritePrimitive(wrapper.AsFlatMut(), row, value.dump());
		break;
	}

	case ColumnType::VarcharArray: {
		WriteVarcharArray(wrapper.AsListMut(), value, row);
		break;
	}

	case ColumnType::Geometry: {
		WriteGeometry(wrapper.AsStructMut(), value, row);
		break;
	}

	case ColumnType::GeographicalExtent: {
		WriteGeographicalExtent(wrapper.AsStructMut(), value, row);
		break;
	}

	case ColumnType::GeometryWKB: {
		// Note: WKB data should be pre-encoded and passed directly
		// For now, we set NULL - actual WKB encoding is done at a higher level
		FlatVector::SetNull(*wrapper.AsFlatMut(), row, true);
		break;
	}

	case ColumnType::GeometryPropertiesStruct: {
		WriteGeometryProperties(wrapper.AsStructMut(), value, row);
		break;
	}

	default:
		throw CityJSONError::Other("Unsupported column type: " + std::string(ColumnTypeUtils::ToString(col.kind)));
	}
}

// ============================================================
// WKB and Geometry Properties Writers
// ============================================================

void WriteGeometryWKB(Vector *blob_vec, const std::vector<uint8_t> &wkb_data, size_t row) {
	if (wkb_data.empty()) {
		FlatVector::SetNull(*blob_vec, row, true);
		return;
	}

	// Create blob from WKB data
	auto blob =
	    StringVector::AddStringOrBlob(*blob_vec, reinterpret_cast<const char *>(wkb_data.data()), wkb_data.size());
	FlatVector::GetData<string_t>(*blob_vec)[row] = blob;
}

// Append one LIST<INTEGER> row from a flat json array of ints, honouring per-item
// nulls (spec: face_semantics items are independently nullable -- a face with no
// semantics). Returns nothing; the caller has already decided the row is non-null.
static void AppendIntList(Vector &list_vec, const json &arr, size_t row) {
	const size_t n = arr.is_array() ? arr.size() : 0;
	auto list_size = ListVector::GetListSize(list_vec);

	FlatVector::GetData<list_entry_t>(list_vec)[row] = list_entry_t(list_size, n);
	ListVector::Reserve(list_vec, list_size + n);

	// Fetch the child data pointer only after Reserve -- reserving can reallocate
	// the child's buffer and invalidate any pointer taken before it.
	auto &child = ListVector::GetEntry(list_vec);
	auto child_data = FlatVector::GetData<int32_t>(child);
	for (size_t i = 0; i < n; i++) {
		if (arr[i].is_number_integer()) {
			child_data[list_size + i] = arr[i].get<int32_t>();
		} else {
			FlatVector::SetNull(child, list_size + i, true);
		}
	}
	ListVector::SetListSize(list_vec, list_size + n);
}

// Append one LIST<LIST<INTEGER>> row from a json array of arrays (spec `shells`:
// per-solid, then per-shell, face counts -- always two levels deep).
static void AppendIntListList(Vector &list_vec, const json &arr, size_t row) {
	const size_t n_solids = arr.is_array() ? arr.size() : 0;
	auto outer_size = ListVector::GetListSize(list_vec);

	FlatVector::GetData<list_entry_t>(list_vec)[row] = list_entry_t(outer_size, n_solids);
	ListVector::Reserve(list_vec, outer_size + n_solids);

	auto &solid_vec = ListVector::GetEntry(list_vec); // LIST<INTEGER>
	auto inner_start = ListVector::GetListSize(solid_vec);

	// Reserve the grandchild once for every count across every solid, so the int
	// buffer cannot move part-way through the fill below.
	size_t total_counts = 0;
	for (size_t s = 0; s < n_solids; s++) {
		total_counts += arr[s].is_array() ? arr[s].size() : 0;
	}
	ListVector::Reserve(solid_vec, inner_start + total_counts);

	auto solid_data = FlatVector::GetData<list_entry_t>(solid_vec);
	auto &count_vec = ListVector::GetEntry(solid_vec); // INTEGER
	auto count_data = FlatVector::GetData<int32_t>(count_vec);

	size_t inner_pos = inner_start;
	for (size_t s = 0; s < n_solids; s++) {
		const auto &shell_counts = arr[s];
		const size_t n_shells = shell_counts.is_array() ? shell_counts.size() : 0;
		solid_data[outer_size + s] = list_entry_t(inner_pos, n_shells);
		for (size_t i = 0; i < n_shells; i++) {
			// `shells` is non-null all the way down where present, so a non-integer
			// entry is malformed input rather than a legitimate null; store 0 so the
			// counts still sum to something the reader can range-check.
			count_data[inner_pos + i] = shell_counts[i].is_number_integer() ? shell_counts[i].get<int32_t>() : 0;
		}
		inner_pos += n_shells;
	}

	ListVector::SetListSize(solid_vec, inner_pos);
	ListVector::SetListSize(list_vec, outer_size + n_solids);
}

// ============================================================
// Arrow-native geometry writers
// ============================================================
//
// The geometry column is five LIST levels deep -- solid, shell, face, ring, then
// the INTEGER vertex-pool index. Each level follows the same idiom the two
// writers above already use: stamp this row's list_entry_t, reserve the child for
// everything this row will add, then fill.
//
// The ordering rule the shells writer records applies at every level here: a
// child's data pointer is only valid after the Reserve that sizes it, because
// reserving can reallocate. So each helper takes its child pointer fresh rather
// than receiving one from its caller, and a level is fully reserved before its
// children are visited.

//! One ring: LIST<INTEGER> of vertex-pool indices.
static void AppendIndexRing(Vector &ring_vec, const std::vector<uint32_t> &ring, size_t row) {
	auto list_size = ListVector::GetListSize(ring_vec);
	FlatVector::GetData<list_entry_t>(ring_vec)[row] = list_entry_t(list_size, ring.size());
	ListVector::Reserve(ring_vec, list_size + ring.size());

	auto &index_vec = ListVector::GetEntry(ring_vec);
	auto index_data = FlatVector::GetData<int32_t>(index_vec);
	for (size_t i = 0; i < ring.size(); i++) {
		// Indices are Int32 by schema. The encoder has already bounds-checked each
		// one against the row's pool, which is far below the Int32 ceiling for any
		// real geometry (design doc, "Nullability & validity invariants").
		index_data[list_size + i] = static_cast<int32_t>(ring[i]);
	}
	ListVector::SetListSize(ring_vec, list_size + ring.size());
}

//! One face: LIST<LIST<INTEGER>>, exterior ring first then any holes.
static void AppendFace(Vector &face_vec, const CompactedFace &face, size_t row) {
	auto list_size = ListVector::GetListSize(face_vec);
	FlatVector::GetData<list_entry_t>(face_vec)[row] = list_entry_t(list_size, face.rings.size());
	ListVector::Reserve(face_vec, list_size + face.rings.size());

	auto &ring_vec = ListVector::GetEntry(face_vec);
	for (size_t i = 0; i < face.rings.size(); i++) {
		AppendIndexRing(ring_vec, face.rings[i], list_size + i);
	}
	ListVector::SetListSize(face_vec, list_size + face.rings.size());
}

//! One shell: LIST<LIST<LIST<INTEGER>>> of faces.
static void AppendShell(Vector &shell_vec, const CompactedShell &shell, size_t row) {
	auto list_size = ListVector::GetListSize(shell_vec);
	FlatVector::GetData<list_entry_t>(shell_vec)[row] = list_entry_t(list_size, shell.faces.size());
	ListVector::Reserve(shell_vec, list_size + shell.faces.size());

	auto &face_vec = ListVector::GetEntry(shell_vec);
	for (size_t i = 0; i < shell.faces.size(); i++) {
		AppendFace(face_vec, shell.faces[i], list_size + i);
	}
	ListVector::SetListSize(shell_vec, list_size + shell.faces.size());
}

//! One solid: LIST<LIST<LIST<LIST<INTEGER>>>> of shells.
static void AppendSolid(Vector &solid_vec, const CompactedSolid &solid, size_t row) {
	auto list_size = ListVector::GetListSize(solid_vec);
	FlatVector::GetData<list_entry_t>(solid_vec)[row] = list_entry_t(list_size, solid.shells.size());
	ListVector::Reserve(solid_vec, list_size + solid.shells.size());

	auto &shell_vec = ListVector::GetEntry(solid_vec);
	for (size_t i = 0; i < solid.shells.size(); i++) {
		AppendShell(shell_vec, solid.shells[i], list_size + i);
	}
	ListVector::SetListSize(solid_vec, list_size + solid.shells.size());
}

void WriteGeometryArrowNative(Vector *vec, const CompactedGeometry &geometry, size_t row) {
	if (geometry.solids.empty()) {
		FlatVector::SetNull(*vec, row, true);
		return;
	}

	auto list_size = ListVector::GetListSize(*vec);
	FlatVector::GetData<list_entry_t>(*vec)[row] = list_entry_t(list_size, geometry.solids.size());
	ListVector::Reserve(*vec, list_size + geometry.solids.size());

	auto &solid_vec = ListVector::GetEntry(*vec);
	for (size_t i = 0; i < geometry.solids.size(); i++) {
		AppendSolid(solid_vec, geometry.solids[i], list_size + i);
	}
	ListVector::SetListSize(*vec, list_size + geometry.solids.size());
}

void WriteGeometryVertices(Vector *vec, const CompactedGeometry &geometry, size_t row) {
	if (geometry.vertices.empty()) {
		FlatVector::SetNull(*vec, row, true);
		return;
	}

	const size_t n = geometry.vertices.size();
	auto list_size = ListVector::GetListSize(*vec);
	FlatVector::GetData<list_entry_t>(*vec)[row] = list_entry_t(list_size, n);
	ListVector::Reserve(*vec, list_size + n);

	// STRUCT(x DOUBLE, y DOUBLE, z DOUBLE) child, taken after the Reserve above:
	// reserving resizes the struct and its own children, so entries fetched before
	// it would point at the old buffers.
	auto &struct_vec = ListVector::GetEntry(*vec);
	auto &coords = StructVector::GetEntries(struct_vec);
	auto x_data = FlatVector::GetData<double>(*coords[0]);
	auto y_data = FlatVector::GetData<double>(*coords[1]);
	auto z_data = FlatVector::GetData<double>(*coords[2]);
	for (size_t i = 0; i < n; i++) {
		x_data[list_size + i] = geometry.vertices[i][0];
		y_data[list_size + i] = geometry.vertices[i][1];
		z_data[list_size + i] = geometry.vertices[i][2];
	}
	ListVector::SetListSize(*vec, list_size + n);
}

void WriteJsonText(Vector *vec, const json &value, size_t row) {
	if (value.is_null()) {
		FlatVector::SetNull(*vec, row, true);
		return;
	}

	std::string json_str = value.dump();
	FlatVector::GetData<string_t>(*vec)[row] = StringVector::AddString(*vec, json_str);
}

void WriteGeometryProperties(Vector *vec, const json &properties, size_t row) {
	// STRUCT("type" VARCHAR, surfaces JSON, face_semantics INTEGER[], shells INTEGER[][])
	auto &children = StructVector::GetEntries(*vec);
	auto &type_vec = *children[0];
	auto &surfaces_vec = *children[1];
	auto &face_semantics_vec = *children[2];
	auto &shells_vec = *children[3];

	// A null struct still needs well-formed children at this row: an uninitialised
	// list_entry_t carries a garbage offset/length that later flatten/copy passes
	// would dereference. So null every child (and give the lists an empty entry)
	// rather than leaving the memory untouched.
	const bool is_null = properties.is_null() || !properties.is_object();
	if (is_null) {
		FlatVector::SetNull(*vec, row, true);
	}

	// `type` -- non-null for any real geometry.
	if (!is_null && properties.contains("type") && properties["type"].is_string()) {
		auto type_str = properties["type"].get<std::string>();
		FlatVector::GetData<string_t>(type_vec)[row] = StringVector::AddString(type_vec, type_str);
	} else {
		FlatVector::SetNull(type_vec, row, true);
	}

	// `surfaces` -- the semantic surface array, verbatim, as JSON text. Physically a
	// VARCHAR, so it takes a plain string write.
	if (!is_null && properties.contains("surfaces") && properties["surfaces"].is_array()) {
		auto surfaces_str = properties["surfaces"].dump();
		FlatVector::GetData<string_t>(surfaces_vec)[row] = StringVector::AddString(surfaces_vec, surfaces_str);
	} else {
		FlatVector::SetNull(surfaces_vec, row, true);
	}

	// `face_semantics` -- null together with `surfaces` when the geometry carries no
	// semantics at all; otherwise one entry per WKB face, items independently nullable.
	if (!is_null && properties.contains("face_semantics") && properties["face_semantics"].is_array()) {
		AppendIntList(face_semantics_vec, properties["face_semantics"], row);
	} else {
		FlatVector::GetData<list_entry_t>(face_semantics_vec)[row] = list_entry_t(0, 0);
		FlatVector::SetNull(face_semantics_vec, row, true);
	}

	// `shells` -- present for solid-family geometry regardless of semantics, absent
	// for the non-solid types.
	if (!is_null && properties.contains("shells") && properties["shells"].is_array()) {
		AppendIntListList(shells_vec, properties["shells"], row);
	} else {
		FlatVector::GetData<list_entry_t>(shells_vec)[row] = list_entry_t(0, 0);
		FlatVector::SetNull(shells_vec, row, true);
	}
}

} // namespace cityjson
} // namespace duckdb
