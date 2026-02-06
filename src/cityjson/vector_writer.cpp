#include "cityjson/vector_writer.hpp"
#include "cityjson/temporal_parser.hpp"
#include "cityjson/cityjson_types.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {
namespace cityjson {

// ============================================================
// VectorWrapper - Task 22
// ============================================================

VectorWrapper::VectorWrapper(VectorType type, Vector *vector) : type_(type), vector_(vector) {
	(void)type; // Type is stored but not strictly enforced
}

Vector *VectorWrapper::AsFlatMut() {
	// DuckDB handles the actual vector type internally
	return vector_;
}

Vector *VectorWrapper::AsListMut() {
	// DuckDB handles the actual vector type internally
	return vector_;
}

Vector *VectorWrapper::AsStructMut() {
	// DuckDB handles the actual vector type internally
	return vector_;
}

// ============================================================
// CreateVectors - Task 23
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

		if (col.kind == ColumnType::VarcharArray) {
			vec_type = VectorType::List;
		} else if (col.kind == ColumnType::Geometry || col.kind == ColumnType::GeographicalExtent) {
			vec_type = VectorType::Struct;
		} else {
			// All primitives and Json (stored as VARCHAR) are Flat
			vec_type = VectorType::Flat;
		}

		wrappers.emplace_back(vec_type, &vec);
	}

	return wrappers;
}

// ============================================================
// Primitive Writers - Task 24
// ============================================================

// Template for numeric types
template <typename T>
void WritePrimitive(Vector *vec, size_t row, T value) {
	auto data = FlatVector::GetData<T>(*vec);
	data[row] = value;
}

// Specialization for VARCHAR
template <>
void WritePrimitive<std::string>(Vector *vec, size_t row, std::string value) {
	FlatVector::GetData<string_t>(*vec)[row] = StringVector::AddString(*vec, value);
}

// Explicit instantiations for common types
template void WritePrimitive<bool>(Vector *vec, size_t row, bool value);
template void WritePrimitive<int32_t>(Vector *vec, size_t row, int32_t value);
template void WritePrimitive<int64_t>(Vector *vec, size_t row, int64_t value);
template void WritePrimitive<double>(Vector *vec, size_t row, double value);

// ============================================================
// WriteVarcharArray - Task 26
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
// WriteGeometry - Task 27
// ============================================================

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

// ============================================================
// WriteGeographicalExtent - Task 27
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
// WriteToVector - Task 25
// ============================================================

void WriteToVector(const Column &col, const json &value, VectorWrapper &wrapper, size_t row) {
	// Handle NULL values
	if (value.is_null()) {
		FlatVector::SetNull(*wrapper.AsFlatMut(), row, true);
		return;
	}

	// Dispatch based on column type
	switch (col.kind) {
	case ColumnType::Boolean: {
		if (!value.is_boolean()) {
			throw CityJSONError::ColumnTypeMismatch("BOOLEAN", value.dump());
		}
		WritePrimitive(wrapper.AsFlatMut(), row, value.get<bool>());
		break;
	}

	case ColumnType::BigInt: {
		if (!value.is_number()) {
			throw CityJSONError::ColumnTypeMismatch("BIGINT", value.dump());
		}
		// Accept both integer and float, converting to int64
		// This handles cases where schema inference saw integers but data has floats
		if (value.is_number_integer()) {
			WritePrimitive(wrapper.AsFlatMut(), row, value.get<int64_t>());
		} else {
			// Convert float to int (truncate)
			WritePrimitive(wrapper.AsFlatMut(), row, static_cast<int64_t>(value.get<double>()));
		}
		break;
	}

	case ColumnType::Double: {
		if (!value.is_number()) {
			throw CityJSONError::ColumnTypeMismatch("DOUBLE", value.dump());
		}
		WritePrimitive(wrapper.AsFlatMut(), row, value.get<double>());
		break;
	}

	case ColumnType::Varchar: {
		if (!value.is_string()) {
			throw CityJSONError::ColumnTypeMismatch("VARCHAR", value.dump());
		}
		WritePrimitive(wrapper.AsFlatMut(), row, value.get<std::string>());
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

	default:
		throw CityJSONError::Other("Unsupported column type: " + std::string(ColumnTypeUtils::ToString(col.kind)));
	}
}

} // namespace cityjson
} // namespace duckdb
