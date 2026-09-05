#include "cityjson/appearance_cell.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {
namespace cityjson {

namespace {

//! A MAP value's children are STRUCT(key, value) entries.
constexpr idx_t kMapEntryKey = 0;
constexpr idx_t kMapEntryValue = 1;

//! Struct field order of a texture ring, fixed by TextureCellType().
constexpr idx_t kRingId = 0;
constexpr idx_t kRingUv = 1;

Value RingValue(const TextureRing &ring, const LogicalType &ring_type) {
	const auto &fields = StructType::GetChildTypes(ring_type);
	const auto &id_type = fields[kRingId].second;
	const auto &uv_type = fields[kRingUv].second;
	const auto &pair_type = ListType::GetChildType(uv_type);
	const auto &coord_type = ListType::GetChildType(pair_type);

	if (!ring.id.has_value()) {
		// A ring with no texture: id and uv are NULL together (spec's invariant).
		return Value::STRUCT(ring_type, {Value(id_type), Value(uv_type)});
	}

	vector<Value> pairs;
	pairs.reserve(ring.uv.size());
	for (const auto &uv : ring.uv) {
		pairs.push_back(Value::LIST(coord_type, {Value::DOUBLE(uv[0]), Value::DOUBLE(uv[1])}));
	}
	return Value::STRUCT(ring_type, {Value::BIGINT(ring.id.value()), Value::LIST(pair_type, std::move(pairs))});
}

//! The map entry's value, or a shape violation.
const Value &MapEntryChild(const Value &entry, idx_t index) {
	const auto &children = StructValue::GetChildren(entry);
	if (children.size() <= index) {
		throw InvalidInputException("appearance cell: a map entry is not a (key, value) pair");
	}
	return children[index];
}

} // namespace

LogicalType MaterialCellType() {
	return LogicalType::MAP(LogicalType::VARCHAR, LogicalType::LIST(LogicalType::BIGINT));
}

LogicalType TextureCellType() {
	child_list_t<LogicalType> ring;
	ring.emplace_back("id", LogicalType::BIGINT);
	ring.emplace_back("uv", LogicalType::LIST(LogicalType::LIST(LogicalType::DOUBLE)));
	return LogicalType::MAP(LogicalType::VARCHAR, LogicalType::LIST(LogicalType::LIST(LogicalType::STRUCT(ring))));
}

Value MaterialCellValue(const MaterialCell &cell) {
	const auto cell_type = MaterialCellType();
	const auto &key_type = MapType::KeyType(cell_type);
	const auto &value_type = MapType::ValueType(cell_type);
	const auto &id_type = ListType::GetChildType(value_type);

	vector<Value> keys;
	vector<Value> values;
	keys.reserve(cell.themes.size());
	values.reserve(cell.themes.size());
	for (const auto &theme : cell.themes) {
		keys.push_back(Value(theme.first));
		vector<Value> ids;
		ids.reserve(theme.second.size());
		for (const auto &id : theme.second) {
			ids.push_back(id.has_value() ? Value::BIGINT(id.value()) : Value(id_type));
		}
		values.push_back(Value::LIST(id_type, std::move(ids)));
	}
	return Value::MAP(key_type, value_type, std::move(keys), std::move(values));
}

Value TextureCellValue(const TextureCell &cell) {
	const auto cell_type = TextureCellType();
	const auto &key_type = MapType::KeyType(cell_type);
	const auto &value_type = MapType::ValueType(cell_type);
	const auto &face_type = ListType::GetChildType(value_type);
	const auto &ring_type = ListType::GetChildType(face_type);

	vector<Value> keys;
	vector<Value> values;
	keys.reserve(cell.themes.size());
	values.reserve(cell.themes.size());
	for (const auto &theme : cell.themes) {
		keys.push_back(Value(theme.first));
		vector<Value> faces;
		faces.reserve(theme.second.size());
		for (const auto &face : theme.second) {
			vector<Value> rings;
			rings.reserve(face.size());
			for (const auto &ring : face) {
				rings.push_back(RingValue(ring, ring_type));
			}
			faces.push_back(Value::LIST(ring_type, std::move(rings)));
		}
		values.push_back(Value::LIST(face_type, std::move(faces)));
	}
	return Value::MAP(key_type, value_type, std::move(keys), std::move(values));
}

MaterialCell MaterialCellFromValue(const Value &value) {
	MaterialCell cell;
	if (value.IsNull()) {
		return cell;
	}
	if (value.type() != MaterialCellType()) {
		throw InvalidInputException("material cell: expected MAP(VARCHAR, BIGINT[]), got %s", value.type().ToString());
	}
	for (const auto &entry : MapValue::GetChildren(value)) {
		const auto &theme_values = MapEntryChild(entry, kMapEntryValue);
		if (theme_values.IsNull()) {
			throw InvalidInputException("material cell: a theme's value is NULL; a theme is either present with a "
			                            "full-length list or absent");
		}
		std::vector<std::optional<int64_t>> ids;
		for (const auto &id : ListValue::GetChildren(theme_values)) {
			ids.push_back(id.IsNull() ? std::optional<int64_t>() : std::optional<int64_t>(id.GetValue<int64_t>()));
		}
		cell.themes.emplace_back(MapEntryChild(entry, kMapEntryKey).ToString(), std::move(ids));
	}
	return cell;
}

TextureCell TextureCellFromValue(const Value &value) {
	TextureCell cell;
	if (value.IsNull()) {
		return cell;
	}
	if (value.type() != TextureCellType()) {
		throw InvalidInputException("texture cell: expected MAP(VARCHAR, STRUCT(id BIGINT, uv DOUBLE[][])[][]), got %s",
		                            value.type().ToString());
	}
	for (const auto &entry : MapValue::GetChildren(value)) {
		const auto &theme_values = MapEntryChild(entry, kMapEntryValue);
		if (theme_values.IsNull()) {
			throw InvalidInputException("texture cell: a theme's value is NULL; a theme is either present with a "
			                            "full-length list or absent");
		}
		std::vector<std::vector<TextureRing>> faces;
		for (const auto &face : ListValue::GetChildren(theme_values)) {
			if (face.IsNull()) {
				throw InvalidInputException("texture cell: a face entry is NULL; every face of the geometry carries "
				                            "a ring list");
			}
			std::vector<TextureRing> rings;
			for (const auto &ring_value : ListValue::GetChildren(face)) {
				if (ring_value.IsNull()) {
					throw InvalidInputException("texture cell: a ring struct is NULL; an untextured ring is a struct "
					                            "with a NULL id and a NULL uv");
				}
				const auto &fields = StructValue::GetChildren(ring_value);
				if (fields.size() != 2) {
					throw InvalidInputException("texture cell: a ring struct does not hold (id, uv)");
				}
				const auto &id = fields[kRingId];
				const auto &uv = fields[kRingUv];
				if (id.IsNull() != uv.IsNull()) {
					throw InvalidInputException("texture cell: a ring's id and uv must be NULL together");
				}
				TextureRing ring;
				if (!id.IsNull()) {
					ring.id = id.GetValue<int64_t>();
					for (const auto &pair : ListValue::GetChildren(uv)) {
						if (pair.IsNull()) {
							throw InvalidInputException("texture cell: a uv pair is NULL");
						}
						const auto &coords = ListValue::GetChildren(pair);
						if (coords.size() != 2 || coords[0].IsNull() || coords[1].IsNull()) {
							throw InvalidInputException("texture cell: a uv entry must hold exactly two non-NULL "
							                            "values");
						}
						ring.uv.push_back({coords[0].GetValue<double>(), coords[1].GetValue<double>()});
					}
				}
				rings.push_back(std::move(ring));
			}
			faces.push_back(std::move(rings));
		}
		cell.themes.emplace_back(MapEntryChild(entry, kMapEntryKey).ToString(), std::move(faces));
	}
	return cell;
}

json MaterialCellToFlatJson(const MaterialCell &cell) {
	json out = json::object();
	for (const auto &theme : cell.themes) {
		json values = json::array();
		for (const auto &id : theme.second) {
			if (id.has_value()) {
				values.push_back(id.value());
			} else {
				values.push_back(nullptr);
			}
		}
		out[theme.first] = json {{"values", std::move(values)}};
	}
	return out;
}

json TextureCellToFlatJson(const TextureCell &cell) {
	json out = json::object();
	for (const auto &theme : cell.themes) {
		json values = json::array();
		for (const auto &face : theme.second) {
			json face_values = json::array();
			for (const auto &ring : face) {
				json ring_values = json::array();
				if (ring.id.has_value()) {
					ring_values.push_back(ring.id.value());
					for (const auto &uv : ring.uv) {
						ring_values.push_back(json::array({uv[0], uv[1]}));
					}
				} else {
					ring_values.push_back(nullptr);
				}
				face_values.push_back(std::move(ring_values));
			}
			values.push_back(std::move(face_values));
		}
		out[theme.first] = json {{"values", std::move(values)}};
	}
	return out;
}

} // namespace cityjson
} // namespace duckdb
