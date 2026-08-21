#ifdef CITYJSON_HAS_FCB

#include "cityjson/fcb_selective_convert.hpp"

#include "cityjson/column_types.hpp"
#include "cityjson/lod_table.hpp"
#include "cityjson/error.hpp"

#include <fcb/cityjson.hpp>
#include <fcb/generated/feature_generated.h>
#include <fcb/generated/header_generated.h>

#include <cstring>
#include <unordered_map>

namespace duckdb {
namespace cityjson {

namespace {

// ---------------------------------------------------------------------------
// Little-endian readers.
//
// Copies of fcb's own (src/cpp/src/attribute.cpp): the wire format is
// little-endian regardless of host endianness, and a memcpy of the raw bytes
// would be wrong on a big-endian machine. Kept byte-for-byte equivalent to the
// reference so the two decoders cannot drift.
// ---------------------------------------------------------------------------

template <typename T>
T GetLE(const uint8_t *data, size_t at) {
	using U = typename std::make_unsigned<T>::type;
	U u = 0;
	for (size_t i = 0; i < sizeof(T); ++i) {
		u |= static_cast<U>(data[at + i]) << (8 * i);
	}
	return static_cast<T>(u);
}

float GetF32(const uint8_t *data, size_t at) {
	const uint32_t bits = GetLE<uint32_t>(data, at);
	float f;
	std::memcpy(&f, &bits, sizeof(f));
	return f;
}

double GetF64(const uint8_t *data, size_t at) {
	const uint64_t bits = GetLE<uint64_t>(data, at);
	double d;
	std::memcpy(&d, &bits, sizeof(d));
	return d;
}

/// Byte width of a fixed-width column's value, EXCLUDING the 2-byte index
/// prefix. Returns 0 for the length-prefixed types, which the caller handles
/// separately; an unknown tag has no width at all and is rejected before this
/// is consulted.
size_t FixedWidth(::ColumnType type) {
	switch (type) {
	case ::ColumnType::Bool:
	case ::ColumnType::Byte:
	case ::ColumnType::UByte:
		return 1;
	case ::ColumnType::Short:
	case ::ColumnType::UShort:
		return 2;
	case ::ColumnType::Int:
	case ::ColumnType::UInt:
	case ::ColumnType::Float:
		return 4;
	case ::ColumnType::Long:
	case ::ColumnType::ULong:
	case ::ColumnType::Double:
		return 8;
	case ::ColumnType::String:
	case ::ColumnType::Json:
	case ::ColumnType::DateTime:
	case ::ColumnType::Binary:
		return 0;
	}
	return 0;
}

bool IsLengthPrefixed(::ColumnType type) {
	return type == ::ColumnType::String || type == ::ColumnType::Json || type == ::ColumnType::DateTime ||
	       type == ::ColumnType::Binary;
}

bool IsKnownColumnType(uint8_t raw) {
	return raw <= static_cast<uint8_t>(::ColumnType::MAX);
}

} // namespace

// ============================================================
// DecodeAttributesFiltered
// ============================================================

nlohmann::json DecodeAttributesFiltered(const uint8_t *data, size_t size, const std::vector<fcb::ColumnInfo> &schema,
                                        const std::optional<std::set<std::string>> &wanted) {
	nlohmann::json out = nlohmann::json::object();
	if (data == nullptr || size == 0) {
		return out;
	}

	std::unordered_map<uint16_t, const fcb::ColumnInfo *> by_index;
	by_index.reserve(schema.size());
	for (const auto &c : schema) {
		by_index.emplace(c.index, &c);
	}

	size_t pos = 0;
	// Overflow-safe: `pos + n` could wrap for an absurd length prefix, and the
	// comparison would then pass on a blob that is nowhere near long enough.
	auto need = [&](size_t n, const char *what) {
		if (pos > size || size - pos < n) {
			throw CityJSONError::Parse(std::string("truncated attribute blob reading ") + what);
		}
	};

	while (pos < size) {
		need(2, "column index");
		const uint16_t col_index = GetLE<uint16_t>(data, pos);
		pos += 2;

		auto found = by_index.find(col_index);
		if (found == by_index.end()) {
			throw CityJSONError::Parse("attribute references unknown column index " + std::to_string(col_index));
		}
		const fcb::ColumnInfo &col = *found->second;
		if (!IsKnownColumnType(col.type)) {
			// Width unknown => every following record is unreachable. Guessing
			// would desynchronise the blob silently, which is strictly worse.
			throw CityJSONError::Parse("column '" + col.name + "' has unknown ColumnType " +
			                           std::to_string(static_cast<int>(col.type)));
		}
		const auto type = static_cast<::ColumnType>(col.type);
		const bool want = !wanted.has_value() || wanted->count(col.name) > 0;

		if (IsLengthPrefixed(type)) {
			need(4, "string length");
			const uint32_t len = GetLE<uint32_t>(data, pos);
			pos += 4;
			// Checked even when the column is masked out: a skip past the end of
			// the blob is corruption, not a saving.
			need(len, "string body");
			if (want) {
				const char *body = reinterpret_cast<const char *>(data) + pos;
				switch (type) {
				case ::ColumnType::Json:
					// Stored as text; re-parse so it nests as real JSON, exactly
					// as fcb::attributes_to_json does (and non-throwing, so a
					// malformed payload becomes `discarded` rather than an error).
					out[col.name] = nlohmann::json::parse(body, body + len, nullptr, /*allow_exceptions=*/false);
					break;
				case ::ColumnType::Binary:
					// Raw bytes have no faithful JSON form; a byte array is what
					// the reference emits.
					out[col.name] = std::vector<uint8_t>(data + pos, data + pos + len);
					break;
				default:
					// String and DateTime alike: DateTime stays a string here,
					// unlike its 12-byte packed B+tree key form.
					out[col.name] = std::string(body, len);
					break;
				}
			}
			pos += len;
			continue;
		}

		const size_t width = FixedWidth(type);
		need(width, col.name.c_str());
		if (want) {
			switch (type) {
			case ::ColumnType::Bool:
				out[col.name] = data[pos] != 0;
				break;
			case ::ColumnType::Byte:
				// Byte is UNSIGNED on the wire, exactly like UByte: upstream's
				// writer stores it as a raw u8 (writer/attribute.rs) and the
				// index path reads it back as u8 (reader/attr_query.rs,
				// key.cpp's key_kind_for_column -> KeyKind::UInt8). Decoding it
				// as i8 turned a stored 200 into -56 and disagreed with the
				// very index used to find the row.
				out[col.name] = static_cast<uint64_t>(GetLE<uint8_t>(data, pos));
				break;
			case ::ColumnType::UByte:
				out[col.name] = static_cast<uint64_t>(GetLE<uint8_t>(data, pos));
				break;
			case ::ColumnType::Short:
				out[col.name] = static_cast<int64_t>(GetLE<int16_t>(data, pos));
				break;
			case ::ColumnType::UShort:
				out[col.name] = static_cast<uint64_t>(GetLE<uint16_t>(data, pos));
				break;
			case ::ColumnType::Int:
				out[col.name] = static_cast<int64_t>(GetLE<int32_t>(data, pos));
				break;
			case ::ColumnType::UInt:
				out[col.name] = static_cast<uint64_t>(GetLE<uint32_t>(data, pos));
				break;
			case ::ColumnType::Long:
				out[col.name] = GetLE<int64_t>(data, pos);
				break;
			case ::ColumnType::ULong:
				out[col.name] = GetLE<uint64_t>(data, pos);
				break;
			case ::ColumnType::Float:
				// Widened to double before emission, as fcb::AttrValue does --
				// emitting a float would round-trip through a different JSON
				// number representation than the full path's.
				out[col.name] = static_cast<double>(GetF32(data, pos));
				break;
			case ::ColumnType::Double:
				out[col.name] = GetF64(data, pos);
				break;
			default:
				break; // unreachable: length-prefixed types returned above
			}
		}
		pos += width;
	}

	return out;
}

// ============================================================
// ConvertFeatureLight
// ============================================================

CityJSONFeature ConvertFeatureLight(const fcb::Feature &feature, const fcb::HeaderView &header,
                                    const FcbFieldMask &mask) {
	const ::CityFeature *cf = feature.raw();
	if (cf == nullptr || cf->objects() == nullptr) {
		throw CityJSONError::Parse("empty feature");
	}

	CityJSONFeature result;
	result.id = feature.id();
	result.type = "CityJSONFeature";

	const size_t n = feature.city_object_count();
	for (size_t i = 0; i < n; ++i) {
		const auto *obj = cf->objects()->Get(static_cast<flatbuffers::uoffset_t>(i));
		if (obj == nullptr) {
			continue;
		}

		CityObject co;
		// extension_type wins verbatim when present; city_object_type_name's
		// "+UnknownCityObject" fallback is only ever reached without it.
		co.type = (obj->extension_type() != nullptr) ? obj->extension_type()->str()
		                                             : fcb::city_object_type_name(static_cast<uint8_t>(obj->type()));

		// TRAP: an object that declares its OWN columns must be decoded against
		// them, not the header's -- records are not self-delimiting, so the wrong
		// schema does not produce wrong values, it desynchronises the whole blob.
		// FlatBuffers PRESENCE selects the override, not vector non-emptiness.
		if (feature.object_has_attributes(i)) {
			auto blob = feature.object_attributes(i);
			if (!blob.empty()) {
				auto own = feature.object_columns(i);
				const std::vector<fcb::ColumnInfo> &schema =
				    feature.object_has_columns(i) ? own : header.info().columns;
				nlohmann::json attrs = DecodeAttributesFiltered(blob.data(), blob.size(), schema, mask.attributes);
				for (auto &[key, value] : attrs.items()) {
					co.attributes[key] = value;
				}
			}
			// A present-but-empty attributes vector is `"attributes": {}` on the
			// full path, which CityObject::FromJson turns into an empty map --
			// which is what `co.attributes` already is. Nothing to do.
		}

		std::array<double, 6> extent {};
		if (feature.object_extent(i, extent)) {
			co.geographical_extent =
			    GeographicalExtent(extent[0], extent[1], extent[2], extent[3], extent[4], extent[5]);
		}

		// Geometry is deliberately never touched -- neither obj->geometry() nor
		// obj->geometry_instances(), and no feature vertex pool or appearance.

		if (obj->children() != nullptr && !obj->children()->empty()) {
			for (const auto *c : *obj->children()) {
				if (c != nullptr) {
					co.children.push_back(c->str());
				}
			}
		}
		if (obj->parents() != nullptr && !obj->parents()->empty()) {
			for (const auto *p : *obj->parents()) {
				if (p != nullptr) {
					co.parents.push_back(p->str());
				}
			}
		}
		// children_roles is intentionally left unset: to_cityjson_feature does
		// not emit it either, so setting it here would make the light path
		// DIFFER from the full path rather than match it.

		result.city_objects[feature.object_id(i)] = std::move(co);
	}

	return result;
}

// ---------------------------------------------------------------------------
// Projection -> field mask
// ---------------------------------------------------------------------------

bool IsGeometryDerivedColumn(const Column &column) {
	switch (column.kind) {
	case ColumnType::Geometry:
	case ColumnType::GeographicalExtent:
	case ColumnType::GeometryWKB:
	case ColumnType::GeometryPropertiesStruct:
	case ColumnType::AppearanceJson:
	case ColumnType::GeometryArrowNative:
	case ColumnType::GeometryVerticesArrowNative:
		return true;
	default:
		break;
	}
	const auto &name = column.name;
	return name == "bbox" || name.rfind("geometry_lod", 0) == 0 || name.rfind("geometry_vertices_lod", 0) == 0 ||
	       name.rfind("geometry_properties_lod", 0) == 0 || name.rfind("material_lod", 0) == 0 ||
	       name.rfind("texture_lod", 0) == 0;
}

namespace {

// Structural columns the light path fills from FlatCityBuf table fields (or leaves
// NULL) rather than from the attribute blob. Derived from GetDefinedColumns() (the
// head reserved run) and LODTableUtils::GetTrailingColumns() (`template`, `other`)
// so the list cannot drift, minus `other` -- which is a defined column but is
// assembled from every attribute the object has (CityObjectUtils::GetAttributeValue),
// so it is handled separately.
const std::set<std::string> &StructuralColumnNames() {
	static const std::set<std::string> structural = [] {
		std::set<std::string> s;
		for (const auto &col : GetDefinedColumns()) {
			s.insert(col.name);
		}
		for (const auto &col : LODTableUtils::GetTrailingColumns()) {
			if (col.name == "other") {
				continue;
			}
			s.insert(col.name);
		}
		return s;
	}();
	return structural;
}

} // namespace

FcbFieldMask ComputeFcbFieldMask(const std::vector<Column> &columns, const std::vector<uint64_t> &column_ids) {
	FcbFieldMask mask;
	mask.geometry = false;
	mask.attributes = std::set<std::string> {};

	for (auto col_id : column_ids) {
		// Covers DuckDB's COLUMN_IDENTIFIER_ROW_ID sentinel (UINT64_MAX) as well as any
		// other id that does not name a column of this schema.
		if (col_id >= columns.size()) {
			continue;
		}
		const auto &column = columns[col_id];
		if (IsGeometryDerivedColumn(column)) {
			mask.geometry = true;
			continue;
		}
		if (column.name == "other") {
			mask.attributes.reset();
			continue;
		}
		if (mask.attributes.has_value() && StructuralColumnNames().count(column.name) == 0) {
			mask.attributes->insert(column.name);
		}
	}

	if (mask.geometry) {
		// The full path decodes every attribute regardless (design doc 4.2); keeping a
		// narrowed set here would only misdescribe what the scan actually gets.
		mask.attributes.reset();
	}
	return mask;
}

} // namespace cityjson
} // namespace duckdb

#endif // CITYJSON_HAS_FCB
