#ifdef CITYJSON_HAS_FCB

#include "cityjson/flatcitybuf_reader.hpp"
#include "cityjson/city_object_utils.hpp"
#include "cityjson/duckdb_fs_range_reader.hpp"
#include "cityjson/json_utils.hpp"
#include <fcb/cityjson.hpp>

namespace duckdb {
namespace cityjson {

using namespace json_utils;

namespace {

// fcb::KeyValue exposes no public numeric getter (by design -- see key.hpp: only
// kind()/original_string(), the latter populated for string kinds only). So a decoded
// JSON attribute value is turned into ANOTHER KeyValue of the same kind and compared
// via fcb::compare_keys, which gives the exact same ordering semantics the B+tree
// index itself uses (including its "ordered_float" float handling) instead of
// reimplementing comparison logic by hand.
std::optional<fcb::KeyValue> KeyValueFromJsonByKind(const json &value, fcb::KeyKind kind) {
	try {
		switch (kind) {
		case fcb::KeyKind::Int8:
			return fcb::KeyValue::from_i8(static_cast<int8_t>(value.get<int64_t>()));
		case fcb::KeyKind::UInt8:
			return fcb::KeyValue::from_u8(static_cast<uint8_t>(value.get<int64_t>()));
		case fcb::KeyKind::Int16:
			return fcb::KeyValue::from_i16(static_cast<int16_t>(value.get<int64_t>()));
		case fcb::KeyKind::UInt16:
			return fcb::KeyValue::from_u16(static_cast<uint16_t>(value.get<int64_t>()));
		case fcb::KeyKind::Int32:
			return fcb::KeyValue::from_i32(static_cast<int32_t>(value.get<int64_t>()));
		case fcb::KeyKind::UInt32:
			return fcb::KeyValue::from_u32(static_cast<uint32_t>(value.get<int64_t>()));
		case fcb::KeyKind::Int64:
			return fcb::KeyValue::from_i64(value.get<int64_t>());
		case fcb::KeyKind::UInt64:
			return fcb::KeyValue::from_u64(value.get<uint64_t>());
		case fcb::KeyKind::Float32:
			return fcb::KeyValue::from_f32(static_cast<float>(value.get<double>()));
		case fcb::KeyKind::Float64:
			return fcb::KeyValue::from_f64(value.get<double>());
		case fcb::KeyKind::Bool:
			return fcb::KeyValue::from_bool(value.get<bool>());
		case fcb::KeyKind::String20:
			return fcb::KeyValue::from_string(fcb::KeyKind::String20,
			                                  value.is_string() ? value.get<std::string>() : value.dump());
		case fcb::KeyKind::String50:
			return fcb::KeyValue::from_string(fcb::KeyKind::String50,
			                                  value.is_string() ? value.get<std::string>() : value.dump());
		case fcb::KeyKind::String100:
			return fcb::KeyValue::from_string(fcb::KeyKind::String100,
			                                  value.is_string() ? value.get<std::string>() : value.dump());
		default:
			return std::nullopt; // DateTime not needed for post-filter v1 -- no test exercises it
		}
	} catch (const json::type_error &) {
		return std::nullopt; // attribute's actual JSON type doesn't match the column's declared type
	}
}

} // namespace

// ============================================================
// Constructor
// ============================================================

FlatCityBufReader::FlatCityBufReader(duckdb::ClientContext &context, const std::string &name,
                                     const std::string &file_path, size_t sample_lines)
    : context_(context), name_(name), file_path_(file_path), sample_lines_(sample_lines) {
}

// ============================================================
// Name
// ============================================================

std::string FlatCityBufReader::Name() const {
	return name_;
}

// ============================================================
// OpenFcbReader
// ============================================================

fcb::FcbReader FlatCityBufReader::OpenFcbReader() const {
	auto transport = std::make_shared<DuckDBRangeReader>(context_, file_path_);
	return fcb::FcbReader::open(transport);
}

// ============================================================
// SetBBoxFilter / SetAttrQueryFilter
// ============================================================

void FlatCityBufReader::SetBBoxFilter(std::array<double, 4> bbox) {
	bbox_ = bbox;
}

void FlatCityBufReader::SetAttrQueryFilter(fcb::AttrQuery query, bool exact_index_only) {
	attr_query_ = std::move(query);
	attr_query_exact_index_only_ = exact_index_only;
}

// ============================================================
// SetFieldMask
// ============================================================

void FlatCityBufReader::SetFieldMask(FcbFieldMask mask) {
	field_mask_ = std::move(mask);
}

// ============================================================
// SelectIterator
// ============================================================

fcb::FeatureIterator FlatCityBufReader::SelectIterator(fcb::FcbReader &reader) const {
	if (bbox_.has_value()) {
		const auto &b = bbox_.value();
		return reader.select_bbox(fcb::BBox {b[0], b[1], b[2], b[3]});
	}
	if (attr_query_.has_value()) {
		return reader.select_attr(attr_query_.value(), fcb::AttrQueryOptions {attr_query_exact_index_only_});
	}
	return reader.select_all();
}

// ============================================================
// MatchesAttrQueryPostFilter
// ============================================================

bool FlatCityBufReader::MatchesAttrQueryPostFilter(const CityJSONFeature &feature) const {
	// Safety net for the "bbox AND attribute filter both set" case (spec §5.4 point 5):
	// select_bbox already ran the real index traversal, so this is a plain per-row
	// check, not a second index query. Harmless (redundant but correct) when
	// attr_query_ was itself the one used for SelectIterator's traversal too.
	if (!attr_query_.has_value()) {
		return true;
	}
	for (const auto &[obj_id, obj] : feature.city_objects) {
		bool all_match = true;
		for (const auto &cond : attr_query_.value()) {
			auto attr_it = obj.attributes.find(cond.field);
			if (attr_it == obj.attributes.end() || attr_it->second.is_null()) {
				all_match = false;
				break;
			}
			auto feature_value = KeyValueFromJsonByKind(attr_it->second, cond.value.kind());
			if (!feature_value.has_value()) {
				all_match = false;
				break;
			}
			// fcb::compare_keys compares the ENCODED (truncated-to-50/100-byte) form for
			// string kinds, matching what the on-disk index itself stores and orders by.
			// That's fine for equality (two strings sharing a >50-byte prefix that also
			// happen to be equal there are indistinguishable to the index either way), but
			// WRONG for relational comparisons: two different strings sharing the first 50
			// bytes would compare equal here even though their full values differ. Since
			// KeyValue::from_string always retains the original, untruncated string (see
			// key.hpp), compare that directly here instead -- unlike encode_key/compare_keys,
			// this post-filter isn't bound to the on-disk index's own truncated ordering.
			int cmp;
			if (cond.value.kind() == fcb::KeyKind::String20 || cond.value.kind() == fcb::KeyKind::String50 ||
			    cond.value.kind() == fcb::KeyKind::String100) {
				cmp = feature_value.value().original_string().compare(cond.value.original_string());
			} else {
				cmp = fcb::compare_keys(feature_value.value(), cond.value);
			}
			bool ok = false;
			switch (cond.op) {
			case fcb::Operator::Eq:
				ok = (cmp == 0);
				break;
			case fcb::Operator::Ne:
				ok = (cmp != 0);
				break;
			case fcb::Operator::Gt:
				ok = (cmp > 0);
				break;
			case fcb::Operator::Ge:
				ok = (cmp >= 0);
				break;
			case fcb::Operator::Lt:
				ok = (cmp < 0);
				break;
			case fcb::Operator::Le:
				ok = (cmp <= 0);
				break;
			}
			if (!ok) {
				all_match = false;
				break;
			}
		}
		if (all_match) {
			return true;
		}
	}
	return false;
}

// ============================================================
// ParseFeatures
// ============================================================

std::vector<CityJSONFeature> FlatCityBufReader::ParseFeatures(std::optional<size_t> limit) const {
	return ParseFeaturesWithMask(limit, field_mask_);
}

std::vector<CityJSONFeature> FlatCityBufReader::ParseFeaturesWithMask(std::optional<size_t> limit,
                                                                     const FcbFieldMask &mask) const {
	auto fcb_reader = OpenFcbReader();
	auto it = SelectIterator(fcb_reader);
	bool need_post_filter = bbox_.has_value() && attr_query_.has_value();

	// The post-filter reads its operands off the DECODED feature, so a caller's
	// mask must never be allowed to starve it. Union them in here rather than in
	// the caller's mask computation, where a future caller could forget.
	FcbFieldMask effective = mask;
	if (attr_query_.has_value() && effective.attributes.has_value()) {
		for (const auto &cond : attr_query_.value()) {
			effective.attributes->insert(cond.field);
		}
	}

	std::vector<CityJSONFeature> features;
	while (it.next()) {
		if (limit.has_value() && features.size() >= limit.value()) {
			break;
		}
		if (!effective.geometry) {
			// Light path: no geometry is decoded, and only the masked-in
			// attributes are. A malformed attribute blob throws out of here, the
			// same way fcb::decode_attributes's own fcb::Error escapes the full
			// path below -- the try/catch there only covers FromJson.
			CityJSONFeature feature = ConvertFeatureLight(it.current(), fcb_reader.header(), effective);
			if (need_post_filter && !MatchesAttrQueryPostFilter(feature)) {
				continue;
			}
			features.push_back(std::move(feature));
			continue;
		}
		json feature_json = fcb::to_cityjson_feature(it.current(), fcb_reader.header());
		try {
			CityJSONFeature feature = CityJSONFeature::FromJson(feature_json);
			if (need_post_filter && !MatchesAttrQueryPostFilter(feature)) {
				continue;
			}
			features.push_back(std::move(feature));
		} catch (const CityJSONError &e) {
			// Skip malformed features
		}
	}

	return features;
}

// ============================================================
// ReadMetadata
// ============================================================

CityJSON FlatCityBufReader::ReadMetadata() const {
	if (cached_metadata_.has_value()) {
		return cached_metadata_.value();
	}
	auto fcb_reader = OpenFcbReader();
	json meta_json = fcb::to_cityjson_metadata(fcb_reader.header());
	cached_metadata_ = CityJSON::FromJson(meta_json);
	return cached_metadata_.value();
}

// ============================================================
// ReadAllChunks
// ============================================================

CityJSONFeatureChunk FlatCityBufReader::ReadAllChunks() const {
	auto features = ParseFeatures(std::nullopt);
	return CityJSONFeatureChunk::CreateChunks(std::move(features), STANDARD_VECTOR_SIZE);
}

// ============================================================
// ReadNFeatures
// ============================================================

std::vector<CityJSONFeature> FlatCityBufReader::ReadNFeatures(size_t n) const {
	return ParseFeatures(n);
}

// ============================================================
// Columns
// ============================================================

std::vector<Column> FlatCityBufReader::Columns() const {
	if (cached_columns_.has_value()) {
		return cached_columns_.value();
	}

	std::vector<Column> columns = GetDefinedColumns();
	// Explicitly FULL-mask, not field_mask_: this list is the table's schema, and
	// it is inferred by sampling decoded features. Sampling under a narrowed mask
	// would drop the very geometry/attribute columns the mask was computed from
	// -- and the mask is computed from projection, which is only known after this
	// has run. Cheap: Columns() is cached, so the full-mask sample happens once.
	std::vector<CityJSONFeature> sample_features = ParseFeaturesWithMask(sample_lines_, FcbFieldMask {});
	std::vector<Column> attr_columns = CityObjectUtils::InferAttributeColumns(sample_features, sample_lines_);
	std::vector<Column> geom_columns = CityObjectUtils::InferGeometryColumns(sample_features, sample_lines_);

	columns.insert(columns.end(), attr_columns.begin(), attr_columns.end());
	columns.insert(columns.end(), geom_columns.begin(), geom_columns.end());

	cached_columns_ = columns;
	return columns;
}

// ============================================================
// IndexedAttributeColumns
// ============================================================

std::vector<std::string> FlatCityBufReader::IndexedAttributeColumns() const {
	auto fcb_reader = OpenFcbReader();
	const auto &header = fcb_reader.header();
	std::vector<std::string> result;
	for (const auto &idx_info : header.attr_indices()) {
		for (const auto &col : header.info().columns) {
			if (col.index == idx_info.column_index) {
				result.push_back(col.name);
				break;
			}
		}
	}
	return result;
}

// ============================================================
// FindColumn
// ============================================================

std::optional<fcb::ColumnInfo> FlatCityBufReader::FindColumn(const std::string &name) const {
	auto fcb_reader = OpenFcbReader();
	for (const auto &col : fcb_reader.header().info().columns) {
		if (col.name == name) {
			return col;
		}
	}
	return std::nullopt;
}

// ============================================================
// Header
// ============================================================

fcb::HeaderView FlatCityBufReader::Header() const {
	return OpenFcbReader().header();
}

} // namespace cityjson
} // namespace duckdb

#endif // CITYJSON_HAS_FCB
