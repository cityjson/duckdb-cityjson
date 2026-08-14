// Assertions for the FlatCityBuf selective-deserialisation layer
// (`FcbFieldMask`, `ConvertFeatureLight`, `DecodeAttributesFiltered`).
//
// The repo convention is SQL tests in test/sql/, and Task 5 will cover the
// projection wiring end to end there. These live at the C++ level because the
// two properties that matter most here are invisible from SQL:
//
//   * the light path must produce a CityJSONFeature INDISTINGUISHABLE from the
//     full path's minus geometry -- which is a field-by-field comparison
//     against `fcb::to_cityjson_feature`, not a query result, and
//   * the filtered blob walk's SKIP arithmetic must be byte-exact for every
//     ColumnType, including the ones no checked-in fixture contains
//     (Byte/UByte/Binary/DateTime). Only a hand-built blob reaches those.
//
// Build/run: see run_fcb_selective_tests.sh next to this file.

#include "cityjson/fcb_selective_convert.hpp"

#include "cityjson/error.hpp"

#include <fcb/cityjson.hpp>
#include <fcb/generated/header_generated.h>
#include <fcb/reader.hpp>
#include <fcb/writer/attribute.hpp>
#include <fcb/writer/fcb_writer.hpp>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>

using duckdb::cityjson::CityJSONError;
using duckdb::cityjson::CityJSONFeature;
using duckdb::cityjson::ConvertFeatureLight;
using duckdb::cityjson::DecodeAttributesFiltered;
using duckdb::cityjson::FcbFieldMask;
using json = nlohmann::json;

static int failures = 0;

#define CHECK(cond)                                                                                                    \
	do {                                                                                                               \
		if (!(cond)) {                                                                                                 \
			std::printf("  FAIL: %s (line %d)\n", #cond, __LINE__);                                                    \
			failures++;                                                                                                \
		}                                                                                                              \
	} while (0)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static std::string DataPath(const char *basename) {
	const char *dir = std::getenv("CITYJSON_TEST_DATA");
	std::string prefix = dir ? dir : "test/data";
	return prefix + "/" + basename;
}

// The full path, verbatim: what FlatCityBufReader::ParseFeatures does today.
static CityJSONFeature FullPath(const fcb::Feature &feature, const fcb::HeaderView &header) {
	return CityJSONFeature::FromJson(fcb::to_cityjson_feature(feature, header));
}

static void PushLE(std::vector<uint8_t> &out, uint64_t value, size_t width) {
	for (size_t i = 0; i < width; ++i) {
		out.push_back(static_cast<uint8_t>((value >> (8 * i)) & 0xFF));
	}
}

static void PushRecordHeader(std::vector<uint8_t> &out, uint16_t column_index) {
	PushLE(out, column_index, 2);
}

static void PushLengthPrefixed(std::vector<uint8_t> &out, const std::string &s) {
	PushLE(out, s.size(), 4);
	out.insert(out.end(), s.begin(), s.end());
}

static void PushF32(std::vector<uint8_t> &out, float f) {
	uint32_t bits;
	std::memcpy(&bits, &f, sizeof(bits));
	PushLE(out, bits, 4);
}

static void PushF64(std::vector<uint8_t> &out, double d) {
	uint64_t bits;
	std::memcpy(&bits, &d, sizeof(bits));
	PushLE(out, bits, 8);
}

static fcb::ColumnInfo Col(uint16_t index, const char *name, ::ColumnType type) {
	fcb::ColumnInfo c;
	c.index = index;
	c.name = name;
	c.type = static_cast<uint8_t>(type);
	c.nullable = true;
	return c;
}

// A blob exercising EVERY ColumnType, in schema order. The point is the skip
// arithmetic: a filtered walk must land on the same record boundaries as an
// unfiltered one, so anything it does not decode still has to be measured
// exactly right.
struct AllTypesBlob {
	std::vector<fcb::ColumnInfo> schema;
	std::vector<uint8_t> bytes;
};

static AllTypesBlob MakeAllTypesBlob() {
	AllTypesBlob b;
	b.schema = {
	    Col(0, "c_bool", ::ColumnType::Bool),         Col(1, "c_byte", ::ColumnType::Byte),
	    Col(2, "c_ubyte", ::ColumnType::UByte),       Col(3, "c_short", ::ColumnType::Short),
	    Col(4, "c_ushort", ::ColumnType::UShort),     Col(5, "c_int", ::ColumnType::Int),
	    Col(6, "c_uint", ::ColumnType::UInt),         Col(7, "c_long", ::ColumnType::Long),
	    Col(8, "c_ulong", ::ColumnType::ULong),       Col(9, "c_float", ::ColumnType::Float),
	    Col(10, "c_double", ::ColumnType::Double),    Col(11, "c_string", ::ColumnType::String),
	    Col(12, "c_json", ::ColumnType::Json),        Col(13, "c_datetime", ::ColumnType::DateTime),
	    Col(14, "c_binary", ::ColumnType::Binary),
	};

	auto &o = b.bytes;
	PushRecordHeader(o, 0);
	o.push_back(1); // Bool true
	PushRecordHeader(o, 1);
	PushLE(o, static_cast<uint64_t>(static_cast<uint8_t>(-7)), 1); // Byte -7
	PushRecordHeader(o, 2);
	PushLE(o, 200, 1); // UByte 200
	PushRecordHeader(o, 3);
	PushLE(o, static_cast<uint64_t>(static_cast<uint16_t>(-300)), 2); // Short -300
	PushRecordHeader(o, 4);
	PushLE(o, 60000, 2); // UShort
	PushRecordHeader(o, 5);
	PushLE(o, static_cast<uint64_t>(static_cast<uint32_t>(-70000)), 4); // Int
	PushRecordHeader(o, 6);
	PushLE(o, 4000000000ULL, 4); // UInt
	PushRecordHeader(o, 7);
	PushLE(o, static_cast<uint64_t>(static_cast<int64_t>(-5000000000LL)), 8); // Long
	PushRecordHeader(o, 8);
	PushLE(o, 9000000000000000000ULL, 8); // ULong
	PushRecordHeader(o, 9);
	PushF32(o, 1.5f);
	PushRecordHeader(o, 10);
	PushF64(o, 2.5);
	PushRecordHeader(o, 11);
	PushLengthPrefixed(o, "hi");
	PushRecordHeader(o, 12);
	PushLengthPrefixed(o, "{\"a\":1}");
	PushRecordHeader(o, 13);
	PushLengthPrefixed(o, "2026-08-14T00:00:00Z");
	PushRecordHeader(o, 14);
	PushLengthPrefixed(o, std::string("\x01\x02\x03", 3));
	return b;
}

// ---------------------------------------------------------------------------
// T1: light path yields no geometry, same ids/types as the full path
// ---------------------------------------------------------------------------

static void T1_LightPathHasNoGeometrySameIdsAndTypes() {
	std::printf("T1: light path drops geometry, keeps ids and types\n");
	auto reader = fcb::FcbReader::open_file(DataPath("fcb_bbox_attr.fcb"));
	auto it = reader.select_all();

	size_t seen = 0;
	while (it.next()) {
		CityJSONFeature full = FullPath(it.current(), reader.header());
		CityJSONFeature light = ConvertFeatureLight(it.current(), reader.header(), FcbFieldMask {false, std::nullopt});

		CHECK(light.id == full.id);
		CHECK(light.type == "CityJSONFeature");
		CHECK(light.city_objects.size() == full.city_objects.size());
		for (const auto &[obj_id, full_obj] : full.city_objects) {
			auto lit = light.city_objects.find(obj_id);
			CHECK(lit != light.city_objects.end());
			if (lit == light.city_objects.end()) {
				continue;
			}
			CHECK(lit->second.type == full_obj.type);
			CHECK(lit->second.geometry.empty());
			CHECK(!full_obj.geometry.empty()); // the fixture really does carry geometry
			CHECK(lit->second.parents == full_obj.parents);
			CHECK(lit->second.children == full_obj.children);
		}
		// Geometry-free means the feature's vertex pool is not built either.
		CHECK(light.vertices.empty());
		seen++;
	}
	CHECK(seen == 3);
}

// ---------------------------------------------------------------------------
// T2: attributes={"height"} decodes height and omits category
// ---------------------------------------------------------------------------

static void T2_MaskedAttributeSubset() {
	std::printf("T2: attribute mask decodes only the named column\n");
	auto reader = fcb::FcbReader::open_file(DataPath("fcb_bbox_attr.fcb"));
	auto it = reader.select_all();

	size_t seen = 0;
	while (it.next()) {
		CityJSONFeature full = FullPath(it.current(), reader.header());
		FcbFieldMask mask;
		mask.geometry = false;
		mask.attributes = std::set<std::string> {"height"};
		CityJSONFeature light = ConvertFeatureLight(it.current(), reader.header(), mask);

		for (const auto &[obj_id, obj] : light.city_objects) {
			CHECK(obj.attributes.size() == 1);
			CHECK(obj.attributes.count("height") == 1);
			CHECK(obj.attributes.count("category") == 0);
			CHECK(obj.attributes.at("height") == full.city_objects.at(obj_id).attributes.at("height"));
		}
		seen++;
	}
	CHECK(seen == 3);
}

// ---------------------------------------------------------------------------
// T3: attributes=nullopt decodes everything, byte-identical to the full path
// ---------------------------------------------------------------------------

static void T3_UnmaskedMatchesFullPath() {
	std::printf("T3: unmasked light path matches the full path's attributes\n");
	auto reader = fcb::FcbReader::open_file(DataPath("fcb_bbox_attr.fcb"));
	auto it = reader.select_all();

	size_t seen = 0;
	while (it.next()) {
		CityJSONFeature full = FullPath(it.current(), reader.header());
		CityJSONFeature light = ConvertFeatureLight(it.current(), reader.header(), FcbFieldMask {false, std::nullopt});

		for (const auto &[obj_id, full_obj] : full.city_objects) {
			const auto &light_obj = light.city_objects.at(obj_id);
			CHECK(light_obj.attributes.size() == full_obj.attributes.size());
			for (const auto &[k, v] : full_obj.attributes) {
				CHECK(light_obj.attributes.count(k) == 1);
				if (light_obj.attributes.count(k) == 1) {
					// json::operator== compares type as well as value: 10.0
					// decoded as an integer would not compare equal here.
					CHECK(light_obj.attributes.at(k) == v);
					CHECK(light_obj.attributes.at(k).type() == v.type());
				}
			}
			CHECK(light_obj.geographical_extent.has_value() == full_obj.geographical_extent.has_value());
		}
		seen++;
	}
	CHECK(seen == 3);
}

// ---------------------------------------------------------------------------
// T4: an EMPTY wanted set decodes nothing but keeps the structural fields
// ---------------------------------------------------------------------------

static void T4_EmptyMaskDecodesNothing() {
	std::printf("T4: empty attribute mask decodes no attributes\n");
	auto reader = fcb::FcbReader::open_file(DataPath("fcb_bbox_attr.fcb"));
	auto it = reader.select_all();

	size_t seen = 0;
	while (it.next()) {
		FcbFieldMask mask;
		mask.geometry = false;
		mask.attributes = std::set<std::string> {};
		CityJSONFeature light = ConvertFeatureLight(it.current(), reader.header(), mask);

		CHECK(!light.id.empty());
		CHECK(light.city_objects.size() == 1);
		for (const auto &[obj_id, obj] : light.city_objects) {
			CHECK(!obj_id.empty());
			CHECK(obj.type == "Building");
			CHECK(obj.attributes.empty());
		}
		seen++;
	}
	CHECK(seen == 3);
}

// ---------------------------------------------------------------------------
// T5: skip arithmetic over every ColumnType
// ---------------------------------------------------------------------------

static void T5_FilteredWalkOverEveryColumnType() {
	std::printf("T5: filtered walk skips every column type by the right width\n");
	AllTypesBlob b = MakeAllTypesBlob();

	// Unfiltered first: this is the oracle for the record boundaries.
	json all = DecodeAttributesFiltered(b.bytes.data(), b.bytes.size(), b.schema, std::nullopt);
	CHECK(all.size() == 15);
	CHECK(all["c_bool"] == true);
	CHECK(all["c_byte"] == -7);
	CHECK(all["c_ubyte"] == 200);
	CHECK(all["c_short"] == -300);
	CHECK(all["c_ushort"] == 60000);
	CHECK(all["c_int"] == -70000);
	CHECK(all["c_uint"] == 4000000000ULL);
	CHECK(all["c_long"] == -5000000000LL);
	CHECK(all["c_ulong"] == 9000000000000000000ULL);
	CHECK(all["c_float"] == 1.5);
	CHECK(all["c_double"] == 2.5);
	CHECK(all["c_string"] == "hi");
	CHECK(all["c_json"].is_object());
	CHECK(all["c_json"]["a"] == 1);
	CHECK(all["c_datetime"] == "2026-08-14T00:00:00Z"); // DateTime stays a string
	CHECK(all["c_binary"] == json::array({1, 2, 3}));

	// Now a subset spanning the whole blob: the last wanted column can only be
	// reached by having skipped all fourteen preceding records exactly.
	std::set<std::string> wanted = {"c_double", "c_string", "c_json", "c_binary"};
	json some = DecodeAttributesFiltered(b.bytes.data(), b.bytes.size(), b.schema, wanted);
	CHECK(some.size() == 4);
	CHECK(some["c_double"] == 2.5);
	CHECK(some["c_string"] == "hi");
	CHECK(some["c_json"]["a"] == 1);
	CHECK(some["c_binary"] == json::array({1, 2, 3}));
	CHECK(!some.contains("c_bool"));
	CHECK(!some.contains("c_datetime"));

	// A mask naming only the very last record proves nothing is materialised on
	// the way there while still landing on its boundary.
	json last = DecodeAttributesFiltered(b.bytes.data(), b.bytes.size(), b.schema, std::set<std::string> {"c_binary"});
	CHECK(last.size() == 1);
	CHECK(last["c_binary"] == json::array({1, 2, 3}));

	// An empty blob is a legal, empty object -- not an error.
	json empty = DecodeAttributesFiltered(nullptr, 0, b.schema, std::nullopt);
	CHECK(empty.is_object());
	CHECK(empty.empty());
}

// ---------------------------------------------------------------------------
// T6: truncated / unknown-column blobs throw
// ---------------------------------------------------------------------------

static void T6_MalformedBlobThrows() {
	std::printf("T6: truncated and unknown-column blobs throw CityJSONError\n");
	AllTypesBlob b = MakeAllTypesBlob();

	// Truncated mid-string-body.
	{
		bool threw = false;
		try {
			DecodeAttributesFiltered(b.bytes.data(), b.bytes.size() - 2, b.schema, std::nullopt);
		} catch (const CityJSONError &) {
			threw = true;
		}
		CHECK(threw);
	}
	// Truncation must be detected even when the truncated column is MASKED OUT:
	// a skip past the end of the blob is corruption, not a saving.
	{
		bool threw = false;
		try {
			DecodeAttributesFiltered(b.bytes.data(), b.bytes.size() - 2, b.schema, std::set<std::string> {"c_bool"});
		} catch (const CityJSONError &) {
			threw = true;
		}
		CHECK(threw);
	}
	// A dangling column index (1 byte left, needs 2).
	{
		bool threw = false;
		try {
			std::vector<uint8_t> one = {0x00};
			DecodeAttributesFiltered(one.data(), one.size(), b.schema, std::nullopt);
		} catch (const CityJSONError &) {
			threw = true;
		}
		CHECK(threw);
	}
	// A column index absent from the schema: its width is unknown, so the rest
	// of the blob is unparseable -- same posture as fcb::decode_attributes.
	{
		bool threw = false;
		try {
			std::vector<uint8_t> bogus;
			PushRecordHeader(bogus, 999);
			PushLE(bogus, 0, 8);
			DecodeAttributesFiltered(bogus.data(), bogus.size(), b.schema, std::nullopt);
		} catch (const CityJSONError &) {
			threw = true;
		}
		CHECK(threw);
	}
}

// ---------------------------------------------------------------------------
// T7: per-object column schema
// ---------------------------------------------------------------------------

// Writes a two-feature .fcb whose SECOND object carries attribute keys the
// header schema does not know, which is exactly the condition under which
// fcb's own writer emits a per-object `columns` table
// (writer/feature_serializer.cpp::to_fcb_attribute). The two schemas assign
// DIFFERENT indices to the shared name, so decoding with the header's schema
// cannot accidentally produce the right answer -- it desynchronises and throws.
static std::string WritePerObjectColumnsFcb() {
	nlohmann::ordered_json meta = {{"type", "CityJSON"},
	                               {"version", "2.0"},
	                               {"transform", {{"scale", {1.0, 1.0, 1.0}}, {"translate", {0.0, 0.0, 0.0}}}}};

	nlohmann::ordered_json f1 = {
	    {"type", "CityJSONFeature"},
	    {"id", "f1"},
	    {"CityObjects",
	     {{"b1",
	       {{"type", "Building"},
	        {"attributes", {{"aaa", "shared"}, {"bbb", 1.5}}},
	        {"geometry", nlohmann::ordered_json::array({{{"type", "MultiSurface"},
	                                                     {"lod", "1"},
	                                                     {"boundaries", {{{0, 1, 2}}}}}})}}}}},
	    {"vertices", {{0, 0, 0}, {10, 0, 0}, {10, 10, 0}}}};

	// b2's attributes introduce "ccc", which the header schema lacks, so the
	// writer gives this object its own schema: bbb->0, ccc->1.
	nlohmann::ordered_json f2 = {
	    {"type", "CityJSONFeature"},
	    {"id", "f2"},
	    {"CityObjects",
	     {{"b2",
	       {{"type", "Building"},
	        {"attributes", {{"bbb", 7.5}, {"ccc", "own-schema"}}},
	        {"geometry", nlohmann::ordered_json::array({{{"type", "MultiSurface"},
	                                                     {"lod", "1"},
	                                                     {"boundaries", {{{0, 1, 2}}}}}})}}}}},
	    {"vertices", {{0, 0, 0}, {10, 0, 0}, {10, 10, 0}}}};

	// The header schema is built from f1 ONLY: aaa->0 (String), bbb->1 (Double).
	fcb::AttributeSchema schema;
	fcb::add_attributes(schema, f1["CityObjects"]["b1"]["attributes"]);

	fcb::FcbWriterOptions options;
	options.write_index = false;
	fcb::FcbWriter writer(meta, options, schema, std::nullopt);
	writer.add_feature(f1);
	writer.add_feature(f2);

	std::string path = "/tmp/cityjson_fcb_per_object_columns.fcb";
	std::ofstream out(path, std::ios::binary);
	writer.write(out);
	out.close();
	return path;
}

static void T7_PerObjectColumnSchema() {
	std::printf("T7: per-object column schema honoured by the light path\n");
	std::string path = WritePerObjectColumnsFcb();
	auto reader = fcb::FcbReader::open_file(path);
	auto it = reader.select_all();

	bool saw_own_schema = false;
	size_t seen = 0;
	while (it.next()) {
		const fcb::Feature &f = it.current();
		if (f.object_has_columns(0)) {
			saw_own_schema = true;
		}
		CityJSONFeature full = FullPath(f, reader.header());
		CityJSONFeature light = ConvertFeatureLight(f, reader.header(), FcbFieldMask {false, std::nullopt});
		for (const auto &[obj_id, full_obj] : full.city_objects) {
			const auto &light_obj = light.city_objects.at(obj_id);
			CHECK(light_obj.attributes.size() == full_obj.attributes.size());
			for (const auto &[k, v] : full_obj.attributes) {
				CHECK(light_obj.attributes.count(k) == 1);
				if (light_obj.attributes.count(k) == 1) {
					CHECK(light_obj.attributes.at(k) == v);
				}
			}
		}
		seen++;
	}
	CHECK(seen == 2);
	// If this fails the fixture is not exercising the trap at all and T7 proves
	// nothing -- louder than a silently-passing test.
	CHECK(saw_own_schema);

	// And the masked walk must select the same schema.
	{
		auto reader2 = fcb::FcbReader::open_file(path);
		auto it2 = reader2.select_all();
		FcbFieldMask mask;
		mask.geometry = false;
		mask.attributes = std::set<std::string> {"ccc"};
		while (it2.next()) {
			CityJSONFeature light = ConvertFeatureLight(it2.current(), reader2.header(), mask);
			for (const auto &[obj_id, obj] : light.city_objects) {
				if (obj_id == "b2") {
					CHECK(obj.attributes.size() == 1);
					CHECK(obj.attributes.at("ccc") == "own-schema");
				} else {
					CHECK(obj.attributes.empty());
				}
			}
		}
	}
	std::remove(path.c_str());
}

// ---------------------------------------------------------------------------
// T8: the projection -> field mask rule (Task 5).
//
// Not SQL-observable: two masks that differ only in how much they decode produce
// identical query results, which is exactly what test/sql/cityjson_fcb_projection.test
// pins. That the narrow case really IS narrow can only be asserted here.
// ---------------------------------------------------------------------------

using duckdb::cityjson::ComputeFcbFieldMask;
// NOT `using duckdb::cityjson::Column/ColumnType;` -- flatbuffers' generated
// header_generated.h declares ::Column and ::ColumnType in the global namespace,
// so an unqualified name here binds to the wrong one (and fails to compile).
using CjColumn = duckdb::cityjson::Column;
using CjColumnType = duckdb::cityjson::ColumnType;

// The wide schema read_flatcitybuf infers for test/data/fcb_bbox_attr.city.jsonl.
static std::vector<CjColumn> WideSchema() {
	return {
	    CjColumn("id", CjColumnType::Varchar),
	    CjColumn("feature_id", CjColumnType::Varchar),
	    CjColumn("object_type", CjColumnType::Varchar),
	    CjColumn("children", CjColumnType::VarcharArray),
	    CjColumn("children_roles", CjColumnType::VarcharArray),
	    CjColumn("parents", CjColumnType::VarcharArray),
	    CjColumn("other", CjColumnType::Json),
	    CjColumn("category", CjColumnType::Varchar),
	    CjColumn("height", CjColumnType::Double),
	    CjColumn("geometry_lod2_2", CjColumnType::GeometryWKB),
	    CjColumn("geometry_properties_lod2_2", CjColumnType::GeometryPropertiesStruct),
	    CjColumn("material_lod2_2", CjColumnType::AppearanceJson),
	    CjColumn("texture_lod2_2", CjColumnType::AppearanceJson),
	    CjColumn("bbox", CjColumnType::GeographicalExtent),
	};
}

static void T8_ProjectionToFieldMask() {
	std::printf("T8: projected column ids narrow the field mask\n");
	const auto schema = WideSchema();

	// COUNT(*) -- no columns at all: nothing to decode.
	{
		auto mask = ComputeFcbFieldMask(schema, {});
		CHECK(!mask.geometry);
		CHECK(mask.attributes.has_value());
		CHECK(mask.attributes->empty());
	}

	// Structural columns only: still no attribute and no geometry.
	{
		auto mask = ComputeFcbFieldMask(schema, {0, 1, 2, 3, 4, 5}); // id..parents
		CHECK(!mask.geometry);
		CHECK(mask.attributes.has_value());
		CHECK(mask.attributes->empty());
	}

	// One attribute: exactly that one.
	{
		auto mask = ComputeFcbFieldMask(schema, {1, 8}); // feature_id, height
		CHECK(!mask.geometry);
		CHECK(mask.attributes.has_value());
		CHECK(mask.attributes->size() == 1);
		CHECK(mask.attributes->count("height") == 1);
		CHECK(mask.attributes->count("category") == 0);
	}

	// Two attributes.
	{
		auto mask = ComputeFcbFieldMask(schema, {7, 8}); // category, height
		CHECK(!mask.geometry);
		CHECK(mask.attributes.has_value());
		CHECK(mask.attributes->size() == 2);
	}

	// `other` is a GetDefinedColumns() name, but it is built from EVERY attribute,
	// so it must widen the mask to "all", not be skipped as structural.
	{
		auto mask = ComputeFcbFieldMask(schema, {6}); // other
		CHECK(!mask.geometry);
		CHECK(!mask.attributes.has_value());
	}

	// Any geometry-derived column selects the full path, which decodes everything.
	for (uint64_t geom_id : {9ull, 10ull, 11ull, 12ull, 13ull}) {
		auto mask = ComputeFcbFieldMask(schema, {1, geom_id});
		CHECK(mask.geometry);
		CHECK(!mask.attributes.has_value());
	}

	// bbox alone is enough -- it is computed from geometry, not stored.
	{
		auto mask = ComputeFcbFieldMask(schema, {13});
		CHECK(mask.geometry);
	}

	// An attribute alongside geometry does not leave a narrowed set behind.
	{
		auto mask = ComputeFcbFieldMask(schema, {8, 9}); // height, geometry_lod2_2
		CHECK(mask.geometry);
		CHECK(!mask.attributes.has_value());
	}

	// COLUMN_IDENTIFIER_ROW_ID (and any other out-of-range id) is ignored, not
	// dereferenced and not turned into an attribute name.
	{
		auto mask = ComputeFcbFieldMask(schema, {UINT64_MAX, 8});
		CHECK(!mask.geometry);
		CHECK(mask.attributes.has_value());
		CHECK(mask.attributes->size() == 1);
		CHECK(mask.attributes->count("height") == 1);
	}

	// Arrow-native geometry columns are geometry-derived too, by kind: their names
	// (geometry_vertices_lod*) are not covered by the WKB grammar's other prefixes.
	{
		std::vector<CjColumn> arrow = {
		    CjColumn("feature_id", CjColumnType::Varchar),
		    CjColumn("geometry_vertices_lod2_2", CjColumnType::GeometryVerticesArrowNative),
		};
		auto mask = ComputeFcbFieldMask(arrow, {1});
		CHECK(mask.geometry);
	}
}

// ---------------------------------------------------------------------------

int main() {
	T1_LightPathHasNoGeometrySameIdsAndTypes();
	T2_MaskedAttributeSubset();
	T3_UnmaskedMatchesFullPath();
	T4_EmptyMaskDecodesNothing();
	T5_FilteredWalkOverEveryColumnType();
	T6_MalformedBlobThrows();
	T7_PerObjectColumnSchema();
	T8_ProjectionToFieldMask();

	if (failures == 0) {
		std::printf("\nAll fcb selective-deserialisation assertions passed.\n");
		return 0;
	}
	std::printf("\n%d assertion(s) FAILED.\n", failures);
	return 1;
}
