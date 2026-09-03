#include "cityjson/obj_parser.hpp"

#include "cityjson/error.hpp"

#include <array>
#include <cstdio>
#include <optional>
#include <string>

using duckdb::cityjson::CityJSONError;
using duckdb::cityjson::MtlDocument;
using duckdb::cityjson::ObjDocument;
using duckdb::cityjson::ParseMtl;
using duckdb::cityjson::ParseObj;

static int failures = 0;
#define CHECK(cond)                                                                                                    \
	do {                                                                                                               \
		if (!(cond)) {                                                                                                 \
			std::fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);                                       \
			failures++;                                                                                                \
		}                                                                                                              \
	} while (0)

//! The message of the CityJSONError `body` throws, or "" when it throws nothing.
template <typename F>
static std::string ErrorOf(F body) {
	try {
		body();
	} catch (const CityJSONError &e) {
		return e.what();
	}
	return "";
}

static void TestObj() {
	// One `.mtl`, named twice: the second `mtllib` of a name already loaded adds nothing,
	// so `brick` keeps ordinal 0.
	auto loader = [](const std::string &name) -> std::optional<std::string> {
		if (name == "t.mtl") {
			return std::string("newmtl brick\nKd 0.7 0.3 0.2\n");
		}
		return std::nullopt;
	};

	const std::string text = "# a hand-written fixture\n"
	                         "mtllib t.mtl missing.mtl\n"
	                         "v 0 0 0 1 0 0\n" // x y z r g b: only the position is read
	                         "v 1 0 \\\n"      // continuation: the line resumes below
	                         "0\n"
	                         "v 1 1 0\n"
	                         "vt 0.3 0.7\n"
	                         "vn 0 0 1\n" // ignored
	                         "s off\n"    // ignored
	                         "g WallSurface\n"
	                         "usemtl brick\n"
	                         "o house\n"
	                         "f -3/1 -2/1 -1/1\n" // negative (relative) indices
	                         "o house\n"          // a repeated `o` resumes its object
	                         "f 1 2 3\n"
	                         "mtllib t.mtl\n";

	ObjDocument doc = ParseObj(text, "stem", loader);

	CHECK(doc.vertices.size() == 3);
	CHECK(doc.vertices[0] == (std::array<double, 3> {0, 0, 0}));
	CHECK(doc.vertices[1] == (std::array<double, 3> {1, 0, 0})); // the continued line
	CHECK(doc.vertices[2] == (std::array<double, 3> {1, 1, 0}));
	CHECK(doc.texcoords.size() == 1);
	CHECK(doc.texcoords[0] == (std::array<double, 2> {0.3, 0.7}));

	CHECK(doc.objects.size() == 1);
	CHECK(doc.objects[0].name == "house");
	CHECK(doc.objects[0].faces.size() == 2);
	if (doc.objects[0].faces.size() == 2) {
		const auto &first = doc.objects[0].faces[0];
		CHECK(first.v == (std::vector<int> {0, 1, 2}));
		CHECK(first.vt == (std::vector<int> {0, 0, 0}));
		CHECK(first.group == "WallSurface");
		CHECK(first.usemtl == "brick");
		CHECK(first.material_index == 0);
		const auto &second = doc.objects[0].faces[1];
		CHECK(second.v == (std::vector<int> {0, 1, 2}));
		CHECK(second.vt == (std::vector<int> {-1, -1, -1})); // no `vt` field on the line
		CHECK(second.group == "WallSurface");                // state persists across `o`
		CHECK(second.material_index == 0);
	}

	CHECK(doc.mtllibs == (std::vector<std::string> {"t.mtl", "missing.mtl"}));
	CHECK(doc.materials.materials.size() == 1);
	CHECK(doc.warnings.size() == 1); // the unreadable second file of the `mtllib` line

	// No `o` at all: one object under the caller's default name.
	ObjDocument bare =
	    ParseObj("v 0 0 0\nv 1 0 0\nv 1 1 0\nf 1 2 3\n", "stem", [](const std::string &) { return std::nullopt; });
	CHECK(bare.objects.size() == 1);
	CHECK(bare.objects[0].name == "stem");
	CHECK(bare.objects[0].faces.size() == 1);
	CHECK(bare.objects[0].faces[0].material_index == -1);

	// Structural errors, with the exact messages the SQL suites pin.
	auto loader_none = [](const std::string &) {
		return std::optional<std::string>();
	};
	CHECK(ErrorOf([&] { ParseObj("v 0 0 0\nv 1 0 0\nf 1 2 9\n", "stem", loader_none); }) ==
	      "face references vertex 9 but 2 vertices have been declared");
	CHECK(ErrorOf([&] { ParseObj("v 0 0 0\nv 1 0 0\nf 1/1 2/1\n", "stem", loader_none); }) ==
	      "a face has fewer than three vertices");
	CHECK(ErrorOf([&] { ParseObj("v 0 0 0\nv 1 0 0\nv 1 1 0\nf 1/4 2/1 3/1\n", "stem", loader_none); }) ==
	      "face references texture coordinate 4 but 0 have been declared");
	CHECK(ErrorOf([&] { ParseObj("v 0 0\n", "stem", loader_none); }) == "line 1 'v' has fewer than three coordinates");
	CHECK(ErrorOf([&] { ParseObj("v 0 0 0\nvt\n", "stem", loader_none); }) == "line 2 'vt' has no u coordinate");
}

static void TestMtl() {
	const std::string text = "# a hand-written fixture\n"
	                         "newmtl GroundSurface\n"
	                         "Kd 0.3 0.3 0.3\n"
	                         "\n"
	                         "newmtl brick\n"
	                         "Kd 0.7 0.3 0.2\n"
	                         "Ks 0.1 0.1 0.1\n"
	                         "Ke 0 0 0\n"
	                         "Ka 0.1 0.2 0.3\n"
	                         "d 0.7\n"
	                         "Tr 0.9\n" // `d` won already, so this is ignored
	                         "Ns 0.7\n"
	                         "map_Kd brick.png\n"
	                         "illum 2\n"
	                         "map_Ks spec.png\n"
	                         "\n"
	                         "newmtl roof\n"
	                         "Tr 0.3\n"; // transparency as written, no `d` in the block

	MtlDocument doc = ParseMtl(text);
	CHECK(doc.materials.size() == 3);
	if (doc.materials.size() != 3) {
		return;
	}

	const auto &ground = doc.materials[0];
	CHECK(ground.name == "GroundSurface");
	CHECK(ground.diffuse == (std::optional<std::array<double, 3>> {{0.3, 0.3, 0.3}}));
	// Presence tracking: what the block does not state stays absent, rather than
	// reporting the black specular colour a zero-initialised record would.
	CHECK(!ground.specular.has_value());
	CHECK(!ground.ambient.has_value());
	CHECK(!ground.emission.has_value());
	CHECK(!ground.shininess.has_value());
	CHECK(!ground.transparency.has_value());
	CHECK(ground.map_kd.empty());
	CHECK(ground.other.empty());

	const auto &brick = doc.materials[1];
	CHECK(brick.name == "brick");
	CHECK(brick.specular == (std::optional<std::array<double, 3>> {{0.1, 0.1, 0.1}}));
	CHECK(brick.emission == (std::optional<std::array<double, 3>> {{0.0, 0.0, 0.0}}));
	CHECK(brick.ambient == (std::optional<std::array<double, 3>> {{0.1, 0.2, 0.3}}));
	CHECK(brick.shininess == std::optional<double> {0.7});
	// `d` wins over `Tr`, and 1 - 0.7 is kept as computed.
	CHECK(brick.transparency == std::optional<double> {1.0 - 0.7});
	CHECK(brick.map_kd == "brick.png");
	// Every directive this mapping does not cover, verbatim -- `map_Kd` excepted.
	CHECK(brick.other.size() == 2);
	CHECK(brick.other.count("illum") == 1 && brick.other.at("illum") == "2");
	CHECK(brick.other.count("map_Ks") == 1 && brick.other.at("map_Ks") == "spec.png");
	CHECK(brick.other.count("map_Kd") == 0);

	// `Tr` with no `d` in the block is the transparency as written: no 1 - (1 - Tr)
	// round trip, which would print 0.30000000000000004.
	CHECK(doc.materials[2].transparency == std::optional<double> {0.3});

	// A file with no `newmtl` at all declares nothing.
	CHECK(ParseMtl("# nothing but this comment\n").materials.empty());
	CHECK(ParseMtl("").materials.empty());
	// A nameless `newmtl` is not a declaration, and does not close the block it is in.
	MtlDocument nameless = ParseMtl("newmtl a\nnewmtl\nKd 1 1 1\n");
	CHECK(nameless.materials.size() == 1);
	if (nameless.materials.size() == 1) {
		CHECK(nameless.materials[0].name == "a");
		CHECK(nameless.materials[0].diffuse == (std::optional<std::array<double, 3>> {{1.0, 1.0, 1.0}}));
	}
}

int main() {
	TestObj();
	TestMtl();
	if (failures == 0) {
		std::printf("obj_parser: all checks passed\n");
	}
	return failures == 0 ? 0 : 1;
}
