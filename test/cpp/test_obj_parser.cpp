#include "cityjson/obj_parser.hpp"

#include "cityjson/error.hpp"

#include <array>
#include <cstdio>
#include <optional>
#include <string>

using duckdb::cityjson::CityJSONError;
using duckdb::cityjson::MtlDocument;
using duckdb::cityjson::MtlSource;
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
	auto loader = [](const std::string &name) -> MtlSource {
		MtlSource source;
		if (name == "t.mtl") {
			source.text = std::string("newmtl brick\nKd 0.7 0.3 0.2\n");
		} else {
			source.error = "no such file";
		}
		return source;
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

	CHECK(doc.materials.materials.size() == 1);
	// The unreadable second file of the `mtllib` line, named with the reason the loader
	// gave -- and nothing else: every `usemtl` here resolves.
	CHECK(doc.warnings.size() == 1);
	if (doc.warnings.size() == 1) {
		CHECK(doc.warnings[0] == "mtllib 'missing.mtl' could not be read: no such file");
	}

	// A comment ending in a backslash does not continue: joining it would make the `#`
	// swallow the line after it, and an exporter writing a Windows path into a comment
	// is not asking for that.
	ObjDocument commented = ParseObj("# exported from C:\\models\\\nv 9 9 9\nv 0 0 0\nv 1 1 0\nf 1 2 3\n", "stem",
	                                 [](const std::string &) { return MtlSource {}; });
	CHECK(commented.vertices.size() == 3);
	if (commented.vertices.size() == 3) {
		CHECK(commented.vertices[0] == (std::array<double, 3> {9, 9, 9}));
	}

	// No `o` at all: one object under the caller's default name.
	ObjDocument bare =
	    ParseObj("v 0 0 0\nv 1 0 0\nv 1 1 0\nf 1 2 3\n", "stem", [](const std::string &) { return MtlSource {}; });
	CHECK(bare.objects.size() == 1);
	CHECK(bare.objects[0].name == "stem");
	CHECK(bare.objects[0].faces.size() == 1);
	CHECK(bare.objects[0].faces[0].material_index == -1);

	// Structural errors, with the exact messages the SQL suites pin.
	auto loader_none = [](const std::string &) {
		return MtlSource {};
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

//! The grammar's corners: line endings, whitespace, face field forms, and the names
//! `g` / `o` / `newmtl` take.
static void TestQuirks() {
	auto none = [](const std::string &) {
		return MtlSource {};
	};

	// CRLF throughout, tabs as separators, and a continuation whose line ends CRLF.
	ObjDocument crlf = ParseObj("# header\r\n"
	                            "v\t0\t0\t0\r\n"
	                            "v 1 0 0\r\n"
	                            "v 1 1 \\\r\n"
	                            "0\r\n"
	                            "f\t1\t2\t3\r\n",
	                            "stem", none);
	CHECK(crlf.vertices.size() == 3);
	if (crlf.vertices.size() == 3) {
		CHECK(crlf.vertices[2] == (std::array<double, 3> {1, 1, 0}));
	}
	CHECK(crlf.objects.size() == 1 && crlf.objects[0].faces.size() == 1);

	// Face fields: v//vn carries no texture coordinate, v/vt/vn does.
	const std::string faces = "v 0 0 0\nv 1 0 0\nv 1 1 0\nvt 0 0\n"
	                          "f 1//1 2//2 3//3\n"
	                          "f 1/1/1 2/1/2 3/1/3\n"
	                          "f \n"; // an `f` with nothing after it declares no face
	ObjDocument fielded = ParseObj(faces, "stem", none);
	CHECK(fielded.objects.size() == 1);
	if (fielded.objects.size() == 1) {
		CHECK(fielded.objects[0].faces.size() == 2);
		if (fielded.objects[0].faces.size() == 2) {
			CHECK(fielded.objects[0].faces[0].vt == (std::vector<int> {-1, -1, -1}));
			CHECK(fielded.objects[0].faces[1].vt == (std::vector<int> {0, 0, 0}));
		}
	}

	// 0 is not an index OBJ has a vertex for, and neither is a token that is not a number.
	CHECK(ErrorOf([&] { ParseObj("v 0 0 0\nv 1 0 0\nv 1 1 0\nf 0 1 2\n", "stem", none); }) ==
	      "face references vertex 0 but 3 vertices have been declared");

	// `g a b` is group `b` (the last token); an `o` name may hold spaces.
	ObjDocument named = ParseObj("v 0 0 0\nv 1 0 0\nv 1 1 0\ng a b\no my  house \nf 1 2 3\n", "stem", none);
	CHECK(named.objects.size() == 1);
	if (named.objects.size() == 1) {
		CHECK(named.objects[0].name == "my  house");
		CHECK(named.objects[0].faces.size() == 1 && named.objects[0].faces[0].group == "b");
	}

	// A `usemtl` naming nothing declared is a warning, once per distinct name however
	// many faces stand under it -- including a `usemtl` that precedes its `mtllib`.
	auto brick = [](const std::string &) {
		MtlSource source;
		source.text = std::string("newmtl brick\nKd 1 0 0\n");
		return source;
	};
	ObjDocument early = ParseObj("v 0 0 0\nv 1 0 0\nv 1 1 0\n"
	                             "usemtl brick\nf 1 2 3\n"
	                             "mtllib t.mtl\n"
	                             "usemtl ghost\nf 1 2 3\nusemtl ghost\nf 1 2 3\n",
	                             "stem", brick);
	CHECK(early.materials.materials.size() == 1);
	CHECK(early.objects.size() == 1 && early.objects[0].faces.size() == 3);
	if (early.objects.size() == 1 && early.objects[0].faces.size() == 3) {
		CHECK(early.objects[0].faces[0].material_index == -1); // the mtllib came later
	}
	CHECK(early.warnings.size() == 2);
	if (early.warnings.size() == 2) {
		CHECK(early.warnings[0] == "usemtl 'brick' names no material declared by any mtllib");
		CHECK(early.warnings[1] == "usemtl 'ghost' names no material declared by any mtllib");
	}

	// A `usemtl` matching a duplicated `newmtl` name takes the first declaration.
	auto twice = [](const std::string &) {
		MtlSource source;
		source.text = std::string("newmtl a\nKd 1 0 0\nnewmtl a\nKd 0 1 0\n");
		return source;
	};
	ObjDocument dup = ParseObj("mtllib t.mtl\nv 0 0 0\nv 1 0 0\nv 1 1 0\nusemtl a\nf 1 2 3\n", "stem", twice);
	CHECK(dup.materials.materials.size() == 2);
	CHECK(dup.warnings.empty());
	if (dup.objects.size() == 1 && dup.objects[0].faces.size() == 1) {
		CHECK(dup.objects[0].faces[0].material_index == 0);
	}

	// A backslash on the very last line has nothing to continue onto, so it stays part
	// of the line and the line is judged as it stands.
	CHECK(ErrorOf([&] { ParseObj("v 0 0 0\nv 1 1 \\", "stem", none); }) ==
	      "line 2 'v' has fewer than three coordinates");
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

	// MTL's texture options come before the file name and are dropped; a name may hold
	// spaces, so what is left is taken to the end of the line.
	MtlDocument mapped = ParseMtl("newmtl a\nmap_Kd -s 1 1 1 -o 0 0 0 brick.png\n"
	                              "newmtl b\nmap_Kd my file.png\n");
	CHECK(mapped.materials.size() == 2);
	if (mapped.materials.size() == 2) {
		CHECK(mapped.materials[0].map_kd == "brick.png");
		CHECK(mapped.materials[1].map_kd == "my file.png");
	}

	// A colour directive with too few components states nothing.
	MtlDocument short_kd = ParseMtl("newmtl a\nKd 0.5 0.5\nNs\n");
	CHECK(short_kd.materials.size() == 1);
	if (short_kd.materials.size() == 1) {
		CHECK(!short_kd.materials[0].diffuse.has_value());
		CHECK(!short_kd.materials[0].shininess.has_value());
	}

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
	TestQuirks();
	TestMtl();
	if (failures == 0) {
		std::printf("obj_parser: all checks passed\n");
	}
	return failures == 0 ? 0 : 1;
}
