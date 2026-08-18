// Scratch harness for ArrowNativeEncoder's compaction algorithm.
//
// This repo has no test/cpp/ -- its convention is SQL-level tests in test/sql/ --
// but the compaction rules (padding dimensions, distinct-source-index compaction)
// are worth pinning directly rather than only through the SQL round-trip, which
// cannot reach them until the vector writers and scan wiring land. The assertions
// here are mirrored into test/sql/ once that path exists.
//
// Build/run: see run_encoder_tests.sh next to this file.

#include "cityjson/arrow_native_encoder.hpp"

#include <cassert>
#include <cstdio>
#include <vector>

using duckdb::cityjson::ArrowNativeEncoder;
using duckdb::cityjson::CompactedGeometry;
using duckdb::cityjson::Geometry;
using json = nlohmann::json;

static int failures = 0;

#define CHECK(cond)                                                                                                    \
	do {                                                                                                               \
		if (!(cond)) {                                                                                                 \
			std::printf("  FAIL: %s (line %d)\n", #cond, __LINE__);                                                    \
			failures++;                                                                                                \
		}                                                                                                              \
	} while (0)

static Geometry MakeGeometry(const char *type, const char *boundaries) {
	Geometry geom;
	geom.type = type;
	geom.boundaries = json::parse(boundaries);
	return geom;
}

// A MultiSurface's two faces share source indices 0 and 2. Compaction is by
// distinct source index, so the pool holds each referenced index once.
static void TestMultiSurfaceSharedVertices() {
	std::printf("MultiSurface shares vertices between faces\n");
	std::vector<std::array<double, 3>> vertices = {{0, 0, 0}, {1, 0, 0}, {1, 1, 0}, {0, 1, 0}};
	auto geom = MakeGeometry("MultiSurface", "[[[0,1,2]],[[0,2,3]]]");

	auto compacted = ArrowNativeEncoder::Encode(geom, vertices, std::nullopt);

	CHECK(compacted.vertices.size() == 4);
	CHECK(compacted.solids.size() == 1);           // padding dimension
	CHECK(compacted.solids[0].shells.size() == 1); // padding dimension
	CHECK(compacted.solids[0].shells[0].faces.size() == 2);
	CHECK((compacted.solids[0].shells[0].faces[0].rings[0] == std::vector<uint32_t> {0, 1, 2}));
	CHECK((compacted.solids[0].shells[0].faces[1].rings[0] == std::vector<uint32_t> {0, 2, 3}));
}

// CityJSON permits two distinct vertex indices to carry identical coordinates,
// and they are topologically distinct data. Compaction must never merge them --
// that would need a float-equality policy that is defined nowhere, and would
// collapse legitimately separate vertices (design doc, round-2 review item 1).
static void TestNeverMergesEqualCoordinates() {
	std::printf("distinct indices with equal coordinates stay distinct\n");
	std::vector<std::array<double, 3>> vertices = {{0, 0, 0}, {0, 0, 0}, {1, 0, 0}};
	auto geom = MakeGeometry("MultiSurface", "[[[0,1,2]]]");

	auto compacted = ArrowNativeEncoder::Encode(geom, vertices, std::nullopt);

	CHECK(compacted.vertices.size() == 3); // NOT 2
	CHECK((compacted.solids[0].shells[0].faces[0].rings[0] == std::vector<uint32_t> {0, 1, 2}));
}

// For a Solid the shell dimension is padding too: every shell's faces -- the
// exterior shell first, then each cavity -- are flattened into ONE shell, exactly
// as the WKB path flattens them into one PolyhedralSurface, and the partition is
// recovered only from geometry_properties.shells.
//
// This is a cross-producer contract, not an internal detail. cityparquet-rs does
// the same (push_padded_solid, arrow_geom_write.rs) and duckdb-3d's
// ST_3DFromArrowNative rejects anything else when shells metadata is present --
// genuine nesting would count a cavity's faces twice.
static void TestSolidFlattensShellsIntoOnePaddedShell() {
	std::printf("Solid flattens its shells into one padded shell\n");
	std::vector<std::array<double, 3>> vertices = {{0, 0, 0}, {1, 0, 0}, {0, 1, 0}, {0, 0, 1}, {1, 0, 1}, {0, 1, 1}};
	auto geom = MakeGeometry("Solid", "[ [[[0,1,2]]], [[[3,4,5]]] ]");

	auto compacted = ArrowNativeEncoder::Encode(geom, vertices, std::nullopt);

	CHECK(compacted.vertices.size() == 6);
	CHECK(compacted.solids.size() == 1);
	CHECK(compacted.solids[0].shells.size() == 1); // padding dimension
	CHECK(compacted.solids[0].shells[0].faces.size() == 2);
	// Source order is preserved across the flattening: exterior face, then cavity.
	CHECK((compacted.solids[0].shells[0].faces[0].rings[0] == std::vector<uint32_t> {0, 1, 2}));
	CHECK((compacted.solids[0].shells[0].faces[1].rings[0] == std::vector<uint32_t> {3, 4, 5}));
}

// A face's rings beyond the first are interior rings (holes); they share the
// face's pool like any other reference.
static void TestFaceWithInteriorRing() {
	std::printf("face keeps interior rings\n");
	std::vector<std::array<double, 3>> vertices = {{0, 0, 0}, {4, 0, 0}, {4, 4, 0}, {0, 4, 0},
	                                               {1, 1, 0}, {2, 1, 0}, {2, 2, 0}};
	auto geom = MakeGeometry("MultiSurface", "[[[0,1,2,3],[4,5,6]]]");

	auto compacted = ArrowNativeEncoder::Encode(geom, vertices, std::nullopt);

	CHECK(compacted.vertices.size() == 7);
	CHECK(compacted.solids[0].shells[0].faces[0].rings.size() == 2);
	CHECK((compacted.solids[0].shells[0].faces[0].rings[1] == std::vector<uint32_t> {4, 5, 6}));
}

// MultiSolid/CompositeSolid use the outer dimension for real -- one entry per solid.
static void TestMultiSolid() {
	std::printf("MultiSolid uses the outer dimension\n");
	std::vector<std::array<double, 3>> vertices = {{0, 0, 0}, {1, 0, 0}, {0, 1, 0}, {0, 0, 1}, {1, 0, 1}, {0, 1, 1}};
	auto geom = MakeGeometry("MultiSolid", "[ [[[[0,1,2]]]], [[[[3,4,5]]]] ]");

	auto compacted = ArrowNativeEncoder::Encode(geom, vertices, std::nullopt);

	CHECK(compacted.solids.size() == 2);
	CHECK(compacted.solids[0].shells.size() == 1);
	CHECK(compacted.solids[1].shells.size() == 1);
	CHECK((compacted.solids[1].shells[0].faces[0].rings[0] == std::vector<uint32_t> {3, 4, 5}));
}

// Only the indices this object actually references reach the pool: a geometry
// referencing a slice of a big feature-scoped vertex list carries just that slice,
// which is the whole point of the row-local remap.
static void TestPoolHoldsOnlyReferencedVertices() {
	std::printf("pool holds only referenced vertices\n");
	std::vector<std::array<double, 3>> vertices = {{0, 0, 0}, {1, 0, 0}, {2, 0, 0}, {3, 0, 0},
	                                               {4, 0, 0}, {5, 0, 0}, {6, 0, 0}};
	auto geom = MakeGeometry("MultiSurface", "[[[5,6,1]]]");

	auto compacted = ArrowNativeEncoder::Encode(geom, vertices, std::nullopt);

	CHECK(compacted.vertices.size() == 3);
	CHECK((compacted.solids[0].shells[0].faces[0].rings[0] == std::vector<uint32_t> {0, 1, 2}));
	CHECK(compacted.vertices[0][0] == 5.0); // first-seen order, dereferenced
	CHECK(compacted.vertices[1][0] == 6.0);
	CHECK(compacted.vertices[2][0] == 1.0);
}

// An out-of-range index is not encodable data -- a writer producing one is a bug.
static void TestOutOfRangeIndexRejected() {
	std::printf("out-of-range index is rejected\n");
	std::vector<std::array<double, 3>> vertices = {{0, 0, 0}, {1, 0, 0}};
	auto geom = MakeGeometry("MultiSurface", "[[[0,1,7]]]");

	bool threw = false;
	try {
		ArrowNativeEncoder::Encode(geom, vertices, std::nullopt);
	} catch (const std::exception &) {
		threw = true;
	}
	CHECK(threw);
}

// Phase 1 covers the building-critical families only.
static void TestUnsupportedTypeRejected() {
	std::printf("out-of-scope geometry type is rejected\n");
	std::vector<std::array<double, 3>> vertices = {{0, 0, 0}};
	auto geom = MakeGeometry("MultiPoint", "[0]");

	bool threw = false;
	try {
		ArrowNativeEncoder::Encode(geom, vertices, std::nullopt);
	} catch (const std::exception &) {
		threw = true;
	}
	CHECK(threw);
}

// CityJSON rings are implicitly closed and should not repeat the first index, but
// real files do contain pre-closed rings. The WKB path already tolerates this (it
// only appends a closing point when front != back); the arrow-native contract is
// the opposite, so a repeated endpoint has to be dropped rather than passed
// through, or the row carries a duplicate vertex and a degenerate final edge.
static void TestPreClosedRingIsNormalised() {
	std::printf("pre-closed ring drops its repeated endpoint\n");
	std::vector<std::array<double, 3>> vertices = {{0, 0, 0}, {1, 0, 0}, {1, 1, 0}};
	auto geom = MakeGeometry("MultiSurface", "[[[0,1,2,0]]]");

	auto compacted = ArrowNativeEncoder::Encode(geom, vertices, std::nullopt);

	CHECK((compacted.solids[0].shells[0].faces[0].rings[0] == std::vector<uint32_t> {0, 1, 2}));
	CHECK(compacted.vertices.size() == 3);
}

// A ring that is genuinely a repeated single point is degenerate either way; the
// point is that normalisation must not empty a ring it should have left alone.
static void TestShortRingSurvivesNormalisation() {
	std::printf("short ring is not over-trimmed\n");
	std::vector<std::array<double, 3>> vertices = {{0, 0, 0}, {1, 0, 0}, {1, 1, 0}};
	auto geom = MakeGeometry("MultiSurface", "[[[0,1,2]]]");

	auto compacted = ArrowNativeEncoder::Encode(geom, vertices, std::nullopt);

	CHECK(compacted.solids[0].shells[0].faces[0].rings[0].size() == 3);
}

int main() {
	TestMultiSurfaceSharedVertices();
	TestNeverMergesEqualCoordinates();
	TestSolidFlattensShellsIntoOnePaddedShell();
	TestFaceWithInteriorRing();
	TestMultiSolid();
	TestPoolHoldsOnlyReferencedVertices();
	TestOutOfRangeIndexRejected();
	TestUnsupportedTypeRejected();
	TestPreClosedRingIsNormalised();
	TestShortRingSurvivesNormalisation();

	if (failures == 0) {
		std::printf("\nAll encoder assertions passed.\n");
		return 0;
	}
	std::printf("\n%d assertion(s) failed.\n", failures);
	return 1;
}
