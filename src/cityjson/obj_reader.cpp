#include "cityjson/obj_reader.hpp"

#include "cityjson/city_object_utils.hpp"
#include "cityjson/column_types.hpp"
#include "cityjson/crs_projjson.hpp"
#include "cityjson/error.hpp"
#include "cityjson/json_utils.hpp"
#include "cityjson/lod_table.hpp"

#include <tiny_obj_loader.h>

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <map>
#include <set>
#include <sstream>
#include <unordered_map>

namespace duckdb {
namespace cityjson {

namespace {

// ------------------------------------------------------------------
// Intermediate parse state, filled by the tinyobjloader callbacks
// ------------------------------------------------------------------

struct FaceRec {
	std::vector<int> v;  // 0-based global vertex indices
	std::vector<int> vt; // 0-based global texcoord indices, -1 = none
	int material = -1;   // index into ParseState::materials, -1 = none
	std::string usemtl;  // the usemtl token in force, "" = none
	std::string group;   // the last `g` name in force, "" = none
};

struct ObjectRec {
	std::string name;
	std::vector<FaceRec> faces;
};

struct ParseState {
	std::vector<std::array<double, 3>> vertices;
	std::vector<std::array<double, 2>> texcoords;
	std::vector<ObjectRec> objects;
	std::vector<tinyobj::material_t> materials;
	std::string current_group;
	std::string current_usemtl;
	int current_material = -1;
	std::string default_object_name;
	size_t current_object = 0;
	std::array<double, 3> origin {0.0, 0.0, 0.0};
	std::string error; // first structural error; LoadObjWithCallback has no way to abort

	// `v` / `vt` reals, parsed correctly-rounded ahead of tinyobjloader by the pre-scan
	// in OBJReader::Load (see ReparseMtlNumbersCorrectlyRounded for why); VertexCb and
	// TexcoordCb consume these in order instead of tinyobjloader's own parse.
	std::vector<std::array<double, 3>> precise_vertices;
	std::vector<std::array<double, 2>> precise_texcoords;
	size_t next_vertex = 0;
	size_t next_texcoord = 0;
	// Set when tinyobjloader's callback count disagrees with the pre-scan's -- a parser
	// disagreement distinct from `error` above, which is for structural face problems.
	std::string line_count_error;
};

ObjectRec &CurrentObject(ParseState &st) {
	if (st.objects.empty()) {
		st.objects.push_back(ObjectRec {st.default_object_name, {}});
		st.current_object = 0;
	}
	return st.objects[st.current_object];
}

void VertexCb(void *user, tinyobj::real_t /*x*/, tinyobj::real_t /*y*/, tinyobj::real_t /*z*/, tinyobj::real_t /*w*/) {
	auto &st = *static_cast<ParseState *>(user);
	if (st.next_vertex >= st.precise_vertices.size()) {
		st.line_count_error = "tinyobjloader reported a 'v' line the pre-scan did not see";
		return;
	}
	const auto &v = st.precise_vertices[st.next_vertex++];
	st.vertices.push_back({v[0] + st.origin[0], v[1] + st.origin[1], v[2] + st.origin[2]});
}

void TexcoordCb(void *user, tinyobj::real_t /*u*/, tinyobj::real_t /*v*/, tinyobj::real_t /*w*/) {
	auto &st = *static_cast<ParseState *>(user);
	if (st.next_texcoord >= st.precise_texcoords.size()) {
		st.line_count_error = "tinyobjloader reported a 'vt' line the pre-scan did not see";
		return;
	}
	st.texcoords.push_back(st.precise_texcoords[st.next_texcoord++]);
}

// tinyobjloader's callback API hands over the RAW face tokens: 1-based, negative =
// relative to the current end of the list, 0 = absent. Resolving them is ours.
int ResolveIndex(int raw, size_t count) {
	if (raw > 0) {
		return raw <= static_cast<int>(count) ? raw - 1 : -2;
	}
	if (raw < 0) {
		auto idx = static_cast<int64_t>(count) + raw;
		return idx >= 0 ? static_cast<int>(idx) : -2;
	}
	return -1; // absent
}

void IndexCb(void *user, tinyobj::index_t *indices, int num_indices) {
	auto &st = *static_cast<ParseState *>(user);
	if (!st.error.empty()) {
		return;
	}
	if (num_indices < 3) {
		st.error = "a face has fewer than three vertices";
		return;
	}
	FaceRec face;
	for (int i = 0; i < num_indices; i++) {
		int v = ResolveIndex(indices[i].vertex_index, st.vertices.size());
		if (v < 0) {
			st.error = "face references vertex " + std::to_string(indices[i].vertex_index) + " but " +
			           std::to_string(st.vertices.size()) + " vertices have been declared";
			return;
		}
		int vt = ResolveIndex(indices[i].texcoord_index, st.texcoords.size());
		if (vt == -2) {
			st.error = "face references texture coordinate " + std::to_string(indices[i].texcoord_index) + " but " +
			           std::to_string(st.texcoords.size()) + " have been declared";
			return;
		}
		face.v.push_back(v);
		face.vt.push_back(vt);
	}
	face.material = st.current_material;
	face.usemtl = st.current_usemtl;
	face.group = st.current_group;
	CurrentObject(st).faces.push_back(std::move(face));
}

void UsemtlCb(void *user, const char *name, int material_id) {
	auto &st = *static_cast<ParseState *>(user);
	st.current_usemtl = name != nullptr ? name : "";
	st.current_material = material_id;
}

void MtllibCb(void *user, const tinyobj::material_t *materials, int num_materials) {
	auto &st = *static_cast<ParseState *>(user);
	st.materials.assign(materials, materials + num_materials);
}

void GroupCb(void *user, const char **names, int num_names) {
	auto &st = *static_cast<ParseState *>(user);
	st.current_group = num_names > 0 ? names[num_names - 1] : "";
}

void ObjectCb(void *user, const char *name) {
	auto &st = *static_cast<ParseState *>(user);
	std::string n = name != nullptr ? name : "";
	// A repeated `o` resumes that object: cjio writes one `o <id>` per geometry, so a
	// multi-geometry object arrives as several blocks under one name.
	for (size_t i = 0; i < st.objects.size(); i++) {
		if (st.objects[i].name == n) {
			st.current_object = i;
			return;
		}
	}
	st.objects.push_back(ObjectRec {n, {}});
	st.current_object = st.objects.size() - 1;
}

// Reads up to `n` whitespace-separated reals from [begin, end) with strtod (correctly
// rounded), stopping at the first token that is not a number or that would overrun
// `end`; writes them into `out` and returns how many were read. Shared by the MTL
// directive re-parse below and the `v`/`vt` pre-scan in OBJReader::Load.
size_t ParseReals(const char *begin, const char *end, double *out, size_t n) {
	const char *p = begin;
	size_t count = 0;
	while (p < end && count < n) {
		while (p < end && (*p == ' ' || *p == '\t')) {
			p++;
		}
		if (p >= end) {
			break;
		}
		char *next = nullptr;
		double value = std::strtod(p, &next);
		if (next == p || next > end) {
			break;
		}
		out[count++] = value;
		p = next;
	}
	return count;
}

// tinyobjloader's own numeric parser (`tryParseDouble` in the vendored header) accumulates
// the decimal part digit by digit in double arithmetic rather than converting the decimal
// literal to binary correctly-rounded, so e.g. "0.3" comes back one ULP off. `real_t` is
// `double` in the vendored copy regardless of TINYOBJLOADER_USE_DOUBLE, so nothing narrows
// that error away downstream. Re-parse the numeric directives ourselves with `strtod`,
// which is correctly rounded, and overwrite what `tinyobj::LoadMtl` already stored --
// mirroring its own `d`-wins-over-`Tr` rule so the two parses never disagree on which
// value won, only on its last bit.
//
// Joins positionally, not by name: tinyobj's `parseString` truncates a `newmtl` name at
// the first space/tab, so a trailing space or extra token on the line would make a
// name-keyed lookup miss silently, and duplicate names (one file, or two `mtllib`s
// sharing the accumulated map) would make it write the wrong material. tinyobjloader
// pushes materials in file order and drops a `newmtl` block whose name is empty (the
// block is parsed but never flushed to `materials`), so the Nth non-empty `newmtl` in
// this content is `(*materials)[base + n]`. Returns how many such blocks it visited, so
// the caller can confirm that matches how many materials tinyobjloader actually added
// for this file -- if it does not, the join has gone out of step and the numbers this
// function "corrected" may belong to the wrong material.
size_t ReparseMtlNumbersCorrectlyRounded(const std::string &content, size_t base,
                                         std::vector<tinyobj::material_t> *materials) {
	auto starts_with_directive = [](const std::string &line, const char *key) {
		size_t len = std::strlen(key);
		return line.size() > len && line.compare(0, len, key) == 0 &&
		       std::isspace(static_cast<unsigned char>(line[len])) != 0;
	};
	auto parse3 = [](const std::string &line, size_t skip, tinyobj::real_t out[3]) {
		double tmp[3];
		size_t got = ParseReals(line.c_str() + skip, line.c_str() + line.size(), tmp, 3);
		for (size_t i = 0; i < got; i++) {
			out[i] = static_cast<tinyobj::real_t>(tmp[i]);
		}
	};
	auto parse1 = [](const std::string &line, size_t skip) {
		double tmp;
		ParseReals(line.c_str() + skip, line.c_str() + line.size(), &tmp, 1);
		return tmp;
	};

	std::istringstream in(content);
	std::string line;
	int current = -1; // index into *materials, or -1 while no block is open
	size_t visited = 0;
	bool has_d = false;
	while (std::getline(in, line)) {
		if (!line.empty() && line.back() == '\r') {
			line.pop_back();
		}
		// Trim trailing whitespace too, matching tinyobjloader's own line preprocessing:
		// otherwise a bare "newmtl" with nothing after it but spaces would recognise here
		// as a (dropped) empty-named block while tinyobjloader -- which trims first, so
		// its own `IS_SPACE(token[6])` check never sees the space -- does not see a
		// `newmtl` directive there at all and simply leaves the previous block open.
		size_t trailing = line.find_last_not_of(" \t");
		line = trailing == std::string::npos ? "" : line.substr(0, trailing + 1);
		size_t start = line.find_first_not_of(" \t");
		if (start == std::string::npos) {
			continue;
		}
		line = line.substr(start);

		if (line.compare(0, 6, "newmtl") == 0 &&
		    (line.size() == 6 || std::isspace(static_cast<unsigned char>(line[6])) != 0)) {
			std::string name = line.substr(6);
			size_t name_start = name.find_first_not_of(" \t");
			name = name_start == std::string::npos ? "" : name.substr(name_start);
			if (name.empty()) {
				// tinyobjloader parses this block but never flushes it to `materials` --
				// there is no index to write into.
				current = -1;
			} else {
				size_t idx = base + visited;
				current = idx < materials->size() ? static_cast<int>(idx) : -1;
				visited++;
			}
			has_d = false;
			continue;
		}
		if (current < 0) {
			continue;
		}
		auto &m = (*materials)[static_cast<size_t>(current)];
		if (starts_with_directive(line, "Ka")) {
			parse3(line, 2, m.ambient);
		} else if (starts_with_directive(line, "Kd")) {
			parse3(line, 2, m.diffuse);
		} else if (starts_with_directive(line, "Ks")) {
			parse3(line, 2, m.specular);
		} else if (starts_with_directive(line, "Ke")) {
			parse3(line, 2, m.emission);
		} else if (starts_with_directive(line, "Ns")) {
			m.shininess = static_cast<tinyobj::real_t>(parse1(line, 2));
		} else if (line.size() > 1 && line[0] == 'd' && std::isspace(static_cast<unsigned char>(line[1])) != 0) {
			m.dissolve = static_cast<tinyobj::real_t>(parse1(line, 1));
			has_d = true;
		} else if (starts_with_directive(line, "Tr") && !has_d) {
			m.dissolve = static_cast<tinyobj::real_t>(1.0 - parse1(line, 2));
		}
	}
	return visited;
}

// `.mtl` files named by `mtllib`, resolved against the OBJ's directory and read through
// DuckDB's FileSystem -- tinyobjloader's own MaterialFileReader would std::ifstream them
// and lose every remote path.
class DuckDBMaterialReader : public tinyobj::MaterialReader {
public:
	DuckDBMaterialReader(ClientContext &context, std::string base_dir)
	    : context_(context), base_dir_(std::move(base_dir)) {
	}
	bool operator()(const std::string &mat_id, std::vector<tinyobj::material_t> *materials,
	                std::map<std::string, int> *mat_map, std::string *warn, std::string *err) override {
		std::string path = base_dir_.empty() ? mat_id : base_dir_ + "/" + mat_id;
		std::string content;
		try {
			content = json_utils::ReadFileContent(context_, path);
		} catch (const CityJSONError &e) {
			if (warn != nullptr) {
				*warn += "mtllib '" + mat_id + "' could not be read: " + e.what() + "\n";
			}
			return false;
		}
		std::istringstream in(content);
		size_t base = materials->size();
		tinyobj::LoadMtl(mat_map, materials, &in, warn, err);
		size_t added = materials->size() - base;
		size_t visited = ReparseMtlNumbersCorrectlyRounded(content, base, materials);
		if (visited != added && err != nullptr) {
			*err += "mtllib '" + mat_id + "': the correctly-rounded re-parse found " + std::to_string(visited) +
			        " named material(s) but tinyobjloader produced " + std::to_string(added) +
			        "; a 'newmtl' name or duplicate has thrown the positional join out of step\n";
		}
		return true;
	}

private:
	ClientContext &context_;
	std::string base_dir_;
};

std::string FileStem(const std::string &path) {
	auto base = path.substr(OBJReader::DirectoryOf(path).empty() ? 0 : OBJReader::DirectoryOf(path).size() + 1);
	auto dot = base.rfind('.');
	return dot == std::string::npos ? base : base.substr(0, dot);
}

std::string UpperExtension(const std::string &path) {
	auto dot = path.rfind('.');
	if (dot == std::string::npos) {
		return "";
	}
	std::string ext = path.substr(dot + 1);
	std::transform(ext.begin(), ext.end(), ext.begin(), [](unsigned char c) { return std::toupper(c); });
	return ext == "JPEG" ? "JPG" : ext;
}

// ------------------------------------------------------------------
// From parse state to CityJSON records
// ------------------------------------------------------------------

//! Closed iff every undirected edge is used by exactly two faces.
bool IsClosed(const ObjectRec &obj) {
	if (obj.faces.size() < 4) {
		return false;
	}
	std::map<std::pair<int, int>, int> edges;
	for (const auto &face : obj.faces) {
		size_t n = face.v.size();
		for (size_t i = 0; i < n; i++) {
			int a = face.v[i];
			int b = face.v[(i + 1) % n];
			edges[{std::min(a, b), std::max(a, b)}]++;
		}
	}
	return std::all_of(edges.begin(), edges.end(), [](const auto &kv) { return kv.second == 2; });
}

//! Surface type a face carries: its usemtl name when that is a surface type, else its
//! group name when that is, else none.
std::optional<std::string> SurfaceOf(const FaceRec &face) {
	if (OBJReader::IsSemanticSurfaceType(face.usemtl)) {
		return face.usemtl;
	}
	if (OBJReader::IsSemanticSurfaceType(face.group)) {
		return face.group;
	}
	return std::nullopt;
}

} // namespace

// ------------------------------------------------------------------
// OBJReader
// ------------------------------------------------------------------

struct OBJReader::Parsed {
	CityJSON header;
	std::vector<CityJSONFeature> features;
};

OBJReader::OBJReader(ClientContext &context, std::string file_path, OBJReadOptions options)
    : context_(context), file_path_(std::move(file_path)), options_(std::move(options)) {
}

OBJReader::~OBJReader() = default;

std::string OBJReader::Name() const {
	return file_path_;
}

std::string OBJReader::DirectoryOf(const std::string &path) {
	auto slash = path.find_last_of("/\\");
	return slash == std::string::npos ? std::string() : path.substr(0, slash);
}

bool OBJReader::IsSemanticSurfaceType(const std::string &name) {
	static const std::set<std::string> kTypes = {"RoofSurface",
	                                             "WallSurface",
	                                             "GroundSurface",
	                                             "ClosureSurface",
	                                             "OuterCeilingSurface",
	                                             "OuterFloorSurface",
	                                             "Window",
	                                             "Door",
	                                             "InteriorWallSurface",
	                                             "CeilingSurface",
	                                             "FloorSurface",
	                                             "WaterSurface",
	                                             "WaterGroundSurface",
	                                             "TrafficArea",
	                                             "AuxiliaryTrafficArea",
	                                             "TransportationMarking",
	                                             "TransportationHole"};
	if (name.empty()) {
		return false;
	}
	return name[0] == '+' || kTypes.count(name) > 0;
}

std::optional<std::array<double, 3>> OBJReader::ParseOriginComment(const std::string &content) {
	size_t pos = 0;
	while (pos < content.size()) {
		auto eol = content.find('\n', pos);
		auto line = content.substr(pos, eol == std::string::npos ? std::string::npos : eol - pos);
		pos = eol == std::string::npos ? content.size() : eol + 1;
		if (line.rfind("# origin ", 0) != 0) {
			if (!line.empty() && line[0] != '#') {
				return std::nullopt; // the header block is over
			}
			continue;
		}
		std::istringstream in(line.substr(9));
		std::array<double, 3> o {};
		if (in >> o[0] >> o[1] >> o[2]) {
			return o;
		}
		return std::nullopt;
	}
	return std::nullopt;
}

const OBJReader::Parsed &OBJReader::Load() const {
	if (parsed_) {
		return *parsed_;
	}
	std::string content = json_utils::ReadFileContent(context_, file_path_);

	ParseState st;
	st.default_object_name = FileStem(file_path_);

	// tinyobjloader's own number parser (see ReparseMtlNumbersCorrectlyRounded above)
	// accumulates digits in double arithmetic and is not correctly rounded, so every
	// real in the file is parsed here, in this single line-by-line pre-scan, with
	// strtod, and kept as the source of truth for `v` / `vt`; tinyobjloader is kept only
	// for structure -- o/g/usemtl/mtllib dispatch and face tokenising. This pass also
	// still catches backslash line continuation, which the parser library does not
	// implement; refuse rather than silently mis-parse the joined line.
	{
		size_t pos = 0;
		size_t line_no = 1;
		while (pos < content.size()) {
			auto eol = content.find('\n', pos);
			auto end = eol == std::string::npos ? content.size() : eol;
			auto last = end;
			while (last > pos && (content[last - 1] == '\r' || content[last - 1] == ' ' || content[last - 1] == '\t')) {
				last--;
			}
			if (last > pos && content[last - 1] == '\\') {
				throw CityJSONError::Parse("line " + std::to_string(line_no) +
				                               " ends with a backslash; line continuation is not supported",
				                           file_path_);
			}

			size_t line_start = pos;
			while (line_start < last && (content[line_start] == ' ' || content[line_start] == '\t')) {
				line_start++;
			}
			if (last - line_start >= 2 && content[line_start] == 'v' && content[line_start + 1] == 't' &&
			    (last - line_start == 2 || std::isspace(static_cast<unsigned char>(content[line_start + 2])) != 0)) {
				// vt: u [v] [w] -- only u and v are used; a missing v defaults to 0.
				double reals[2];
				size_t got = ParseReals(content.data() + line_start + 2, content.data() + last, reals, 2);
				if (got == 0) {
					throw CityJSONError::Parse("line " + std::to_string(line_no) + " 'vt' has no u coordinate",
					                           file_path_);
				}
				st.precise_texcoords.push_back({reals[0], got > 1 ? reals[1] : 0.0});
			} else if (last - line_start >= 1 && content[line_start] == 'v' &&
			           (last - line_start == 1 ||
			            std::isspace(static_cast<unsigned char>(content[line_start + 1])) != 0)) {
				// v: x y z [w] or the vertex-color extension x y z r g b -- only the first
				// three (the position) are ever used.
				double reals[3];
				size_t got = ParseReals(content.data() + line_start + 1, content.data() + last, reals, 3);
				if (got < 3) {
					throw CityJSONError::Parse(
					    "line " + std::to_string(line_no) + " 'v' has fewer than three coordinates", file_path_);
				}
				st.precise_vertices.push_back({reals[0], reals[1], reals[2]});
			}

			pos = eol == std::string::npos ? content.size() : eol + 1;
			line_no++;
		}
	}

	if (auto origin = ParseOriginComment(content)) {
		st.origin = origin.value();
	}

	tinyobj::callback_t cb;
	cb.vertex_cb = VertexCb;
	cb.texcoord_cb = TexcoordCb;
	cb.index_cb = IndexCb;
	cb.usemtl_cb = UsemtlCb;
	cb.mtllib_cb = MtllibCb;
	cb.group_cb = GroupCb;
	cb.object_cb = ObjectCb;

	DuckDBMaterialReader mat_reader(context_, DirectoryOf(file_path_));
	std::istringstream in(content);
	std::string warn;
	std::string err;
	bool ok = tinyobj::LoadObjWithCallback(in, cb, &st, &mat_reader, &warn, &err);
	if (!ok || !err.empty()) {
		throw CityJSONError::Parse(err.empty() ? "tinyobjloader failed" : err, file_path_);
	}
	// Checked before `st.error`: a `v`/`vt` count disagreement between the pre-scan and
	// tinyobjloader's own callbacks can itself produce a short `st.vertices` and a
	// downstream face-index error there, and the parser-disagreement message is the one
	// the user needs to see, not the symptom it causes.
	if (!st.line_count_error.empty()) {
		throw CityJSONError::Parse(st.line_count_error, file_path_);
	}
	if (st.next_vertex != st.precise_vertices.size()) {
		throw CityJSONError::Parse("pre-scan found " + std::to_string(st.precise_vertices.size()) +
		                               " 'v' line(s) but tinyobjloader reported " + std::to_string(st.next_vertex),
		                           file_path_);
	}
	if (st.next_texcoord != st.precise_texcoords.size()) {
		throw CityJSONError::Parse("pre-scan found " + std::to_string(st.precise_texcoords.size()) +
		                               " 'vt' line(s) but tinyobjloader reported " + std::to_string(st.next_texcoord),
		                           file_path_);
	}
	if (!st.error.empty()) {
		throw CityJSONError::InvalidGeometry(st.error, file_path_);
	}

	auto parsed = std::make_shared<Parsed>();

	// --- Appearance definitions: the MTL materials, and a texture per textured material.
	Appearance appearance;
	std::vector<int> texture_of_material(st.materials.size(), -1);
	for (size_t i = 0; i < st.materials.size(); i++) {
		const auto &m = st.materials[i];
		Material mat;
		mat.name = m.name;
		mat.diffuse_color = std::vector<double> {m.diffuse[0], m.diffuse[1], m.diffuse[2]};
		mat.specular_color = std::vector<double> {m.specular[0], m.specular[1], m.specular[2]};
		mat.emissive_color = std::vector<double> {m.emission[0], m.emission[1], m.emission[2]};
		mat.ambient_intensity = (m.ambient[0] + m.ambient[1] + m.ambient[2]) / 3.0;
		mat.transparency = 1.0 - m.dissolve;
		mat.shininess = std::min(1.0, std::max(0.0, static_cast<double>(m.shininess) / 1000.0));
		if (!m.unknown_parameter.empty()) {
			mat.other = json::object();
			for (const auto &kv : m.unknown_parameter) {
				mat.other[kv.first] = kv.second;
			}
		}
		appearance.materials.push_back(std::move(mat));
		if (!m.diffuse_texname.empty()) {
			Texture tex;
			tex.image_uri = m.diffuse_texname;
			tex.image_type = UpperExtension(m.diffuse_texname);
			tex.wrap_mode = "wrap";
			tex.texture_type = "unknown";
			texture_of_material[i] = static_cast<int>(appearance.textures.size());
			appearance.textures.push_back(std::move(tex));
		}
	}
	appearance.vertices_texture = st.texcoords;
	if (!appearance.Empty()) {
		parsed->header.appearance = appearance;
	}

	// --- Extent over every vertex, for obj_metadata.
	if (!st.vertices.empty()) {
		GeographicalExtent extent(st.vertices[0][0], st.vertices[0][1], st.vertices[0][2], st.vertices[0][0],
		                          st.vertices[0][1], st.vertices[0][2]);
		for (const auto &v : st.vertices) {
			extent = extent.Union(GeographicalExtent(v[0], v[1], v[2], v[0], v[1], v[2]));
		}
		Metadata md;
		md.geographical_extent = extent;
		parsed->header.metadata = md;
	}
	if (options_.crs.has_value()) {
		if (!parsed->header.metadata.has_value()) {
			parsed->header.metadata = Metadata();
		}
		parsed->header.metadata->reference_system = options_.crs.value();
	}

	// --- One feature per object.
	const bool force_solid = options_.geometry_type == "Solid";
	const bool force_multi = options_.geometry_type == "MultiSurface";
	for (const auto &obj : st.objects) {
		if (obj.faces.empty()) {
			continue;
		}
		CityJSONFeature feature(obj.name);
		std::unordered_map<int, int> local_of_global;
		auto local_index = [&](int g) {
			auto it = local_of_global.find(g);
			if (it != local_of_global.end()) {
				return it->second;
			}
			int idx = static_cast<int>(feature.vertices.size());
			feature.vertices.push_back(st.vertices[g]);
			local_of_global.emplace(g, idx);
			return idx;
		};

		const bool solid = force_solid || (!force_multi && IsClosed(obj));

		json faces = json::array();      // face -> [ring]
		json sem_values = json::array(); // face -> surface index | null
		json mat_values = json::array(); // face -> material id | null
		json tex_values = json::array(); // face -> [ [texId, uv...] ]
		std::vector<std::string> surfaces;
		bool any_surface = false;
		bool any_material = false;
		bool any_texture = false;

		for (const auto &face : obj.faces) {
			json ring = json::array();
			for (int g : face.v) {
				ring.push_back(local_index(g));
			}
			faces.push_back(json::array({ring}));

			if (auto surface = SurfaceOf(face)) {
				auto it = std::find(surfaces.begin(), surfaces.end(), surface.value());
				if (it == surfaces.end()) {
					surfaces.push_back(surface.value());
					it = surfaces.end() - 1;
				}
				sem_values.push_back(static_cast<int>(it - surfaces.begin()));
				any_surface = true;
			} else {
				sem_values.push_back(nullptr);
			}

			if (face.material >= 0) {
				mat_values.push_back(face.material);
				any_material = true;
			} else {
				mat_values.push_back(nullptr);
			}

			int tex = face.material >= 0 ? texture_of_material[face.material] : -1;
			bool has_uv = std::all_of(face.vt.begin(), face.vt.end(), [](int i) { return i >= 0; });
			if (tex >= 0 && has_uv) {
				json tring = json::array({tex});
				for (int vt : face.vt) {
					tring.push_back(vt);
				}
				tex_values.push_back(json::array({tring}));
				any_texture = true;
			} else {
				tex_values.push_back(json::array({json::array({nullptr})}));
			}
		}

		Geometry geom;
		geom.lod = options_.lod;
		if (solid) {
			geom.type = "Solid";
			geom.boundaries = json::array({faces});
		} else {
			geom.type = "MultiSurface";
			geom.boundaries = faces;
		}
		auto nest = [&](json values) {
			return solid ? json::array({std::move(values)}) : values;
		};
		if (any_surface) {
			json surfs = json::array();
			for (const auto &s : surfaces) {
				surfs.push_back(json {{"type", s}});
			}
			geom.semantics = json {{"surfaces", surfs}, {"values", nest(sem_values)}};
		}
		if (any_material) {
			geom.material = json {{"visual", json {{"values", nest(mat_values)}}}};
		}
		if (any_texture) {
			geom.texture = json {{"visual", json {{"values", nest(tex_values)}}}};
		}

		CityObject city_object(options_.object_type);
		city_object.feature_id = obj.name;
		city_object.geometry.push_back(std::move(geom));
		feature.city_objects[obj.name] = std::move(city_object);
		parsed->features.push_back(std::move(feature));
	}

	parsed_ = parsed;
	return *parsed_;
}

CityJSON OBJReader::ReadMetadata() const {
	return Load().header;
}

CityJSONFeatureChunk OBJReader::ReadAllChunks() const {
	auto features = Load().features;
	return CityJSONFeatureChunk::CreateChunks(std::move(features), STANDARD_VECTOR_SIZE);
}

std::vector<CityJSONFeature> OBJReader::ReadNFeatures(size_t n) const {
	const auto &features = Load().features;
	auto count = static_cast<std::ptrdiff_t>(std::min(n, features.size()));
	return std::vector<CityJSONFeature>(features.begin(), features.begin() + count);
}

size_t OBJReader::CountCityObjects() const {
	return Load().features.size();
}

size_t OBJReader::CountFeatures() const {
	return Load().features.size();
}

std::vector<Column> OBJReader::Columns() const {
	if (cached_columns_.has_value()) {
		return cached_columns_.value();
	}
	// Same order as LocalCityJSONReader::Columns: head, bbox + geometry group, trailing.
	// No attribute columns: an OBJ carries none.
	std::vector<Column> columns = GetDefinedColumns();
	const auto &features = Load().features;
	auto geom_columns = CityObjectUtils::InferGeometryColumns(features, features.size());
	columns.insert(columns.end(), geom_columns.begin(), geom_columns.end());
	auto trailing = LODTableUtils::GetTrailingColumns();
	columns.insert(columns.end(), trailing.begin(), trailing.end());
	cached_columns_ = columns;
	return columns;
}

} // namespace cityjson
} // namespace duckdb
