#include "cityjson/obj_parser.hpp"

#include "cityjson/error.hpp"

#include <cstdint>
#include <cstdlib>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace duckdb {
namespace cityjson {

namespace {

//! OBJ's own idea of intra-line whitespace, which is neither `isspace` nor locale-aware.
bool IsSpace(char c) {
	return c == ' ' || c == '\t';
}

std::string_view TrimBoth(std::string_view s) {
	size_t begin = 0;
	while (begin < s.size() && IsSpace(s[begin])) {
		begin++;
	}
	size_t end = s.size();
	while (end > begin && IsSpace(s[end - 1])) {
		end--;
	}
	return s.substr(begin, end - begin);
}

//! Reads up to `n` whitespace-separated reals from [begin, end) with strtod (correctly
//! rounded, unlike a digit-by-digit accumulation in double arithmetic), stopping at the
//! first token that is not a number or that would overrun `end`; writes them into `out`
//! and returns how many were read.
size_t ParseReals(const char *begin, const char *end, double *out, size_t n) {
	const char *p = begin;
	size_t count = 0;
	while (p < end && count < n) {
		while (p < end && IsSpace(*p)) {
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

//! A face's index field, with `atoi`'s tolerance: leading whitespace, an optional sign,
//! then digits until anything else. Nothing to parse -- an empty `v//vn` field, a
//! non-numeric token -- is 0, which OBJ has no vertex for and which the caller reports.
int ParseIndex(std::string_view field) {
	size_t i = 0;
	while (i < field.size() && IsSpace(field[i])) {
		i++;
	}
	bool negative = false;
	if (i < field.size() && (field[i] == '-' || field[i] == '+')) {
		negative = field[i] == '-';
		i++;
	}
	int64_t value = 0;
	for (; i < field.size() && field[i] >= '0' && field[i] <= '9'; i++) {
		value = value * 10 + (field[i] - '0');
		if (value > 2147483647) { // saturate rather than wrap; the caller rejects it anyway
			value = 2147483647;
		}
	}
	return static_cast<int>(negative ? -value : value);
}

//! A raw OBJ index resolved against a list of `count` items: 1-based, negative =
//! relative to the current end of the list, 0 = absent. Returns the 0-based index, -1
//! for absent, -2 for out of range.
int ResolveIndex(int raw, size_t count) {
	if (raw > 0) {
		return raw <= static_cast<int>(count) ? raw - 1 : -2;
	}
	if (raw < 0) {
		auto idx = static_cast<int64_t>(count) + raw;
		return idx >= 0 ? static_cast<int>(idx) : -2;
	}
	return -1;
}

//! The last whitespace-separated token of `s`, or "" when there is none. What a `g` line
//! names its group: `g a b` is group `b`, and a bare `g` with nothing after it clears it.
std::string_view LastToken(std::string_view s) {
	std::string_view last;
	size_t i = 0;
	while (i < s.size()) {
		while (i < s.size() && IsSpace(s[i])) {
			i++;
		}
		if (i >= s.size()) {
			break;
		}
		size_t j = i;
		while (j < s.size() && !IsSpace(s[j])) {
			j++;
		}
		last = s.substr(i, j - i);
		i = j;
	}
	return last;
}

//! Splits `s` on whitespace, in order.
std::vector<std::string_view> Tokenise(std::string_view s) {
	std::vector<std::string_view> tokens;
	size_t i = 0;
	while (i < s.size()) {
		while (i < s.size() && IsSpace(s[i])) {
			i++;
		}
		if (i >= s.size()) {
			break;
		}
		size_t j = i;
		while (j < s.size() && !IsSpace(s[j])) {
			j++;
		}
		tokens.push_back(s.substr(i, j - i));
		i = j;
	}
	return tokens;
}

//! Hands out one logical line at a time, with the number of the physical line it starts
//! on. `\r` is dropped, so a CRLF file reads like an LF one. With `join_continuations`,
//! a line whose last non-blank character is `\` continues on the next: the backslash
//! becomes a single space and the lines are joined before anything tokenises them.
class LineReader {
public:
	LineReader(std::string_view text, bool join_continuations) : text_(text), join_continuations_(join_continuations) {
	}

	bool Next(std::string_view &line, size_t &number) {
		if (pos_ >= text_.size()) {
			return false;
		}
		number = next_line_no_;
		joined_.clear();
		bool joined = false;
		while (true) {
			auto eol = text_.find('\n', pos_);
			size_t end = eol == std::string_view::npos ? text_.size() : eol;
			std::string_view physical = text_.substr(pos_, end - pos_);
			if (!physical.empty() && physical.back() == '\r') {
				physical.remove_suffix(1);
			}
			pos_ = eol == std::string_view::npos ? text_.size() : eol + 1;
			next_line_no_++;

			size_t last = physical.size();
			while (last > 0 && IsSpace(physical[last - 1])) {
				last--;
			}
			const bool continues = join_continuations_ && last > 0 && physical[last - 1] == '\\' && pos_ < text_.size();
			if (!continues) {
				if (joined) {
					joined_.append(physical);
					line = joined_;
				} else {
					line = physical;
				}
				return true;
			}
			joined_.append(physical.substr(0, last - 1));
			joined_.push_back(' ');
			joined = true;
		}
	}

private:
	std::string_view text_;
	bool join_continuations_;
	size_t pos_ = 0;
	size_t next_line_no_ = 1;
	//! Backing store for a joined line, alive until the next Next().
	std::string joined_;
};

//! True when `line` (leading whitespace already gone) opens with directive `name`
//! followed by whitespace -- the way OBJ and MTL keywords are recognised.
bool IsDirective(std::string_view line, std::string_view name) {
	return line.size() > name.size() && line.compare(0, name.size(), name) == 0 && IsSpace(line[name.size()]);
}

} // namespace

MtlDocument ParseMtl(std::string_view text) {
	MtlDocument doc;
	// The block a directive belongs to, or npos while none is open. A `.mtl` may open a
	// block before this parser has a material for it -- a nameless `newmtl` -- and
	// directives before the first `newmtl` belong to nothing.
	size_t current = std::string::npos;
	bool has_d = false;

	auto parse3 = [](std::string_view rest) -> std::optional<std::array<double, 3>> {
		double tmp[3];
		if (ParseReals(rest.data(), rest.data() + rest.size(), tmp, 3) < 3) {
			return std::nullopt;
		}
		return std::array<double, 3> {tmp[0], tmp[1], tmp[2]};
	};
	auto parse1 = [](std::string_view rest) -> std::optional<double> {
		double tmp = 0.0;
		if (ParseReals(rest.data(), rest.data() + rest.size(), &tmp, 1) < 1) {
			return std::nullopt;
		}
		return tmp;
	};

	// A `.mtl` is read line by line, never joined: a `map_Kd` naming a Windows path ends
	// in a backslash often enough that continuation there would cost more than it buys.
	LineReader reader(text, /*join_continuations=*/false);
	std::string_view line;
	size_t line_no = 0;
	while (reader.Next(line, line_no)) {
		line = TrimBoth(line);
		if (line.empty() || line[0] == '#') {
			continue;
		}
		size_t key_end = line.find_first_of(" \t");
		std::string_view key = line.substr(0, key_end);
		std::string_view rest;
		if (key_end != std::string_view::npos) {
			rest = TrimBoth(line.substr(key_end));
		}

		if (key == "newmtl") {
			// A nameless `newmtl` declares nothing and leaves the block it is in open.
			if (rest.empty()) {
				continue;
			}
			MtlMaterial material;
			material.name = std::string(rest);
			doc.materials.push_back(std::move(material));
			current = doc.materials.size() - 1;
			has_d = false;
			continue;
		}
		if (current == std::string::npos) {
			continue;
		}
		auto &material = doc.materials[current];
		if (key == "Ka") {
			material.ambient = parse3(rest);
		} else if (key == "Kd") {
			material.diffuse = parse3(rest);
		} else if (key == "Ks") {
			material.specular = parse3(rest);
		} else if (key == "Ke") {
			material.emission = parse3(rest);
		} else if (key == "Ns") {
			material.shininess = parse1(rest);
		} else if (key == "d") {
			auto dissolve = parse1(rest);
			if (dissolve.has_value()) {
				material.transparency = 1.0 - dissolve.value();
				has_d = true;
			}
		} else if (key == "Tr") {
			// Transparency as written, not 1 - (1 - Tr): the round trip through a dissolve
			// costs a rounding step ("0.3" would print as 0.30000000000000004). A `d` in
			// the same block wins, as MTL says it does.
			if (!has_d) {
				material.transparency = parse1(rest);
			}
		} else if (key == "map_Kd") {
			material.map_kd = std::string(rest);
		} else {
			material.other[std::string(key)] = std::string(rest);
		}
	}
	return doc;
}

ObjDocument ParseObj(std::string_view text, std::string_view default_object_name, const MtlLoader &load_mtl) {
	ObjDocument doc;
	// Name -> index into `doc.objects`, so a repeated `o` resumes its object in constant
	// time. A national tile runs to hundreds of thousands of `o` lines and a linear scan
	// per line makes the parse quadratic.
	std::unordered_map<std::string, size_t> object_index;
	// Material name -> ordinal. First declaration wins, so a duplicate `newmtl` name
	// cannot make a `usemtl` point at the later block.
	std::unordered_map<std::string, int> material_index;
	std::unordered_set<std::string> seen_mtllibs;
	std::string current_group;
	std::string current_usemtl;
	int current_material = -1;
	size_t current_object = 0;

	auto current = [&]() -> ObjObject & {
		if (doc.objects.empty()) {
			doc.objects.push_back(ObjObject {std::string(default_object_name), {}});
			object_index.emplace(std::string(default_object_name), 0);
			current_object = 0;
		}
		return doc.objects[current_object];
	};

	LineReader reader(text, /*join_continuations=*/true);
	std::string_view line;
	size_t line_no = 0;
	while (reader.Next(line, line_no)) {
		size_t start = 0;
		while (start < line.size() && IsSpace(line[start])) {
			start++;
		}
		std::string_view token = line.substr(start);
		if (token.empty() || token[0] == '#') {
			continue;
		}
		const char *end = token.data() + token.size();

		// vt: u [v] [w] -- only u and v are used; a missing v defaults to 0.
		if (token.size() >= 2 && token[0] == 'v' && token[1] == 't' && (token.size() == 2 || IsSpace(token[2]))) {
			double reals[2];
			size_t got = ParseReals(token.data() + 2, end, reals, 2);
			if (got == 0) {
				throw CityJSONError::Parse("line " + std::to_string(line_no) + " 'vt' has no u coordinate");
			}
			doc.texcoords.push_back({reals[0], got > 1 ? reals[1] : 0.0});
			continue;
		}
		// v: x y z [w], or the vertex-colour extension x y z r g b -- only the first
		// three (the position) are ever used.
		if (token[0] == 'v' && (token.size() == 1 || IsSpace(token[1]))) {
			double reals[3];
			if (ParseReals(token.data() + 1, end, reals, 3) < 3) {
				throw CityJSONError::Parse("line " + std::to_string(line_no) + " 'v' has fewer than three coordinates");
			}
			doc.vertices.push_back({reals[0], reals[1], reals[2]});
			continue;
		}
		if (token[0] == 'f' && token.size() > 1 && IsSpace(token[1])) {
			// The raw fields first, resolved only once the face is known to be a face:
			// a two-corner `f` is reported as such, not as a bad index.
			std::vector<std::pair<int, int>> raw; // vertex, texcoord as written
			for (auto field : Tokenise(token.substr(2))) {
				auto slash = field.find('/');
				int raw_v = ParseIndex(slash == std::string_view::npos ? field : field.substr(0, slash));
				int raw_vt = 0;
				if (slash != std::string_view::npos) {
					auto rest = field.substr(slash + 1);
					auto second = rest.find('/');
					raw_vt = ParseIndex(second == std::string_view::npos ? rest : rest.substr(0, second));
				}
				raw.emplace_back(raw_v, raw_vt);
			}
			if (raw.empty()) {
				continue; // an `f` with nothing after it declares no face
			}
			if (raw.size() < 3) {
				throw CityJSONError::InvalidGeometry("a face has fewer than three vertices");
			}
			ObjFace face;
			face.v.reserve(raw.size());
			face.vt.reserve(raw.size());
			for (auto [raw_v, raw_vt] : raw) {
				int v = ResolveIndex(raw_v, doc.vertices.size());
				if (v < 0) {
					throw CityJSONError::InvalidGeometry("face references vertex " + std::to_string(raw_v) + " but " +
					                                     std::to_string(doc.vertices.size()) +
					                                     " vertices have been declared");
				}
				int vt = ResolveIndex(raw_vt, doc.texcoords.size());
				if (vt == -2) {
					throw CityJSONError::InvalidGeometry("face references texture coordinate " +
					                                     std::to_string(raw_vt) + " but " +
					                                     std::to_string(doc.texcoords.size()) + " have been declared");
				}
				face.v.push_back(v);
				face.vt.push_back(vt);
			}
			face.usemtl = current_usemtl;
			face.group = current_group;
			face.material_index = current_material;
			current().faces.push_back(std::move(face));
			continue;
		}
		if (token[0] == 'g' && token.size() > 1 && IsSpace(token[1])) {
			current_group = std::string(LastToken(token.substr(1)));
			continue;
		}
		if (token[0] == 'o' && token.size() > 1 && IsSpace(token[1])) {
			std::string name(TrimBoth(token.substr(2)));
			// A repeated `o` resumes that object: cjio writes one `o <id>` per geometry,
			// so a multi-geometry object arrives as several blocks under one name.
			auto it = object_index.find(name);
			if (it != object_index.end()) {
				current_object = it->second;
				continue;
			}
			doc.objects.push_back(ObjObject {name, {}});
			current_object = doc.objects.size() - 1;
			object_index.emplace(std::move(name), current_object);
			continue;
		}
		if (IsDirective(token, "usemtl")) {
			current_usemtl = std::string(TrimBoth(token.substr(6)));
			auto it = material_index.find(current_usemtl);
			current_material = it == material_index.end() ? -1 : it->second;
			continue;
		}
		if (IsDirective(token, "mtllib")) {
			// Every file the line names, in order -- a `.mtl` that declares nothing does
			// not stop the next one from being read. A name already attempted is skipped,
			// so a file named twice contributes its materials once.
			for (auto name_view : Tokenise(token.substr(6))) {
				std::string name(name_view);
				if (!seen_mtllibs.insert(name).second) {
					continue;
				}
				doc.mtllibs.push_back(name);
				auto content = load_mtl(name);
				if (!content.has_value()) {
					doc.warnings.push_back("mtllib '" + name + "' could not be read");
					continue;
				}
				MtlDocument mtl = ParseMtl(content.value());
				if (mtl.materials.empty()) {
					doc.warnings.push_back("mtllib '" + name + "' declares no material");
					continue;
				}
				for (auto &material : mtl.materials) {
					material_index.emplace(material.name, static_cast<int>(doc.materials.materials.size()));
					doc.materials.materials.push_back(std::move(material));
				}
			}
			continue;
		}
		// vn, vp, s, l, mg, and anything else this reader has no use for.
	}
	return doc;
}

} // namespace cityjson
} // namespace duckdb
