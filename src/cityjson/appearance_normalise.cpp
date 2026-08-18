#include "cityjson/appearance_normalise.hpp"

#include <functional>
#include <sstream>

namespace duckdb {
namespace cityjson {

namespace {

void AppendOptional(std::ostringstream &out, const std::optional<std::string> &value) {
	out << (value.has_value() ? value.value() : std::string("\x1f")) << '\x1e';
}

void AppendOptional(std::ostringstream &out, const std::optional<double> &value) {
	if (value.has_value()) {
		out << value.value();
	}
	out << '\x1e';
}

void AppendOptional(std::ostringstream &out, const std::optional<bool> &value) {
	if (value.has_value()) {
		out << (value.value() ? '1' : '0');
	}
	out << '\x1e';
}

void AppendOptional(std::ostringstream &out, const std::optional<std::vector<double>> &values) {
	if (values.has_value()) {
		for (const auto value : values.value()) {
			out << value << ',';
		}
	}
	out << '\x1e';
}

//! Intern `key`, returning its existing id or assigning the next one.
template <typename T>
int64_t Intern(const std::string &key, const T &definition, std::map<std::string, int64_t> &seen, std::vector<T> &out) {
	auto existing = seen.find(key);
	if (existing != seen.end()) {
		return existing->second;
	}
	const auto id = static_cast<int64_t>(out.size());
	out.push_back(definition);
	seen[key] = id;
	return id;
}

} // namespace

std::string MaterialKey(const Material &material) {
	// Field separators are ASCII record/unit separators, so a value containing a comma
	// or a name containing a separator cannot forge a collision between two distinct
	// definitions.
	std::ostringstream out;
	AppendOptional(out, material.name);
	AppendOptional(out, material.ambient_intensity);
	AppendOptional(out, material.diffuse_color);
	AppendOptional(out, material.specular_color);
	AppendOptional(out, material.emissive_color);
	AppendOptional(out, material.transparency);
	AppendOptional(out, material.shininess);
	AppendOptional(out, material.is_smooth);
	out << (material.other.is_null() ? std::string() : material.other.dump());
	return out.str();
}

std::string TextureKey(const Texture &texture) {
	std::ostringstream out;
	AppendOptional(out, texture.image_uri);
	AppendOptional(out, texture.image_type);
	AppendOptional(out, texture.wrap_mode);
	AppendOptional(out, texture.texture_type);
	AppendOptional(out, texture.border_color);
	out << (texture.other.is_null() ? std::string() : texture.other.dump());
	return out.str();
}

int64_t AppearanceIndex::ResolveMaterial(const std::string &feature_id, int64_t local_index) const {
	auto entry = material_map.find(feature_id);
	if (entry == material_map.end()) {
		// No definitions of its own: the local index is already the global id. This is
		// the plain-CityJSON case, where there is only one appearance object.
		return local_index >= 0 && local_index < static_cast<int64_t>(materials.size()) ? local_index : -1;
	}
	if (local_index < 0 || local_index >= static_cast<int64_t>(entry->second.size())) {
		return -1;
	}
	return entry->second[static_cast<size_t>(local_index)];
}

int64_t AppearanceIndex::ResolveTexture(const std::string &feature_id, int64_t local_index) const {
	auto entry = texture_map.find(feature_id);
	if (entry == texture_map.end()) {
		return local_index >= 0 && local_index < static_cast<int64_t>(textures.size()) ? local_index : -1;
	}
	if (local_index < 0 || local_index >= static_cast<int64_t>(entry->second.size())) {
		return -1;
	}
	return entry->second[static_cast<size_t>(local_index)];
}

AppearanceIndex AppearanceIndex::Build(const CityJSON &header, const std::vector<CityJSONFeature> &features) {
	AppearanceIndex index;
	std::map<std::string, int64_t> material_ids;
	std::map<std::string, int64_t> texture_ids;

	// Header first, so its entries keep their ordinal positions as ids. A plain CityJSON
	// document has only this, so its ids are exactly the source array positions.
	if (header.appearance.has_value()) {
		for (const auto &material : header.appearance->materials) {
			Intern(MaterialKey(material), material, material_ids, index.materials);
		}
		for (const auto &texture : header.appearance->textures) {
			Intern(TextureKey(texture), texture, texture_ids, index.textures);
		}
	}

	// Then each feature's own definitions, in feature order. A definition that repeats
	// one already seen resolves to the same id rather than being duplicated.
	for (const auto &feature : features) {
		if (!feature.appearance.has_value()) {
			continue;
		}
		const auto &appearance = feature.appearance.value();
		if (!appearance.materials.empty()) {
			auto &mapping = index.material_map[feature.id];
			for (const auto &material : appearance.materials) {
				mapping.push_back(Intern(MaterialKey(material), material, material_ids, index.materials));
			}
		}
		if (!appearance.textures.empty()) {
			auto &mapping = index.texture_map[feature.id];
			for (const auto &texture : appearance.textures) {
				mapping.push_back(Intern(TextureKey(texture), texture, texture_ids, index.textures));
			}
		}
	}

	return index;
}

namespace {

//! Map every integer leaf through `remap`, preserving the nesting. Material values nest
//! per shell/surface depending on geometry type, so recursion beats a fixed depth.
json RemapMaterialValues(const json &node, const std::function<int64_t(int64_t)> &remap) {
	if (node.is_number_integer()) {
		const auto mapped = remap(node.get<int64_t>());
		return mapped < 0 ? json(nullptr) : json(mapped);
	}
	if (!node.is_array()) {
		return node;
	}
	json out = json::array();
	for (const auto &child : node) {
		out.push_back(RemapMaterialValues(child, remap));
	}
	return out;
}

//! `node` is a ring when its elements are scalars: [texId, uvIdx, uvIdx, ...].
bool IsRing(const json &node) {
	return node.is_array() && !node.empty() && !node[0].is_array();
}

json RemapTextureValues(const json &node, const std::function<int64_t(int64_t)> &remap,
                        const std::vector<std::array<double, 2>> &uv_pool) {
	if (!node.is_array()) {
		return node;
	}
	if (!IsRing(node)) {
		json out = json::array();
		for (const auto &child : node) {
			out.push_back(RemapTextureValues(child, remap, uv_pool));
		}
		return out;
	}

	// A ring with no texture is a single null and stays that way.
	if (node[0].is_null()) {
		return node;
	}

	json ring = json::array();
	const auto mapped = node[0].is_number_integer() ? remap(node[0].get<int64_t>()) : -1;
	ring.push_back(mapped < 0 ? json(nullptr) : json(mapped));
	for (size_t i = 1; i < node.size(); i++) {
		if (!node[i].is_number_integer()) {
			ring.push_back(node[i]);
			continue;
		}
		const auto uv_index = node[i].get<int64_t>();
		if (uv_index < 0 || uv_index >= static_cast<int64_t>(uv_pool.size())) {
			// A UV index outside the pool cannot be inlined. Emitting null keeps the
			// ring's arity intact rather than silently shortening it.
			ring.push_back(json(nullptr));
			continue;
		}
		const auto &uv = uv_pool[static_cast<size_t>(uv_index)];
		ring.push_back(json::array({uv[0], uv[1]}));
	}
	return ring;
}

//! Apply `rewrite` to each theme's `values` / `value` member, leaving the theme keys
//! (a dynamic, open key set) untouched.
json RewriteThemes(const json &map, const std::function<json(const json &)> &rewrite_values,
                   const std::function<json(const json &)> &rewrite_value) {
	if (!map.is_object()) {
		return map;
	}
	json out = json::object();
	for (const auto &entry : map.items()) {
		if (!entry.value().is_object()) {
			out[entry.key()] = entry.value();
			continue;
		}
		json theme = entry.value();
		auto values = theme.find("values");
		if (values != theme.end()) {
			theme["values"] = rewrite_values(*values);
		}
		auto value = theme.find("value");
		if (value != theme.end() && rewrite_value) {
			theme["value"] = rewrite_value(*value);
		}
		out[entry.key()] = std::move(theme);
	}
	return out;
}

} // namespace

json NormaliseMaterialMap(const json &material_map, const AppearanceIndex &index, const std::string &feature_id) {
	auto remap = [&](int64_t local) {
		return index.ResolveMaterial(feature_id, local);
	};
	return RewriteThemes(
	    material_map, [&](const json &values) { return RemapMaterialValues(values, remap); },
	    [&](const json &value) { return RemapMaterialValues(value, remap); });
}

json NormaliseTextureMap(const json &texture_map, const AppearanceIndex &index, const std::string &feature_id,
                         const std::vector<std::array<double, 2>> &uv_pool) {
	auto remap = [&](int64_t local) {
		return index.ResolveTexture(feature_id, local);
	};
	return RewriteThemes(
	    texture_map, [&](const json &values) { return RemapTextureValues(values, remap, uv_pool); }, nullptr);
}

} // namespace cityjson
} // namespace duckdb
