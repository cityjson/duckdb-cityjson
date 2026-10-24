#include "cityjson/appearance_source.hpp"

#include "cityjson/cityjson_types.hpp"
#include "cityjson/error.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/client_context.hpp"

#include <algorithm>
#include <cctype>

namespace duckdb {
namespace cityjson {

namespace {

std::optional<std::array<double, 3>> Triple(const std::optional<std::vector<double>> &v) {
	if (!v.has_value() || v->size() < 3) {
		return std::nullopt;
	}
	return std::array<double, 3> {(*v)[0], (*v)[1], (*v)[2]};
}

MeshMaterial FromMaterial(const Material &m, int64_t id) {
	MeshMaterial out;
	out.name = m.name.value_or("material_" + std::to_string(id));
	if (auto d = Triple(m.diffuse_color)) {
		out.diffuse = *d;
	}
	out.specular = Triple(m.specular_color);
	out.emissive = Triple(m.emissive_color);
	out.transparency = m.transparency.value_or(0.0);
	out.shininess = m.shininess;
	return out;
}

MeshTexture FromTexture(const Texture &t) {
	MeshTexture out;
	out.image_uri = t.image_uri.value_or("");
	out.image_type = t.image_type.value_or("");
	out.wrap_mode = t.wrap_mode.value_or("wrap");
	return out;
}

std::optional<std::array<double, 3>> TripleFromList(const Value &v) {
	if (v.IsNull() || v.type().id() != LogicalTypeId::LIST) {
		return std::nullopt;
	}
	auto &children = ListValue::GetChildren(v);
	if (children.size() < 3) {
		return std::nullopt;
	}
	// A NULL component is reachable from a user-typed *_query (e.g. [0.5, NULL, 0.2]);
	// Value::GetValue on a NULL throws InternalException, which DuckDB treats as
	// database-invalidating, so refuse rather than let one through.
	if (children[0].IsNull() || children[1].IsNull() || children[2].IsNull()) {
		return std::nullopt;
	}
	return std::array<double, 3> {children[0].GetValue<double>(), children[1].GetValue<double>(),
	                              children[2].GetValue<double>()};
}

} // namespace

AppearanceSource AppearanceSource::FromLocal(const std::optional<json> &header,
                                             const std::map<std::string, json> &by_feature) {
	AppearanceSource src;
	src.sidecar_ = false;
	CityJSON doc;
	if (header.has_value()) {
		doc.appearance = Appearance::FromJson(header.value());
	}
	std::vector<CityJSONFeature> features;
	for (const auto &kv : by_feature) {
		CityJSONFeature f(kv.first);
		f.appearance = Appearance::FromJson(kv.second);
		features.push_back(std::move(f));
	}
	src.index_ = AppearanceIndex::Build(doc, features);
	for (size_t i = 0; i < src.index_.materials.size(); i++) {
		src.materials_[static_cast<int64_t>(i)] = FromMaterial(src.index_.materials[i], static_cast<int64_t>(i));
	}
	for (size_t i = 0; i < src.index_.textures.size(); i++) {
		src.textures_[static_cast<int64_t>(i)] = FromTexture(src.index_.textures[i]);
	}
	return src;
}

AppearanceSource AppearanceSource::FromQueries(ClientContext &context,
                                               const std::optional<std::string> &materials_query,
                                               const std::optional<std::string> &textures_query) {
	// At least one of materials_query / textures_query is expected to be set -- the
	// caller (Task 6's bind) only reaches FromQueries when one of the *_query COPY
	// options is present. Neither present yields an empty (still valid) sidecar source.
	AppearanceSource src;
	src.sidecar_ = true;
	// A separate connection: the COPY bind holds the current one (see ParseMetadataFromQuery).
	Connection conn(*context.db);
	auto column = [](const vector<string> &names, const char *name) -> idx_t {
		for (idx_t i = 0; i < names.size(); i++) {
			if (StringUtil::Lower(names[i]) == StringUtil::Lower(name)) {
				return i;
			}
		}
		return DConstants::INVALID_INDEX;
	};
	auto get = [](DataChunk &chunk, idx_t col, idx_t row) -> Value {
		return col == DConstants::INVALID_INDEX ? Value() : chunk.data[col].GetValue(row);
	};

	if (materials_query.has_value()) {
		auto result = conn.Query(materials_query.value());
		if (result->HasError()) {
			throw BinderException("materials_query failed: " + result->GetError());
		}
		auto c_id = column(result->names, "id");
		if (c_id == DConstants::INVALID_INDEX) {
			throw BinderException("materials_query must return an `id` column (materials.parquet shape)");
		}
		auto c_name = column(result->names, "name");
		auto c_diff = column(result->names, "diffuseColor");
		auto c_spec = column(result->names, "specularColor");
		auto c_emis = column(result->names, "emissiveColor");
		auto c_tran = column(result->names, "transparency");
		auto c_shin = column(result->names, "shininess");
		while (auto chunk = result->Fetch()) {
			for (idx_t row = 0; row < chunk->size(); row++) {
				auto id_val = get(*chunk, c_id, row);
				if (id_val.IsNull()) {
					continue;
				}
				int64_t id = id_val.GetValue<int64_t>();
				MeshMaterial m;
				auto name = get(*chunk, c_name, row);
				m.name = name.IsNull() ? "material_" + std::to_string(id) : name.ToString();
				if (auto d = TripleFromList(get(*chunk, c_diff, row))) {
					m.diffuse = *d;
				}
				m.specular = TripleFromList(get(*chunk, c_spec, row));
				m.emissive = TripleFromList(get(*chunk, c_emis, row));
				auto t = get(*chunk, c_tran, row);
				m.transparency = t.IsNull() ? 0.0 : t.GetValue<double>();
				auto s = get(*chunk, c_shin, row);
				if (!s.IsNull()) {
					m.shininess = s.GetValue<double>();
				}
				src.materials_[id] = std::move(m);
			}
		}
	}
	if (textures_query.has_value()) {
		auto result = conn.Query(textures_query.value());
		if (result->HasError()) {
			throw BinderException("textures_query failed: " + result->GetError());
		}
		auto c_id = column(result->names, "id");
		if (c_id == DConstants::INVALID_INDEX) {
			throw BinderException("textures_query must return an `id` column (textures.parquet shape)");
		}
		auto c_uri = column(result->names, "image_uri");
		auto c_data = column(result->names, "image_data");
		auto c_type = column(result->names, "image_type");
		auto c_wrap = column(result->names, "wrapMode");
		while (auto chunk = result->Fetch()) {
			for (idx_t row = 0; row < chunk->size(); row++) {
				auto id_val = get(*chunk, c_id, row);
				if (id_val.IsNull()) {
					continue;
				}
				int64_t id = id_val.GetValue<int64_t>();
				MeshTexture t;
				auto uri = get(*chunk, c_uri, row);
				t.image_uri = uri.IsNull() ? "" : uri.ToString();
				auto type = get(*chunk, c_type, row);
				t.image_type = type.IsNull() ? "" : type.ToString();
				auto wrap = get(*chunk, c_wrap, row);
				t.wrap_mode = wrap.IsNull() ? "wrap" : wrap.ToString();
				auto data = get(*chunk, c_data, row);
				if (!data.IsNull()) {
					// StringValue::Get only D_ASSERTs the physical type before
					// dereferencing: a non-BLOB/VARCHAR value (an INTEGER column, say)
					// either null-derefs in a release build or throws InternalException
					// (database-invalidating). Refuse with an ordinary user-facing error
					// instead -- reachable straight from a user-typed textures_query.
					if (data.type().InternalType() != PhysicalType::VARCHAR) {
						throw InvalidInputException("textures_query: image_data must be BLOB, got %s",
						                            data.type().ToString());
					}
					auto &blob = StringValue::Get(data);
					t.image_data.assign(blob.begin(), blob.end());
				}
				src.textures_[id] = std::move(t);
			}
		}
	}
	return src;
}

std::optional<int64_t> AppearanceSource::ResolveMaterial(const std::string &feature_id, int64_t ref) const {
	int64_t id = sidecar_ ? ref : index_.ResolveMaterial(feature_id, ref);
	if (id < 0 || materials_.count(id) == 0) {
		return std::nullopt;
	}
	return id;
}

std::optional<int64_t> AppearanceSource::ResolveTexture(const std::string &feature_id, int64_t ref) const {
	int64_t id = sidecar_ ? ref : index_.ResolveTexture(feature_id, ref);
	if (id < 0 || textures_.count(id) == 0) {
		return std::nullopt;
	}
	return id;
}

std::optional<std::array<double, 2>> AppearanceSource::UV(const std::string &feature_id, const json &uv_ref) const {
	if (uv_ref.is_array() && uv_ref.size() >= 2 && uv_ref[0].is_number() && uv_ref[1].is_number()) {
		return std::array<double, 2> {uv_ref[0].get<double>(), uv_ref[1].get<double>()};
	}
	if (!uv_ref.is_number()) {
		return std::nullopt;
	}
	// Every UV is inlined as a [u, v] pair regardless of appearance mode (spec §11,
	// appearance sidecars) -- only the material/texture id is feature-local in one
	// mode and dataset-global in the other. A bare number here is never a valid UV
	// element in either mode, so it is refused rather than resolved against a pool
	// that no longer exists.
	throw InvalidInputException(
	    "texture cell of feature '%s' carries a UV index (%s), not an inline [u, v] pair; every UV is inlined "
	    "regardless of appearance mode",
	    feature_id, uv_ref.dump());
}

bool AppearanceSource::LoadImage(ClientContext &context, int64_t texture_id, const std::string &base_dir,
                                 std::string &warning) {
	auto it = textures_.find(texture_id);
	if (it == textures_.end()) {
		warning = "texture " + std::to_string(texture_id) + " is not defined";
		return false;
	}
	auto &tex = it->second;
	// Inferred ahead of the early returns below: a texture row can carry bytes already
	// (sidecar image_data) but no image_type, and it should still get one from the URI.
	if (tex.image_type.empty() && !tex.image_uri.empty()) {
		auto dot = tex.image_uri.rfind('.');
		std::string ext = dot == std::string::npos ? "" : tex.image_uri.substr(dot + 1);
		std::transform(ext.begin(), ext.end(), ext.begin(), [](unsigned char c) { return std::toupper(c); });
		tex.image_type = ext == "JPEG" ? "JPG" : ext;
	}
	if (!tex.image_data.empty()) {
		return true;
	}
	if (tex.image_uri.empty()) {
		warning = "texture " + std::to_string(texture_id) + " has neither image_data nor image_uri";
		return false;
	}
	const bool absolute = tex.image_uri.find("://") != std::string::npos || tex.image_uri.rfind('/', 0) == 0;
	std::string path = absolute || base_dir.empty() ? tex.image_uri : base_dir + "/" + tex.image_uri;
	try {
		auto content = json_utils::ReadFileContent(context, path);
		tex.image_data.assign(content.begin(), content.end());
	} catch (const std::exception &e) {
		// Catches CityJSONError (OpenFile failures) and every plain DuckDB exception
		// ReadFileContent lets through uncaught -- httpfs autoload, GetFileSize, Read --
		// so a remote or otherwise-unreadable image_uri warns instead of aborting the COPY.
		warning = "texture " + std::to_string(texture_id) + ": could not read '" + path + "': " + e.what();
		return false;
	}
	return true;
}

} // namespace cityjson
} // namespace duckdb
