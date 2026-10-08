#include "cityjson/copy_function.hpp"
#include "cityjson/appearance_cell.hpp"
#include "cityjson/appearance_source.hpp"
#include "cityjson/cityjson_writer.hpp"
#include "cityjson/cityparquet_package.hpp"
#include "cityjson/column_types.hpp"
#include "cityjson/city_object_utils.hpp"
#include "cityjson/wkb_decoder.hpp"
#include "duckdb/logging/logger.hpp"
#include "cityjson/copy_source_ref.hpp"
#include "cityjson/lod_table.hpp"
#include "cityjson/mesh_copy.hpp"
#include "cityjson/mesh_model.hpp"
#include "cityjson/obj_reader.hpp"
#include "cityjson/reader.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/geometry.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include <fstream>
#include <tuple>
#include <map>
#include <cstring>
#include <algorithm>
#include <limits>

namespace duckdb {
namespace cityjson {

// json typedef is available from duckdb::cityjson namespace via json_utils.hpp

// ============================================================
// Column role detection
// ============================================================

CopyColumnRole DetectColumnRole(const std::string &name) {
	if (name == "id") {
		return CopyColumnRole::Id;
	}
	if (name == "feature_id") {
		return CopyColumnRole::FeatureId;
	}
	if (name == "object_type") {
		return CopyColumnRole::ObjectType;
	}
	if (name == "children") {
		return CopyColumnRole::Children;
	}
	if (name == "parents") {
		return CopyColumnRole::Parents;
	}
	if (name == "children_roles") {
		return CopyColumnRole::ChildrenRoles;
	}
	// Check properties before geometry so the shared "geometry_" prefix on the wide
	// CityParquet columns does not misclassify "geometry_properties_lod*" as geometry.
	if (name == "geometry_properties" || name.rfind("geometry_properties", 0) == 0) {
		return CopyColumnRole::GeometryProperties;
	}
	// "geometry" (non-LOD), wide "geometry_lod*" and legacy "geom_lod*" are all WKB geometry.
	if (name == "geometry" || name.rfind("geometry_lod", 0) == 0 || name.rfind("geom_lod", 0) == 0) {
		return CopyColumnRole::GeometryWKB;
	}
	// Per-LoD appearance columns (and their un-suffixed single-LoD-mode forms),
	// matched by the exact suffix grammar so `material_lodging` stays an attribute.
	if (IsAppearanceColumnName(name)) {
		return CopyColumnRole::Appearance;
	}
	// bbox is derived from the geometry and recomputed on read; never round-tripped as data.
	if (name == "bbox") {
		return CopyColumnRole::Bbox;
	}
	if (name == "other") {
		return CopyColumnRole::Other;
	}
	// Reserved (spec 02-object-table-schema.mdx): rebuilt into the CityObject's own
	// `address` member and GeometryInstance, never written as a CityJSON attribute nor
	// declared into an FCB header's attribute schema (declared_attr_columns).
	if (name == "address") {
		return CopyColumnRole::Address;
	}
	if (name == "implicit_geometry") {
		return CopyColumnRole::ImplicitGeometry;
	}
	return CopyColumnRole::Attribute;
}

// ============================================================
// CityJSONCopyBindData
// ============================================================

unique_ptr<FunctionData> CityJSONCopyBindData::Copy() const {
	auto result = make_uniq<CityJSONCopyBindData>();
	result->file_path = file_path;
	result->format = format;
	result->mesh_lod = mesh_lod;
	result->mesh_origin = mesh_origin;
	result->obj_triangulate = obj_triangulate;
	result->obj_precision = obj_precision;
	result->gltf_attributes = gltf_attributes;
	result->materials_query = materials_query;
	result->textures_query = textures_query;
	result->appearance_source = appearance_source;
	result->version = version;
	result->crs = crs;
	result->transform = transform;
	result->title = title;
	result->identifier = identifier;
	result->reference_date = reference_date;
	result->geographical_extent = geographical_extent;
	result->point_of_contact = point_of_contact;
	result->extension_declarations = extension_declarations;
	result->source_extensions = source_extensions;
	result->output_names = output_names;
	result->column_names = column_names;
	result->column_types = column_types;
	result->column_roles = column_roles;
	result->id_col = id_col;
	result->feature_id_col = feature_id_col;
	result->object_type_col = object_type_col;
	result->children_col = children_col;
	result->parents_col = parents_col;
	result->children_roles_col = children_roles_col;
	result->geometry_col = geometry_col;
	result->bbox_col = bbox_col;
	result->other_col = other_col;
	result->address_col = address_col;
	result->implicit_geometry_col = implicit_geometry_col;
	result->implicit_geometries_query = implicit_geometries_query;
	result->geometry_templates = geometry_templates;
	result->templates_by_id = templates_by_id;
	result->template_count = template_count;
	result->templates_from_query = templates_from_query;
	result->geometry_properties_col = geometry_properties_col;
	result->geometry_properties_by_name = geometry_properties_by_name;
	result->appearance_by_name = appearance_by_name;
	result->source_ref = source_ref;
	result->source_appearance_header = source_appearance_header;
	result->source_appearance_by_feature = source_appearance_by_feature;
	return result;
}

bool CityJSONCopyBindData::Equals(const FunctionData &other) const {
	auto &o = other.Cast<CityJSONCopyBindData>();
	return file_path == o.file_path && format == o.format;
}

// ============================================================
// Helper: parse metadata_query result
// ============================================================

static void ParseMetadataFromQuery(ClientContext &context, const std::string &query, CityJSONCopyBindData &bind_data) {
	// Use a separate connection to avoid deadlock — the COPY bind already holds a lock
	// on the current connection, so running a nested query on it would deadlock.
	Connection conn(*context.db);
	auto result = conn.Query(query);
	if (result->HasError()) {
		throw BinderException("metadata_query failed: " + result->GetError());
	}

	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		throw BinderException("metadata_query returned no rows");
	}

	// Get column names from the result
	auto &col_names = result->names;

	// Helper to extract {x, y, z} struct into array<double, 3>
	auto extract_xyz_struct = [](const Value &v) -> std::optional<std::array<double, 3>> {
		if (v.IsNull() || v.type().id() != LogicalTypeId::STRUCT) {
			return std::nullopt;
		}
		auto &children = StructValue::GetChildren(v);
		if (children.size() < 3) {
			return std::nullopt;
		}
		// Fields are x, y, z
		if (children[0].IsNull() || children[1].IsNull() || children[2].IsNull()) {
			return std::nullopt;
		}
		return std::array<double, 3> {children[0].GetValue<double>(), children[1].GetValue<double>(),
		                              children[2].GetValue<double>()};
	};

	for (idx_t col = 0; col < col_names.size(); col++) {
		auto &name = col_names[col];
		auto val = chunk->data[col].GetValue(0);

		if (val.IsNull()) {
			continue;
		}

		if (name == "version") {
			bind_data.version = val.ToString();
		} else if (name == "title") {
			bind_data.title = val.ToString();
		} else if (name == "identifier") {
			bind_data.identifier = val.ToString();
		} else if (name == "reference_date") {
			bind_data.reference_date = val.ToString();
		} else if (name == "reference_system" || name == "crs") {
			// Handle STRUCT {base_url, authority, version, code} or plain string
			if (val.type().id() == LogicalTypeId::STRUCT) {
				auto &children = StructValue::GetChildren(val);
				// Reconstruct CRS URI: base_url + authority/version/code
				std::string base_url = !children.empty() && !children[0].IsNull() ? children[0].ToString() : "";
				std::string authority = children.size() > 1 && !children[1].IsNull() ? children[1].ToString() : "";
				std::string version_str = children.size() > 2 && !children[2].IsNull() ? children[2].ToString() : "";
				std::string code = children.size() > 3 && !children[3].IsNull() ? children[3].ToString() : "";
				if (!base_url.empty() && !authority.empty()) {
					bind_data.crs = base_url + authority + "/" + version_str + "/" + code;
				} else if (!authority.empty() && !code.empty()) {
					bind_data.crs = "https://www.opengis.net/def/crs/" + authority + "/" + version_str + "/" + code;
				}
			} else {
				bind_data.crs = val.ToString();
			}
		} else if (name == "transform_scale") {
			if (val.type().id() == LogicalTypeId::STRUCT) {
				auto parsed = extract_xyz_struct(val);
				if (parsed.has_value()) {
					if (!bind_data.transform.has_value()) {
						bind_data.transform = Transform();
					}
					bind_data.transform->scale = parsed.value();
				}
			} else {
				auto s = val.ToString();
				std::array<double, 3> scale;
				if (sscanf(s.c_str(), "%lf,%lf,%lf", &scale[0], &scale[1], &scale[2]) == 3 ||
				    sscanf(s.c_str(), "[%lf,%lf,%lf]", &scale[0], &scale[1], &scale[2]) == 3) {
					if (!bind_data.transform.has_value()) {
						bind_data.transform = Transform();
					}
					bind_data.transform->scale = scale;
				}
			}
		} else if (name == "transform_translate") {
			if (val.type().id() == LogicalTypeId::STRUCT) {
				auto parsed = extract_xyz_struct(val);
				if (parsed.has_value()) {
					if (!bind_data.transform.has_value()) {
						bind_data.transform = Transform();
					}
					bind_data.transform->translate = parsed.value();
				}
			} else {
				auto s = val.ToString();
				std::array<double, 3> translate;
				if (sscanf(s.c_str(), "%lf,%lf,%lf", &translate[0], &translate[1], &translate[2]) == 3 ||
				    sscanf(s.c_str(), "[%lf,%lf,%lf]", &translate[0], &translate[1], &translate[2]) == 3) {
					if (!bind_data.transform.has_value()) {
						bind_data.transform = Transform();
					}
					bind_data.transform->translate = translate;
				}
			}
		} else if (name == "geographical_extent") {
			if (val.type().id() == LogicalTypeId::STRUCT) {
				auto &children = StructValue::GetChildren(val);
				if (children.size() >= 6 && !children[0].IsNull() && !children[1].IsNull() && !children[2].IsNull() &&
				    !children[3].IsNull() && !children[4].IsNull() && !children[5].IsNull()) {
					bind_data.geographical_extent = GeographicalExtent(
					    children[0].GetValue<double>(), children[1].GetValue<double>(), children[2].GetValue<double>(),
					    children[3].GetValue<double>(), children[4].GetValue<double>(), children[5].GetValue<double>());
				}
			}
		} else if (name == "extensions") {
			// A package's `city.extensions` (cityparquet_city_field(city, 'extensions')).
			json parsed;
			try {
				parsed = json_utils::ParseJson(val.ToString());
			} catch (const std::exception &e) {
				throw BinderException("metadata_query: `extensions` is not valid JSON: " + std::string(e.what()));
			}
			bind_data.extension_declarations = DeclarationsFromFooterJson(parsed, "metadata_query");
		} else if (name == "point_of_contact") {
			if (val.type().id() == LogicalTypeId::STRUCT) {
				auto &children = StructValue::GetChildren(val);
				// Fields: contact_name, email_address, contact_type, role, phone, website, address
				if (children.size() >= 2 && !children[0].IsNull() && !children[1].IsNull()) {
					PointOfContact poc(children[0].ToString(), children[1].ToString());
					if (children.size() > 2 && !children[2].IsNull()) {
						poc.contact_type = children[2].ToString();
					}
					if (children.size() > 3 && !children[3].IsNull()) {
						poc.role = children[3].ToString();
					}
					if (children.size() > 4 && !children[4].IsNull()) {
						poc.phone = children[4].ToString();
					}
					if (children.size() > 5 && !children[5].IsNull()) {
						poc.website = children[5].ToString();
					}
					bind_data.point_of_contact = poc;
				}
			}
		}
	}
}

// ============================================================
// Helper: parse comma-separated doubles
// ============================================================

static std::optional<std::array<double, 3>> ParseDoubleTriple(const std::string &s) {
	std::array<double, 3> result;
	if (sscanf(s.c_str(), "%lf,%lf,%lf", &result[0], &result[1], &result[2]) == 3) {
		return result;
	}
	return std::nullopt;
}

#ifdef CITYJSON_HAS_FCB
// branching_factor/index_node_size are BIGINT options narrowed into a uint16_t for
// fcb::FcbWriterOptions -- validate the range first rather than let a plain
// static_cast<uint16_t> silently wrap (e.g. 65536 -> 0, disabling the tree entirely;
// a negative value -> some large, unintended positive size). A node/branching factor
// below 2 isn't a valid tree shape either.
static uint16_t ParseTreeTuningOption(const Value &val, const std::string &option_name) {
	auto raw = val.GetValue<int64_t>();
	if (raw < 2 || raw > std::numeric_limits<uint16_t>::max()) {
		throw BinderException(option_name + " must be between 2 and 65535, got " + std::to_string(raw));
	}
	return static_cast<uint16_t>(raw);
}
#endif

// ============================================================
// COPY TO Bind (shared between cityjson and cityjsonseq)
// ============================================================

// ============================================================
// Helper: carry appearance definitions over from the source file
// ============================================================

// Read the source's `appearance` objects verbatim.
//
// CityJSON has one top-level block. CityJSONSeq has one on the header line and,
// independently, one per feature -- and a feature's material/texture refs are
// LOCAL indices into its own block. So they are collected per feature id and
// re-emitted onto the matching output feature rather than merged.
// The source ref arrives as its own parameter rather than being read back out of
// bind_data.source_ref: the caller has already established the optional holds a value,
// and dereferencing it again here would be an unchecked access.
// `reader` and `source_meta` are the caller's already-open reader and its already-read
// metadata: an OBJ source has no JSON to lift a per-feature block from, so its branch
// reuses them rather than opening a second OBJReader over the same path and parsing
// the whole file again.
// Replace each texture ring's UV indices with the [u, v] pairs of `uv_pool`, in place:
// a ring is an array of numbers (its texture id, then one UV index per vertex) or
// [null]. The writer re-interns inline pairs into the pool it writes, exactly as it
// does an object's.
static void InlineTemplateUVs(json &values, const json &uv_pool) {
	if (!values.is_array()) {
		return;
	}
	const bool ring = !values.empty() && std::all_of(values.begin(), values.end(), [](const json &v) {
		return v.is_number_integer() || v.is_null();
	});
	if (!ring) {
		for (auto &child : values) {
			InlineTemplateUVs(child, uv_pool);
		}
		return;
	}
	if (!values[0].is_number_integer()) {
		return; // [null]: no texture
	}
	for (size_t k = 1; k < values.size(); k++) {
		if (!values[k].is_number_unsigned() || values[k].get<uint64_t>() >= uv_pool.size()) {
			throw InvalidInputException("COPY: a geometry template's texture refers to UV index %s, which the "
			                            "source's vertices-texture does not have",
			                            values[k].dump());
		}
		values[k] = uv_pool[values[k].get<size_t>()];
	}
}

// The discovered source's `geometry-templates` member, unless implicit_geometries_query
// already supplied the templates. `doc` is the whole document or the CityJSONSeq header.
static void TakeSourceTemplates(const json &doc, CityJSONCopyBindData &bind_data) {
	if (bind_data.templates_from_query) {
		return;
	}
	auto found = doc.find("geometry-templates");
	if (found == doc.end() || !found->is_object()) {
		return;
	}
	json templates = *found;
	static const json no_uvs = json::array();
	const json *uv_pool = &no_uvs;
	auto appearance = doc.find("appearance");
	if (appearance != doc.end() && appearance->is_object() && appearance->contains("vertices-texture")) {
		uv_pool = &(*appearance)["vertices-texture"];
	}
	if (templates.contains("templates") && templates["templates"].is_array()) {
		for (auto &geometry : templates["templates"]) {
			if (!geometry.is_object() || !geometry.contains("texture") || !geometry["texture"].is_object()) {
				continue;
			}
			for (auto &theme : geometry["texture"].items()) {
				if (theme.value().is_object() && theme.value().contains("values")) {
					InlineTemplateUVs(theme.value()["values"], *uv_pool);
				}
			}
		}
	}
	const auto list = templates.find("templates");
	bind_data.template_count = list != templates.end() && list->is_array() ? list->size() : 0;
	bind_data.geometry_templates = std::move(templates);
}

static void LoadTemplatesFromQuery(ClientContext &context, const std::string &query, CityJSONCopyBindData &bind_data);

static void LoadSourceAppearance(ClientContext &context, const CopySourceRef &source_ref, CityJSONReader &reader,
                                 const CityJSON &source_meta, CityJSONCopyBindData &bind_data) {
	if (source_ref.kind == ReaderKind::Obj) {
		if (!source_meta.appearance.has_value() || source_meta.appearance->Empty()) {
			return;
		}
		auto appearance_json = source_meta.appearance->ToJson();
		bind_data.source_appearance_header = appearance_json;

		// An OBJ's texture refs are file-global indices into the file's one `vt` pool,
		// and the reader never renumbers them per object -- unlike position vertices,
		// which do get a per-feature-local pool. `BuildAppearanceBlock`/`BuildTexturePool`
		// (cityjson_writer.cpp) rebuild each textured feature's actual `vertices-texture`
		// from the inline `[u, v]` pairs `apply_appearance` leaves on its own geometry --
		// compacted and re-interned in first-use order -- so nothing stamped here ends up
		// in the output file itself. This loop's only job is to mark which features are
		// textured at all: an empty `vertices-texture` marker makes `BuildAppearanceBlock`
		// treat the feature as having a definition to rebuild, rather than skipping it as
		// texture-less, just as well as the full pool would.
		if (appearance_json.find("vertices-texture") == appearance_json.end()) {
			return;
		}
		json feature_marker = json {{"vertices-texture", json::array()}};
		for (const auto &feature : reader.ReadAllChunks().records) {
			bool textured = false;
			for (const auto &entry : feature.city_objects) {
				for (const auto &geometry : entry.second.geometry) {
					if (geometry.texture.has_value()) {
						textured = true;
						break;
					}
				}
				if (textured) {
					break;
				}
			}
			if (textured) {
				bind_data.source_appearance_by_feature[feature.id] = feature_marker;
			}
		}
		return;
	}

	auto content = json_utils::ReadFileContent(context, source_ref.path);

	auto take_appearance = [](const json &doc) -> std::optional<json> {
		auto it = doc.find("appearance");
		if (it == doc.end() || !it->is_object() || it->empty()) {
			return std::nullopt;
		}
		// Explicit in_place: nlohmann::json's templated converting constructor makes
		// a plain `return *it;` ambiguous against optional's own converting ctor.
		return std::optional<json>(std::in_place, *it);
	};

	if (source_ref.kind != ReaderKind::CityJSONSeq) {
		// Whole-document CityJSON: one block, and no per-feature blocks exist.
		auto doc = json_utils::ParseJson(content);
		bind_data.source_appearance_header = take_appearance(doc);
		TakeSourceTemplates(doc, bind_data);
		return;
	}

	size_t line_start = 0;
	bool first_line = true;
	while (line_start < content.size()) {
		auto line_end = content.find('\n', line_start);
		auto len = (line_end == std::string::npos ? content.size() : line_end) - line_start;
		auto line = content.substr(line_start, len);
		line_start = (line_end == std::string::npos ? content.size() : line_end + 1);

		if (line.find_first_not_of(" \t\r") == std::string::npos) {
			continue;
		}
		json doc;
		try {
			doc = json_utils::ParseJson(line);
		} catch (const std::exception &) {
			continue; // a malformed line is the reader's problem to report, not ours
		}

		if (first_line) {
			bind_data.source_appearance_header = take_appearance(doc);
			TakeSourceTemplates(doc, bind_data);
			first_line = false;
			continue;
		}
		auto appearance = take_appearance(doc);
		if (!appearance.has_value()) {
			continue;
		}
		auto id_it = doc.find("id");
		if (id_it == doc.end() || !id_it->is_string()) {
			continue;
		}
		bind_data.source_appearance_by_feature[id_it->get<std::string>()] = std::move(appearance.value());
	}
}

unique_ptr<FunctionData> CityJSONCopyToBind(ClientContext &context, CopyFunctionBindInput &input,
                                            const vector<string> &names, const vector<LogicalType> &sql_types) {
	auto bind_data = make_uniq<CityJSONCopyBindData>();
	bind_data->file_path = input.info.file_path;
	const auto &fmt = input.info.format;
	bind_data->format = fmt == "cityjsonseq"   ? CopyFormat::CityJSONSeq
	                    : fmt == "flatcitybuf" ? CopyFormat::FlatCityBuf
	                    : fmt == "obj"         ? CopyFormat::Obj
	                    : fmt == "gltf"        ? CopyFormat::Gltf
	                    : fmt == "glb"         ? CopyFormat::Glb
	                                           : CopyFormat::CityJSON;

	// Explicit metadata wins over anything inherited from the source, so record
	// which of them the user actually supplied.
	bool has_explicit_crs = false;
	bool has_metadata_query = false;
	std::string explicit_metadata_from;

	// Parse options
	for (auto &option : input.info.options) {
		auto loption = StringUtil::Lower(option.first);
		if (option.second.empty()) {
			continue;
		}

		auto &val = option.second[0];

		if (loption == "version") {
			bind_data->version = val.ToString();
		} else if (loption == "crs") {
			bind_data->crs = val.ToString();
			has_explicit_crs = true;
		} else if (loption == "metadata_from") {
			explicit_metadata_from = val.ToString();
		} else if (loption == "metadata_query") {
			ParseMetadataFromQuery(context, val.ToString(), *bind_data);
			has_metadata_query = true;
		} else if (loption == "transform_scale") {
			auto parsed = ParseDoubleTriple(val.ToString());
			if (parsed.has_value()) {
				// value_or, then assign the whole optional back: scale and translate are
				// set independently and either may arrive first, so the existing Transform
				// has to survive. Writing through bind_data->transform-> after engaging it
				// in a branch above would say the same thing, but reads as an unchecked
				// access -- to a reviewer as much as to clang-tidy.
				auto transform = bind_data->transform.value_or(Transform());
				transform.scale = parsed.value();
				bind_data->transform = transform;
			}
		} else if (loption == "transform_translate") {
			auto parsed = ParseDoubleTriple(val.ToString());
			if (parsed.has_value()) {
				auto transform = bind_data->transform.value_or(Transform());
				transform.translate = parsed.value();
				bind_data->transform = transform;
			}
		} else if (loption == "attr_index") {
			auto columns_str = val.ToString();
			std::vector<std::string> columns;
			size_t start = 0;
			while (start <= columns_str.size()) {
				auto comma = columns_str.find(',', start);
				auto piece = columns_str.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
				// Trim surrounding whitespace so "a, b" and "a,b" behave the same.
				size_t first = piece.find_first_not_of(" \t");
				size_t last = piece.find_last_not_of(" \t");
				if (first != std::string::npos) {
					columns.push_back(piece.substr(first, last - first + 1));
				}
				if (comma == std::string::npos) {
					break;
				}
				start = comma + 1;
			}
			bind_data->fcb_attr_index_columns = columns;
		} else if (loption == "branching_factor") {
			bind_data->fcb_branching_factor = ParseTreeTuningOption(val, "branching_factor");
		} else if (loption == "index_node_size") {
			bind_data->fcb_index_node_size = ParseTreeTuningOption(val, "index_node_size");
		} else if (loption == "lod") {
			bind_data->mesh_lod = LODTableUtils::NormalizeLOD(val.ToString());
		} else if (loption == "origin") {
			auto text = val.ToString();
			if (text != "auto" && text != "none" && !ParseOriginOption(text).has_value()) {
				throw BinderException("origin must be 'auto', 'none' or 'x,y,z', got '" + text + "'");
			}
			bind_data->mesh_origin = text;
		} else if (loption == "triangulate") {
			bind_data->obj_triangulate = val.GetValue<bool>();
		} else if (loption == "precision") {
			auto p = val.GetValue<int64_t>();
			if (p < 1 || p > 17) {
				throw BinderException("precision must be between 1 and 17 significant digits");
			}
			bind_data->obj_precision = static_cast<int>(p);
		} else if (loption == "attributes") {
			bind_data->gltf_attributes = val.GetValue<bool>();
		} else if (loption == "materials_query") {
			bind_data->materials_query = val.ToString();
		} else if (loption == "textures_query") {
			bind_data->textures_query = val.ToString();
		} else if (loption == "implicit_geometries_query") {
			bind_data->implicit_geometries_query = val.ToString();
		} else {
			// A misspelled option used to be dropped silently, writing the output with
			// the default the user thought they had overridden. Reject anything the
			// bind does not know rather than guess.
			throw BinderException("COPY TO " + input.info.format + ": unknown option '" + option.first + "'");
		}
	}

	// Resolve the source file, then inherit its metadata.
	//
	// COPY binds a relation, not a file, so nothing the source carries at file level
	// is reachable from the rows -- which is why a plain
	//   COPY (SELECT * FROM read_cityjsonseq('delft.city.jsonl')) TO 'out' (FORMAT cityjsonseq)
	// used to write no `metadata` key at all, silently turning EPSG:7415 data into
	// unreferenced coordinates. Recover the path from the parsed SELECT, and fall
	// back to an explicit metadata_from for the shapes that cannot be discovered
	// (a plain table, a join, a computed path).
	//
	// Precedence: crs / metadata_query  >  metadata_from  >  discovered source.
	//
	// The SELECT is walked either way. metadata_from overrides which FILE the metadata
	// and appearance definitions come from, but it cannot know which appearance FORM
	// the reader was asked for -- that is a property of the reader call, not of a path.
	// Taking `sidecar_appearance` from the discovered ref regardless is what keeps
	// metadata_from from disarming the mesh refusal rule below.
	std::optional<CopySourceRef> discovered;
	if (input.info.select_statement) {
		discovered = FindCopySourceRef(*input.info.select_statement);
	}
	if (!explicit_metadata_from.empty()) {
		CopySourceRef ref;
		ref.path = explicit_metadata_from;
		ref.kind = ReaderKindForPath(explicit_metadata_from);
		ref.sidecar_appearance = discovered.has_value() && discovered->sidecar_appearance;
		bind_data->source_ref = std::move(ref);
	} else {
		bind_data->source_ref = std::move(discovered);
	}

	if (bind_data->source_ref.has_value()) {
		// Bound once, here, rather than dereferenced at each use inside the try: the
		// optional is settled by this point and nothing below reassigns it.
		//
		// The suppression is for the analyser's reach, not for a doubt about the
		// value. bind_data is a unique_ptr, and clang-tidy's optional model does not
		// carry a has_value() across the deref -- it cannot prove the two
		// `bind_data->` on these adjacent lines name the same object. Binding here
		// rather than inside the try is what keeps this to one such spot.
		// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
		const auto &source_ref = *bind_data->source_ref;
		try {
			// Reopen with the reader the query itself used, not by re-detecting the
			// format: a CityJSONSeq file named `*.city.json` is misread as a whole
			// CityJSON document by auto-detection, which drops its metadata (CRS above
			// all) and logs a warning instead.
			auto reader = OpenCityJSONFileOfKind(context, source_ref.kind, source_ref.path, 1);
			auto source_meta = reader->ReadMetadata();

			// Only fill what the user did not state. An explicit crs must win, so it
			// is checked per-field rather than skipping the whole inheritance.
			if (!has_explicit_crs && !bind_data->crs.has_value() && source_meta.metadata.has_value() &&
			    source_meta.metadata->reference_system.has_value()) {
				bind_data->crs = source_meta.metadata->reference_system.value();
			}
			if (source_meta.metadata.has_value()) {
				if (!bind_data->title.has_value()) {
					bind_data->title = source_meta.metadata->title;
				}
				if (!bind_data->identifier.has_value()) {
					bind_data->identifier = source_meta.metadata->identifier;
				}
				if (!bind_data->reference_date.has_value()) {
					bind_data->reference_date = source_meta.metadata->reference_date;
				}
				if (!bind_data->geographical_extent.has_value()) {
					bind_data->geographical_extent = source_meta.metadata->geographical_extent;
				}
				if (!bind_data->point_of_contact.has_value()) {
					bind_data->point_of_contact = source_meta.metadata->point_of_contact;
				}
			}
			// The source's `+` names reach the rows unchanged, so its declarations go
			// back out as they came in.
			if (!source_meta.extensions.empty()) {
				json extensions = json::object();
				for (const auto &entry : source_meta.extensions) {
					json extension = {{"url", entry.second.url}, {"version", entry.second.version}};
					if (entry.second.extra_properties.has_value()) {
						for (const auto &extra : entry.second.extra_properties->items()) {
							extension[extra.key()] = extra.value();
						}
					}
					extensions[entry.first] = std::move(extension);
				}
				bind_data->source_extensions = std::move(extensions);
			}
			LoadSourceAppearance(context, source_ref, *reader, source_meta, *bind_data);
		} catch (const std::exception &e) {
			// An unreadable source is not fatal -- the rows are what is being copied,
			// and the metadata is a bonus. Warn rather than fail the whole COPY.
			DUCKDB_LOG_WARNING(context, "cityjson: could not read metadata from source '" + source_ref.path +
			                                "': " + std::string(e.what()));
		}
	} else if (!has_explicit_crs && !has_metadata_query) {
		DUCKDB_LOG_WARNING(context, "cityjson: could not determine the source file for this COPY, so no metadata "
		                            "(including the CRS) is carried across; pass metadata_from or crs to set it "
		                            "explicitly");
	}

	// Mesh formats must be told which appearance form the cells are in: material cells
	// look identical in both, and resolving sidecar ids against the source's local
	// blocks would silently recolour every face.
	if (IsMeshFormat(bind_data->format) && !bind_data->materials_query.has_value() &&
	    !bind_data->textures_query.has_value() && bind_data->source_ref.has_value()) {
		// Bound once, as above: clang-tidy's optional model does not carry the
		// has_value() across the unique_ptr deref.
		// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
		const auto &source_ref = *bind_data->source_ref;
		if (source_ref.sidecar_appearance) {
			throw InvalidInputException(
			    "COPY TO " + input.info.format +
			    ": the source was read with appearance := 'sidecar', so its material/texture cells hold "
			    "sidecar ids; pass materials_query / textures_query (e.g. materials_query 'SELECT * FROM "
			    "cityjson_materials(''" +
			    source_ref.path + "'')') so they can be resolved");
		}
	}
	if (IsMeshFormat(bind_data->format)) {
		bind_data->appearance_source = BuildAppearanceSource(context, *bind_data);
	} else if (bind_data->source_ref.has_value()) {
		// The CityJSON family re-attaches each feature's own appearance block
		// verbatim (LoadSourceAppearance/CityJSONWriter) -- LOCAL indices into that
		// block's `materials`/`textures` arrays. A sidecar-read source's cells hold
		// dataset-global ids instead, which those LOCAL arrays do not index into, and
		// unlike the mesh formats there is no materials_query/textures_query escape
		// hatch here to resolve them against -- so this refuses unconditionally
		// rather than write a file whose refs and definitions silently disagree.
		// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
		const auto &source_ref = *bind_data->source_ref;
		if (source_ref.sidecar_appearance) {
			throw InvalidInputException(
			    "COPY TO " + input.info.format +
			    ": the source was read with appearance := 'sidecar', so its material/texture cells hold "
			    "dataset-global ids that the feature-local appearance blocks this format re-attaches cannot "
			    "resolve; read the source with appearance := 'local' (the default) for a CityJSON-family COPY");
		}
		// A whole-document CityJSON has one top-level `appearance` block
		// (WriteCityJSON re-attaches only `source_appearance_header`), but a
		// CityJSONSeq source's features each carry their own `materials`/`textures`
		// definitions under LOCAL indices (LoadSourceAppearance). There is nowhere in
		// one document to put a second feature's blocks without re-pointing every
		// ref, so this refuses rather than silently drop them -- CityJSONSeq is the
		// format with room for a block per feature.
		if (bind_data->format == CopyFormat::CityJSON) {
			for (const auto &entry : bind_data->source_appearance_by_feature) {
				if (entry.second.contains("materials") || entry.second.contains("textures")) {
					throw InvalidInputException(
					    "COPY TO cityjson: the source's features carry their own materials or textures "
					    "definitions, which a whole-document CityJSON has no place for; write CityJSONSeq "
					    "instead");
				}
			}
		}
	}

	// Default quantisation: when neither an explicit transform_scale/translate nor a
	// metadata_query supplied a transform, quantise vertices at 1 mm rather than the
	// identity transform. Identity rounds every vertex to the nearest integer, which
	// silently destroys sub-metre precision on real projected coordinates (e.g. RD New
	// eastings ~85 000 m). A 0.001 scale with zero translate is the CityJSON default and
	// keeps round-trips lossless. Users can still request coarser/finer quantisation, or
	// carry the source transform, via the transform_* options / metadata_query.
	if (!bind_data->transform.has_value()) {
		bind_data->transform = Transform({0.001, 0.001, 0.001}, {0.0, 0.0, 0.0});
	}

	// Map columns to roles
	bind_data->column_names = names;
	bind_data->column_types.assign(sql_types.begin(), sql_types.end());

	for (idx_t i = 0; i < names.size(); i++) {
		auto role = DetectColumnRole(names[i]);
		bind_data->column_roles.push_back(role);

		switch (role) {
		case CopyColumnRole::Id:
			bind_data->id_col = i;
			break;
		case CopyColumnRole::FeatureId:
			bind_data->feature_id_col = i;
			break;
		case CopyColumnRole::ObjectType:
			bind_data->object_type_col = i;
			break;
		case CopyColumnRole::Children:
			bind_data->children_col = i;
			break;
		case CopyColumnRole::Parents:
			bind_data->parents_col = i;
			break;
		case CopyColumnRole::ChildrenRoles:
			bind_data->children_roles_col = i;
			break;
		case CopyColumnRole::GeometryWKB:
			bind_data->geometry_col = i;
			break;
		case CopyColumnRole::GeometryProperties:
			// Record every per-LOD properties column by name; keep the first as the legacy
			// single-column fallback for geometries that have no per-LOD counterpart.
			bind_data->geometry_properties_by_name[names[i]] = i;
			if (bind_data->geometry_properties_col == DConstants::INVALID_INDEX) {
				bind_data->geometry_properties_col = i;
			}
			break;
		case CopyColumnRole::Appearance:
			bind_data->appearance_by_name[names[i]] = i;
			break;
		case CopyColumnRole::Bbox:
			bind_data->bbox_col = i;
			break;
		case CopyColumnRole::Other:
			bind_data->other_col = i;
			break;
		case CopyColumnRole::Address:
			bind_data->address_col = i;
			break;
		case CopyColumnRole::ImplicitGeometry:
			bind_data->implicit_geometry_col = i;
			break;
		default:
			break;
		}
	}

	// `address` is read field by field in the sink, and its `location` as WKB bytes, so
	// its shape is checked here: the spec's LIST of STRUCT, `location` a BLOB.
	if (bind_data->address_col != DConstants::INVALID_INDEX) {
		const auto &type = sql_types[bind_data->address_col];
		const bool list_of_struct =
		    type.id() == LogicalTypeId::LIST && ListType::GetChildType(type).id() == LogicalTypeId::STRUCT;
		bool location_blob = list_of_struct;
		if (list_of_struct) {
			for (const auto &field : StructType::GetChildTypes(ListType::GetChildType(type))) {
				if (field.first == "location" && field.second.id() != LogicalTypeId::BLOB) {
					location_blob = false;
				}
			}
		}
		if (!location_blob) {
			throw BinderException("COPY TO %s: the `address` column must be a LIST of STRUCT whose `location` is a "
			                      "WKB BLOB (spec 02-object-table-schema.mdx, \"Addresses\"), got %s",
			                      input.info.format, type.ToString());
		}
	}

	// `implicit_geometry` is read field by field in the sink, `point` as WKB bytes: the
	// spec's STRUCT(id BIGINT, point BLOB, transformationMatrix DOUBLE[]).
	if (bind_data->implicit_geometry_col != DConstants::INVALID_INDEX) {
		const auto &type = sql_types[bind_data->implicit_geometry_col];
		bool shaped = type.id() == LogicalTypeId::STRUCT;
		if (shaped) {
			for (const auto &field : StructType::GetChildTypes(type)) {
				if (field.first == "id") {
					shaped = shaped && field.second.IsIntegral();
				} else if (field.first == "point") {
					shaped = shaped && field.second.id() == LogicalTypeId::BLOB;
				} else if (field.first == "transformationMatrix") {
					shaped = shaped && field.second.id() == LogicalTypeId::LIST &&
					         ListType::GetChildType(field.second).IsNumeric();
				}
			}
		}
		if (!shaped) {
			throw BinderException("COPY TO %s: the `implicit_geometry` column must be STRUCT(id BIGINT, point BLOB, "
			                      "transformationMatrix DOUBLE[]) (spec 04-appearance-templates.mdx), got %s",
			                      input.info.format, type.ToString());
		}
	}
	if (bind_data->implicit_geometries_query.has_value()) {
		// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
		LoadTemplatesFromQuery(context, *bind_data->implicit_geometries_query, *bind_data);
	}

	for (const auto &name : names) {
		bind_data->output_names.push_back(RestorePlusName(name, bind_data->extension_declarations));
	}

	// Validate mandatory columns
	if (bind_data->id_col == DConstants::INVALID_INDEX) {
		throw BinderException("COPY TO cityjson requires an 'id' column");
	}
	if (bind_data->feature_id_col == DConstants::INVALID_INDEX) {
		throw BinderException("COPY TO cityjson requires a 'feature_id' column");
	}
	if (bind_data->object_type_col == DConstants::INVALID_INDEX) {
		throw BinderException("COPY TO cityjson requires an 'object_type' column");
	}

	return std::move(bind_data);
}

// ============================================================
// COPY TO Initialize Global
// ============================================================

unique_ptr<GlobalFunctionData> CityJSONCopyToInitGlobal(ClientContext &context, FunctionData &bind_data,
                                                        const string &file_path) {
	auto gstate = make_uniq<CityJSONCopyGlobalState>();
	gstate->temp_file_path = file_path;
	return std::move(gstate);
}

// ============================================================
// COPY TO Initialize Local
// ============================================================

unique_ptr<LocalFunctionData> CityJSONCopyToInitLocal(ExecutionContext &context, FunctionData &bind_data) {
	return make_uniq<CityJSONCopyLocalState>();
}

// ============================================================
// Helper: convert DuckDB Value to JSON
// ============================================================

static json ValueToJson(const Value &val) {
	if (val.IsNull()) {
		return json();
	}

	auto &type = val.type();
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return json(BooleanValue::Get(val));
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
		return json(val.GetValue<int64_t>());
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		return json(val.GetValue<double>());
	case LogicalTypeId::VARCHAR:
		return json(StringValue::Get(val));
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ: {
		// CityJSON dates are ISO-8601. Without an explicit arm here the default
		// below rendered DuckDB's *display* form ("2010-10-13 12:43:04"), which is
		// not valid ISO-8601 -- silently rewriting every timestamp attribute on the
		// way out. The loss is invisible to any row-level comparison, because the
		// value re-parses to the same TIMESTAMP; only the file changes.
		//
		// UTC is not a guess -- it is what the stored value already is.
		// ParseTimestampString (temporal_parser.cpp) genuinely subtracts the
		// source's own +HH:MM/-HH:MM offset while parsing, converting the reading
		// to a true UTC instant before it ever reaches DuckDB's TIMESTAMP storage
		// (InferTemporalType only classifies the string's shape; it plays no part
		// in that conversion). "Z" here states a fact about the stored instant,
		// not an assumption about the source.
		date_t date;
		dtime_t time;
		Timestamp::Convert(TimestampValue::Get(val), date, time);
		return json(Date::ToString(date) + "T" + Time::ToString(time) + "Z");
	}
	case LogicalTypeId::DATE:
		return json(Date::ToString(DateValue::Get(val)));
	case LogicalTypeId::TIME:
		return json(Time::ToString(dtime_t(TimeValue::Get(val))));
	default:
		// For complex types, try to convert to string
		return json(val.ToString());
	}
}

// Convert a DuckDB LIST value (possibly nested, possibly with NULL elements) to
// json, mapping SQL NULL elements to json null so a face_semantics entry with no
// surface round-trips as `null` rather than `0`. Integer leaves cast to int64.
static json ListValueToJson(const Value &v) {
	if (v.IsNull()) {
		return json(nullptr);
	}
	if (v.type().id() == LogicalTypeId::LIST) {
		json arr = json::array();
		for (auto &e : ListValue::GetChildren(v)) {
			arr.push_back(ListValueToJson(e));
		}
		return arr;
	}
	// Emit non-negative counts/indices as unsigned so downstream unsigned checks
	// (e.g. SolidShellCounts' is_number_unsigned) accept them, matching the JSON path.
	int64_t n = v.GetValue<int64_t>();
	return n >= 0 ? json(static_cast<uint64_t>(n)) : json(n);
}

// Build a spec §8 geometry_properties json object from a CityParquet
// geometry_properties_lod* STRUCT value:
//   STRUCT("type" VARCHAR, surfaces VARCHAR, face_semantics INTEGER[], shells INTEGER[][])
// so the shared reconstruction (shell re-nesting + semantics.values) works whether
// the payload arrived as a STRUCT (CityParquet) or as VARCHAR JSON (read_cityjson).
// Fields are dispatched by name; unknown fields are ignored.
static json StructPropsToJson(const Value &sval) {
	json props = json::object();
	auto &fields = StructType::GetChildTypes(sval.type());
	auto &children = StructValue::GetChildren(sval);
	for (idx_t i = 0; i < fields.size(); i++) {
		auto &name = fields[i].first;
		auto &child = children[i];
		if (child.IsNull()) {
			continue; // absent key -> the reconstruction's own gates degrade cleanly
		}
		if (name == "type" || name == "lod") {
			props[name] = child.ToString();
		} else if (name == "surfaces") {
			// `surfaces` is a VARCHAR holding a JSON array (extended +attributes and all).
			auto s = child.ToString();
			if (s.empty()) {
				continue;
			}
			try {
				auto arr = json_utils::ParseJson(s);
				if (arr.is_array()) {
					props["surfaces"] = std::move(arr);
				}
			} catch (...) {
				// leave `surfaces` unset -> no semantics emitted (better than garbage)
			}
		} else if (name == "face_semantics" || name == "shells") {
			props[name] = ListValueToJson(child);
		}
	}
	return props;
}

// Helper: turn a children/parents/children_roles cell into a JSON string array.
// The value arrives as a DuckDB LIST(VARCHAR) from read_cityjson / CityParquet
// tables; a VARCHAR cell holding JSON text is accepted too for hand-built input.
static json ParseJsonArrayValue(const Value &val) {
	if (val.IsNull()) {
		return json::array();
	}
	if (val.type().id() == LogicalTypeId::LIST) {
		json arr = json::array();
		for (const auto &entry : ListValue::GetChildren(val)) {
			// children_roles is positionally aligned with children (CityParquet
			// spec §object-table-schema: "length MUST equal children, with null
			// for a child that has no role"); dropping a null here would shift
			// every later role onto the wrong child, so keep the slot.
			if (entry.IsNull()) {
				arr.push_back(nullptr);
			} else {
				arr.push_back(entry.ToString());
			}
		}
		return arr;
	}
	auto str = val.ToString();
	try {
		auto parsed = json_utils::ParseJson(str);
		if (parsed.is_array()) {
			return parsed;
		}
	} catch (...) {
	}
	return json::array();
}

// ============================================================
// Spec §8 geometry_properties reconstruction (G7)
// ============================================================

// Split a flat array `flat` into consecutive groups sized by `counts`. A count
// larger than the remaining elements is clamped; leftover elements are ignored.
static json PartitionFlat(const json &flat, const std::vector<size_t> &counts) {
	json groups = json::array();
	size_t pos = 0;
	const size_t n = flat.is_array() ? flat.size() : 0;
	for (size_t c : counts) {
		json group = json::array();
		for (size_t i = 0; i < c && pos < n; ++i, ++pos) {
			group.push_back(flat[pos]);
		}
		groups.push_back(std::move(group));
	}
	return groups;
}

// Read a flat list of per-shell face counts out of one solid's `shells` entry.
static std::vector<size_t> ShellCountsOfSolid(const json &solid_shells) {
	std::vector<size_t> counts;
	if (solid_shells.is_array()) {
		for (const auto &n : solid_shells) {
			counts.push_back(n.is_number_unsigned() ? n.get<size_t>() : 0);
		}
	}
	return counts;
}

// Read the per-shell face counts of a `Solid` out of a spec `shells` value.
//
// The spec form is nested one array per solid, so a Solid -- which has exactly
// one -- is [[12, 4]] and its counts live in shells[0]. A flat [12, 4] is
// accepted as well: it is what this extension itself wrote before the encoding
// was corrected, and what a third-party producer with the same bug may still
// emit. The two are told apart by whether the first element is an array, which
// is unambiguous down to the degenerate single-shell case ([1] vs [[1]]).
static std::vector<size_t> SolidShellCounts(const json &shells) {
	if (!shells.is_array() || shells.empty()) {
		return {};
	}
	if (shells[0].is_array()) {
		return ShellCountsOfSolid(shells[0]); // spec form: [[12, 4]]
	}
	return ShellCountsOfSolid(shells); // legacy flat form: [12, 4]
}

static size_t SumCounts(const std::vector<size_t> &counts) {
	size_t s = 0;
	for (size_t c : counts) {
		s += c;
	}
	return s;
}

// Rebuild CityJSON nested `semantics.values` from the flat, face-aligned
// `face_semantics` (spec §8), using `shells` to recover the shell/solid nesting.
static json RenestValues(const std::string &type, const json &face_semantics, const json &shells) {
	const size_t n = face_semantics.is_array() ? face_semantics.size() : 0;
	if (type == "Solid") {
		auto counts = SolidShellCounts(shells);
		// Only trust `shells` when it accounts for exactly the faces present; a
		// mismatch (wrong/inconsistent shells) falls back to a single shell so no
		// face is dropped and values stays aligned with the single-shell boundaries.
		if (!counts.empty() && SumCounts(counts) == n) {
			return PartitionFlat(face_semantics, counts);
		}
		json single = json::array();
		single.push_back(face_semantics);
		return single;
	}
	if (type == "MultiSolid" || type == "CompositeSolid") {
		// shells is nested: one array of per-shell counts per solid. Trust it only
		// when the total matches the face count; otherwise wrap the whole thing as
		// one solid / one shell rather than dropping faces.
		size_t total = 0;
		bool ok = shells.is_array();
		if (ok) {
			for (const auto &solid_shells : shells) {
				if (!solid_shells.is_array()) {
					ok = false;
					break;
				}
				total += SumCounts(ShellCountsOfSolid(solid_shells));
			}
		}
		if (!ok || total != n) {
			json single_shell = json::array();
			single_shell.push_back(face_semantics);
			json single_solid = json::array();
			single_solid.push_back(std::move(single_shell));
			return single_solid;
		}
		json out = json::array();
		size_t pos = 0;
		for (const auto &solid_shells : shells) {
			json solid_values = json::array();
			for (const auto &cnt : solid_shells) {
				size_t c = cnt.is_number_unsigned() ? cnt.get<size_t>() : 0;
				json shell_values = json::array();
				for (size_t i = 0; i < c && pos < n; ++i, ++pos) {
					shell_values.push_back(face_semantics[pos]);
				}
				solid_values.push_back(std::move(shell_values));
			}
			out.push_back(std::move(solid_values));
		}
		return out;
	}
	// MultiSurface / CompositeSurface: values is the flat per-surface array.
	return face_semantics;
}

// Re-nest a WKB-decoded Solid/MultiSolid boundary set (which the decoder returns
// as a single flattened shell) back into its original shells, using `shells`.
static json RenestBoundaries(const std::string &type, const json &boundaries, const json &shells) {
	if (type == "Solid" && boundaries.is_array() && !boundaries.empty() && boundaries[0].is_array()) {
		const json &flat = boundaries[0];
		auto counts = SolidShellCounts(shells);
		// Re-nest only when `shells` accounts for exactly the decoded faces;
		// otherwise keep the single-shell decode so no face is lost.
		if (!counts.empty() && SumCounts(counts) == flat.size()) {
			return PartitionFlat(flat, counts);
		}
		return boundaries;
	}
	if ((type == "MultiSolid" || type == "CompositeSolid") && boundaries.is_array() && shells.is_array() &&
	    shells.size() == boundaries.size()) {
		json out = json::array();
		for (size_t soi = 0; soi < boundaries.size(); ++soi) {
			const json &solid = boundaries[soi];
			const json flat = (solid.is_array() && !solid.empty() && solid[0].is_array()) ? solid[0] : json::array();
			auto counts = ShellCountsOfSolid(shells[soi]);
			if (!counts.empty() && SumCounts(counts) == flat.size()) {
				out.push_back(PartitionFlat(flat, counts));
			} else {
				out.push_back(solid); // keep this solid's single-shell decode
			}
		}
		return out;
	}
	return boundaries;
}

// Count the faces a geometry's own already-nested `boundaries` carries (spec §7.1's
// nesting per type), by the point `apply_appearance` runs -- after `apply_properties`
// has re-nested a WKB decode's flattened shell via `RenestBoundaries`, or left a
// legacy STRUCT geometry's already-nested boundaries alone. Used to reject an
// appearance cell shorter than the geometry it is attached to: `RenestValues`' own
// fallback (a mismatched count wraps the whole flat list as one shell) exists to
// tolerate a `shells` property that does not account for every face, not to paper
// over a cell the caller built with too few entries.
static size_t CountFaces(const std::string &type, const json &boundaries) {
	if (!boundaries.is_array()) {
		return 0;
	}
	if (type == "Solid") {
		size_t total = 0;
		for (const auto &shell : boundaries) {
			if (shell.is_array()) {
				total += shell.size();
			}
		}
		return total;
	}
	if (type == "MultiSolid" || type == "CompositeSolid") {
		size_t total = 0;
		for (const auto &solid : boundaries) {
			if (!solid.is_array()) {
				continue;
			}
			for (const auto &shell : solid) {
				if (shell.is_array()) {
					total += shell.size();
				}
			}
		}
		return total;
	}
	// MultiSurface / CompositeSurface: one entry per surface.
	return boundaries.size();
}

// ============================================================
// implicit_geometries_query: sidecar rows -> geometry-templates
// ============================================================

static bool IsCoordinate(const json &node) {
	return node.is_array() && node.size() == 3 && node[0].is_number() && node[1].is_number() && node[2].is_number();
}

// Replace every [x, y, z] in `node` by its index in `pool`, interning exact doubles:
// template vertices are the relative geometry's own local coordinates, never quantised.
static void InternTemplateVertices(json &node, std::map<std::tuple<uint64_t, uint64_t, uint64_t>, size_t> &index,
                                   json &pool) {
	if (!node.is_array()) {
		return;
	}
	for (auto &child : node) {
		if (!IsCoordinate(child)) {
			InternTemplateVertices(child, index, pool);
			continue;
		}
		std::array<uint64_t, 3> bits {};
		for (size_t axis = 0; axis < 3; axis++) {
			const double value = child[axis].get<double>();
			std::memcpy(&bits[axis], &value, sizeof(value));
		}
		const auto key = std::make_tuple(bits[0], bits[1], bits[2]);
		auto found = index.find(key);
		size_t position;
		if (found != index.end()) {
			position = found->second;
		} else {
			position = pool.size();
			pool.push_back(child);
			index.emplace(key, position);
		}
		child = static_cast<int64_t>(position);
	}
}

// The rows of a materials or textures sidecar query as CityJSON `appearance`
// definitions, in id order, each sidecar column under the member name it was read
// from (spec 04-appearance-templates.mdx; `other`'s members restored alongside), and
// each id's position in that array.
static json SidecarDefinitions(ClientContext &context, const std::string &query, const char *what,
                               std::map<int64_t, int64_t> &position) {
	Connection connection(*context.db);
	auto result = connection.Query(query);
	if (result->HasError()) {
		throw BinderException("%s query failed: %s", what, result->GetError());
	}
	static const std::map<std::string, std::string> renamed = {{"image_uri", "image"}, {"image_type", "type"}};
	const auto &names = result->names;
	idx_t id_col = DConstants::INVALID_INDEX;
	for (idx_t col = 0; col < names.size(); col++) {
		if (StringUtil::Lower(names[col]) == "id" && result->types[col].IsIntegral()) {
			id_col = col;
		}
	}
	if (id_col == DConstants::INVALID_INDEX) {
		throw BinderException("%s query: no integer `id` column", what);
	}
	std::map<int64_t, json> by_id;
	for (idx_t row = 0; row < result->RowCount(); row++) {
		const auto id_value = result->GetValue(id_col, row);
		if (id_value.IsNull()) {
			throw BinderException("%s query: a row has a NULL id", what);
		}
		json definition = json::object();
		for (idx_t col = 0; col < names.size(); col++) {
			const auto value = result->GetValue(col, row);
			if (col == id_col || value.IsNull() || names[col] == "image_data") {
				continue;
			}
			if (names[col] == "other") {
				json other;
				try {
					other = json_utils::ParseJson(value.ToString());
				} catch (const std::exception &) {
					throw BinderException("%s query: `other` of %d is not JSON", what, id_value.GetValue<int64_t>());
				}
				if (other.is_object()) {
					for (auto &member : other.items()) {
						definition[member.key()] = member.value();
					}
				}
				continue;
			}
			const auto found = renamed.find(names[col]);
			const auto key = found == renamed.end() ? names[col] : found->second;
			switch (value.type().id()) {
			case LogicalTypeId::BOOLEAN:
				definition[key] = BooleanValue::Get(value);
				break;
			case LogicalTypeId::LIST: {
				json list = json::array();
				for (const auto &item : ListValue::GetChildren(value)) {
					list.push_back(item.IsNull() ? json() : json(item.GetValue<double>()));
				}
				definition[key] = std::move(list);
				break;
			}
			default:
				if (value.type().IsNumeric()) {
					definition[key] = value.GetValue<double>();
				} else {
					definition[key] = value.ToString();
				}
			}
		}
		by_id[id_value.GetValue<int64_t>()] = std::move(definition);
	}
	json definitions = json::array();
	for (auto &entry : by_id) {
		position[entry.first] = static_cast<int64_t>(definitions.size());
		definitions.push_back(std::move(entry.second));
	}
	return definitions;
}

// The `implicit_geometries` sidecar rows of `query` as the document's
// geometry-templates: one template per row, in id order, each from the row's one
// populated geometry_lod* column (its LoD from the column name) and its
// geometry_properties. A relative geometry's material/texture cells hold sidecar ids,
// which no CityJSON appearance block this COPY writes can resolve, so they are not
// carried.
static void LoadTemplatesFromQuery(ClientContext &context, const std::string &query, CityJSONCopyBindData &bind_data) {
	Connection connection(*context.db);
	auto result = connection.Query(query);
	if (result->HasError()) {
		throw BinderException("implicit_geometries_query failed: " + result->GetError());
	}
	const auto &names = result->names;
	const auto &types = result->types;
	idx_t id_col = DConstants::INVALID_INDEX;
	std::vector<std::pair<idx_t, std::string>> geometry_cols; // column, LoD suffix
	std::map<std::string, idx_t> properties_cols;
	std::map<std::string, idx_t> material_cols;
	std::map<std::string, idx_t> texture_cols;
	for (idx_t col = 0; col < names.size(); col++) {
		const auto lowered = StringUtil::Lower(names[col]);
		if (lowered == "id") {
			if (!types[col].IsIntegral()) {
				throw BinderException("implicit_geometries_query: `id` must be an integer column, got %s",
				                      types[col].ToString());
			}
			id_col = col;
		} else if (lowered.rfind("material_lod", 0) == 0 && types[col] == MaterialCellType()) {
			material_cols[lowered.substr(std::string("material_").size())] = col;
		} else if (lowered.rfind("texture_lod", 0) == 0 && types[col] == TextureCellType()) {
			texture_cols[lowered.substr(std::string("texture_").size())] = col;
		} else if (lowered.rfind("geometry_properties_lod", 0) == 0) {
			if (types[col].id() == LogicalTypeId::STRUCT) {
				properties_cols[lowered.substr(std::string("geometry_properties_").size())] = col;
			}
		} else if (lowered.rfind("geometry_lod", 0) == 0) {
			if (types[col].id() != LogicalTypeId::BLOB) {
				throw BinderException("implicit_geometries_query: `%s` must be a WKB BLOB, got %s", names[col],
				                      types[col].ToString());
			}
			geometry_cols.emplace_back(col, lowered.substr(std::string("geometry_").size()));
		}
	}
	if (id_col == DConstants::INVALID_INDEX) {
		throw BinderException("implicit_geometries_query: the query has no `id` column");
	}

	std::map<int64_t, json> by_id;
	for (idx_t row = 0; row < result->RowCount(); row++) {
		const auto id_value = result->GetValue(id_col, row);
		if (id_value.IsNull()) {
			throw BinderException("implicit_geometries_query: a row has a NULL id");
		}
		const auto id = id_value.GetValue<int64_t>();
		json geometry;
		// A relative geometry is one geometry at one LoD (spec 04-appearance-templates
		// .mdx): exactly one populated LoD group per row.
		idx_t populated = 0;
		for (const auto &entry : geometry_cols) {
			populated += result->GetValue(entry.first, row).IsNull() ? 0 : 1;
		}
		if (populated != 1) {
			throw BinderException("implicit_geometries_query: relative geometry %d populates %d LoD geometry columns; "
			                      "a relative geometry is one geometry at one LoD",
			                      id, static_cast<int64_t>(populated));
		}
		for (const auto &entry : geometry_cols) {
			const auto value = result->GetValue(entry.first, row);
			if (value.IsNull()) {
				continue;
			}
			const auto &blob = StringValue::Get(value);
			WKBDecodeResult decoded;
			try {
				decoded = WKBDecoder::Decode(reinterpret_cast<const uint8_t *>(blob.data()), blob.size());
			} catch (const CityJSONError &e) {
				throw BinderException("implicit_geometries_query: relative geometry %d is not valid WKB: %s", id,
				                      e.what());
			}
			geometry["type"] = decoded.cityjson_type;
			geometry["lod"] = LODTableUtils::ParseLODFromSuffix(entry.second);
			geometry["boundaries"] = decoded.boundaries;
			auto properties = properties_cols.find(entry.second);
			if (properties != properties_cols.end()) {
				const auto props_value = result->GetValue(properties->second, row);
				if (!props_value.IsNull()) {
					const auto props = StructPropsToJson(props_value);
					if (props.contains("type") && props["type"].is_string()) {
						geometry["type"] = props["type"];
					}
					const std::string type = geometry["type"].get<std::string>();
					const json no_shells = json::array();
					const json &shells = props.contains("shells") ? props["shells"] : no_shells;
					geometry["boundaries"] = RenestBoundaries(type, geometry["boundaries"], shells);
					if (props.contains("surfaces") && props.contains("face_semantics")) {
						geometry["semantics"] = {{"surfaces", props["surfaces"]},
						                         {"values", RenestValues(type, props["face_semantics"], shells)}};
					}
					geometry["__shells"] = shells;
				}
			}
			// Appearance in sidecar ids, flat per face; resolved below, once the
			// definitions are known.
			auto material = material_cols.find(entry.second);
			if (material != material_cols.end() && !result->GetValue(material->second, row).IsNull()) {
				geometry["__material"] =
				    MaterialCellToFlatJson(MaterialCellFromValue(result->GetValue(material->second, row)));
			}
			auto texture = texture_cols.find(entry.second);
			if (texture != texture_cols.end() && !result->GetValue(texture->second, row).IsNull()) {
				geometry["__texture"] =
				    TextureCellToFlatJson(TextureCellFromValue(result->GetValue(texture->second, row)));
			}
		}
		if (geometry.is_null()) {
			throw BinderException("implicit_geometries_query: relative geometry %d has no geometry", id);
		}
		if (!by_id.emplace(id, std::move(geometry)).second) {
			throw BinderException("implicit_geometries_query: id %d occurs twice", id);
		}
	}

	// The definitions the relative geometries' appearance names, as the document's
	// `appearance` block, and each sidecar id's position in it.
	bool wants_materials = false;
	bool wants_textures = false;
	for (const auto &entry : by_id) {
		wants_materials = wants_materials || entry.second.contains("__material");
		wants_textures = wants_textures || entry.second.contains("__texture");
	}
	json appearance = json::object();
	std::map<int64_t, int64_t> material_position;
	std::map<int64_t, int64_t> texture_position;
	auto load_definitions = [&](const std::optional<std::string> &definitions_query, const char *what,
	                            const char *option, std::map<int64_t, int64_t> &position) {
		if (!definitions_query.has_value()) {
			throw BinderException("implicit_geometries_query: the relative geometries carry %s, which are sidecar ids; "
			                      "pass %s (e.g. 'SELECT * FROM pkg.%s') so they can be written",
			                      what, option, what);
		}
		// NOLINTNEXTLINE(bugprone-unchecked-optional-access) -- checked just above
		appearance[what] = SidecarDefinitions(context, *definitions_query, what, position);
	};
	if (wants_materials) {
		load_definitions(bind_data.materials_query, "materials", "materials_query", material_position);
	}
	if (wants_textures) {
		load_definitions(bind_data.textures_query, "textures", "textures_query", texture_position);
	}
	if (!appearance.empty()) {
		if (bind_data.source_appearance_header.has_value() && !bind_data.source_appearance_header->empty()) {
			throw BinderException("implicit_geometries_query: the relative geometries' appearance would replace the "
			                      "discovered source's own appearance block; COPY from the package's tables instead");
		}
		bind_data.source_appearance_header = appearance;
	}
	auto position_of = [](const std::map<int64_t, int64_t> &position, const json &id, const char *what) -> json {
		if (!id.is_number_integer()) {
			return id;
		}
		auto found = position.find(id.get<int64_t>());
		if (found == position.end()) {
			throw BinderException("implicit_geometries_query: a relative geometry refers to %s id %d, which the "
			                      "query's definitions do not have",
			                      what, id.get<int64_t>());
		}
		return json(found->second);
	};
	for (auto &entry : by_id) {
		auto &geometry = entry.second;
		const std::string type = geometry["type"].get<std::string>();
		const json shells = geometry.contains("__shells") ? geometry["__shells"] : json::array();
		if (geometry.contains("__material")) {
			json nested = json::object();
			for (auto &theme : geometry["__material"].items()) {
				json values = theme.value().value("values", json::array());
				for (auto &value : values) {
					value = position_of(material_position, value, "material");
				}
				nested[theme.key()] = json {{"values", RenestValues(type, values, shells)}};
			}
			geometry["material"] = std::move(nested);
		}
		if (geometry.contains("__texture")) {
			json nested = json::object();
			for (auto &theme : geometry["__texture"].items()) {
				json values = theme.value().value("values", json::array());
				for (auto &face : values) {
					for (auto &ring : face) {
						if (ring.is_array() && !ring.empty()) {
							ring[0] = position_of(texture_position, ring[0], "texture");
						}
					}
				}
				nested[theme.key()] = json {{"values", RenestValues(type, values, shells)}};
			}
			geometry["texture"] = std::move(nested);
		}
		geometry.erase("__material");
		geometry.erase("__texture");
		geometry.erase("__shells");
	}

	json templates = json::array();
	json pool = json::array();
	std::map<std::tuple<uint64_t, uint64_t, uint64_t>, size_t> index;
	for (auto &entry : by_id) {
		InternTemplateVertices(entry.second["boundaries"], index, pool);
		bind_data.templates_by_id[entry.first] = static_cast<int64_t>(templates.size());
		templates.push_back(std::move(entry.second));
	}
	bind_data.template_count = templates.size();
	bind_data.geometry_templates = json {{"templates", std::move(templates)}, {"vertices-templates", std::move(pool)}};
	bind_data.templates_from_query = true;
}

// One `implicit_geometry` cell as a CityJSON GeometryInstance whose one boundary is the
// reference point's coordinate (the writer indexes it into the vertex pool it builds).
// The cell's types were checked at bind; its bytes and values are still checked here.
static json GeometryInstanceFromCell(const Value &cell, const CityJSONCopyBindData &bind_data,
                                     const std::string &object_id) {
	std::optional<int64_t> id;
	const std::string *point = nullptr;
	Value matrix;
	const auto &fields = StructType::GetChildTypes(cell.type());
	const auto &values = StructValue::GetChildren(cell);
	for (idx_t f = 0; f < fields.size(); f++) {
		if (values[f].IsNull()) {
			continue;
		}
		if (fields[f].first == "id") {
			id = values[f].GetValue<int64_t>();
		} else if (fields[f].first == "point") {
			point = &StringValue::Get(values[f]);
		} else if (fields[f].first == "transformationMatrix") {
			matrix = values[f];
		}
	}
	if (!id.has_value() || point == nullptr) {
		throw InvalidInputException("object %s: an implicit_geometry must carry both `id` and `point` (spec "
		                            "04-appearance-templates.mdx)",
		                            object_id);
	}
	if (!bind_data.geometry_templates.has_value()) {
		throw InvalidInputException("object %s: has an implicit geometry, but this COPY has no geometry templates to "
		                            "place it with; pass implicit_geometries_query (e.g. 'SELECT * FROM "
		                            "pkg.implicit_geometries'), or COPY from the CityJSON source that has them",
		                            object_id);
	}
	int64_t template_index = id.value();
	if (bind_data.templates_from_query) {
		auto found = bind_data.templates_by_id.find(id.value());
		if (found == bind_data.templates_by_id.end()) {
			throw InvalidInputException("object %s: its implicit_geometry refers to id %d, but there is no "
			                            "implicit_geometries row with id %d",
			                            object_id, id.value(), id.value());
		}
		template_index = found->second;
	}
	if (template_index < 0 || static_cast<size_t>(template_index) >= bind_data.template_count) {
		throw InvalidInputException("object %s: its implicit_geometry refers to template %d, which the "
		                            "geometry-templates do not have",
		                            object_id, template_index);
	}
	WKBDecodeResult decoded;
	try {
		decoded = WKBDecoder::Decode(reinterpret_cast<const uint8_t *>(point->data()), point->size());
	} catch (const CityJSONError &e) {
		throw InvalidInputException("object %s: implicit_geometry.point is not valid WKB: %s", object_id, e.what());
	}
	if (decoded.cityjson_type != "Point") {
		throw InvalidInputException("object %s: implicit_geometry.point is a WKB %s, not the PointZ the spec requires",
		                            object_id, decoded.cityjson_type);
	}
	// Absent means identity (spec); CityJSON requires the member.
	json transformation = json::array({1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0});
	if (!matrix.IsNull()) {
		const auto &entries = ListValue::GetChildren(matrix);
		if (entries.size() != 16 ||
		    std::any_of(entries.begin(), entries.end(), [](const Value &v) { return v.IsNull(); })) {
			throw InvalidInputException("object %s: implicit_geometry.transformationMatrix must be 16 numbers, a "
			                            "row-major 4x4",
			                            object_id);
		}
		transformation = json::array();
		for (const auto &entry : entries) {
			transformation.push_back(entry.GetValue<double>());
		}
	}
	return json {{"type", "GeometryInstance"},
	             {"template", template_index},
	             {"boundaries", decoded.boundaries},
	             {"transformationMatrix", std::move(transformation)}};
}

// ============================================================
// COPY TO Sink
// ============================================================

void CityJSONCopyToSink(ExecutionContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate_p,
                        LocalFunctionData &lstate_p, DataChunk &input) {
	auto &bind_data = bind_data_p.Cast<CityJSONCopyBindData>();
	auto &lstate = lstate_p.Cast<CityJSONCopyLocalState>();

	// Per-chunk WKB views for core GEOMETRY geometry columns (a GeoParquet LoD0
	// footprint arrives as LogicalTypeId::GEOMETRY, not BLOB). Geometry::ToBinary is
	// a zero-copy reinterpret of the already-WKB internal form in v1.5.x; these
	// views are lazily materialised on first use and valid for this input chunk.
	vector<unique_ptr<Vector>> wkb_views(input.ColumnCount());

	// Shared WKB → CityJSON boundaries + LoD-from-column-name, used by both the BLOB
	// and GEOMETRY geometry branches so the decode stays single-sourced.
	auto decode_wkb = [](json &geom, const string_t &wkb, const std::string &col_name) {
		auto decoded = WKBDecoder::Decode(reinterpret_cast<const uint8_t *>(wkb.GetData()), wkb.GetSize());
		geom["type"] = decoded.cityjson_type;
		geom["boundaries"] = decoded.boundaries;
		// Derive LOD from the column name: legacy "geom_lod2_2" and wide
		// "geometry_lod2_2" → "2.2".
		if (col_name.rfind("geom_lod", 0) == 0 && col_name.size() > 8) {
			std::string lod = col_name.substr(8);
			std::replace(lod.begin(), lod.end(), '_', '.');
			geom["lod"] = lod;
		} else if (col_name.rfind("geometry_lod", 0) == 0 && col_name.size() > 12) {
			std::string lod = col_name.substr(12);
			std::replace(lod.begin(), lod.end(), '_', '.');
			geom["lod"] = lod;
		}
	};

	for (idx_t row = 0; row < input.size(); row++) {
		// Extract key columns
		auto id_val = input.data[bind_data.id_col].GetValue(row);
		auto feature_id_val = input.data[bind_data.feature_id_col].GetValue(row);
		auto object_type_val = input.data[bind_data.object_type_col].GetValue(row);

		if (id_val.IsNull() || feature_id_val.IsNull() || object_type_val.IsNull()) {
			continue; // Skip rows with null key columns
		}

		std::string city_obj_id = id_val.ToString();
		std::string feature_id = feature_id_val.ToString();
		// The stored vocabulary is the CityGML class name; restore the CityJSON
		// spelling for the four classes that differ (identity for everything else).
		// An extension class is stored with its namespace prefix; its `+` comes back.
		std::string object_type =
		    RestorePlusName(CityJSONTypeForCityGMLClass(object_type_val.ToString()), bind_data.extension_declarations);

		// Build CityObject JSON
		json city_obj;
		city_obj["type"] = object_type;

		// Children
		if (bind_data.children_col != DConstants::INVALID_INDEX) {
			auto val = input.data[bind_data.children_col].GetValue(row);
			if (!val.IsNull()) {
				city_obj["children"] = ParseJsonArrayValue(val);
			}
		}

		// Parents
		if (bind_data.parents_col != DConstants::INVALID_INDEX) {
			auto val = input.data[bind_data.parents_col].GetValue(row);
			if (!val.IsNull()) {
				city_obj["parents"] = ParseJsonArrayValue(val);
			}
		}

		// Children roles
		if (bind_data.children_roles_col != DConstants::INVALID_INDEX) {
			auto val = input.data[bind_data.children_roles_col].GetValue(row);
			if (!val.IsNull()) {
				city_obj["children_roles"] = ParseJsonArrayValue(val);
			}
		}

		// Geometry columns: "geometry", wide "geometry_lod*", or legacy "geom_lod*".
		// Each geometry column is paired with its own properties column so per-LOD
		// semantics/material/texture survive the wide CityParquet layout. The properties
		// column name mirrors the geometry column name:
		//   geometry          -> geometry_properties
		//   geometry_lod2_2   -> geometry_properties_lod2_2
		// Legacy geom_lod* columns have no per-LOD counterpart, so they fall back to the
		// single geometry_properties column (if any).
		auto find_properties_col = [&](const std::string &geom_name) -> idx_t {
			static const std::string kGeometryPrefix = "geometry";
			if (geom_name == kGeometryPrefix || geom_name.rfind(kGeometryPrefix, 0) == 0) {
				// WKB layout: require the EXACT per-column properties match. These
				// columns re-nest solid shells from their own `shells` (spec §8), so
				// borrowing a different column's properties would apply the wrong
				// shell partition. If the exact match is absent, apply no properties
				// (leaving the geometry intact) rather than a mismatched one.
				std::string props_name = (geom_name == kGeometryPrefix)
				                             ? "geometry_properties"
				                             : "geometry_properties" + geom_name.substr(kGeometryPrefix.size());
				auto it = bind_data.geometry_properties_by_name.find(props_name);
				return (it != bind_data.geometry_properties_by_name.end()) ? it->second : DConstants::INVALID_INDEX;
			}
			// Legacy geom_lod* STRUCT columns have no per-LOD counterpart and their
			// boundaries are already nested (from_wkb == false, no re-nesting), so the
			// single default properties column is safe for semantics/material/texture.
			return bind_data.geometry_properties_col;
		};

		// Apply a spec §8 geometry_properties JSON payload onto a single geometry
		// object. `from_wkb` is true when the geometry came from a flattened WKB
		// column (so its shells must be recovered from `shells`); false for the
		// legacy STRUCT layout, whose boundaries are already nested.
		// Returns the parsed `props` json so `apply_appearance` (below) can read its
		// `shells` without re-fetching and re-parsing the same column value.
		auto apply_properties = [&](json &geom, idx_t props_col, bool from_wkb) -> json {
			if (props_col == DConstants::INVALID_INDEX) {
				return json();
			}
			auto pval = input.data[props_col].GetValue(row);
			if (pval.IsNull()) {
				return json();
			}
			// CityParquet stores geometry_properties as a STRUCT; read_cityjson emits
			// the same payload as VARCHAR JSON. Obtain a json object from whichever we
			// got, then run the shared reconstruction below unchanged.
			json props;
			if (bind_data.column_types[props_col].id() == LogicalTypeId::STRUCT) {
				props = StructPropsToJson(pval);
			} else {
				try {
					props = json_utils::ParseJson(pval.ToString());
				} catch (...) {
					return json(); // invalid JSON text -> apply no properties
				}
			}
			try {
				// The precise CityJSON geometry type is authoritative (spec §8 `type`).
				if (props.contains("type") && props["type"].is_string()) {
					geom["type"] = props["type"].get<std::string>();
				}
				// LoD: normally set from the column name; an un-suffixed column carries
				// it inside the JSON instead (spec §8 permitted extra key).
				if (!geom.contains("lod") && props.contains("lod") && props["lod"].is_string()) {
					geom["lod"] = props["lod"].get<std::string>();
				}
				const std::string geom_type = geom.contains("type") ? geom.value("type", "") : "";
				// Recover shell nesting for solids that the WKB flattened (spec §7.1).
				if (from_wkb && props.contains("shells") && geom.contains("boundaries")) {
					geom["boundaries"] = RenestBoundaries(geom_type, geom["boundaries"], props["shells"]);
				}
				// Rebuild CityJSON nested semantics from the flattened form (G7).
				// CityJSON semantics requires both `surfaces` and `values`, so only
				// emit a semantics object when the flattened form carries both halves
				// (surfaces + face_semantics); a lone `surfaces` is skipped rather than
				// written as an invalid values-less semantics object.
				if (props.contains("surfaces") && !bind_data.extension_declarations.empty()) {
					RenameSurfaceTypes(props["surfaces"], [&](const std::string &type) {
						return RestorePlusName(type, bind_data.extension_declarations);
					});
				}
				if (props.contains("surfaces") && props.contains("face_semantics")) {
					const json empty = json::array();
					const json &shells = props.contains("shells") ? props["shells"] : empty;
					json semantics;
					semantics["surfaces"] = props["surfaces"];
					semantics["values"] = RenestValues(geom_type, props["face_semantics"], shells);
					geom["semantics"] = std::move(semantics);
				}
				if (props.contains("material")) {
					geom["material"] = props["material"];
				}
				if (props.contains("texture")) {
					geom["texture"] = props["texture"];
				}
			} catch (...) {
				// Ignore parse errors in geometry properties.
			}
			return props;
		};

		// Re-attach per-LoD appearance (§11) onto a geometry from its matching
		// material_lod*/texture_lod* MAP column. The appearance column shares the
		// geometry column's LoD suffix: geometry_lod3 -> material_lod3 / texture_lod3;
		// the un-suffixed "geometry" -> "material" / "texture". Legacy geom_lod*
		// STRUCT geometry carries its appearance in the struct itself, so it has no
		// suffix match here and is left untouched.
		//
		// A mesh format (OBJ/glTF/GLB) gets the flat, WKB-face-aligned cell verbatim
		// (appearance_cell.hpp's `*CellToFlatJson` shape) -- mesh_model.cpp indexes it
		// by face position and never re-nests it. The CityJSON family (CityJSON /
		// CityJSONSeq / FlatCityBuf) instead needs the spec's per-shell nesting,
		// recovered with the same `RenestValues` the semantics reconstruction above
		// already applies to `face_semantics`, keyed off this geometry's own `type`
		// and the same `props["shells"]` `apply_properties` already parsed for this
		// row (passed in by the caller, rather than re-fetching and re-parsing the
		// same `geometry_properties_*` column value here). A texture's per-face
		// element is its whole ring array, re-nested as one unit; the `[u, v]` pairs
		// inside stay inline here and are
		// re-interned into a per-feature (or, for a single-document CityJSON,
		// per-document) `vertices-texture` pool only once every row of the output has
		// been collected, in CityJSONWriter::WriteCityJSONSeq / WriteCityJSON -- a
		// feature's rows can be processed on different threads during this sink, and
		// only the writer sees every one of them together.
		auto apply_appearance = [&](json &geom, const std::string &geom_name, const json &props) {
			std::string suffix;
			static const std::string kGeometryPrefix = "geometry";
			if (geom_name == kGeometryPrefix) {
				suffix = "";
			} else if (geom_name.rfind(kGeometryPrefix, 0) == 0) {
				suffix = geom_name.substr(kGeometryPrefix.size()); // e.g. "_lod3"
			} else {
				return;
			}
			auto attach = [&](const std::string &prefix, const char *key, bool is_material) {
				auto it = bind_data.appearance_by_name.find(prefix + suffix);
				if (it == bind_data.appearance_by_name.end()) {
					return;
				}
				auto v = input.data[it->second].GetValue(row);
				if (v.IsNull()) {
					return;
				}
				json flat;
				try {
					flat = is_material ? MaterialCellToFlatJson(MaterialCellFromValue(v))
					                   : TextureCellToFlatJson(TextureCellFromValue(v));
				} catch (const InvalidInputException &e) {
					// A cell that violates the column's own invariants (a NULL theme
					// value, `id`/`uv` disagreeing on nullness) is invalid input, not an
					// appearance to silently drop -- the `other` column is refused just
					// as loudly for the same reason elsewhere in this function.
					// ErrorData unwraps the plain message: an Exception's own what()
					// returns the client-protocol JSON envelope, not the raw text.
					throw InvalidInputException("object %s: %s: %s", city_obj_id, prefix + suffix,
					                            ErrorData(e).RawMessage());
				}
				const std::string geom_type = geom.value("type", "");
				// A cell shorter than the geometry it is attached to is invalid input,
				// not a shape `RenestValues` should paper over: its own fallback for a
				// short/mismatched list is to wrap the whole thing as one shell, which
				// for a cell built by hand (rather than round-tripped from a wider one)
				// silently attaches values to the wrong faces instead of failing loudly.
				const size_t face_count = CountFaces(geom_type, geom.value("boundaries", json::array()));
				for (auto theme_it = flat.begin(); theme_it != flat.end(); ++theme_it) {
					size_t entries = theme_it.value().value("values", json::array()).size();
					if (entries != face_count) {
						throw InvalidInputException("object %s: %s: theme '%s' has %d entries for %d faces",
						                            city_obj_id, prefix + suffix, theme_it.key(),
						                            static_cast<int>(entries), static_cast<int>(face_count));
					}
				}
				if (IsMeshFormat(bind_data.format)) {
					geom[key] = std::move(flat);
					return;
				}
				static const json kEmptyShells = json::array();
				const json &shells = props.contains("shells") ? props["shells"] : kEmptyShells;
				json nested = json::object();
				for (auto theme_it = flat.begin(); theme_it != flat.end(); ++theme_it) {
					json values = theme_it.value().value("values", json::array());
					nested[theme_it.key()] = json {{"values", RenestValues(geom_type, values, shells)}};
				}
				geom[key] = std::move(nested);
			};
			attach("material", "material", true);
			attach("texture", "texture", false);
		};

		json geometries = json::array();
		for (idx_t col = 0; col < bind_data.column_roles.size(); col++) {
			if (bind_data.column_roles[col] != CopyColumnRole::GeometryWKB) {
				continue;
			}
			auto val = input.data[col].GetValue(row);
			if (val.IsNull()) {
				continue;
			}

			auto &col_type = bind_data.column_types[col];
			auto &col_name = bind_data.column_names[col];

			json geom;
			bool produced = false;
			bool from_wkb = false;

			if (col_type.id() == LogicalTypeId::STRUCT) {
				// Non-LOD STRUCT geometry: {lod, type, boundaries, semantics, material, texture}
				auto &children = StructValue::GetChildren(val);
				auto &struct_type = StructType::GetChildTypes(col_type);

				for (idx_t c = 0; c < struct_type.size(); c++) {
					auto &field_name = struct_type[c].first;
					auto &child_val = children[c];
					if (child_val.IsNull()) {
						continue;
					}

					if (field_name == "type") {
						geom["type"] = child_val.ToString();
					} else if (field_name == "lod") {
						geom["lod"] = child_val.ToString();
					} else if (field_name == "boundaries") {
						try {
							geom["boundaries"] = json_utils::ParseJson(child_val.ToString());
						} catch (...) {
							geom["boundaries"] = json::array();
						}
					} else if (field_name == "semantics") {
						try {
							geom["semantics"] = json_utils::ParseJson(child_val.ToString());
						} catch (...) {
						}
					} else if (field_name == "material") {
						try {
							geom["material"] = json_utils::ParseJson(child_val.ToString());
						} catch (...) {
						}
					} else if (field_name == "texture") {
						try {
							geom["texture"] = json_utils::ParseJson(child_val.ToString());
						} catch (...) {
						}
					}
				}
				if (!geom.contains("type")) {
					geom["type"] = "MultiSurface";
				}
				if (!geom.contains("boundaries")) {
					geom["boundaries"] = json::array();
				}
				produced = true;

			} else if (col_type.id() == LogicalTypeId::BLOB) {
				// WKB BLOB geometry.
				decode_wkb(geom, val.GetValueUnsafe<string_t>(), col_name);
				produced = true;
				from_wkb = true;
			} else if (col_type.id() == LogicalTypeId::GEOMETRY) {
				// DuckDB-core GEOMETRY (e.g. a GeoParquet LoD0 footprint). Serialise to
				// WKB via the core serialiser — a zero-copy view of the already-WKB
				// internal form — then decode via the same CityParquet-scoped WKBDecoder
				// as the BLOB path (footprints are MultiPolygon Z; the decoder targets
				// the CityParquet WKB subset, not arbitrary geometry).
				if (!wkb_views[col]) {
					wkb_views[col] = make_uniq<Vector>(LogicalType::BLOB);
					// Fully qualified: cityjson has its own `Geometry` type.
					::duckdb::Geometry::ToBinary(input.data[col], *wkb_views[col], input.size());
				}
				auto wkb_val = wkb_views[col]->GetValue(row);
				if (!wkb_val.IsNull()) {
					decode_wkb(geom, wkb_val.GetValueUnsafe<string_t>(), col_name);
					produced = true;
					from_wkb = true;
				}
			}

			if (produced) {
				auto props = apply_properties(geom, find_properties_col(col_name), from_wkb);
				apply_appearance(geom, col_name, props);
				geometries.push_back(std::move(geom));
			}
		}

		// The CityJSON family only: FlatCityBuf and the mesh formats carry neither an
		// address nor a GeometryInstance.
		const bool cityjson_family =
		    bind_data.format == CopyFormat::CityJSON || bind_data.format == CopyFormat::CityJSONSeq;
		if (cityjson_family && bind_data.implicit_geometry_col != DConstants::INVALID_INDEX) {
			auto cell = input.data[bind_data.implicit_geometry_col].GetValue(row);
			if (!cell.IsNull()) {
				geometries.push_back(GeometryInstanceFromCell(cell, bind_data, city_obj_id));
			}
		}

		if (!geometries.empty()) {
			city_obj["geometry"] = geometries;
		} else {
			city_obj["geometry"] = json::array();
		}

		// Attributes (all non-reserved columns)
		json attributes = json::object();
		for (idx_t col = 0; col < bind_data.column_roles.size(); col++) {
			if (bind_data.column_roles[col] == CopyColumnRole::Attribute) {
				auto val = input.data[col].GetValue(row);
				if (!val.IsNull()) {
					attributes[bind_data.output_names[col]] = ValueToJson(val);
				}
			}
		}
		// `other` carries source data with no column of its own; the format's
		// reader rule is that every entry is restored as an attribute. Without
		// this, an attribute whose name collides with a reserved column is read
		// into `other` and then written nowhere.
		//
		// An `other` cell that is not parseable JSON, or that parses to something
		// other than a JSON object, is invalid input (spec 02-object-table-schema.mdx,
		// "The `other` column"): `other` stores an object's worth of attributes, so a
		// reader must reject a cell that cannot supply that rather than silently
		// treating it as contributing nothing. A SQL NULL cell is unaffected -- it
		// means "no other attributes" and stays fine.
		//
		// An `other` entry whose key duplicates an attribute already decoded from
		// its own column is likewise invalid input: the two copies cannot be
		// reconciled without inventing a preference the format does not state, so
		// this is an error rather than a silent keep-the-column-drop-the-other-copy
		// resolution.
		if (bind_data.other_col != DConstants::INVALID_INDEX) {
			auto other_val = input.data[bind_data.other_col].GetValue(row);
			if (!other_val.IsNull()) {
				json other_json;
				try {
					other_json = json_utils::ParseJson(other_val.ToString());
				} catch (const CityJSONError &e) {
					throw InvalidInputException("COPY TO cityjson: object '%s' has an `other` cell that is not "
					                            "valid JSON (%s); a reader must reject this rather than silently "
					                            "treat the cell as contributing no attributes",
					                            city_obj_id, e.what());
				}
				if (!other_json.is_object()) {
					throw InvalidInputException(
					    "COPY TO cityjson: object '%s' has an `other` cell that is not a JSON object (%s); a "
					    "reader must reject this rather than silently treat the cell as contributing no attributes",
					    city_obj_id, other_json.type_name());
				}
				for (auto it = other_json.begin(); it != other_json.end(); ++it) {
					if (attributes.contains(it.key())) {
						throw InvalidInputException(
						    "COPY TO cityjson: object '%s' has an `other` entry for key '%s' that "
						    "duplicates the value already decoded from its own attribute column; a "
						    "reader must reject this rather than silently keep one copy",
						    city_obj_id, it.key());
					}
					attributes[it.key()] = it.value();
				}
			}
		}
		if (!attributes.empty()) {
			city_obj["attributes"] = attributes;
		}

		// The `address` column back into the CityObject's `address` member (spec
		// 07-mapping-cityjson.mdx), each field under the member name it is read from;
		// `location` goes out as a MultiPoint of coordinates, which the writer turns into
		// indices into the vertex pool it builds.
		if (cityjson_family && bind_data.address_col != DConstants::INVALID_INDEX) {
			auto address_val = input.data[bind_data.address_col].GetValue(row);
			if (!address_val.IsNull() && address_val.type().id() == LogicalTypeId::LIST) {
				json addresses = json::array();
				for (const auto &entry : ListValue::GetChildren(address_val)) {
					if (entry.IsNull() || entry.type().id() != LogicalTypeId::STRUCT) {
						continue;
					}
					json address = json::object();
					const auto &fields = StructType::GetChildTypes(entry.type());
					const auto &values = StructValue::GetChildren(entry);
					for (idx_t f = 0; f < fields.size(); f++) {
						if (values[f].IsNull()) {
							continue;
						}
						if (fields[f].first == "location") {
							// BLOB, checked at bind; the bytes are still untrusted WKB.
							const auto &blob = StringValue::Get(values[f]);
							WKBDecodeResult decoded;
							try {
								decoded =
								    WKBDecoder::Decode(reinterpret_cast<const uint8_t *>(blob.data()), blob.size());
							} catch (const CityJSONError &e) {
								throw InvalidInputException("object %s: address location is not valid WKB: %s",
								                            city_obj_id, e.what());
							}
							if (decoded.cityjson_type != "MultiPoint") {
								throw InvalidInputException(
								    "object %s: address location is a WKB %s, not the MultiPointZ the spec requires",
								    city_obj_id, decoded.cityjson_type);
							}
							address["location"] = {{"type", "MultiPoint"}, {"boundaries", decoded.boundaries}};
							continue;
						}
						for (const auto &name : AddressMemberNames()) {
							if (name.first == fields[f].first) {
								address[name.second] = values[f].ToString();
							}
						}
					}
					addresses.push_back(std::move(address));
				}
				if (!addresses.empty()) {
					city_obj["address"] = std::move(addresses);
				}
			}
		}

		// `geographicalExtent` is derived from `bbox`: the format stores one
		// spatial extent per row and `bbox` is it. `other` is no longer
		// consulted -- it carries attributes now, not the source extent.
		if (bind_data.bbox_col != DConstants::INVALID_INDEX) {
			auto bbox_val = input.data[bind_data.bbox_col].GetValue(row);
			if (!bbox_val.IsNull() && bbox_val.type().id() == LogicalTypeId::STRUCT) {
				auto &children = StructValue::GetChildren(bbox_val);
				if (children.size() >= 6 && !children[0].IsNull() && !children[1].IsNull() && !children[2].IsNull() &&
				    !children[3].IsNull() && !children[4].IsNull() && !children[5].IsNull()) {
					city_obj["geographicalExtent"] =
					    json::array({children[0].GetValue<double>(), children[1].GetValue<double>(),
					                 children[2].GetValue<double>(), children[3].GetValue<double>(),
					                 children[4].GetValue<double>(), children[5].GetValue<double>()});
				}
			}
		}

		// Add to local buffer
		if (lstate.local_objects.find(feature_id) == lstate.local_objects.end()) {
			lstate.local_feature_order.push_back(feature_id);
		}
		lstate.local_objects[feature_id].emplace_back(city_obj_id, std::move(city_obj));
	}
}

// ============================================================
// COPY TO Combine
// ============================================================

void CityJSONCopyToCombine(ExecutionContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate_p,
                           LocalFunctionData &lstate_p) {
	auto &gstate = gstate_p.Cast<CityJSONCopyGlobalState>();
	auto &lstate = lstate_p.Cast<CityJSONCopyLocalState>();

	std::lock_guard<std::mutex> lock(gstate.mutex);

	for (const auto &fid : lstate.local_feature_order) {
		if (gstate.feature_objects.find(fid) == gstate.feature_objects.end()) {
			gstate.feature_order.push_back(fid);
		}

		auto &global_objs = gstate.feature_objects[fid];
		auto &local_objs = lstate.local_objects[fid];
		global_objs.insert(global_objs.end(), std::make_move_iterator(local_objs.begin()),
		                   std::make_move_iterator(local_objs.end()));
	}

	lstate.local_objects.clear();
	lstate.local_feature_order.clear();
}

// ============================================================
// COPY TO Finalize
// ============================================================

void CityJSONCopyToFinalize(ClientContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate_p) {
	auto &bind_data = bind_data_p.Cast<CityJSONCopyBindData>();
	auto &gstate = gstate_p.Cast<CityJSONCopyGlobalState>();

	// Build write metadata from bind data
	CityJSONWriteMetadata write_meta;
	write_meta.version = bind_data.version;
	write_meta.crs = bind_data.crs;
	write_meta.transform = bind_data.transform;
	write_meta.title = bind_data.title;
	write_meta.identifier = bind_data.identifier;
	write_meta.reference_date = bind_data.reference_date;
	write_meta.geographical_extent = bind_data.geographical_extent;
	write_meta.point_of_contact = bind_data.point_of_contact;
	if (!bind_data.extension_declarations.empty()) {
		write_meta.extensions = DeclarationsToCityJSON(bind_data.extension_declarations);
	} else {
		write_meta.extensions = bind_data.source_extensions;
	}
	write_meta.geometry_templates = bind_data.geometry_templates;

	// Write to the temp file path — DuckDB will rename it to the final path after Finalize
	auto &output_path = gstate.temp_file_path;

	switch (bind_data.format) {
	case CopyFormat::CityJSONSeq:
		CityJSONWriter::WriteCityJSONSeq(output_path, write_meta, gstate.feature_objects, gstate.feature_order,
		                                 bind_data.source_appearance_header, bind_data.source_appearance_by_feature);
		break;
#ifdef CITYJSON_HAS_FCB
	case CopyFormat::FlatCityBuf: {
		// The relation's attribute columns, not the ones that happened to carry a
		// value. An attribute that is NULL in every row is omitted from the JSON by
		// the sink above, so without this list the FCB header never learns it exists
		// and the column is lost.
		std::vector<std::string> declared_attr_columns;
		for (idx_t col = 0; col < bind_data.column_roles.size(); col++) {
			if (bind_data.column_roles[col] == CopyColumnRole::Attribute) {
				declared_attr_columns.push_back(bind_data.output_names[col]);
			}
		}
		CityJSONWriter::WriteFlatCityBuf(output_path, write_meta, gstate.feature_objects, gstate.feature_order,
		                                 bind_data.fcb_attr_index_columns, bind_data.fcb_branching_factor,
		                                 bind_data.fcb_index_node_size, declared_attr_columns,
		                                 bind_data.source_appearance_header, bind_data.source_appearance_by_feature);
		break;
	}
#else
	case CopyFormat::FlatCityBuf:
		throw InternalException("flatcitybuf COPY format bound without FCB support");
#endif
	case CopyFormat::Obj:
		FinalizeObj(context, bind_data, gstate);
		break;
	case CopyFormat::Gltf:
	case CopyFormat::Glb:
		FinalizeGltf(context, bind_data, gstate);
		break;
	case CopyFormat::CityJSON:
		CityJSONWriter::WriteCityJSON(output_path, write_meta, gstate.feature_objects, gstate.feature_order,
		                              bind_data.source_appearance_header);
		break;
	}
}

// ============================================================
// Registration
// ============================================================

void RegisterCityJSONCopyFunction(ExtensionLoader &loader) {
	CopyFunction function("cityjson");
	function.extension = "city.json";
	function.copy_to_bind = CityJSONCopyToBind;
	function.copy_to_initialize_global = CityJSONCopyToInitGlobal;
	function.copy_to_initialize_local = CityJSONCopyToInitLocal;
	function.copy_to_sink = CityJSONCopyToSink;
	function.copy_to_combine = CityJSONCopyToCombine;
	function.copy_to_finalize = CityJSONCopyToFinalize;
	loader.RegisterFunction(function);
}

void RegisterCityJSONSeqCopyFunction(ExtensionLoader &loader) {
	CopyFunction function("cityjsonseq");
	function.extension = "city.jsonl";
	function.copy_to_bind = CityJSONCopyToBind;
	function.copy_to_initialize_global = CityJSONCopyToInitGlobal;
	function.copy_to_initialize_local = CityJSONCopyToInitLocal;
	function.copy_to_sink = CityJSONCopyToSink;
	function.copy_to_combine = CityJSONCopyToCombine;
	function.copy_to_finalize = CityJSONCopyToFinalize;
	loader.RegisterFunction(function);
}

#ifdef CITYJSON_HAS_FCB
void RegisterFlatCityBufCopyFunction(ExtensionLoader &loader) {
	CopyFunction function("flatcitybuf");
	function.extension = "fcb";
	function.copy_to_bind = CityJSONCopyToBind;
	function.copy_to_initialize_global = CityJSONCopyToInitGlobal;
	function.copy_to_initialize_local = CityJSONCopyToInitLocal;
	function.copy_to_sink = CityJSONCopyToSink;
	function.copy_to_combine = CityJSONCopyToCombine;
	function.copy_to_finalize = CityJSONCopyToFinalize;
	loader.RegisterFunction(function);
}
#endif

} // namespace cityjson
} // namespace duckdb
