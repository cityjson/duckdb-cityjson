#include "cityjson/copy_function.hpp"
#include "cityjson/cityjson_writer.hpp"
#include "cityjson/cityparquet_package.hpp"
#include "cityjson/column_types.hpp"
#include "cityjson/wkb_decoder.hpp"
#include "duckdb/logging/logger.hpp"
#include "cityjson/copy_source_ref.hpp"
#include "cityjson/reader.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/geometry.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
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
	return CopyColumnRole::Attribute;
}

// ============================================================
// CityJSONCopyBindData
// ============================================================

unique_ptr<FunctionData> CityJSONCopyBindData::Copy() const {
	auto result = make_uniq<CityJSONCopyBindData>();
	result->file_path = file_path;
	result->is_seq = is_seq;
	result->is_fcb = is_fcb;
	result->version = version;
	result->crs = crs;
	result->transform = transform;
	result->title = title;
	result->identifier = identifier;
	result->reference_date = reference_date;
	result->geographical_extent = geographical_extent;
	result->point_of_contact = point_of_contact;
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
	return file_path == o.file_path && is_seq == o.is_seq && is_fcb == o.is_fcb;
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
static void LoadSourceAppearance(ClientContext &context, const CopySourceRef &source_ref,
                                 CityJSONCopyBindData &bind_data) {
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

	if (!source_ref.is_seq) {
		// Whole-document CityJSON: one block, and no per-feature blocks exist.
		bind_data.source_appearance_header = take_appearance(json_utils::ParseJson(content));
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

static unique_ptr<FunctionData> CityJSONCopyToBind(ClientContext &context, CopyFunctionBindInput &input,
                                                   const vector<string> &names, const vector<LogicalType> &sql_types) {
	auto bind_data = make_uniq<CityJSONCopyBindData>();
	bind_data->file_path = input.info.file_path;
	bind_data->is_seq = (input.info.format == "cityjsonseq");
	bind_data->is_fcb = (input.info.format == "flatcitybuf");

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
	if (!explicit_metadata_from.empty()) {
		CopySourceRef ref;
		ref.path = explicit_metadata_from;
		ref.is_seq = bind_data->is_seq;
		ref.is_fcb = bind_data->is_fcb;
		bind_data->source_ref = std::move(ref);
	} else if (input.info.select_statement) {
		bind_data->source_ref = FindCopySourceRef(*input.info.select_statement);
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
			auto reader = OpenAnyCityJSONFile(context, source_ref.path, 1);
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
			LoadSourceAppearance(context, source_ref, *bind_data);
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
		// Reject arrow-native geometry before anything is written. Decoding it back to
		// CityJSON is out of scope for phase 1, and both of its columns fail quietly
		// otherwise: geometry_lod* is a nested LIST this writer cannot decode, so the
		// object would come out with an empty "geometry", and geometry_vertices_lod*
		// shares the "geometry_" prefix without matching any geometry rule, so it would
		// be emitted as an ordinary attribute holding a rendered struct string.
		const bool is_vertices_column = names[i].rfind("geometry_vertices", 0) == 0;
		const bool is_arrow_native_geometry =
		    DetectColumnRole(names[i]) == CopyColumnRole::GeometryWKB && sql_types[i].id() == LogicalTypeId::LIST;
		if (is_vertices_column || is_arrow_native_geometry) {
			throw BinderException(
			    "COPY ... TO (FORMAT cityjson): column '" + names[i] +
			    "' uses the arrow-native geometry encoding, which this writer cannot decode. Re-read the source "
			    "without geometry_encoding := 'arrow-native' to write CityJSON.");
		}

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
		default:
			break;
		}
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

static unique_ptr<GlobalFunctionData> CityJSONCopyToInitGlobal(ClientContext &context, FunctionData &bind_data,
                                                               const string &file_path) {
	auto gstate = make_uniq<CityJSONCopyGlobalState>();
	gstate->temp_file_path = file_path;
	return std::move(gstate);
}

// ============================================================
// COPY TO Initialize Local
// ============================================================

static unique_ptr<LocalFunctionData> CityJSONCopyToInitLocal(ExecutionContext &context, FunctionData &bind_data) {
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
		// UTC is the only offset we can honestly claim: InferTemporalType matches
		// the date and time fields alone, so any source offset is already discarded
		// at read time and "Z" loses nothing that is not already lost.
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

// ============================================================
// COPY TO Sink
// ============================================================

static void CityJSONCopyToSink(ExecutionContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate_p,
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
		std::string object_type = CityJSONTypeForCityGMLClass(object_type_val.ToString());

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
		auto apply_properties = [&](json &geom, idx_t props_col, bool from_wkb) {
			if (props_col == DConstants::INVALID_INDEX) {
				return;
			}
			auto pval = input.data[props_col].GetValue(row);
			if (pval.IsNull()) {
				return;
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
					return; // invalid JSON text -> apply no properties
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
		};

		// Re-attach per-LoD appearance (§11) onto a geometry from its matching
		// material_lod*/texture_lod* columns. The appearance column shares the
		// geometry column's LoD suffix: geometry_lod3 -> material_lod3 / texture_lod3;
		// the un-suffixed "geometry" -> "material" / "texture". Legacy geom_lod*
		// STRUCT geometry carries its appearance in the struct itself, so it has no
		// suffix match here and is left untouched.
		auto apply_appearance = [&](json &geom, const std::string &geom_name) {
			std::string suffix;
			static const std::string kGeometryPrefix = "geometry";
			if (geom_name == kGeometryPrefix) {
				suffix = "";
			} else if (geom_name.rfind(kGeometryPrefix, 0) == 0) {
				suffix = geom_name.substr(kGeometryPrefix.size()); // e.g. "_lod3"
			} else {
				return;
			}
			auto attach = [&](const std::string &prefix, const char *key) {
				auto it = bind_data.appearance_by_name.find(prefix + suffix);
				if (it == bind_data.appearance_by_name.end()) {
					return;
				}
				auto v = input.data[it->second].GetValue(row);
				if (v.IsNull()) {
					return;
				}
				try {
					geom[key] = json_utils::ParseJson(v.ToString());
				} catch (...) {
					// Ignore parse errors in appearance JSON.
				}
			};
			attach("material", "material");
			attach("texture", "texture");
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
				apply_properties(geom, find_properties_col(col_name), from_wkb);
				apply_appearance(geom, col_name);
				geometries.push_back(std::move(geom));
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
					attributes[bind_data.column_names[col]] = ValueToJson(val);
				}
			}
		}
		if (!attributes.empty()) {
			city_obj["attributes"] = attributes;
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

static void CityJSONCopyToCombine(ExecutionContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate_p,
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

static void CityJSONCopyToFinalize(ClientContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate_p) {
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

	// Write to the temp file path — DuckDB will rename it to the final path after Finalize
	auto &output_path = gstate.temp_file_path;

	if (bind_data.is_seq) {
		CityJSONWriter::WriteCityJSONSeq(output_path, write_meta, gstate.feature_objects, gstate.feature_order,
		                                 bind_data.source_appearance_header, bind_data.source_appearance_by_feature);
#ifdef CITYJSON_HAS_FCB
	} else if (bind_data.is_fcb) {
		// The relation's attribute columns, not the ones that happened to carry a
		// value. An attribute that is NULL in every row is omitted from the JSON by
		// the sink above, so without this list the FCB header never learns it exists
		// and the column is lost.
		std::vector<std::string> declared_attr_columns;
		for (idx_t col = 0; col < bind_data.column_roles.size(); col++) {
			if (bind_data.column_roles[col] == CopyColumnRole::Attribute) {
				declared_attr_columns.push_back(bind_data.column_names[col]);
			}
		}
		CityJSONWriter::WriteFlatCityBuf(output_path, write_meta, gstate.feature_objects, gstate.feature_order,
		                                 bind_data.fcb_attr_index_columns, bind_data.fcb_branching_factor,
		                                 bind_data.fcb_index_node_size, declared_attr_columns);
#endif
	} else {
		CityJSONWriter::WriteCityJSON(output_path, write_meta, gstate.feature_objects, gstate.feature_order,
		                              bind_data.source_appearance_header);
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
