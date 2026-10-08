#include "cityjson/cityparquet_extensions.hpp"

#include "cityjson/function_docs.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"

#include <set>

namespace duckdb {
namespace cityjson {

std::string DeriveExtensionNamespace(const std::string &name) {
	std::string ns;
	for (const auto raw : name) {
		const auto c = static_cast<char>(std::tolower(static_cast<unsigned char>(raw)));
		if ((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')) {
			ns.push_back(c);
		}
	}
	return ns;
}

bool IsValidExtensionNamespace(const std::string &ns) {
	if (ns.empty() || ns[0] < 'a' || ns[0] > 'z') {
		return false;
	}
	for (const auto c : ns) {
		if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9'))) {
			return false;
		}
	}
	return true;
}

namespace {

void CheckUnique(const std::vector<ExtensionDeclaration> &declarations, const ExtensionDeclaration &candidate,
                 const std::string &function) {
	for (const auto &existing : declarations) {
		if (existing.ns == candidate.ns) {
			throw InvalidInputException("%s: the extensions '%s' and '%s' both derive the namespace '%s'; a namespace "
			                            "denotes one extension in a package",
			                            function, existing.name, candidate.name, candidate.ns);
		}
	}
}

} // namespace

std::vector<ExtensionDeclaration> DeclarationsFromCityJSON(const std::map<std::string, Extension> &extensions,
                                                           const std::string &function) {
	std::vector<ExtensionDeclaration> result;
	for (const auto &entry : extensions) {
		ExtensionDeclaration declaration;
		declaration.name = entry.first;
		declaration.ns = DeriveExtensionNamespace(entry.first);
		declaration.url = entry.second.url;
		if (!entry.second.version.empty()) {
			declaration.version = entry.second.version;
		}
		if (!IsValidExtensionNamespace(declaration.ns)) {
			throw InvalidInputException("%s: the extension '%s' derives the namespace '%s', which an extension "
			                            "namespace cannot be: it must match ^[a-z][a-z0-9]*$",
			                            function, declaration.name, declaration.ns);
		}
		CheckUnique(result, declaration, function);
		result.push_back(std::move(declaration));
	}
	return result;
}

json DeclarationsToFooterJson(const std::vector<ExtensionDeclaration> &declarations) {
	json result = json::object();
	for (const auto &declaration : declarations) {
		json entry = json::object();
		entry["name"] = declaration.name;
		entry["url"] = declaration.url;
		if (declaration.version.has_value()) {
			entry["version"] = declaration.version.value();
		}
		result[declaration.ns] = std::move(entry);
	}
	return result;
}

std::vector<ExtensionDeclaration> DeclarationsFromFooterJson(const json &extensions, const std::string &function) {
	if (!extensions.is_object()) {
		throw InvalidInputException("%s: city.extensions must be a JSON object keyed by namespace, got %s", function,
		                            extensions.type_name());
	}
	std::vector<ExtensionDeclaration> result;
	for (auto it = extensions.begin(); it != extensions.end(); ++it) {
		const auto &value = it.value();
		if (!IsValidExtensionNamespace(it.key())) {
			throw InvalidInputException("%s: city.extensions has the key '%s', which is not an extension namespace "
			                            "(^[a-z][a-z0-9]*$)",
			                            function, it.key());
		}
		if (!value.is_object() || !value.contains("name") || !value["name"].is_string() || !value.contains("url") ||
		    !value["url"].is_string()) {
			throw InvalidInputException(
			    "%s: city.extensions['%s'] must be an object with a string `name` and a string `url`", function,
			    it.key());
		}
		ExtensionDeclaration declaration;
		declaration.ns = it.key();
		declaration.name = value["name"].get<std::string>();
		declaration.url = value["url"].get<std::string>();
		if (value.contains("version") && value["version"].is_string()) {
			declaration.version = value["version"].get<std::string>();
		}
		result.push_back(std::move(declaration));
	}
	return result;
}

json DeclarationsToCityJSON(const std::vector<ExtensionDeclaration> &declarations) {
	json result = json::object();
	for (const auto &declaration : declarations) {
		json entry = json::object();
		entry["url"] = declaration.url;
		if (declaration.version.has_value()) {
			entry["version"] = declaration.version.value();
		}
		result[declaration.name] = std::move(entry);
	}
	return result;
}

bool IsPlusName(const std::string &name) {
	return name.size() > 1 && name[0] == '+';
}

std::string PrefixPlusName(const std::string &plus_name, const std::string &ns) {
	return ns + "_" + plus_name.substr(1);
}

const ExtensionDeclaration *DeclarationForName(const std::string &name,
                                               const std::vector<ExtensionDeclaration> &declarations) {
	for (const auto &declaration : declarations) {
		const auto prefix = declaration.ns + "_";
		if (name.size() > prefix.size() && name.compare(0, prefix.size(), prefix) == 0) {
			return &declaration;
		}
	}
	return nullptr;
}

std::string RestorePlusName(const std::string &name, const std::vector<ExtensionDeclaration> &declarations) {
	const auto *declaration = DeclarationForName(name, declarations);
	if (declaration == nullptr) {
		return name;
	}
	return "+" + name.substr(declaration->ns.size() + 1);
}

void RenameSurfaceTypes(json &surfaces, const std::function<std::string(const std::string &)> &rename) {
	if (!surfaces.is_array()) {
		return;
	}
	for (auto &surface : surfaces) {
		if (!surface.is_object()) {
			continue;
		}
		auto type = surface.find("type");
		if (type != surface.end() && type->is_string()) {
			*type = rename(type->get<std::string>());
		}
	}
}

void CollectSurfaceTypes(const json &surfaces, std::set<std::string> &types) {
	if (!surfaces.is_array()) {
		return;
	}
	for (const auto &surface : surfaces) {
		if (!surface.is_object()) {
			continue;
		}
		auto type = surface.find("type");
		if (type != surface.end() && type->is_string()) {
			types.insert(type->get<std::string>());
		}
	}
}

namespace {

json ParseOrThrow(const std::string &text, const char *function, const char *what) {
	try {
		return json_utils::ParseJson(text);
	} catch (const std::exception &e) {
		throw InvalidInputException("%s: %s is not valid JSON: %s", function, what, e.what());
	}
}

void PrefixSurfaceTypesFunction(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    args.data[0], args.data[1], result, args.size(), [&](string_t surfaces_text, string_t ns_text) {
		    const auto ns = ns_text.GetString();
		    if (!IsValidExtensionNamespace(ns)) {
			    throw InvalidInputException("cityparquet_prefix_surface_types: '%s' is not an extension namespace "
			                                "(^[a-z][a-z0-9]*$)",
			                                ns);
		    }
		    auto surfaces = ParseOrThrow(surfaces_text.GetString(), "cityparquet_prefix_surface_types", "surfaces");
		    RenameSurfaceTypes(surfaces, [&ns](const std::string &type) {
			    return IsPlusName(type) ? PrefixPlusName(type, ns) : type;
		    });
		    return StringVector::AddString(result, surfaces.dump());
	    });
}

//! `city` with `extensions` merged into its `extensions` object. NULL `city` stands for
//! an empty footer; NULL `extensions` leaves `city` as it is. A namespace the two sides
//! declare differently is refused: it would denote two extensions in one package.
void MergeExtensionsFunction(DataChunk &args, ExpressionState &, Vector &result) {
	auto &city_vector = args.data[0];
	auto &extensions_vector = args.data[1];
	UnifiedVectorFormat city_format;
	UnifiedVectorFormat extensions_format;
	city_vector.ToUnifiedFormat(args.size(), city_format);
	extensions_vector.ToUnifiedFormat(args.size(), extensions_format);
	auto cities = UnifiedVectorFormat::GetData<string_t>(city_format);
	auto incoming = UnifiedVectorFormat::GetData<string_t>(extensions_format);

	result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
	auto out = FlatVector::GetData<string_t>(result);
	auto &validity = FlatVector::Validity(result);

	static const char *const FUNCTION = "cityparquet_merge_extensions";
	for (idx_t row = 0; row < args.size(); row++) {
		const auto city_index = city_format.sel->get_index(row);
		const auto extensions_index = extensions_format.sel->get_index(row);
		const bool has_city = city_format.validity.RowIsValid(city_index);
		const bool has_extensions = extensions_format.validity.RowIsValid(extensions_index);
		if (!has_extensions) {
			if (has_city) {
				out[row] = StringVector::AddString(result, cities[city_index]);
			} else {
				validity.SetInvalid(row);
			}
			continue;
		}
		json city = json::object();
		if (has_city) {
			city = ParseOrThrow(cities[city_index].GetString(), FUNCTION, "city");
			if (!city.is_object()) {
				throw InvalidInputException("%s: city is not a JSON object", FUNCTION);
			}
		}
		const auto additions = DeclarationsFromFooterJson(
		    ParseOrThrow(incoming[extensions_index].GetString(), FUNCTION, "extensions"), FUNCTION);
		std::vector<ExtensionDeclaration> merged;
		if (city.contains("extensions") && !city["extensions"].is_null()) {
			merged = DeclarationsFromFooterJson(city["extensions"], FUNCTION);
		}
		for (const auto &addition : additions) {
			bool present = false;
			for (const auto &existing : merged) {
				if (existing.ns != addition.ns) {
					continue;
				}
				present = true;
				if (!(existing == addition)) {
					throw InvalidInputException(
					    "%s: the namespace '%s' is declared for the extension '%s' (%s) and for '%s' (%s); a "
					    "namespace denotes one extension in a package, declared the same way in every file",
					    FUNCTION, addition.ns, existing.name, existing.url, addition.name, addition.url);
				}
			}
			if (!present) {
				merged.push_back(addition);
			}
		}
		city["extensions"] = DeclarationsToFooterJson(merged);
		out[row] = StringVector::AddString(result, city.dump());
	}
	if (args.AllConstant()) {
		result.SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);
	}
}

} // namespace

void RegisterCityParquetExtensionFunctions(ExtensionLoader &loader) {
	ScalarFunction prefix("cityparquet_prefix_surface_types",
	                      {LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR)},
	                      LogicalType(LogicalTypeId::VARCHAR), PrefixSurfaceTypesFunction);
	prefix.SetFallible();
	RegisterDocumented(
	    loader, std::move(prefix),
	    {{"surfaces", "namespace"},
	     "Rewrites every CityJSON Extension surface type (a `+` name) in a geometry_properties surfaces JSON array to "
	     "its namespace-prefixed CityParquet form: +PartyWallSurface becomes energy_PartyWallSurface.",
	     R"(cityparquet_prefix_surface_types('[{"type": "+PartyWallSurface"}]', 'energy'))",
	     {"cityparquet", "extensions"}});

	ScalarFunction merge("cityparquet_merge_extensions",
	                     {LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR)},
	                     LogicalType(LogicalTypeId::VARCHAR), MergeExtensionsFunction);
	merge.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	merge.SetFallible();
	RegisterDocumented(
	    loader, std::move(merge),
	    {{"city", "extensions"},
	     "Returns a CityParquet city footer JSON with a city.extensions object (keyed by namespace) merged into its "
	     "extensions; a namespace declared differently on the two sides is an error.",
	     R"(cityparquet_merge_extensions(NULL, '{"energy": {"name": "Energy", "url": "https://example.org/e.json"}}'))",
	     {"cityparquet", "extensions"}});
}

} // namespace cityjson
} // namespace duckdb
