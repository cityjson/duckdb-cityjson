#pragma once

#include "cityjson/cityjson_types.hpp"
#include "cityjson/json_utils.hpp"
#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <functional>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <vector>

namespace duckdb {
namespace cityjson {

/**
 * Extension namespaces (spec 06-extensions.mdx).
 *
 * Every name a CityJSON Extension adds to a package carries the extension's
 * namespace as a prefix: the attribute `+heatCapacity` becomes the column
 * `energy_heatCapacity`, the semantic surface `+PartyWallSurface` becomes
 * `energy_PartyWallSurface`, and the class `+Building` becomes the `object_type`
 * `energy_Building`. The package declares each namespace once, in the footer's
 * `city.extensions` object; on export the prefix becomes CityJSON's `+` again and the
 * document's `extensions` member is rebuilt from the declarations.
 *
 * Attribution is implemented for a source that declares exactly one extension: every
 * `+` name belongs to it. Attributing names across several extensions needs their
 * schema documents, which this extension does not read, so such a source is refused.
 */

//! One entry of `city.extensions`, keyed there by `ns`.
struct ExtensionDeclaration {
	//! The namespace: `^[a-z][a-z0-9]*$`, unique within a package.
	std::string ns;
	//! The extension's name in the source: the key of the CityJSON `extensions` member.
	std::string name;
	//! The extension's schema document.
	std::string url;
	std::optional<std::string> version;

	bool operator==(const ExtensionDeclaration &other) const {
		return ns == other.ns && name == other.name && url == other.url && version == other.version;
	}
};

//! A CityJSON extension name lower-cased, with every character outside `[a-z0-9]`
//! removed: "Energy" -> "energy", "Energy ADE" -> "energyade". The result may be empty
//! or start with a digit; IsValidExtensionNamespace says whether it may be used.
std::string DeriveExtensionNamespace(const std::string &name);

//! True when `ns` matches `^[a-z][a-z0-9]*$`.
bool IsValidExtensionNamespace(const std::string &ns);

//! The declarations for a CityJSON document's `extensions` member, in the member's key
//! order. Throws InvalidInputException, naming `function`, when a namespace derives
//! empty or not starting with a letter, or when two extensions derive the same one.
std::vector<ExtensionDeclaration> DeclarationsFromCityJSON(const std::map<std::string, Extension> &extensions,
                                                           const std::string &function);

//! The `city.extensions` object for these declarations.
json DeclarationsToFooterJson(const std::vector<ExtensionDeclaration> &declarations);

//! Parses a `city.extensions` object. Throws InvalidInputException, naming `function`,
//! when it is not an object of `{name, url[, version]}` entries keyed by valid namespaces.
std::vector<ExtensionDeclaration> DeclarationsFromFooterJson(const json &extensions, const std::string &function);

//! The CityJSON `extensions` member rebuilt from declarations: `name` is the key,
//! `url` and `version` its members.
json DeclarationsToCityJSON(const std::vector<ExtensionDeclaration> &declarations);

//! True when `name` carries CityJSON's extension marker.
bool IsPlusName(const std::string &name);

//! `+heatCapacity` with namespace `energy` -> `energy_heatCapacity`.
std::string PrefixPlusName(const std::string &plus_name, const std::string &ns);

//! The declaration whose prefix (`<ns>_`) starts `name`, or nullptr. Exact, case-sensitive
//! match: the namespace is lower-case by construction.
const ExtensionDeclaration *DeclarationForName(const std::string &name,
                                               const std::vector<ExtensionDeclaration> &declarations);

//! `energy_heatCapacity` -> `+heatCapacity` when `energy` is declared; unchanged otherwise.
std::string RestorePlusName(const std::string &name, const std::vector<ExtensionDeclaration> &declarations);

//! Applies `rename` to the `type` of every surface object in a semantics `surfaces`
//! array, in place. Anything that is not an array of objects is left as it is.
void RenameSurfaceTypes(json &surfaces, const std::function<std::string(const std::string &)> &rename);

//! The `type` of every surface object in a semantics `surfaces` array.
void CollectSurfaceTypes(const json &surfaces, std::set<std::string> &types);

//! Registers the scalar helpers the package layer's generated SQL calls:
//! `cityparquet_prefix_surface_types(surfaces, namespace)` and
//! `cityparquet_merge_extensions(city, extensions)`.
void RegisterCityParquetExtensionFunctions(ExtensionLoader &loader);

} // namespace cityjson
} // namespace duckdb
