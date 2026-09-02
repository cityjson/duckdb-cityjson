# Overlay for tinygltf 2.9.7 (baseline 84bab45d415d22042bd0b9081aea57f362da3f35).
#
# vcpkg's port at that baseline records a stale SHA512 for the v2.9.7 GitHub tag
# archive: GitHub has since regenerated syoyo/tinygltf's archive for that tag,
# changing its bytes (and therefore its hash) without changing the tag's
# content. Verified independently with `curl … | sha512sum`, matching what
# vcpkg's own download produced -- not a transient network fault. Upstream has
# fixed the analogous problem for tinygltf 3.0.0 (microsoft/vcpkg#53226,
# 2026-08-06) but never touched the 2.9.7 entry. Delete this overlay port once
# upstream corrects `versions/t-/tinygltf.json`'s 2.9.7 entry, or once the
# project moves off 2.9.7 (see `overrides` in ../../vcpkg.json).
#
# Header-only library
vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO syoyo/tinygltf
    REF "v${VERSION}"
    SHA512 553c7ad329da5a4d46235747db9d937957d5698e74c8d1751c17da5a1d09d35f4212e2476652a63ee1a12ff74220531b5e288eaaddb1014d47000a82d30f03a2
    HEAD_REF master
)

# Put the licence file where vcpkg expects it
# Copy the tinygltf header files and fix the path to json
vcpkg_replace_string("${SOURCE_PATH}/tiny_gltf.h" "#include \"json.hpp\"" "#include <nlohmann/json.hpp>")
file(INSTALL "${SOURCE_PATH}/tiny_gltf.h" DESTINATION "${CURRENT_PACKAGES_DIR}/include")

vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/LICENSE")
