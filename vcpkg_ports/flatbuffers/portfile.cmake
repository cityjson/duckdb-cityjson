vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO google/flatbuffers
    REF v25.9.23
    SHA512 259ae6c0b024c19c882d87c93d6ba156c15f14a61b11846170ac1b9e9c051cd3e80ae93cfe20ccb1aa30f2085cdbd4127ffa229b42cabbfed6b035ca4851c127
)

vcpkg_cmake_configure(
    SOURCE_PATH "${SOURCE_PATH}"
    OPTIONS
        -DFLATBUFFERS_BUILD_TESTS=OFF
        -DFLATBUFFERS_BUILD_FLATC=OFF
        -DFLATBUFFERS_BUILD_FLATHASH=OFF
        -DFLATBUFFERS_BUILD_FLATLIB=ON
        -DFLATBUFFERS_BUILD_SHAREDLIB=OFF
        # The x64-linux triplet builds static libs without -fPIC by default (see
        # vcpkg's own triplets/x64-linux.cmake), but this static lib ends up linked
        # into a DuckDB *loadable* extension (.duckdb_extension, a shared object) --
        # needs PIC explicitly.
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
)

vcpkg_cmake_install()
vcpkg_cmake_config_fixup(PACKAGE_NAME flatbuffers CONFIG_PATH lib/cmake/flatbuffers)
vcpkg_copy_pdbs()

file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")

file(INSTALL "${SOURCE_PATH}/LICENSE" DESTINATION "${CURRENT_PACKAGES_DIR}/share/${PORT}" RENAME copyright)
