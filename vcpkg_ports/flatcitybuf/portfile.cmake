vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO cityjson/flatcitybuf
    REF 72e5b68d469aa00a75ccba23780e2063751e3cff
    SHA512 779f0be2840d7b7869a165ae855821537c4ee069ef76c1f904c4ae0f8d548a0d3ac372009324864c4d4e8f02f23fa1528e04d921fb3758f683e081024e2ab131
)

vcpkg_cmake_configure(
    SOURCE_PATH "${SOURCE_PATH}/src/cpp"
    OPTIONS
        -DFCB_WITH_JSON=ON
        -DFCB_WITH_CURL=OFF
        -DFCB_BUILD_TESTS=OFF
        -DFCB_BUILD_EXAMPLES=OFF
)

vcpkg_cmake_install()
vcpkg_cmake_config_fixup(PACKAGE_NAME flatcitybuf CONFIG_PATH lib/cmake/flatcitybuf)
vcpkg_copy_pdbs()

file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")

file(INSTALL "${SOURCE_PATH}/LICENSE" DESTINATION "${CURRENT_PACKAGES_DIR}/share/${PORT}" RENAME copyright)
