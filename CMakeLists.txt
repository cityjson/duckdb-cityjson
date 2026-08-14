# 3.24, not the previous 3.5: find_package(... GLOBAL) below (flatcitybuf) requires it.
cmake_minimum_required(VERSION 3.24)

# Set extension name here
set(TARGET_NAME cityjson)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
find_package(nlohmann_json REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(CJSEQ_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src/external/cjseq)

set(EXTENSION_SOURCES
    src/cityjson_extension.cpp
    # Foundation layer
    src/cityjson/error.cpp
    src/cityjson/types.cpp
    src/cityjson/json_utils.cpp
    src/cityjson/cityjson_types.cpp
    src/cityjson/column_types.cpp
    src/cityjson/temporal_parser.cpp
    # CRS -> PROJJSON resolution (embedded EPSG table)
    src/cityjson/crs_projjson.cpp
    src/cityjson/epsg_projjson_data.cpp
    src/cityjson/geoparquet_table_function.cpp
    # Utility layer
    src/cityjson/city_object_utils.cpp
    # Reader layer
    src/cityjson/reader.cpp
    src/cityjson/local_cityjson_reader.cpp
    src/cityjson/local_cityjsonseq_reader.cpp
    src/cityjson/reader_factory.cpp
    # Geometry encoding layer
    src/cityjson/wkb_encoder.cpp
    src/cityjson/wkb_decoder.cpp
    src/cityjson/arrow_native_encoder.cpp
    src/cityjson/wkb_extent.cpp
    src/cityjson/geometry_properties.cpp
    src/cityjson/lod_table.cpp
    # Vector writer layer
    src/cityjson/vector_writer.cpp
    # Table function layer
    src/cityjson/bind_data.cpp
    src/cityjson/global_state.cpp
    src/cityjson/local_state.cpp
    src/cityjson/bind_function.cpp
    src/cityjson/init_global.cpp
    src/cityjson/init_local.cpp
    src/cityjson/scan_function.cpp
    src/cityjson/optional_callbacks.cpp
    src/cityjson/table_function_registration.cpp
    # Metadata table
    src/cityjson/metadata_table.cpp
    src/cityjson/metadata_table_function.cpp
    # FlatCityBuf (compiled always but guarded by #ifdef CITYJSON_HAS_FCB)
    src/cityjson/duckdb_fs_range_reader.cpp
    src/cityjson/fcb_selective_convert.cpp
    src/cityjson/flatcitybuf_reader.cpp
    src/cityjson/flatcitybuf_table_function.cpp
    # COPY TO
    src/cityjson/copy_function.cpp
    src/cityjson/cityjson_writer.cpp
    # CityParquet package layer
    src/cityjson/cityparquet_package.cpp
    src/cityjson/cityparquet_validate.cpp
    src/cityjson/cityparquet_reconcile.cpp
    src/cityjson/cityparquet_delete.cpp
    src/cityjson/cityparquet_insert.cpp
    src/cityjson/cityparquet_merge.cpp
    src/cityjson/cityparquet_write.cpp
    src/cityjson/cityparquet_appearance.cpp
    src/cityjson/appearance_normalise.cpp
    src/cityjson/appearance_table_function.cpp
)

# Declared here rather than next to the find_package below, because the wasm block
# immediately after this needs to test it and runs earlier than that.
option(CITYJSON_ENABLE_FCB "Enable FlatCityBuf (.fcb) support" ON)

# Wasm: hand flatcitybuf's archives to the side-module link.
#
# On every other platform the loadable extension is a real shared library, so the
# LINK_LIBRARIES property set further down (flatcitybuf::flatcitybuf) is what pulls
# fcb:: in. Under Emscripten it is not: build_loadable_extension() makes the target a
# *static library*, for which LINK_LIBRARIES is inert, and then post-builds the real
# artifact with a separate command --
#   emcc <the archive> -o <name>.wasm -O3 -sSIDE_MODULE=2 -sEXPORTED_FUNCTIONS=... ${TO_BE_LINKED}
# (duckdb/extension/extension_build_tools.cmake:186-198). Only our own objects are on
# that line, and -sSIDE_MODULE leaves unresolved symbols as *imports* rather than
# erroring -- so the build succeeded while emitting a .wasm with 50 undefined fcb::
# imports, which dlopen then rejected at LOAD time with
#   bad export type for '_ZN3fcb11RangeReader10read_batch...': undefined
#
# TO_BE_LINKED is read from DUCKDB_EXTENSION_<NAME>_LINKED_LIBS, DuckDB's sanctioned
# knob for exactly this. It is consumed inside build_loadable_extension(), hence set
# here, before the call. The generator expressions are fine to reference targets that
# find_package(flatcitybuf) only creates further down: they resolve at generate time,
# not at this line. Space-separated, because the function runs separate_arguments() on
# it. flatbuffers is flatcitybuf's own dependency and needs listing explicitly for the
# same reason -- nothing transitive survives the trip through a plain emcc invocation.
if(EMSCRIPTEN AND CITYJSON_ENABLE_FCB)
  set(DUCKDB_EXTENSION_CITYJSON_LINKED_LIBS
      "$<TARGET_FILE:flatcitybuf::flatcitybuf> $<TARGET_FILE:flatbuffers::flatbuffers>")
endif()

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Optionally add cjseq C++ sources if available
set(CJSEQ_CPP_DIR ${CJSEQ_DIR}/cpp)
if(EXISTS ${CJSEQ_CPP_DIR})
  # If cjseq/cpp provides its own CMakeLists.txt, prefer that
  if(EXISTS ${CJSEQ_CPP_DIR}/CMakeLists.txt)
    add_subdirectory(${CJSEQ_CPP_DIR} cjseq_cpp_build)
  else()
    file(GLOB_RECURSE CJSEQ_CPP_SOURCES CONFIGURE_DEPENDS
      ${CJSEQ_CPP_DIR}/*.cc
      ${CJSEQ_CPP_DIR}/*.cpp
      ${CJSEQ_CPP_DIR}/src/*.cc
      ${CJSEQ_CPP_DIR}/src/*.cpp)
    add_library(cjseq STATIC ${CJSEQ_CPP_SOURCES})
    # Common include locations (adjust if cjseq/cpp uses different layout)
    target_include_directories(cjseq PUBLIC
      ${CJSEQ_CPP_DIR}
      ${CJSEQ_CPP_DIR}/include
      ${CJSEQ_CPP_DIR}/src)
  endif()
  # Link cjseq C++ library into both DuckDB extension targets if the target exists
  if(TARGET cjseq)
    set_property(TARGET ${EXTENSION_NAME} APPEND PROPERTY LINK_LIBRARIES cjseq)
    set_property(TARGET ${LOADABLE_EXTENSION_NAME} APPEND PROPERTY LINK_LIBRARIES cjseq)
  endif()
endif()

# Optional: FlatCityBuf support (native C++ library). In CI it is resolved from the
# HideBa/vcpkg git registry (flatcitybuf@0.8.1, upstream tag cpp-v0.8.1) — see the
# `registries` entry in vcpkg.json; local dev uses `just vendor-fcb`'s .vendor/prefix.
# Portable C++ source, so no platform/arch restrictions. The option itself is declared
# further up, next to build_loadable_extension, which needs to branch on it.
if(CITYJSON_ENABLE_FCB)
  # GLOBAL (CMake 3.24+): promotes the imported flatcitybuf::flatcitybuf target (and
  # its own transitive flatbuffers::flatbuffers dependency) to global visibility.
  # Without it, this find_package call's imported targets are only visible within
  # this subdirectory's own scope -- invisible to sibling directories (tools/,
  # tools/shell/, test/extension/, other extensions/) that DUCKDB_EXTRA_LINK_FLAGS
  # below needs to reach.
  find_package(flatcitybuf CONFIG REQUIRED GLOBAL)
  # Plain (non-keyword) set_property, not target_link_libraries(... PRIVATE ...): both
  # targets already receive plain-signature target_link_libraries calls elsewhere in
  # DuckDB's own extension_build_tools.cmake, and CMake forbids mixing plain and keyword
  # signatures for the same target (matches the existing cjseq linking above).
  set_property(TARGET ${EXTENSION_NAME} APPEND PROPERTY LINK_LIBRARIES flatcitybuf::flatcitybuf)
  set_property(TARGET ${LOADABLE_EXTENSION_NAME} APPEND PROPERTY LINK_LIBRARIES flatcitybuf::flatcitybuf)

  # The static extension (.a) gets linked into the shell/duckdb/unittest/plan_serializer
  # binaries as a raw archive file path (see tools/CMakeLists.txt, tools/shell/
  # CMakeLists.txt), not via target_link_libraries(<exe> cityjson_extension) -- so
  # CMake's normal transitive INTERFACE_LINK_LIBRARIES propagation from an imported
  # target never reaches them, and flatcitybuf's own symbols stay unresolved at that
  # final link step. DUCKDB_EXTRA_LINK_FLAGS is DuckDB's own sanctioned mechanism for
  # exactly this case (see its uses in extension_build_tools.cmake and tools/
  # CMakeLists.txt): unlike the old code's use of it (which stuffed literal
  # "OpenSSL::SSL"/"OpenSSL::Crypto" strings in here, referencing a target that this
  # file's own find_package(OpenSSL REQUIRED) below hadn't created yet on a fresh
  # configure -- see git history), this references flatcitybuf::flatcitybuf directly,
  # a target that already exists by this point, so there's no such ordering hazard.
  #
  # DUCKDB_EXTRA_LINK_FLAGS gets inserted BEFORE extension/cityjson/libcityjson_extension.a
  # in these binaries' link lines (its insertion point is controlled by tools/CMakeLists.txt
  # and extension_build_tools.cmake, not by us) -- but GNU ld resolves static-archive
  # symbols in a single left-to-right pass, so a dependency listed before its dependent
  # is invisible to it. --whole-archive/--no-whole-archive sidesteps that entirely by
  # forcing every object file in flatcitybuf (and its own flatbuffers dependency) into
  # the final binary unconditionally, regardless of position or reference order.
  # --whole-archive/--no-whole-archive is GNU ld/lld syntax -- Apple's ld64 and MSVC's
  # linker use different flags (and, unlike GNU's, need the resolved archive file path
  # rather than a bare target name), hence the three-way branch.
  if(APPLE)
    set(DUCKDB_EXTRA_LINK_FLAGS "-Wl,-force_load,$<TARGET_FILE:flatcitybuf::flatcitybuf>"
        CACHE INTERNAL "Extra link flags" FORCE)
  elseif(MSVC)
    set(DUCKDB_EXTRA_LINK_FLAGS "/WHOLEARCHIVE:$<TARGET_FILE:flatcitybuf::flatcitybuf>"
        CACHE INTERNAL "Extra link flags" FORCE)
  else()
    set(DUCKDB_EXTRA_LINK_FLAGS "-Wl,--whole-archive" "flatcitybuf::flatcitybuf" "-Wl,--no-whole-archive"
        CACHE INTERNAL "Extra link flags" FORCE)
  endif()

  # DUCKDB_EXTRA_LINK_FLAGS only reaches the binaries that actually reference it.
  # tools/shell/CMakeLists.txt:32 and tools/CMakeLists.txt:8 do, so the `duckdb`
  # shell links; duckdb/test/CMakeLists.txt:56 does NOT --
  #   target_link_libraries(unittest duckdb_static test_helpers)
  # -- so the test runner links the cityjson archive without flatcitybuf behind it
  # and fails on undefined fcb:: symbols, i.e. the whole SQL suite is unbuildable
  # whenever FCB is on (which is the default). The duckdb/ submodule is upstream and
  # not ours to patch, so fix it up from here instead.
  #
  # The `unittest` target does not exist yet at this point -- duckdb/test is added
  # later in the configure -- so the fixup is deferred to the end of the top-level
  # directory's processing, by which time it does. Guarded by if(TARGET) because
  # duckdb builds with tests disabled never create it.
  function(cityjson_fcb_link_unittest)
    if(TARGET unittest)
      set_property(TARGET unittest APPEND PROPERTY LINK_LIBRARIES ${DUCKDB_EXTRA_LINK_FLAGS})
    endif()
  endfunction()
  cmake_language(DEFER DIRECTORY ${CMAKE_SOURCE_DIR} CALL cityjson_fcb_link_unittest)

  target_compile_definitions(${EXTENSION_NAME} PRIVATE CITYJSON_HAS_FCB)
  target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE CITYJSON_HAS_FCB)
  message(STATUS "FlatCityBuf support enabled (native C++, vendored)")
endif()

# Find OpenSSL after DuckDB targets are set up
find_package(OpenSSL REQUIRED)

# Link OpenSSL and nlohmann_json in both the static library and the loadable extension
# Use imported targets if available, fall back to raw variables otherwise
if(TARGET OpenSSL::SSL)
  set_property(TARGET ${EXTENSION_NAME} APPEND PROPERTY LINK_LIBRARIES OpenSSL::SSL OpenSSL::Crypto nlohmann_json::nlohmann_json)
  set_property(TARGET ${LOADABLE_EXTENSION_NAME} APPEND PROPERTY LINK_LIBRARIES OpenSSL::SSL OpenSSL::Crypto nlohmann_json::nlohmann_json)
else()
  target_include_directories(${EXTENSION_NAME} PRIVATE ${OPENSSL_INCLUDE_DIR})
  target_include_directories(${LOADABLE_EXTENSION_NAME} PRIVATE ${OPENSSL_INCLUDE_DIR})
  set_property(TARGET ${EXTENSION_NAME} APPEND PROPERTY LINK_LIBRARIES ${OPENSSL_LIBRARIES} nlohmann_json::nlohmann_json)
  set_property(TARGET ${LOADABLE_EXTENSION_NAME} APPEND PROPERTY LINK_LIBRARIES ${OPENSSL_LIBRARIES} nlohmann_json::nlohmann_json)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
