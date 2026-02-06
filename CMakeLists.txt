cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME cityjson)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
find_package(OpenSSL REQUIRED)
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
    # Utility layer
    src/cityjson/city_object_utils.cpp
    # Reader layer
    src/cityjson/reader.cpp
    src/cityjson/local_cityjson_reader.cpp
    src/cityjson/local_cityjsonseq_reader.cpp
    src/cityjson/reader_factory.cpp
    # Geometry encoding layer
    src/cityjson/wkb_encoder.cpp
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
)

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

# Link OpenSSL and nlohmann_json in both the static library as the loadable extension
set_property(TARGET ${EXTENSION_NAME} APPEND PROPERTY LINK_LIBRARIES OpenSSL::SSL OpenSSL::Crypto nlohmann_json::nlohmann_json)
set_property(TARGET ${LOADABLE_EXTENSION_NAME} APPEND PROPERTY LINK_LIBRARIES OpenSSL::SSL OpenSSL::Crypto nlohmann_json::nlohmann_json)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
