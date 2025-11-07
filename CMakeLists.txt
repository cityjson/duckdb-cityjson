cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME cityjson)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
find_package(OpenSSL REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(CJSEQ_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src/external/cjseq)

set(EXTENSION_SOURCES src/cityjson_extension.cpp)

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

# Link OpenSSL in both the static library as the loadable extension
set_property(TARGET ${EXTENSION_NAME} APPEND PROPERTY LINK_LIBRARIES OpenSSL::SSL OpenSSL::Crypto)
set_property(TARGET ${LOADABLE_EXTENSION_NAME} APPEND PROPERTY LINK_LIBRARIES OpenSSL::SSL OpenSSL::Crypto)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
