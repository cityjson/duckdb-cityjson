cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME cityjson)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
find_package(nlohmann_json REQUIRED)
find_package(pugixml REQUIRED)
find_package(OpenSSL QUIET)

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
    src/cityjson/wkb_decoder.cpp
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
    src/cityjson/flatcitybuf_reader.cpp
    src/cityjson/flatcitybuf_table_function.cpp
    # CityGML reader
    src/cityjson/citygml_parser.cpp
    src/cityjson/local_citygml_reader.cpp
    # COPY TO
    src/cityjson/copy_function.cpp
    src/cityjson/cityjson_writer.cpp
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

# Optional: FlatCityBuf support
# To enable, set -DCITYJSON_ENABLE_FCB=ON
# Downloads pre-built libfcb_cpp binaries from GitHub releases
option(CITYJSON_ENABLE_FCB "Enable FlatCityBuf (.fcb) support" ON)
set(FCB_VERSION "v0.7.6" CACHE STRING "FlatCityBuf release version to download")

# Auto-disable FCB on Linux when the pre-built binary is incompatible.
# The libfcb_cpp.a binary requires glibc 2.32+ (__libc_single_threaded symbol)
# and dynamically links OpenSSL via native-tls. Disable on musl and old glibc.
if(CITYJSON_ENABLE_FCB AND CMAKE_SYSTEM_NAME STREQUAL "Linux")
  include(CheckSymbolExists)
  include(CheckCSourceCompiles)
  check_symbol_exists(__GLIBC__ "stdlib.h" HAVE_GLIBC)
  if(NOT HAVE_GLIBC)
    message(STATUS "FlatCityBuf: disabling on non-glibc Linux (pre-built binary requires glibc)")
    set(CITYJSON_ENABLE_FCB OFF)
  else()
    # Check for glibc 2.32+ symbol required by the pre-built binary
    check_c_source_compiles("
      extern char __libc_single_threaded;
      int main() { return __libc_single_threaded; }
    " HAVE_LIBC_SINGLE_THREADED)
    if(NOT HAVE_LIBC_SINGLE_THREADED)
      message(STATUS "FlatCityBuf: disabling (glibc too old, missing __libc_single_threaded from glibc 2.32+)")
      set(CITYJSON_ENABLE_FCB OFF)
    endif()
  endif()
endif()

if(CITYJSON_ENABLE_FCB)
  # Detect platform — gracefully disable on unsupported platforms
  if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(FCB_OS "macos")
  elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(FCB_OS "linux")
  elseif(CMAKE_SYSTEM_NAME STREQUAL "Windows")
    set(FCB_OS "windows")
  else()
    message(STATUS "FlatCityBuf: disabling on unsupported platform ${CMAKE_SYSTEM_NAME}")
    set(CITYJSON_ENABLE_FCB OFF)
  endif()
endif()

if(CITYJSON_ENABLE_FCB)
  # Normalise architecture name.
  # On macOS, CMAKE_OSX_ARCHITECTURES reflects the actual target arch during
  # cross-compilation (e.g. building x86_64 on an arm64 host), whereas
  # CMAKE_SYSTEM_PROCESSOR always reports the host architecture.
  set(FCB_DETECTED_ARCH "${CMAKE_SYSTEM_PROCESSOR}")
  if(CMAKE_SYSTEM_NAME STREQUAL "Darwin" AND CMAKE_OSX_ARCHITECTURES)
    set(FCB_DETECTED_ARCH "${CMAKE_OSX_ARCHITECTURES}")
  endif()

  if(FCB_DETECTED_ARCH MATCHES "arm64|aarch64|ARM64")
    set(FCB_ARCH "aarch64")
  elseif(FCB_DETECTED_ARCH MATCHES "x86_64|AMD64|amd64")
    set(FCB_ARCH "x86_64")
  else()
    message(STATUS "FlatCityBuf: disabling on unsupported architecture ${FCB_DETECTED_ARCH}")
    set(CITYJSON_ENABLE_FCB OFF)
  endif()

  # The macOS x86_64 pre-built binary in the current release is actually arm64
  # (upstream packaging issue). Disable FCB for this target until fixed.
  if(FCB_OS STREQUAL "macos" AND FCB_ARCH STREQUAL "x86_64")
    message(STATUS "FlatCityBuf: disabling on macOS x86_64 (no valid pre-built binary)")
    set(CITYJSON_ENABLE_FCB OFF)
  endif()
endif()

if(CITYJSON_ENABLE_FCB)
  # Download pre-built binary
  set(FCB_DIR "${CMAKE_BINARY_DIR}/fcb_cpp")
  if(FCB_OS STREQUAL "windows")
    set(FCB_ARCHIVE "fcb_cpp-${FCB_OS}-${FCB_ARCH}.zip")
  else()
    set(FCB_ARCHIVE "fcb_cpp-${FCB_OS}-${FCB_ARCH}.tar.gz")
  endif()
  set(FCB_URL "https://github.com/cityjson/flatcitybuf/releases/download/${FCB_VERSION}/${FCB_ARCHIVE}")
  set(FCB_DOWNLOAD_PATH "${CMAKE_BINARY_DIR}/${FCB_ARCHIVE}")

  if(NOT EXISTS "${FCB_DIR}/libfcb_cpp.a" AND NOT EXISTS "${FCB_DIR}/fcb_cpp.lib")
    message(STATUS "Downloading FlatCityBuf ${FCB_VERSION} for ${FCB_OS}-${FCB_ARCH}...")
    file(DOWNLOAD "${FCB_URL}" "${FCB_DOWNLOAD_PATH}" STATUS FCB_DL_STATUS)
    list(GET FCB_DL_STATUS 0 FCB_DL_CODE)
    if(NOT FCB_DL_CODE EQUAL 0)
      list(GET FCB_DL_STATUS 1 FCB_DL_MSG)
      message(WARNING "Failed to download FlatCityBuf: ${FCB_DL_MSG}\nURL: ${FCB_URL}")
      set(CITYJSON_ENABLE_FCB OFF)
    endif()
  endif()
endif()

if(CITYJSON_ENABLE_FCB)
  if(NOT EXISTS "${FCB_DIR}/libfcb_cpp.a" AND NOT EXISTS "${FCB_DIR}/fcb_cpp.lib")
    file(MAKE_DIRECTORY "${FCB_DIR}")
    # Use "xf" (auto-detect) rather than "xzf" (gzip-only) so both .tar.gz
    # and .zip archives are extracted correctly.
    execute_process(
      COMMAND ${CMAKE_COMMAND} -E tar xf "${FCB_DOWNLOAD_PATH}"
      WORKING_DIRECTORY "${FCB_DIR}"
      RESULT_VARIABLE FCB_EXTRACT_RESULT
    )
    if(NOT FCB_EXTRACT_RESULT EQUAL 0)
      message(WARNING "Failed to extract FlatCityBuf archive")
      set(CITYJSON_ENABLE_FCB OFF)
    endif()
  endif()
endif()

if(CITYJSON_ENABLE_FCB)
  # Verify the expected library file exists after download/extraction
  if(FCB_OS STREQUAL "windows")
    set(FCB_LIB_CHECK "${FCB_DIR}/fcb_cpp.lib")
  else()
    set(FCB_LIB_CHECK "${FCB_DIR}/libfcb_cpp.a")
  endif()
  if(NOT EXISTS "${FCB_LIB_CHECK}")
    message(WARNING "FlatCityBuf: expected library not found at ${FCB_LIB_CHECK}, disabling")
    set(CITYJSON_ENABLE_FCB OFF)
  endif()
endif()

if(CITYJSON_ENABLE_FCB)
  # Determine the static library path
  if(FCB_OS STREQUAL "windows")
    set(FCB_LIB_PATH "${FCB_DIR}/fcb_cpp.lib")
  else()
    set(FCB_LIB_PATH "${FCB_DIR}/libfcb_cpp.a")
  endif()

  # Compile lib.rs.cc (CXX bridge source) as part of the extension
  target_sources(${EXTENSION_NAME} PRIVATE "${FCB_DIR}/lib.rs.cc")
  target_sources(${LOADABLE_EXTENSION_NAME} PRIVATE "${FCB_DIR}/lib.rs.cc")

  # Add FCB include directory
  target_include_directories(${EXTENSION_NAME} PRIVATE "${FCB_DIR}")
  target_include_directories(${LOADABLE_EXTENSION_NAME} PRIVATE "${FCB_DIR}")

  # Set compile definition
  target_compile_definitions(${EXTENSION_NAME} PRIVATE CITYJSON_HAS_FCB)
  target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE CITYJSON_HAS_FCB)

  # Link the FCB static library + platform dependencies.
  # The loadable extension (.duckdb_extension shared lib) links directly.
  set_property(TARGET ${LOADABLE_EXTENSION_NAME} APPEND PROPERTY LINK_LIBRARIES "${FCB_LIB_PATH}")

  # Platform-specific dependencies required by the FCB Rust static library.
  # Use find_library() on macOS to get absolute framework paths (avoids
  # "-framework X" space-in-string issues with CMake list operations).
  if(FCB_OS STREQUAL "macos")
    find_library(SECURITY_FRAMEWORK Security REQUIRED)
    find_library(COREFOUNDATION_FRAMEWORK CoreFoundation REQUIRED)
    find_library(SYSTEMCONFIGURATION_FRAMEWORK SystemConfiguration REQUIRED)
    set(FCB_PLATFORM_LIBS ${SECURITY_FRAMEWORK} ${COREFOUNDATION_FRAMEWORK} ${SYSTEMCONFIGURATION_FRAMEWORK})
  elseif(FCB_OS STREQUAL "linux")
    set(FCB_PLATFORM_LIBS pthread dl m OpenSSL::SSL OpenSSL::Crypto)
  elseif(FCB_OS STREQUAL "windows")
    set(FCB_PLATFORM_LIBS ws2_32 bcrypt userenv ntdll)
  endif()
  set_property(TARGET ${LOADABLE_EXTENSION_NAME} APPEND PROPERTY LINK_LIBRARIES ${FCB_PLATFORM_LIBS})

  # For the static extension: propagate FCB dependencies to any target that
  # links libcityjson_extension.a (unittest, libduckdb.dylib, shell/duckdb).
  # INTERFACE_LINK_LIBRARIES ensures transitive propagation through CMake.
  set_property(TARGET ${EXTENSION_NAME} APPEND PROPERTY INTERFACE_LINK_LIBRARIES "${FCB_LIB_PATH}" ${FCB_PLATFORM_LIBS})

  # Also set DUCKDB_EXTRA_LINK_FLAGS for the shell target (belt-and-suspenders).
  set(DUCKDB_EXTRA_LINK_FLAGS "${FCB_LIB_PATH};${FCB_PLATFORM_LIBS}" CACHE INTERNAL "Extra link flags" FORCE)

  message(STATUS "FlatCityBuf support enabled (${FCB_VERSION}, ${FCB_OS}-${FCB_ARCH})")
else()
  # When FCB is disabled, replace the FCB test file with a no-op so the test
  # runner doesn't try to call non-existent flatcitybuf functions.
  set(FCB_TEST_FILE "${CMAKE_CURRENT_SOURCE_DIR}/test/sql/cityjson_e2e_fcb.test")
  if(EXISTS "${FCB_TEST_FILE}")
    file(WRITE "${FCB_TEST_FILE}"
      "# name: test/sql/cityjson_e2e_fcb.test\n"
      "# description: FlatCityBuf tests (skipped — FCB not available on this platform)\n"
      "# group: [sql]\n"
      "\n"
      "require cityjson\n"
      "\n"
      "# FCB support was disabled at build time\n"
      "query I\n"
      "SELECT 1;\n"
      "----\n"
      "1\n"
    )
    message(STATUS "FlatCityBuf support disabled — FCB test replaced with no-op")
  endif()
endif()

# Find OpenSSL (already found early in the file to make IMPORTED targets
# available before DuckDB generates its extension targets)
if(NOT TARGET OpenSSL::SSL)
  find_package(OpenSSL REQUIRED)
endif()

# Link OpenSSL and nlohmann_json in both the static library and the loadable extension
# Use imported targets if available, fall back to raw variables otherwise
if(TARGET OpenSSL::SSL)
  set_property(TARGET ${EXTENSION_NAME} APPEND PROPERTY LINK_LIBRARIES OpenSSL::SSL OpenSSL::Crypto nlohmann_json::nlohmann_json pugixml::pugixml)
  set_property(TARGET ${EXTENSION_NAME} APPEND PROPERTY INTERFACE_LINK_LIBRARIES OpenSSL::SSL OpenSSL::Crypto nlohmann_json::nlohmann_json pugixml::pugixml)
  set_property(TARGET ${LOADABLE_EXTENSION_NAME} APPEND PROPERTY LINK_LIBRARIES OpenSSL::SSL OpenSSL::Crypto nlohmann_json::nlohmann_json pugixml::pugixml)
else()
  target_include_directories(${EXTENSION_NAME} PRIVATE ${OPENSSL_INCLUDE_DIR})
  target_include_directories(${LOADABLE_EXTENSION_NAME} PRIVATE ${OPENSSL_INCLUDE_DIR})
  set_property(TARGET ${EXTENSION_NAME} APPEND PROPERTY LINK_LIBRARIES ${OPENSSL_LIBRARIES} nlohmann_json::nlohmann_json pugixml::pugixml)
  set_property(TARGET ${EXTENSION_NAME} APPEND PROPERTY INTERFACE_LINK_LIBRARIES ${OPENSSL_LIBRARIES} nlohmann_json::nlohmann_json pugixml::pugixml)
  set_property(TARGET ${LOADABLE_EXTENSION_NAME} APPEND PROPERTY LINK_LIBRARIES ${OPENSSL_LIBRARIES} nlohmann_json::nlohmann_json pugixml::pugixml)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
