// The single translation unit that instantiates the header-only third-party
// libraries. Every other file includes the headers without the *_IMPLEMENTATION
// macro; defining one of them twice is an ODR violation that only shows up at link.
#define TINYOBJLOADER_IMPLEMENTATION
#include <tiny_obj_loader.h>

// tinygltf: JSON via the nlohmann-json already linked (the vcpkg port rewrote its
// include); images never decoded -- they travel as raw bytes in bufferViews or files.
#define TINYGLTF_IMPLEMENTATION
#include <tiny_gltf.h>
