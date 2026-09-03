// The single translation unit that instantiates the header-only third-party
// library. Every other file includes the header without the *_IMPLEMENTATION
// macro; defining it twice is an ODR violation that only shows up at link.
//
// tinygltf: JSON via the nlohmann-json already linked (the vcpkg port rewrote its
// include); images never decoded -- they travel as raw bytes in bufferViews or files.
#define TINYGLTF_IMPLEMENTATION
#include <tiny_gltf.h>
