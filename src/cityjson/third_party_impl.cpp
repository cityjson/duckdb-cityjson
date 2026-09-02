// The single translation unit that instantiates the header-only third-party
// libraries. Every other file includes the headers without the *_IMPLEMENTATION
// macro; defining one of them twice is an ODR violation that only shows up at link.
#define TINYOBJLOADER_IMPLEMENTATION
#include <tiny_obj_loader.h>
