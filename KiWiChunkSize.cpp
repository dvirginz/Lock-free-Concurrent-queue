#define MAX_ITEMS 50
// TODO: restore this include and fix the compiler errors.
//#include "config.hpp"
#include <atomic>

std::atomic<int> g_chunk_size{MAX_ITEMS};
