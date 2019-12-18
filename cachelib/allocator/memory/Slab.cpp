#include "cachelib/allocator/memory/Slab.h"

using namespace facebook::cachelib;

// definition to inline static const for ODR
// http://en.cppreference.com/w/cpp/language/definition refer odr-use here.
constexpr size_t Slab::kSize;
constexpr ClassId Slab::kInvalidClassId;
constexpr PoolId Slab::kInvalidPoolId;
