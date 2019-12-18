#include "cachelib/allocator/CacheAllocator.h"

namespace facebook {
namespace cachelib {
template class CacheAllocator<LruCacheTrait>;
template class CacheAllocator<LruCacheWithSpinBucketsTrait>;
template class CacheAllocator<Lru2QCacheTrait>;
template class CacheAllocator<TinyLFUCacheTrait>;
} // namespace cachelib
} // namespace facebook
