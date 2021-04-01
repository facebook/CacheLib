#pragma once

#include <folly/container/F14Map.h>
#include <folly/logging/xlog.h>

#include <map>
#include <memory>
#include <scoped_allocator>
#include <string>
#include <vector>

#include "cachelib/experimental/objcache/Allocator.h"
#include "cachelib/experimental/objcache/ObjectCache.h"

namespace facebook {
namespace cachelib {
namespace benchmark {
using namespace objcache;

using LruObjectCache =
    ObjectCache<CacheDescriptor<LruAllocator>,
                MonotonicBufferResource<CacheDescriptor<LruAllocator>>>;

template <typename T>
using LruObjectCacheAlloc = typename LruObjectCache::Alloc<T>;

using String =
    std::basic_string<char, std::char_traits<char>, LruObjectCacheAlloc<char>>;
template <typename K, typename V>
using Map =
    std::map<K, V, std::less<K>, LruObjectCacheAlloc<std::pair<const K, V>>>;
template <typename T>
using Vector = std::vector<T, LruObjectCacheAlloc<char>>;

template <typename K, typename V>
using F14Map = folly::F14FastMap<K,
                                 V,
                                 std::hash<K>,
                                 std::equal_to<K>,
                                 LruObjectCacheAlloc<std::pair<const K, V>>>;
} // namespace benchmark
} // namespace cachelib
} // namespace facebook
