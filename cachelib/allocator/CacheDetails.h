#pragma once

#include <string>

// Conatains common symbols shared across different build targets. We provide
// a thin wrapper in ReadOnlySharedCacheView that depends on symbols used in
// allocator

namespace facebook {
namespace cachelib {
namespace detail {
// identifier for the metadata info
extern const std::string kShmInfoName;

// identifier for the main cache segment
extern const std::string kShmCacheName;

// identifier for the main hash table if used
extern const std::string kShmHashTableName;

// identifier for the auxilary hash table for chained items
extern const std::string kShmChainedItemHashTableName;

} // namespace detail
} // namespace cachelib
} // namespace facebook
