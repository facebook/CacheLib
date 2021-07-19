#pragma once
#include "cachelib/allocator/ChainedHashTable.h"
#include "cachelib/allocator/MM2Q.h"
#include "cachelib/allocator/MMLru.h"
#include "cachelib/allocator/MMTinyLFU.h"
#include "cachelib/common/Mutex.h"

namespace facebook {
namespace cachelib {
// The cache traits supported by CacheLib.
// Cache trait is a combination of MMType, AccessType and AccesTypeLock.
// MMType is the type of MM (memory management) container used by the cache,
// which controls a cache item's life time.
// AccessType is the type of access container, which controls how an item is
// accessed.
// AccessTypeLock is the lock type for the access container that supports
// multiple locking primitives
struct LruCacheTrait {
  using MMType = MMLru;
  using AccessType = ChainedHashTable;
  using AccessTypeLocks = SharedMutexBuckets;
};

struct LruCacheWithSpinBucketsTrait {
  using MMType = MMLru;
  using AccessType = ChainedHashTable;
  using AccessTypeLocks = SpinBuckets;
};

struct Lru2QCacheTrait {
  using MMType = MM2Q;
  using AccessType = ChainedHashTable;
  using AccessTypeLocks = SharedMutexBuckets;
};

struct TinyLFUCacheTrait {
  using MMType = MMTinyLFU;
  using AccessType = ChainedHashTable;
  using AccessTypeLocks = SharedMutexBuckets;
};

} // namespace cachelib
} // namespace facebook
