#pragma once
#include "cachelib/allocator/ChainedHashTable.h"
#include "cachelib/allocator/MM2Q.h"
#include "cachelib/allocator/MMLru.h"
#include "cachelib/allocator/MMTinyLFU.h"
#include "cachelib/common/Mutex.h"

namespace facebook {
namespace cachelib {

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
