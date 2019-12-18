#pragma once

#include "cachelib/shm/Shm.h"
#include "cachelib/shm/ShmManager.h"

namespace facebook {
namespace cachelib {

namespace tests {
template <typename AllocatorT>
class BaseAllocatorTest;
}

namespace detail {
constexpr folly::StringPiece kTempShmCacheName = "temp_shm_cache";
}

// Manages shared memory mappings that are temporary i.e dropped when the
// cache process exits. It creates a shared memory segment of the given
// size and immediately marks it for removal.
// This is for use by CacheAllocator for applications that don't care about
// persisting shared memory segments and reattaching to them on restart.
// They want to use shared memory segments so that they can
// use the memory advising for OOM prevention, which is only supported when
// the cache is on a shared memory segment.
class TempShmMapping {
 public:
  explicit TempShmMapping(size_t size);
  ~TempShmMapping();

  void* getAddr() const { return addr_; }

 private:
  static std::unique_ptr<ShmManager> createShmManager(
      const std::string& cacheDir);
  static void* createShmMapping(ShmManager& shmManager,
                                size_t size,
                                const std::string& cacheDir);

  size_t size_{0};
  std::string tempCacheDir_;
  std::unique_ptr<ShmManager> shmManager_;
  void* addr_{nullptr};

  // test
  template <typename AllocatorT>
  friend class facebook::cachelib::tests::BaseAllocatorTest;
};

} // namespace cachelib
} // namespace facebook
