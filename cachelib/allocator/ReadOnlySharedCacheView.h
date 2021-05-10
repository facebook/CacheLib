#pragma once

#include <memory>
#include <string>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#include <folly/Format.h>
#pragma GCC diagnostic pop

#include "cachelib/allocator/CacheDetails.h"
#include "cachelib/shm/ShmManager.h"

namespace facebook {
namespace cachelib {

// used as a read only into a shared cache. The cache is owned by another
// process and we peek into the items in the cache based on their offsets.
class ReadOnlySharedCacheView {
 public:
  // tries to attach to an existing cache with the cacheDir if present under
  // the correct shm mode.
  //
  // @param cacheDir    the directory that identifies the cache
  // @param usePosix    the posix compatbility status of original cache. This
  //                    would be part of the config that was used to
  //                    initialize the cache
  // @param addr        starting address that this segment should be mapped to
  //                    (exception will be thrown if it is not mounted to the
  //                     given address)
  //                    if nullptr, the segment will be mapped to a random
  //                    address chosen by the kernel
  explicit ReadOnlySharedCacheView(const std::string& cacheDir,
                                   bool usePosixShm,
                                   void* addr = nullptr)
      : shm_(ShmManager::attachShmReadOnly(
            cacheDir, detail::kShmCacheName, usePosixShm, addr)) {}

  // returns the absolute address at which the shared memory mapping is mounted.
  // The caller can add a relative offset obtained from
  // CacheAllocator::getItemPtrAsOffset to this address in order to compute the
  // address at which that item is stored (within the process which manages this
  // ReadOnlySharedCacheView).
  uintptr_t getShmMappingAddress() const noexcept {
    auto mapping = shm_->getCurrentMapping();
    return reinterpret_cast<uintptr_t>(mapping.addr);
  }

  // computes an aboslute address in the cache, given a relative offset that
  // was obtained from CacheAllocator::getItemPtrAsOffset. It is the caller's
  // responsibility to ensure the memory backing the offset corresponds to an
  // active ItemHandle in the system.
  //
  // Returns a valid pointer if the offset is valid. Returns nullptr if no
  // shared memory mapping is mounted. Throws if the given offset is invalid.
  const void* getItemPtrFromOffset(uintptr_t offset) {
    auto mapping = shm_->getCurrentMapping();
    if (mapping.addr == nullptr) {
      return nullptr;
    }

    if (offset >= mapping.size) {
      throw std::invalid_argument(folly::sformat(
          "Invalid offset {} with mapping of size {}", offset, mapping.size));
    }
    return reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(mapping.addr) +
                                   offset);
  }

 private:
  // the segment backing the cache
  std::unique_ptr<ShmSegment> shm_;
};

} // namespace cachelib
} // namespace facebook
