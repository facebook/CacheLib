#include <sys/mman.h>

#include <folly/logging/xlog.h>

#include "cachelib/allocator/TempShmMapping.h"
#include "cachelib/allocator/memory/Slab.h"
#include "cachelib/common/Utils.h"

namespace facebook {
namespace cachelib {

TempShmMapping::TempShmMapping(size_t size)
    : size_(size),
      tempCacheDir_(util::getUniqueTempDir("cachedir")),
      shmManager_(createShmManager(tempCacheDir_)),
      addr_(createShmMapping(*shmManager_.get(), size, tempCacheDir_)) {}

TempShmMapping::~TempShmMapping() {
  try {
    if (addr_) {
      shmManager_->removeShm(detail::kTempShmCacheName.str());
    }
    if (shmManager_) {
      shmManager_.reset();
      util::removePath(tempCacheDir_);
    }
  } catch (...) {
    if (shmManager_) {
      XLOG(CRITICAL, "Failed to drop temporary shared memory segment");
    } else {
      XLOGF(
          ERR, "Failed to remove temporary cache directory: {}", tempCacheDir_);
    }
  }
}

std::unique_ptr<ShmManager> TempShmMapping::createShmManager(
    const std::string& cacheDir) {
  try {
    return std::make_unique<ShmManager>(cacheDir, false /* posix */);
  } catch (...) {
    util::removePath(cacheDir);
    throw;
  }
}

void* TempShmMapping::createShmMapping(ShmManager& shmManager,
                                       size_t size,
                                       const std::string& cacheDir) {
  void* addr = nullptr;
  void* shmAddr = nullptr;
  try {
    addr =
        util::mmapAlignedZeroedMemory(sizeof(Slab), size, true /* readOnly */);
    shmAddr =
        shmManager.createShm(detail::kTempShmCacheName.str(), size, addr).addr;
    // Mark the shared memory segment to be removed on exit. This will ensure
    // that the segment is dropped on exit.
    auto& shm = shmManager.getShmByName(detail::kTempShmCacheName.str());
    shm.markForRemoval();
    return shmAddr;
  } catch (...) {
    if (shmAddr) {
      shmManager.removeShm(detail::kTempShmCacheName.str());
    } else {
      munmap(addr, size);
    }
    util::removePath(cacheDir);
    throw;
  }
}

} // namespace cachelib
} // namespace facebook
