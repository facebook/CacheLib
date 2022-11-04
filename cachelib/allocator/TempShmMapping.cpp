/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "cachelib/allocator/TempShmMapping.h"

#include <folly/logging/xlog.h>
#include <sys/mman.h>

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
