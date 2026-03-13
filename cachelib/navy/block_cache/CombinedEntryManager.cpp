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

#include "cachelib/navy/block_cache/CombinedEntryManager.h"

namespace facebook {
namespace cachelib {
namespace navy {

size_t CombinedEntryManager::getRequiredPreallocSize() const {
  // Need to preallocate buffers for those info which needs to be persistent
  // TODO: Currently, it will maintain only CEBs for the specified number of
  // streams. This needs to be changed since there's time that each CEB cannot
  // be flushed immediately, and in that case we need spare buffers to store
  // added entries while it's waiting to be flushed.
  return cebSize_ * numCebStreams_;
}

void CombinedEntryManager::reset() {
  XLOGF(INFO,
        "Resetting BlockCache Combined entry block buffers: Total {}, Size {}",
        numCebStreams_, cebSize_);

  // check if we don't have shm enabled
  void* baseAddr =
      shmManager_
          ? shmManager_
                ->createShm(std::string(kShmCebManagerName) + "_" + name_,
                            getRequiredPreallocSize())
                .addr
          : util::mmapAlignedZeroedMemory(util::getPageSize(),
                                          getRequiredPreallocSize());
  // For now, the only thing needs to be stored in shm is CEB buffers
  cebBuffers_ = reinterpret_cast<uint8_t*>(baseAddr);
}

void CombinedEntryManager::persist() const {
  // For now, it's persisted by using shm and there's nothing specific to do
  // here
  XLOG(INFO,
       "Finished persisting BlockCache Combined entry block buffers: Total {}",
       numCebStreams_);
}

void CombinedEntryManager::recover() {
  XLOGF(INFO,
        "Recovering BlockCache Combined entry block buffers: Total {}, Size {}",
        numCebStreams_, cebSize_);

  // If recover() fails for whatever reason, it will throw exception. This
  // exception will be caught in BlockCache::recover() and it will proceed with
  // reset() to have empty cache entries and empty combined entry block buffers.

  if (!shmManager_) {
    // Can't support persistency. Exception will be caught in
    // BlockCache::recover()
    throw std::runtime_error(
        "Cannot recover Combined entry blocks without shm");
  }

  auto baseAddr =
      shmManager_->attachShm(std::string(kShmCebManagerName) + "_" + name_);

  // For now, the only thing needs to be stored in shm is CEB buffers
  cebBuffers_ = reinterpret_cast<uint8_t*>(baseAddr.addr);

  XLOG(INFO) << "Finished recovering BlockCache Combined entry block buffers";
}
} // namespace navy
} // namespace cachelib
} // namespace facebook
