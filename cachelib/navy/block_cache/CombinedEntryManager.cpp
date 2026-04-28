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

CombinedEntryBlock* CombinedEntryManager::getCombinedEntryBlock(
    uint64_t stream) {
  XDCHECK(stream < numCebStreams_);

  // If we don't have the combined entry block for the stream, create one
  if (cebStreams_[stream] == nullptr) {
    // TODO: Let's just use CombinedEntryBlock as it is for now though it won't
    // use shm for the buffer.
    cebStreams_[stream] = std::make_unique<CombinedEntryBlock>(cebSize_);
    totalCebs_++;
  }

  return cebStreams_[stream].get();
}

CombinedEntryStatus CombinedEntryManager::addIndexEntryToStream(
    uint64_t stream, uint64_t bid, uint64_t key, const EntryRecord& record) {
  auto ceb = getCombinedEntryBlock(stream);
  if (!ceb) {
    XLOGF(ERR,
          "Can't add an index entry (bid {}, key {}) to the stream: CEB is not "
          "available for the given stream {}",
          bid, key, stream);
    return CombinedEntryStatus::kError;
  }

  auto res = ceb->addIndexEntry(bid, key, record);
  if (res == CombinedEntryStatus::kFull) {
    auto writeStatus = writeCebCb_(stream, *ceb);
    if (writeStatus == Status::Ok) {
      // Write to flash was successful and all the index entries for the CEB
      // were updated at this point.
      ceb->clear();
      // Now we can add the given index entry and it has to succeed.
      res = ceb->addIndexEntry(bid, key, record);
    } else {
      // TODO: writing CEB can return Status::Retry when it can't find the clean
      // region and can't allocate. We will handle this case gracefully by
      // maintaining to-be-written CEBs, but for now, it will just give up on
      // adding the given entry and return.
      XDCHECK(writeStatus == Status::Retry);
      XLOGF(WARN,
            "Can't add an index entry (bid {}, key {}) to the stream: CEB is "
            "full "
            "for the given stream {}",
            bid, key, stream);
      return CombinedEntryStatus::kFull;
    }
  }

  XDCHECK(res == CombinedEntryStatus::kUpdated ||
          res == CombinedEntryStatus::kOk);
  return res;
}

// Check all the active combined entry block(s) for the stream to see if we have
// a valid entry for the key.
// (If it's already flushed to flash, index will be updated to point to the
// flash location, so it will be handled differently and this won't be called
// for that case)
bool CombinedEntryManager::peekIndexEntryFromStream(uint64_t stream,
                                                    uint64_t key) {
  XDCHECK(stream < numCebStreams_);

  if (cebStreams_[stream] == nullptr) {
    return false;
  }

  // TODO: for now, only one active CEB for each stream. It'll be expanded
  // to handle multiple in case there are pending flushes.
  return cebStreams_[stream]->peekIndexEntry(key);
}

// Get the index entry for the given key from the active combined entry block(s)
// for the stream. When there are multiple CEBs open for the stream, it will
// look up starting from the latest one to get the latest entry in case the same
// key entry was updated. (If it's already flushed to flash, index will be
// updated to point to the flash location, so it will be handled differently and
// this won't be called for that case)
folly::Expected<EntryRecord, CombinedEntryStatus>
CombinedEntryManager::getIndexEntryFromStream(uint64_t stream, uint64_t key) {
  XDCHECK(stream < numCebStreams_);

  if (cebStreams_[stream] == nullptr) {
    return folly::makeUnexpected(CombinedEntryStatus::kNotFound);
  }

  // TODO: for now, only one active CEB for each stream. It'll be expanded
  // to handle multiple in case there are pending flushes.
  return cebStreams_[stream]->getIndexEntry(key);
}

// Remove a index entry for the given key from the active combined entry
// block(s) for the stream. When there are multiple CEBs open for the stream, it
// will check and remove starting from the latest one first.
CombinedEntryStatus CombinedEntryManager::removeIndexEntryFromStream(
    uint64_t stream, uint64_t key) {
  XDCHECK(stream < numCebStreams_);

  if (cebStreams_[stream] == nullptr) {
    return CombinedEntryStatus::kNotFound;
  }

  // TODO: for now, only one active CEB for each stream.
  return cebStreams_[stream]->removeIndexEntry(key);
}

} // namespace navy
} // namespace cachelib
} // namespace facebook
