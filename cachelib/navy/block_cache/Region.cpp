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

#include "cachelib/navy/block_cache/Region.h"

namespace facebook {
namespace cachelib {
namespace navy {
bool Region::readyForReclaim() {
  std::lock_guard<std::mutex> l{lock_};
  flags_ |= kBlockAccess;
  return activeOpenLocked() == 0;
}

uint32_t Region::activeOpenLocked() {
  return activePhysReaders_ + activeInMemReaders_ + activeWriters_;
}

std::tuple<RegionDescriptor, RelAddress> Region::openAndAllocate(
    uint32_t size) {
  std::lock_guard<std::mutex> l{lock_};
  XDCHECK(!(flags_ & kBlockAccess));
  if (!canAllocateLocked(size)) {
    return std::make_tuple(RegionDescriptor{OpenStatus::Error}, RelAddress{});
  }
  activeWriters_++;
  return std::make_tuple(
      RegionDescriptor::makeWriteDescriptor(OpenStatus::Ready, regionId_),
      allocateLocked(size));
}

RegionDescriptor Region::openForRead() {
  std::lock_guard<std::mutex> l{lock_};
  if (flags_ & kBlockAccess) {
    // Region is currently in reclaim, retry later
    return RegionDescriptor{OpenStatus::Retry};
  }
  bool physReadMode = false;
  if (isFlushedLocked() || !buffer_) {
    physReadMode = true;
    activePhysReaders_++;
  } else {
    activeInMemReaders_++;
  }
  return RegionDescriptor::makeReadDescriptor(
      OpenStatus::Ready, regionId_, physReadMode);
}

// This function flushes the attached buffer if there are no active writers
// by calling the callBack function that is expected to write the buffer to
// underlying device. If there are active writers, the caller is expected
// to call this function again.
Region::FlushRes Region::flushBuffer(
    std::function<bool(RelAddress, BufferView)> callBack) {
  std::unique_lock<std::mutex> lock{lock_};
  if (activeWriters_ != 0) {
    return FlushRes::kRetryPendingWrites;
  }
  if (!isFlushedLocked()) {
    lock.unlock();
    if (callBack(RelAddress{regionId_, 0}, buffer_->view())) {
      lock.lock();
      flags_ |= kFlushed;
      return FlushRes::kSuccess;
    }
    return FlushRes::kRetryDeviceFailure;
  }
  return FlushRes::kSuccess;
}

bool Region::cleanupBuffer(std::function<void(RegionId, BufferView)> callBack) {
  std::unique_lock<std::mutex> lock{lock_};
  if (activeWriters_ != 0) {
    return false;
  }
  if (!isCleanedupLocked()) {
    lock.unlock();
    callBack(regionId_, buffer_->view());
    lock.lock();
    flags_ |= kCleanedup;
  }
  return true;
}

void Region::reset() {
  std::lock_guard<std::mutex> l{lock_};
  XDCHECK_EQ(activeOpenLocked(), 0U);
  priority_ = 0;
  flags_ = 0;
  activeWriters_ = 0;
  activePhysReaders_ = 0;
  activeInMemReaders_ = 0;
  lastEntryEndOffset_ = 0;
  numItems_ = 0;
}

void Region::close(RegionDescriptor&& desc) {
  std::lock_guard<std::mutex> l{lock_};
  switch (desc.mode()) {
  case OpenMode::Write:
    activeWriters_--;
    break;
  case OpenMode::Read:
    if (desc.isPhysReadMode()) {
      activePhysReaders_--;
    } else {
      activeInMemReaders_--;
    }
    break;
  default:
    XDCHECK(false);
  }
}

RelAddress Region::allocateLocked(uint32_t size) {
  XDCHECK(canAllocateLocked(size));
  auto offset = lastEntryEndOffset_;
  lastEntryEndOffset_ += size;
  numItems_++;
  return RelAddress{regionId_, offset};
}

void Region::writeToBuffer(uint32_t offset, BufferView buf) {
  std::lock_guard l{lock_};
  XDCHECK_NE(buffer_, nullptr);
  auto size = buf.size();
  XDCHECK_LE(offset + size, buffer_->size());
  memcpy(buffer_->data() + offset, buf.data(), size);
}

void Region::readFromBuffer(uint32_t fromOffset,
                            MutableBufferView outBuf) const {
  std::lock_guard l{lock_};
  XDCHECK_NE(buffer_, nullptr);
  XDCHECK_LE(fromOffset + outBuf.size(), buffer_->size());
  memcpy(outBuf.data(), buffer_->data() + fromOffset, outBuf.size());
}

} // namespace navy
} // namespace cachelib
} // namespace facebook
