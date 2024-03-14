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

#include "cachelib/navy/common/NavyThread.h"

namespace facebook::cachelib::navy {

bool Region::readyForReclaim(bool wait) {
  std::unique_lock<TimedMutex> l{lock_};
  flags_ |= kBlockAccess;
  bool ready = false;
  while (!(ready = (activeOpenLocked() == 0UL)) && wait) {
    cond_.wait(l);
  }

  return ready;
}

uint32_t Region::activeOpenLocked() {
  return activePhysReaders_ + activeInMemReaders_ + activeWriters_;
}

std::tuple<RegionDescriptor, RelAddress> Region::openAndAllocate(
    uint32_t size) {
  std::lock_guard<TimedMutex> l{lock_};
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
  std::unique_lock<TimedMutex> l{lock_};
  if (flags_ & kBlockAccess) {
    // Region is currently in reclaim, retry later
    if (getCurrentNavyThread()) {
      // If we are on fiber, we can just sleep here
      cond_.wait(l);
    }
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

std::unique_ptr<Buffer> Region::detachBuffer() {
  std::unique_lock<TimedMutex> l{lock_};
  XDCHECK_NE(buffer_, nullptr);
  while (activeInMemReaders_ != 0) {
    cond_.wait(l);
  }

  XDCHECK_EQ(activeWriters_, 0UL);
  auto retBuf = std::move(buffer_);
  buffer_ = nullptr;
  return retBuf;
}

// This function flushes the attached buffer if there are no active writers
// by calling the callBack function that is expected to write the buffer to
// underlying device. If there are active writers, the caller is expected
// to call this function again.
Region::FlushRes Region::flushBuffer(
    std::function<bool(RelAddress, BufferView)> callBack) {
  std::unique_lock<TimedMutex> lock{lock_};
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

void Region::cleanupBuffer(std::function<void(RegionId, BufferView)> callBack) {
  std::unique_lock<TimedMutex> lock{lock_};
  while (activeWriters_ != 0) {
    cond_.wait(lock);
  }
  if (!isCleanedupLocked()) {
    lock.unlock();
    callBack(regionId_, buffer_->view());
    lock.lock();
    flags_ |= kCleanedup;
  }
}

void Region::reset() {
  std::lock_guard<TimedMutex> l{lock_};
  XDCHECK_EQ(activeOpenLocked(), 0U);
  priority_ = 0;
  flags_ = 0;
  activeWriters_ = 0;
  activePhysReaders_ = 0;
  activeInMemReaders_ = 0;
  lastEntryEndOffset_ = 0;
  numItems_ = 0;
  cond_.notifyAll();
}

void Region::close(RegionDescriptor&& desc) {
  std::lock_guard<TimedMutex> l{lock_};
  switch (desc.mode()) {
  case OpenMode::Write:
    XDCHECK_GT(activeWriters_, 0u);
    if (--activeWriters_ == 0) {
      cond_.notifyAll();
    }
    break;
  case OpenMode::Read:
    if (desc.isPhysReadMode()) {
      XDCHECK_GT(activePhysReaders_, 0u);
      if (--activePhysReaders_ == 0) {
        cond_.notifyAll();
      }
    } else {
      XDCHECK_GT(activeInMemReaders_, 0u);
      if (--activeInMemReaders_ == 0) {
        cond_.notifyAll();
      }
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

} // namespace facebook::cachelib::navy
