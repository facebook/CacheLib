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

#include "cachelib/navy/bighash/BucketStorage.h"

namespace facebook {
namespace cachelib {
namespace navy {
static_assert(sizeof(BucketStorage) == 12,
              "BucketStorage overhead. Changing this may require changing "
              "the sizes used in unit tests as well");

const uint32_t BucketStorage::kAllocationOverhead = sizeof(BucketStorage::Slot);

// This is very simple as it only tries to allocate starting from the
// tail of the storage. Returns null view() if we don't have any more space.
BucketStorage::Allocation BucketStorage::allocate(uint32_t size) {
  if (!canAllocate(size)) {
    return {};
  }

  auto* slot = new (data_ + endOffset_) Slot(size);
  endOffset_ += slotSize(size);
  numAllocations_++;
  return {MutableBufferView{slot->size, slot->data}, numAllocations_ - 1};
}

void BucketStorage::remove(Allocation alloc) {
  remove(std::vector<Allocation>({alloc}));
}

void BucketStorage::remove(const std::vector<Allocation>& allocs) {
  // Remove triggers a compaction.
  //
  //                         tail
  //  |---|REMOVED|-----|REMOVED|-----|~~~~|
  //
  // after compaction
  //                  tail
  //  |---------------|~~~~~~~~~~~|
  if (!allocs.size()) {
    return;
  }

  uint32_t srcOffset = 0;
  uint32_t dstOffset = 0;
  for (auto& alloc : allocs) {
    uint32_t allocOffset = alloc.view().data() - data_;
    uint32_t removedOffset = allocOffset - kAllocationOverhead;
    // We have valid data from [srcOffset, removedOffset)
    if (srcOffset != removedOffset) {
      uint32_t len = removedOffset - srcOffset;
      if (dstOffset != srcOffset) {
        std::memmove(data_ + dstOffset, data_ + srcOffset, len);
      }
      dstOffset += len;
    }
    // update the offset which (could) contain next valid data
    srcOffset = allocOffset + alloc.view().size();
    numAllocations_--;
  }

  // copy the rest of data after the last removed alloc if any
  if (srcOffset != endOffset_) {
    uint32_t len = endOffset_ - srcOffset;
    std::memmove(data_ + dstOffset, data_ + srcOffset, len);
    dstOffset += len;
  }
  // update end offset to point the right next byte of the data copied
  endOffset_ = dstOffset;
}

void BucketStorage::removeUntil(Allocation alloc) {
  // Remove everything until (and include) "alloc"
  //
  //                         tail
  //  |----------------|-----|~~~~|
  //  ^                ^
  //  begin            offset
  //  remove this whole range
  //
  //        tail
  //  |-----|~~~~~~~~~~~~~~~~~~~~~|
  if (alloc.done()) {
    return;
  }

  uint32_t offset = alloc.view().data() + alloc.view().size() - data_;
  if (offset > endOffset_) {
    return;
  }

  std::memmove(data_, data_ + offset, endOffset_ - offset);
  endOffset_ -= offset;
  numAllocations_ -= alloc.position() + 1;
}

BucketStorage::Allocation BucketStorage::getFirst() const {
  if (endOffset_ == 0) {
    return {};
  }
  auto* slot = reinterpret_cast<Slot*>(data_);
  return {MutableBufferView{slot->size, slot->data}, 0};
}

BucketStorage::Allocation BucketStorage::getNext(
    BucketStorage::Allocation alloc) const {
  if (alloc.done()) {
    return {};
  }

  auto* next =
      reinterpret_cast<Slot*>(alloc.view().data() + alloc.view().size());
  if (reinterpret_cast<uint8_t*>(next) - data_ >= endOffset_) {
    return {};
  }
  return {MutableBufferView{next->size, next->data}, alloc.position() + 1};
}
} // namespace navy
} // namespace cachelib
} // namespace facebook
