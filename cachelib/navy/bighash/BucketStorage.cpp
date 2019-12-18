#include "BucketStorage.h"

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
  // Remove triggers a compaction.
  //
  //                         tail
  //  |--------|REMOVED|-----|~~~~|
  //
  // after compaction
  //                  tail
  //  |---------------|~~~~~~~~~~~|
  if (alloc.done()) {
    return;
  }

  const uint32_t removedSize = slotSize(alloc.view().size());
  uint8_t* removed = alloc.view().data() - kAllocationOverhead;
  std::memmove(removed,
               removed + removedSize,
               (data_ + endOffset_) - removed - removedSize);
  endOffset_ -= removedSize;
  numAllocations_--;
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
