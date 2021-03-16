#include "cachelib/navy/kangaroo/RripBucketStorage.h"

namespace facebook {
namespace cachelib {
namespace navy {
static_assert(sizeof(RripBucketStorage) == 12,
              "RripBucketStorage overhead. Changing this may require changing "
              "the sizes used in unit tests as well");

const uint32_t RripBucketStorage::kAllocationOverhead = sizeof(RripBucketStorage::Slot);

// This is very simple as it only tries to allocate starting from the
// tail of the storage. Returns null view() if we don't have any more space.
RripBucketStorage::Allocation RripBucketStorage::allocate(uint32_t size, uint8_t rrip) {
  // Allocate at the beginning of the right rrip value
  //
  //              tail
  // |-6--|3|--0--|~~~~~~~~~~~~~|
  //
  // after allocating object with 3 
  //                  tail
  // |-6--|3|NEW|--0--|~~~~~~~~~|
  if (!canAllocate(size)) {
    return {};
  }

  auto itr = getFirst();
  uint32_t position = 0;
  while (!itr.done() && itr.rrip() >= rrip) {
    itr = getNext(itr);
    position++;
  }

  uint32_t totalNewSize = slotSize(size);
  uint8_t* start = data_ + endOffset_;
  if (!itr.done()) {
    start = itr.view().data() - kAllocationOverhead;
  }
  std::memmove(start + totalNewSize,
               start,
               (data_ + endOffset_) - start);

  auto* slot = new (start) Slot(size, rrip);
  endOffset_ += totalNewSize;
  numAllocations_++;
  return {MutableBufferView{slot->size, slot->data}, 
    position, (uint8_t) slot->rrip};
}

RripBucketStorage::Allocation RripBucketStorage::remove(Allocation alloc) {
  // Remove triggers a compaction.
  //
  //                         tail
  //  |--------|REMOVED|-----|~~~~|
  //
  // after compaction
  //                  tail
  //  |---------------|~~~~~~~~~~~|
  if (alloc.done()) {
    return alloc;
  }

  const uint32_t removedSize = slotSize(alloc.view().size());
  uint8_t* removed = alloc.view().data() - kAllocationOverhead;
  uint32_t position = alloc.position();
  std::memmove(removed,
               removed + removedSize,
               (data_ + endOffset_) - removed - removedSize);
  endOffset_ -= removedSize;
  numAllocations_--;
  
  auto* current =
      reinterpret_cast<Slot*>(removed);
  if (reinterpret_cast<uint8_t*>(current) - data_ >= endOffset_) {
    return {};
  }
  return {MutableBufferView{current->size, current->data}, position, 
      (uint8_t) current->rrip};
}

void RripBucketStorage::removeUntil(Allocation alloc) {
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

RripBucketStorage::Allocation RripBucketStorage::getFirst() const {
  if (endOffset_ == 0) {
    return {};
  }
  auto* slot = reinterpret_cast<Slot*>(data_);
  return {MutableBufferView{slot->size, slot->data}, 0, (uint8_t) slot->rrip};
}

RripBucketStorage::Allocation RripBucketStorage::getNext(
    RripBucketStorage::Allocation alloc) const {
  if (alloc.done()) {
    return {};
  }

  auto* next =
      reinterpret_cast<Slot*>(alloc.view().data() + alloc.view().size());
  if (reinterpret_cast<uint8_t*>(next) - data_ >= endOffset_) {
    return {};
  }
  return {MutableBufferView{next->size, next->data}, alloc.position() + 1, 
      (uint8_t) next->rrip};
}

void RripBucketStorage::incrementRrip(Allocation alloc, int8_t increment) {
  uint8_t* current_slot = alloc.view().data() - kAllocationOverhead;
  auto* slot =
      reinterpret_cast<Slot*>(current_slot);
  XDCHECK(increment + slot->rrip <= 7);
  slot->rrip += increment;
}
} // namespace navy
} // namespace cachelib
} // namespace facebook
