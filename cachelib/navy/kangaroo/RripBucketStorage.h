#pragma once

#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/common/CompilerUtils.h"
#include "cachelib/navy/common/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {
// This is a very simple FIFO allocator that once full the only
// way to free up more space is by removing entries at the
// front. It is used for managing alloactions inside a bucket.
class FOLLY_PACK_ATTR RripBucketStorage {
 public:
  // This is an allocation that is returned to user when they
  // allocate from the RripBucketStorage. "view" is for reading
  // and modifying data allocated from the storage. "position"
  // indicates where it is in the storage and is used internally to
  // iterate to the next allocation.
  //
  // User should only have reference to one "allocation" at a time.
  // Calling remove, allocate, or removeUntil API on an allocation will invalidate
  // all the other references to allocations.
  class Allocation {
   public:
    Allocation() = default;

    bool done() const { return view_.isNull(); }

    MutableBufferView view() const { return view_; }

    uint32_t position() const { return position_; }

    uint8_t rrip() const { return rrip_; }

   private:
    friend RripBucketStorage;

    Allocation(MutableBufferView v, uint32_t p, uint8_t rrip) : view_{v}, position_{p}, rrip_{rrip} {}

    MutableBufferView view_{};
    uint32_t position_{};
    uint8_t rrip_{};
  };

  static uint32_t slotSize(uint32_t size) { return kAllocationOverhead + size; }

  explicit RripBucketStorage(uint32_t capacity) : capacity_{capacity} {}

  // Other allocators are only valid if rrip value = 0
  Allocation allocate(uint32_t size, uint8_t rrip);

  uint32_t capacity() const { return capacity_; }

  uint32_t remainingCapacity() const { return capacity_ - endOffset_; }

  uint32_t numAllocations() const { return numAllocations_; }

  void incrementRrip(Allocation alloc, int8_t increment);

  void clear() { 
    endOffset_ = 0; 
    numAllocations_ = 0;
  } 

  Allocation remove(Allocation alloc);

  // Removes every single allocation from the beginning, including this one.
  void removeUntil(Allocation alloc);
 
  Allocation getFirst() const;
  Allocation getNext(Allocation alloc) const;

 private:
  // Slot represents a physical slot in the storage. User does not use
  // this directly but instead uses Allocation.
  struct FOLLY_PACK_ATTR Slot {
    uint16_t rrip : 3;
    uint16_t size : 13;
    uint8_t data[];
    explicit Slot(uint16_t s, uint8_t rrip) : rrip{rrip}, size{s} {}
  };

  bool canAllocate(uint32_t size) const {
    return static_cast<uint64_t>(endOffset_) + slotSize(size) <= capacity_;
  }

  static const uint32_t kAllocationOverhead;

  const uint32_t capacity_{};
  uint32_t numAllocations_{};
  uint32_t endOffset_{};
  mutable uint8_t data_[];
};
} // namespace navy
} // namespace cachelib
} // namespace facebook
