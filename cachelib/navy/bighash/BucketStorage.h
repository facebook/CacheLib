#pragma once

#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/common/CompilerUtils.h"

namespace facebook {
namespace cachelib {
namespace navy {
// TODO: (beyondsora) T31519237 Change BucketStorage to allocate backwards
//                              for better performance
// This is a very simple FIFO allocator that once full the only
// way to free up more space is by removing entries at the
// front. It is used for managing alloactions inside a bucket.
class FOLLY_PACK_ATTR BucketStorage {
 public:
  // This is an allocation that is returned to user when they
  // allocate from the BucketStorage. "view" is for reading
  // and modifying data allocated from the storage. "position"
  // indicates where it is in the storage and is used internally to
  // iterate to the next allocation.
  //
  // User should only have reference to one "allocation" at a time.
  // Calling remove or removeUntil API on an allocation will invalidate
  // all the other references to allocations.
  class Allocation {
   public:
    bool done() const { return view_.isNull(); }

    MutableBufferView view() const { return view_; }

    uint32_t position() const { return position_; }

   private:
    friend BucketStorage;

    Allocation() = default;
    Allocation(MutableBufferView v, uint32_t p) : view_{v}, position_{p} {}

    MutableBufferView view_{};
    uint32_t position_{};
  };

  static uint32_t slotSize(uint32_t size) { return kAllocationOverhead + size; }

  explicit BucketStorage(uint32_t capacity) : capacity_{capacity} {}

  Allocation allocate(uint32_t size);

  uint32_t capacity() const { return capacity_; }

  uint32_t remainingCapacity() const { return capacity_ - endOffset_; }

  uint32_t numAllocations() const { return numAllocations_; }

  void remove(Allocation alloc);

  // Removes every single allocation from the beginning, including this one.
  void removeUntil(Allocation alloc);

  Allocation getFirst() const;
  Allocation getNext(Allocation alloc) const;

 private:
  // Slot represents a physical slot in the storage. User does not use
  // this directly but instead uses Allocation.
  struct FOLLY_PACK_ATTR Slot {
    uint32_t size{};
    uint8_t data[];
    explicit Slot(uint32_t s) : size{s} {}
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
