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

#pragma once

#include <limits>

#include "cachelib/allocator/TypedHandle.h"
#include "cachelib/allocator/memory/Slab.h"
#include "cachelib/common/Exceptions.h"
#include "cachelib/common/Hash.h"
#include "cachelib/common/Iterators.h"
#include "cachelib/datatype/DataTypes.h"

namespace facebook {
namespace cachelib {

namespace tests {
template <typename AllocatorT>
class BufferManagerTest;
}

namespace detail {
class FOLLY_PACK_ATTR Buffer {
 private:
  class Slot;

 public:
  // Compute the overall storage required for given capacity
  static uint32_t computeStorageSize(uint32_t capacity) {
    return static_cast<uint32_t>(sizeof(Buffer)) + capacity;
  }

  // Get the total allocation size for given size
  static uint32_t getAllocSize(uint32_t size) {
    return Slot::getAllocSize(size);
  }

  explicit Buffer(uint32_t capacity) : capacity_(capacity) {}
  Buffer(uint32_t capacity, const Buffer& other);

  // Allocate a slot that has `size` usable memory.
  // Return kInvalidOffset if insufficient space.
  static constexpr uint32_t kInvalidOffset =
      std::numeric_limits<uint32_t>::max();
  uint32_t allocate(uint32_t size);

  // Mark a slot allocated at the offset deleted.
  // @throw std::invalid_argument if offset is invalid
  void remove(uint32_t offset);

  // Return a pointer to the allocation given the offset
  void* getData(uint32_t offset);

  // Return a const pointer to the allocation given the offset
  const void* getData(uint32_t offset) const;

  // Eliminate deleted bytes by rearranging existing slots into destination
  // This does not change the current buffer's memory layout
  void compact(Buffer& dest) const;

  uint32_t capacity() const { return capacity_; }
  uint32_t remainingBytes() const { return capacity_ - nextByte_; }
  uint32_t wastedBytes() const { return deletedBytes_; }

  bool canAllocate(uint32_t size) const;
  bool canAllocateWithoutCompaction(uint32_t size) const;

  class Iterator
      : public IteratorFacade<Iterator, uint8_t, std::forward_iterator_tag> {
   public:
    Iterator() = default;
    explicit Iterator(Buffer& buffer)
        : buffer_(&buffer), curr_(buffer.getFirstSlot()) {
      if (!curr_) {
        buffer_ = nullptr;
      } else if (curr_->isRemoved()) {
        // If current slot is marked as removed, increment to the next valid one
        increment();
      }
    }

    // @throw std::runtime_error if we're dereferencing a null iterator
    uint8_t& dereference() const {
      if (!curr_) {
        throw std::runtime_error(
            "BufferIterator:: deferencing a null Iterator.");
      }
      XDCHECK(!curr_->isRemoved());
      return *reinterpret_cast<uint8_t*>(curr_->getData());
    }

    // Reaching the end of this iterator will reset the "buffer_" to nullptr
    // @throw std::out_of_range if we move pass the end pointer
    void increment() {
      if (!curr_) {
        throw std::out_of_range(
            fmt::format("Moving past end pointer. Buffer: {}",
                        reinterpret_cast<uintptr_t>(buffer_)));
      }

      while ((curr_ = buffer_->getNextSlot(*curr_))) {
        if (!curr_->isRemoved()) {
          return;
        }
      }

      // curr_ == nullptr, we've reached the end
      // setting buffer_ also to nullptr
      buffer_ = nullptr;
    }

    bool equal(const Iterator& other) const {
      return buffer_ == other.buffer_ && curr_ == other.curr_;
    }

    uint32_t getDataOffset() const {
      if (!curr_) {
        return kInvalidOffset;
      }
      return buffer_->getDataOffset(*curr_);
    }

   private:
    // BEGIN private members
    Buffer* buffer_{nullptr};
    mutable Slot* curr_{nullptr};
    // END private members
  };
  Iterator begin() { return Iterator{*this}; }
  Iterator end() { return {}; }

 private:
  // Slot can address up to 4MB of data
  class FOLLY_PACK_ATTR Slot {
   public:
    static uint32_t getAllocSize(uint32_t size) {
      return static_cast<uint32_t>(sizeof(Slot)) + size;
    }

    explicit Slot(uint32_t size) : size_{size & kSizeMask} {}

    uint32_t getSize() const { return size_ & kSizeMask; }
    void* getData() const { return reinterpret_cast<void*>(&data_); }

    // Get the total size this Slot occupies
    uint32_t getAllocSize() const { return getAllocSize(getSize()); }

    void markRemoved() { size_ |= kRemovalMask; }
    bool isRemoved() const { return size_ & kRemovalMask; }

   private:
    uint32_t size_;
    mutable uint8_t data_[];

    static constexpr uint32_t kRemovalMask = 1u << 31;
    static constexpr uint32_t kSizeMask = (1u << 22) - 1;
  };

  const uint32_t capacity_{0}; // how many bytes this buffer has in total
  uint32_t deletedBytes_{0};   // number of bytes from deleted allocations
  uint32_t nextByte_{0};       // next free byte that can be allocated
  uint8_t data_[];

  // Get the slot starting at this data offset
  Buffer::Slot* getSlot(uint32_t dataOffset);
  const Buffer::Slot* getSlot(uint32_t dataOffset) const;
  const Buffer::Slot* getSlotImpl(uint32_t dataOffset) const;

  // Get the first slot
  // @return nullptr if there is no slot allocated
  Buffer::Slot* getFirstSlot();

  // Get the next slot after this one
  // @return nullptr if there is no more slot
  Buffer::Slot* getNextSlot(const Slot& curSlot);

  // Get the data offset given this slot
  uint32_t getDataOffset(const Slot& slot) const;

  friend void testBufferSlot();
};

// This is how we represent an allocation's offset from Buffer Manager's
// perspective. BufferAddr is opaque to the user.
//
// BufferAddr's itemoffset
class FOLLY_PACK_ATTR BufferAddr {
 private:
  static constexpr uint32_t kInvalidOffset =
      std::numeric_limits<uint32_t>::max();

  // We allow up to 22bits for byte offset because it cannot be bigger
  // than 4MB which is how big a slab is in cachelib.
  static constexpr uint32_t kByteOffsetBits = Slab::kNumSlabBits;
  static constexpr uint32_t kByteOffsetMask =
      (static_cast<uint32_t>(1) << kByteOffsetBits) - static_cast<uint32_t>(1);

 public:
  static constexpr uint32_t kMaxNumChainedItems =
      (static_cast<uint32_t>(1)
       << (cachelib::NumBits<uint32_t>::value - kByteOffsetBits)) -
      static_cast<uint32_t>(1);

  BufferAddr() = default;
  /* implicit */ BufferAddr(std::nullptr_t) : BufferAddr() {}
  BufferAddr(uint32_t itemOffset, uint32_t byteOffset)
      : offset_(compress(itemOffset, byteOffset)) {}

  BufferAddr(const BufferAddr&) = default;
  BufferAddr& operator=(const BufferAddr&) = default;

  bool operator==(std::nullptr_t) const { return offset_ == kInvalidOffset; }
  bool operator!=(std::nullptr_t) const { return !(*this == nullptr); }

  operator bool() const { return *this != nullptr; }

  // getItemOffset is only valid if the bufferAddr != nullptr
  uint32_t getItemOffset() const { return offset_ >> kByteOffsetBits; }

  // getByteOffset is only valid if the bufferAddr != nullptr
  uint32_t getByteOffset() const { return offset_ & kByteOffsetMask; }

  bool operator==(const BufferAddr& rhs) const {
    return getItemOffset() == rhs.getItemOffset() &&
           getByteOffset() == rhs.getByteOffset();
  }

  bool operator!=(const BufferAddr& rhs) const { return !(*this == rhs); }

 private:
  // bits[22:32) are for item offset, [0 - 1024) chained items
  // bits[0:22) are for byte offset, [0 - 4MB) address range
  uint32_t offset_{kInvalidOffset};

  // Compress a item offset and a byte offset into one single uint32_t
  static uint32_t compress(uint32_t itemOffset, uint32_t byteOffset) {
    XDCHECK_LE(itemOffset, kMaxNumChainedItems);
    XDCHECK_LE(byteOffset, kByteOffsetMask);
    const uint32_t upperBits = itemOffset << kByteOffsetBits;
    return upperBits | byteOffset;
  }
};
inline bool operator==(std::nullptr_t, const BufferAddr& rhs) {
  return rhs == nullptr;
}
inline bool operator!=(std::nullptr_t, const BufferAddr& rhs) {
  return rhs != nullptr;
}

template <typename T, typename Mgr>
class BufferManagerIterator;

// BufferManager is an allocator that managements allocations that live
// on cachelib items. It takes a parent handle, and uses *only* chained
// items associated with that parent for allocations. The parent item's
// memory content is never modified. BufferManager will initially start
// with one chained item sized at a user-customized size. It will grow
// as user keeps allocating and eventually when it has reached maximum
// size for the chained item, it will allocate a second one at the max
// capacity. And so on, until we reach the 1024 chained items limit. The
// limit is defined as BufferAddr::kMaxNumChainedItems.
//
// The ordering of allocation is always to fill up the first chained item
// and then allocate a new one, fill it up as well, and so on. If a user
// removes allocations, it creates holes that will eventually be compacted
// after the size of the holes reach a certain threshold.
//
// BufferManager does NOT own any allocation it is managing. It is only
// a convenience class that offers an allocator semantics making use of
// chained items. The ownership rests solely on whoever owns the parent.
template <typename C>
class BufferManager {
 public:
  using CacheType = C;
  using ChainedAllocs = typename CacheType::ChainedAllocs;
  using Item = typename CacheType::Item;
  using WriteHandle = typename Item::WriteHandle;

  // TODO: T95574601 remove compaction callback in favor of using iterator to
  //       update hash table.
  // Compaction callback
  // @param void*       pointer to a copy of the original allocation in the
  //                    new buffer
  // @param BufferAddr  the location of the allocation in the new buffer
  using CompactionCB = std::function<void(void*, BufferAddr)>;

  // Maximum size for a single chained item.
  // TODO: T37573713 This is just under 1MB to allow some room for the chained
  //       item header and buffer header. We can make this configurable.
  static constexpr uint32_t kMaxBufferCapacity = 1024 * 1024 - 100;

  BufferManager() = default;

  // Construct a null BufferManager
  /* implicit */ BufferManager(std::nullptr_t) : BufferManager() {}

  // Construct a new BufferManager
  // @throw cachelib::exceptions::OutOfMemory if failing to add a new buffer
  //        std::invalid_argument if initialCapacity is bigger than the max
  BufferManager(CacheType& cache, WriteHandle& parent, uint32_t initialCapacity)
      : cache_(&cache), parent_(&parent) {
    if (initialCapacity > kMaxBufferCapacity) {
      throw std::invalid_argument(
          folly::sformat("A buffer's max capacity is {}, but requested "
                         "initialCapacity is: {}",
                         kMaxBufferCapacity, initialCapacity));
    }

    auto* buffer = addNewBuffer(initialCapacity);
    if (!buffer) {
      throw cachelib::exception::OutOfMemory(
          folly::sformat("Couldn't allocate a new buffer for parent item: {}",
                         parent->toString()));
    }
  }

  // Initialize a BufferManager with an existing parent
  BufferManager(CacheType& cache, WriteHandle& parent)
      : cache_(&cache), parent_(&parent) {
    if (parent) {
      materializeChainedAllocs();
    }
  }

  BufferManager(BufferManager&& rhs) noexcept
      : cache_(rhs.cache_),
        parent_(rhs.parent_),
        buffers_(std::move(rhs.buffers_)) {
    rhs.cache_ = nullptr;
    rhs.parent_ = nullptr;
  }

  BufferManager& operator=(BufferManager&& rhs) {
    if (this != &rhs) {
      this->~BufferManager();
      new (this) BufferManager(std::move(rhs));
    }
    return *this;
  }

  // If true it means there's no valid cache or parent handle assocaited with
  // this BufferManager, no operation is valid.
  bool empty() const { return !cache_; }

  // Allocate a new allocation from existing buffers.
  // If there isn't enough storage in the existing buffers in BufferManager,
  // expand should be called, after which allocate can be tried again.
  //
  // Returns null BuffAddr if fails.
  BufferAddr allocate(uint32_t size);

  // Expand buffer or add new buffer for a new allocation. allocate should be
  // called after it.
  // Returns false if fails.
  bool expand(uint32_t size);

  // Remove allocation starting this addr
  // Operation is O(1)
  //
  // @throw std::invalid_argument on addr being nullptr
  void remove(BufferAddr addr);

  // Get an object stored at the corresponding itemOffset and byteOffset
  //
  // @throw std::invalid_argument on addr being nullptr
  template <typename T>
  T* get(BufferAddr addr) const;

  // Clone a buffer manager within the same cache under another parent
  BufferManager<C> clone(WriteHandle& parent) const;

  // Compact buffers underneath. The layout of existing allocations may change
  // as a result.
  void compact();

  // Return bytes left unused (can be used for future allocaitons)
  size_t remainingBytes() const;

  // Returns bytes left behind by removed allocations
  size_t wastedBytes() const;

  // Percentage of amount of space wasted
  size_t wastedBytesPct() const;

 private:
  // Allocate an allocation from an existing buffer
  BufferAddr allocateFrom(Buffer* buffer,
                          uint32_t size,
                          uint32_t nthChainedItem);

  // Add a new Buffer by adding a new chained item to the head of the chain
  Buffer* addNewBuffer(uint32_t capacity);

  // Expand the buffer with minAdditionalSize or more
  Buffer* expandBuffer(Item& itemToExpand, uint32_t minAdditionalSize);

  // Translates BufferAddr into raw void*
  void* getImpl(BufferAddr addr) const;

  // Get a buffer corresponding to the chained item index
  Buffer* getBuffer(uint32_t index) const;

  // Get a list of all chained allocs upfront, in reverse order
  void materializeChainedAllocs();

  // BEGIN private members
  CacheType* cache_{nullptr};
  WriteHandle* parent_{nullptr};
  std::vector<Item*> buffers_{};
  // END private members

  // This is the factor of expansion we use to grow our initiial chained item
  static constexpr uint32_t kExpansionFactor = 2;

  template <typename T, typename Mgr>
  friend class BufferManagerIterator;
  template <typename AllocatorT>
  friend class facebook::cachelib::tests::BufferManagerTest;
};

template <typename T, typename Mgr>
class BufferManagerIterator
    : public IteratorFacade<BufferManagerIterator<T, Mgr>,
                            T,
                            std::forward_iterator_tag> {
 public:
  explicit BufferManagerIterator(const Mgr& mgr)
      : mgr_(mgr),
        curr_(mgr_.cache_->viewAsWritableChainedAllocs(*mgr_.parent_)
                  .getNthInChain(index_)
                  ->template getMemoryAs<Buffer>()
                  ->begin()),
        numChainedItems_(static_cast<uint32_t>(
            mgr_.cache_->viewAsChainedAllocs(*mgr_.parent_)
                .computeChainLength())) {
    if (curr_ == Buffer::Iterator()) {
      // Currently, curr_ is invalid. So we increment to try to find
      // an valid iterator
      incrementIntoNextBuffer();
    }
  }

  enum EndT { End };
  BufferManagerIterator(const Mgr& mgr, EndT) : mgr_(mgr) {}

  BufferAddr getAsBufferAddr() const {
    return BufferAddr{numChainedItems_ - index_ - 1 /* itemOffset */,
                      curr_.getDataOffset()};
  }

  // Calling increment when we have reached the end will result in
  // a null iterator.
  // @throw std::out_of_range if moving the iterator past the end
  void increment() {
    if (curr_ == Buffer::Iterator{}) {
      throw std::out_of_range(
          "BufferManagerIterator:: Moving past the end of all buffers.");
    }

    ++curr_;
    if (curr_ == Buffer::Iterator{}) {
      incrementIntoNextBuffer();
    }
  }

  // @throw std::runtime_error if we're dereferencing a null iterator
  T& dereference() const {
    if (curr_ == Buffer::Iterator{}) {
      throw std::runtime_error(
          "BufferManagerIterator:: deferencing a null Iterator.");
    }
    return reinterpret_cast<T&>(curr_.dereference());
  }

  bool equal(const BufferManagerIterator& other) const {
    return &mgr_ == &other.mgr_ && curr_ == other.curr_;
  }

 private:
  void incrementIntoNextBuffer() {
    while (curr_ == Buffer::Iterator{}) {
      auto allocs = mgr_.cache_->viewAsWritableChainedAllocs(*mgr_.parent_);
      auto* item = allocs.getNthInChain(++index_);
      if (!item) {
        // we've reached the end of BufferManager
        return;
      }

      curr_ = item->template getMemoryAs<Buffer>()->begin();
    }
  }

  uint32_t index_{0};
  const Mgr& mgr_;
  Buffer::Iterator curr_{};
  const uint32_t numChainedItems_{0};
};

} // namespace detail
} // namespace cachelib
} // namespace facebook

#include "cachelib/datatype/Buffer-inl.h"
