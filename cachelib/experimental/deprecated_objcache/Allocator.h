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

#include <cstddef>
#include <memory>
#include <scoped_allocator>

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/common/Exceptions.h"

namespace facebook::cachelib::detail {
// Object-cache's c++ allocator will need to create a zero refcount handle in
// order to access CacheAllocator API. We don't need to have any refcount
// pending for this, because the contract of ObjectCache is that user must
// hold an outstanding refcount (via an ItemHandle or ObjectHandle) at any
// time when they're accessing the data structures backed by the item.
template <typename ItemHandle, typename Item, typename Cache>
ItemHandle* objcacheInitializeZeroRefcountHandle(void* handleStorage,
                                                 Item* it,
                                                 Cache& alloc) {
  return new (handleStorage) typename Cache::WriteHandle{it, alloc};
}

// Object-cache's c++ allocator needs to access CacheAllocator directly from
// an item handle in order to access CacheAllocator APIs
template <typename ItemHandle2>
typename ItemHandle2::CacheT& objcacheGetCache(const ItemHandle2& hdl) {
  return hdl.getCache();
}

template <typename T>
constexpr size_t toMultipleOfAlignment(size_t bytes) {
  constexpr size_t alignment = alignof(T);
  return (bytes % alignment == 0u) ? bytes
                                   : (bytes / alignment + 1) * alignment;
}
} // namespace facebook::cachelib::detail

namespace facebook::cachelib::objcache {
// Allocator that conforms to the std::allocator interface. This is meant
// to be a thin shim that wraps the various cachelib-backed allocators
// underneath. This allocator is stateful and two allocators may compare equal
// only if the underlying allocator resource is equal. We also explicitly
// disable propagation on copy-assignment, move-assignment, and swap in order
// to prevent user from mixing cachelib-memory with jemalloc-memory.
//
// The relationship of components in this file is as follows:
//   Allocator
//      |
//   AllocatorResource
//      |
//   <implementation of a cachelib-backed allocator>
//
// The reason we use private inheritance is to avoid the allocator resource
// taking up unnecessary space in case it is empty.
template <typename T, typename AllocatorResource>
class Allocator : private AllocatorResource {
 public:
  using value_type = T;
  using propagate_on_container_copy_assignment = std::false_type;
  using propagate_on_container_move_assignment = std::false_type;
  using propagate_on_container_swap = std::false_type;

  Allocator() = default;
  explicit Allocator(AllocatorResource resource)
      : AllocatorResource{resource} {}

  // We allow copy construction from an allocator of a different value_type and
  // AllocatorResource, as long as the AllocatorResource is compatible. E.g. all
  // cachelib-backed AllocatorResource must have the same type of ItemHandle.
  template <typename T2, typename AllocatorResource2>
  friend class Allocator;
  template <typename T2, typename AllocatorResource2>
  /* implicit */ Allocator(
      const Allocator<T2, AllocatorResource2>& other) noexcept
      : AllocatorResource{static_cast<const AllocatorResource2&>(other)} {}

  // @param size    the size of the allocation in units of sizeof(T)
  // @return        a valid pointer pointing at the allocation with at
  //                least (size * sizeof(T)) bytes
  // @throw         ObjectCacheAllocationError on failure
  value_type* allocate(size_t size) {
    size_t bytes = size * sizeof(value_type);
    return reinterpret_cast<value_type*>(
        static_cast<AllocatorResource*>(this)->allocate(
            bytes, std::alignment_of<value_type>()));
  }

  // @param alloc   a valid pointer pointing the allocation to be freed
  // @param size    size of the allocation. It must be the same as the
  //                original requested size in units of sizeof(T)
  // @throw         ObjectCacheDeallocationBadArgs if bad arguments
  void deallocate(value_type* alloc, size_t size) {
    size_t bytes = size * sizeof(value_type);
    static_cast<AllocatorResource*>(this)->deallocate(
        alloc, bytes, std::alignment_of<value_type>());
  }

  AllocatorResource& getAllocatorResource() {
    return *static_cast<AllocatorResource*>(this);
  }

  template <typename U, typename V>
  friend bool operator==(const Allocator<T, AllocatorResource>& a,
                         const Allocator<U, V>& b) noexcept {
    return a.isEqual(b);
  }

  template <typename U, typename V>
  friend bool operator!=(const Allocator<T, AllocatorResource>& a,
                         const Allocator<U, V>& b) noexcept {
    return !(a == b);
  }
};

// Base class for a cachelib-backed allocator. This component enfores
// the same interface across all cachelib-backed allocator, and also
// takes care of equality-comparison between different allocators. In
// addition, in the case of an "empty" AllocatorResource, this will
// fall back to global allocator.
template <typename Impl, typename CacheDescriptor>
class AllocatorResource {
 public:
  template <typename Impl2, typename CacheDescriptor2>
  friend class AllocatorResource;

  using ItemHandle = typename CacheDescriptor::ItemHandle;
  using Item = typename CacheDescriptor::Item;
  using ChainedItem = typename CacheDescriptor::ChainedItem;

  AllocatorResource() = default;
  explicit AllocatorResource(ItemHandle* hdl) : hdl_{hdl} { XDCHECK(*hdl_); }

  template <typename Impl2, typename CacheDescriptor2>
  explicit AllocatorResource(
      const AllocatorResource<Impl2, CacheDescriptor2>& other) noexcept
      : hdl_{other.hdl_} {}

  // @param bytes       the size of the allocation in units of bytes
  // @param alignment   align the returning address to this
  // @return        a valid pointer pointing at the allocation with at
  //                least requested bytes
  // @throw         ObjectCacheAllocationError on failure
  void* allocate(size_t bytes, size_t alignment) {
    if (!hdl_) {
      return allocateFallback(bytes, alignment);
    }
    return static_cast<Impl*>(this)->allocateImpl(bytes, alignment);
  }

  // @param alloc       a valid pointer pointing the allocation to be freed
  // @param size        size of the allocation. It must be the same as the
  //                    original requested bytes
  // @param alignment   the alloc must be aligned to this
  // @throw         ObjectCacheDeallocationBadArgs if bad arguments
  void deallocate(void* alloc, size_t bytes, size_t alignment) {
    if (!hdl_) {
      deallocateFallback(alloc, bytes, alignment);
      return;
    }
    return static_cast<Impl*>(this)->deallocateImpl(alloc, bytes, alignment);
  }

  // @return true if two instances of AllocatorResource is compatible,
  //         false otherwise.
  template <typename Impl2, typename CacheDescriptor2>
  bool isEqual(
      const AllocatorResource<Impl2, CacheDescriptor2>& other) const noexcept {
    return hdl_ == other.hdl_;
  }

 private:
  // Fall-back allocation and deallocation when this allocator is NOT associated
  // with a cache item. We use the global allocator for fallback.
  void* allocateFallback(size_t bytes, size_t alignment);
  void deallocateFallback(void* alloc, size_t bytes, size_t alignment);

 protected:
  // TODO: can we use some form of a compressed pointer? As-is, each allocator
  // is 8 bytes. This means for small structures that need allocator, this
  // can be a hefty space overhead.
  //
  // Pointer to the item handle. The actual storage is located in the parent
  // item's memory. Note that this handle is a special one. It does not have
  // any outstanding refcount. The reason we need a handle is to access
  // CacheAllocator APIs for allocating additional chained items.
  ItemHandle* hdl_{};
};

// Traits that describe what cache we use that underlies our allocator
template <typename CacheT>
struct CacheDescriptor {
  using Cache = CacheT;
  using ItemHandle = typename CacheT::WriteHandle;
  using Item = typename CacheT::Item;
  using ChainedItem = typename CacheT::ChainedItem;
};

template <typename CacheDescriptor>
class MonotonicBufferResource
    : public AllocatorResource<MonotonicBufferResource<CacheDescriptor>,
                               CacheDescriptor> {
 public:
  using Base = AllocatorResource<MonotonicBufferResource<CacheDescriptor>,
                                 CacheDescriptor>;
  using ItemHandle = typename Base::ItemHandle;
  using Item = typename Base::Item;
  using ChainedItem = typename Base::ChainedItem;

  // Metadata for this allocator. This is located in the beginning of a cache
  // item associated with this allocator.
  struct FOLLY_PACK_ATTR Metadata {
    // This is a one-byte value for us to detect this is a cachelib-backed
    // allocator. This is best effort since it is possible an arbitrary
    // user-created blob in cache could also contain this byte.
    uint16_t magicCookie{0xbeef};
    // TODO: consider updating CacheAllocator to allow us using an "item"
    //       for manipulating chained items. That would mean we no longer
    //       have to store "item handle" but just a pair of pointers to
    //       the item and the cache allocator. That would require 16 bytes
    //       compared to the 40 bytes right now.
    // Storaged for item handle we need for accessing CacheAllocator API
    uint8_t itemHandleStorage[sizeof(ItemHandle)]{};
    // Number of bytes currently used by structures backed by this allocator
    uint32_t usedBytes{};
    // Remaining bytes that can be allocated for new allocations
    uint32_t remainingBytes{};
    // Pointer to the beginning of free buffer
    uint8_t* buffer{};
  };
  static_assert(58 == sizeof(Metadata), "Incorrect size for metadata");

  static constexpr uint32_t metadataSize() { return sizeof(Metadata); }

  // Undefined behavior if the allocator was not initialized with a
  // reserved storage
  static void* getReservedStorage(void* memory, size_t alignment) {
    auto ptr = reinterpret_cast<uintptr_t>(memory);
    ptr += metadataSize();
    ptr = (ptr + alignment - 1) / alignment * alignment;
    return reinterpret_cast<void*>(ptr);
  }

  using AllocatorResource<MonotonicBufferResource<CacheDescriptor>,
                          CacheDescriptor>::AllocatorResource;

  // Implement AllocatorResource::allocate
  void* allocateImpl(size_t bytes, size_t alignment);

  // Implement AllocatorResource::deallocate
  void deallocateImpl(void* alloc, size_t byte, size_t alignment);

  // Return a read-only view of metdata. Useful for tests and debugging.
  const Metadata* viewMetadata() const {
    return const_cast<MonotonicBufferResource*>(this)->getMetadata();
  }

 private:
  Metadata* getMetadata() {
    XDCHECK(this->hdl_);
    return reinterpret_cast<Metadata*>((*this->hdl_)->getMemory());
  }

  // Allocate from existing storage in the allocator
  // @return a valid pointer pointing at the allocation with at least requested
  //         bytes. Nullptr if insufficient storage.
  void* allocateFast(size_t bytes, size_t alignment);

  // Allocate additional storage from Cache to satisfy this allocation
  // @return a valid pointer pointing at the allocation with at
  //         least requested bytes
  // @throw  AllocationError on failure
  void* allocateSlow(size_t bytes, size_t alignment);
};

template <typename Impl, typename CacheDescriptor>
void* AllocatorResource<Impl, CacheDescriptor>::allocateFallback(
    size_t bytes, size_t alignment) {
  switch (alignment) {
  default:
    return std::aligned_alloc(alignof(uint8_t),
                              detail::toMultipleOfAlignment<uint8_t>(bytes));

  case alignof(uint16_t):
    return std::aligned_alloc(alignof(uint16_t),
                              detail::toMultipleOfAlignment<uint16_t>(bytes));

  case alignof(uint32_t):
    return std::aligned_alloc(alignof(uint32_t),
                              detail::toMultipleOfAlignment<uint32_t>(bytes));

  case alignof(uint64_t):
    return std::aligned_alloc(alignof(uint64_t),
                              detail::toMultipleOfAlignment<uint64_t>(bytes));

  case alignof(std::max_align_t):
    return std::aligned_alloc(
        alignof(std::max_align_t),
        detail::toMultipleOfAlignment<std::max_align_t>(bytes));
  }
}

template <typename Impl, typename CacheDescriptor>
void AllocatorResource<Impl, CacheDescriptor>::deallocateFallback(
    void* alloc, size_t /* bytes */, size_t /* alignment */) {
  return std::free(alloc);
}

template <typename CacheDescriptor>
void MonotonicBufferResource<CacheDescriptor>::deallocateImpl(
    void* /* alloc */, size_t bytes, size_t /* alignment */) {
  getMetadata()->usedBytes -= bytes;
  // Do not deallocate since we don't reuse any previously freed bytes
}

template <typename CacheDescriptor>
void* MonotonicBufferResource<CacheDescriptor>::allocateImpl(size_t bytes,
                                                             size_t alignment) {
  auto* alloc = allocateFast(bytes, alignment);
  if (!alloc) {
    return allocateSlow(bytes, alignment);
  }
  return alloc;
}

template <typename CacheDescriptor>
void* MonotonicBufferResource<CacheDescriptor>::allocateFast(size_t bytes,
                                                             size_t alignment) {
  auto* metadata = getMetadata();
  if (metadata->remainingBytes < bytes) {
    return nullptr;
  }

  alignment = alignment >= std::alignment_of<std::max_align_t>()
                  ? std::alignment_of<std::max_align_t>()
                  : alignment;
  auto alloc = reinterpret_cast<uintptr_t>(metadata->buffer);
  auto alignedAlloc = (alloc + alignment - 1) / alignment * alignment;
  size_t padding = alignedAlloc - alloc;
  size_t allocBytes = bytes + padding;
  if (metadata->remainingBytes < allocBytes) {
    return nullptr;
  }
  metadata->usedBytes += allocBytes;
  metadata->buffer += allocBytes;
  metadata->remainingBytes -= allocBytes;
  return reinterpret_cast<void*>(alignedAlloc);
}

template <typename CacheDescriptor>
void* MonotonicBufferResource<CacheDescriptor>::allocateSlow(size_t bytes,
                                                             size_t alignment) {
  // Minimal new buffer size is 2 times the requested size for now
  constexpr uint32_t kNewBufferSizeFactor = 2;
  uint32_t newBufferSize =
      std::max(getMetadata()->usedBytes * 2,
               static_cast<uint32_t>(bytes) * kNewBufferSizeFactor);
  newBufferSize =
      std::min(newBufferSize, static_cast<uint32_t>(Slab::kSize) - 1000);

  if (bytes > newBufferSize) {
    throw std::runtime_error(
        fmt::format("Alloc request exceeding maximum. Requested: {}, Max: {}",
                    bytes, newBufferSize));
  }
  // The layout in our chained item is as follows.
  //
  // |-header-|-key-|-GAP-|-storage-|
  //
  // The storage always start at aligned boundary
  alignment = alignment >= std::alignment_of<std::max_align_t>()
                  ? std::alignment_of<std::max_align_t>()
                  : alignment;
  // Key is 4 bytes for chained item because it is a compressed pointer
  uint32_t itemTotalSize = sizeof(ChainedItem) + 4 + newBufferSize;

  // We may have to add 8 bytes additional to an item if our alignment
  // is larger than 8 bytes boundary (which means 16 bytes aka std::max_align_t)
  uint32_t alignedItemTotalSize =
      alignment > std::alignment_of<uint64_t>()
          ? util::getAlignedSize(itemTotalSize + 8, alignment)
          : util::getAlignedSize(itemTotalSize, alignment);
  uint32_t extraBytes = alignedItemTotalSize - itemTotalSize;

  auto chainedItemHandle =
      detail::objcacheGetCache(*this->hdl_)
          .allocateChainedItem(*this->hdl_, newBufferSize + extraBytes);
  if (!chainedItemHandle) {
    throw exception::ObjectCacheAllocationError(
        folly::sformat("Slow Path. Failed to allocate a chained item. "
                       "Requested size: {}. Associated Item: {}",
                       newBufferSize, (*this->hdl_)->toString()));
  }

  uintptr_t bufferStart =
      reinterpret_cast<uintptr_t>(chainedItemHandle->getMemory());
  uintptr_t alignedBufferStart =
      (bufferStart + (alignment - 1)) / alignment * alignment;

  detail::objcacheGetCache(*this->hdl_)
      .addChainedItem(*this->hdl_, std::move(chainedItemHandle));

  auto* metadata = getMetadata();
  uint8_t* alloc = reinterpret_cast<uint8_t*>(alignedBufferStart);
  metadata->usedBytes += bytes;
  metadata->buffer = alloc + bytes;
  metadata->remainingBytes = newBufferSize - bytes;
  return alloc;
}

// Create a new monotonic allocator
//
// @param cache   cache where we allocate bytes for this allocator
// @param poolId  which cache pool we will be using for this allocator
// @param key     key associated with this allocator
// @param reservedBytes     storage reserved for user-manipulation, which
//                          will not be used for allocation
// @param additionalBytes   additional storage in the parent item, which
//                          will be used to serve the initial allocations
// @param alignment         align reservedBytes
//
// @return [ItemHandle, MonotonicBufferResource] if successful
//          The lifetime of this allocator is tied to the ItemHandle.
//          The allocator itself holds no reference and can only be used
//          if there is at least one outstanding item handle in scope.
// @throw  ObjectCacheAllocationError on failure
template <typename MonotonicBufferResource, typename Cache>
std::pair<typename Cache::WriteHandle, MonotonicBufferResource>
createMonotonicBufferResource(Cache& cache,
                              PoolId poolId,
                              folly::StringPiece key,
                              uint32_t reservedBytes,
                              uint32_t additionalBytes,
                              size_t alignment,
                              uint32_t ttlSecs = 0,
                              uint32_t creationTime = 0) {
  // The layout in our parent item is as follows.
  //
  // |-header-|-key-|-metadata-|-GAP-|-reserved-|-additional-|
  //
  // The "reserved" storage always start at aligned boundary
  alignment = alignment >= std::alignment_of<std::max_align_t>()
                  ? std::alignment_of<std::max_align_t>()
                  : alignment;

  uint32_t sizeOfMetadata = sizeof(typename MonotonicBufferResource::Metadata);
  uint32_t bytes = sizeOfMetadata + reservedBytes + additionalBytes;

  uint32_t extraBytes = 0;
  if (reservedBytes > 0) {
    // We add at least "alignment" bytes to we can align the buffer start.
    // In addition, if the alignment is greater than 8 bytes, we pad an
    // additional 8 bytes because cachelib only guarantees alignment on 8
    // bytes boundary.
    extraBytes = alignment + 8 * (alignment > std::alignment_of<uint64_t>());
  }

  auto hdl =
      cache.allocate(poolId, key, bytes + extraBytes, ttlSecs, creationTime);
  if (!hdl) {
    throw exception::ObjectCacheAllocationError(folly::sformat(
        "Unable to allocate a new item for allocator. Key: {}, Requested "
        "bytes: {}",
        key, bytes));
  }

  auto* bufferStart =
      reinterpret_cast<uint8_t*>(MonotonicBufferResource::getReservedStorage(
          hdl->getMemory(), alignment)) +
      reservedBytes;
  auto* metadata =
      new (hdl->getMemory()) typename MonotonicBufferResource::Metadata();
  metadata->remainingBytes = additionalBytes;
  metadata->buffer = bufferStart;

  auto sharedHdl =
      detail::objcacheInitializeZeroRefcountHandle<typename Cache::WriteHandle>(
          &metadata->itemHandleStorage, hdl.get(), cache);

  return {std::move(hdl), MonotonicBufferResource{sharedHdl}};
}
} // namespace facebook::cachelib::objcache
