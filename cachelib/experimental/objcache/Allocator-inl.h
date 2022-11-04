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

namespace facebook {
namespace cachelib {
namespace detail {
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
} // namespace detail

namespace objcache {
template <typename Impl, typename CacheDescriptor>
void* AllocatorResource<Impl, CacheDescriptor>::allocateFallback(
    size_t bytes, size_t alignment) {
  switch (alignment) {
  default:
    return std::allocator<uint8_t>{}.allocate(bytes);
  case std::alignment_of<uint16_t>():
    return std::allocator<uint16_t>{}.allocate(bytes);
  case std::alignment_of<uint32_t>():
    return std::allocator<uint32_t>{}.allocate(bytes);
  case std::alignment_of<uint64_t>():
    return std::allocator<uint64_t>{}.allocate(bytes);
  case std::alignment_of<std::max_align_t>():
    return std::allocator<std::max_align_t>{}.allocate(bytes);
  }
}

template <typename Impl, typename CacheDescriptor>
void AllocatorResource<Impl, CacheDescriptor>::deallocateFallback(
    void* alloc, size_t bytes, size_t alignment) {
  switch (alignment) {
  default:
    std::allocator<uint8_t>{}.deallocate(reinterpret_cast<uint8_t*>(alloc),
                                         bytes);
    break;
  case std::alignment_of<uint16_t>():
    std::allocator<uint16_t>{}.deallocate(reinterpret_cast<uint16_t*>(alloc),
                                          bytes);
    break;
  case std::alignment_of<uint32_t>():
    std::allocator<uint32_t>{}.deallocate(reinterpret_cast<uint32_t*>(alloc),
                                          bytes);
    break;
  case std::alignment_of<uint64_t>():
    std::allocator<uint64_t>{}.deallocate(reinterpret_cast<uint64_t*>(alloc),
                                          bytes);
    break;
  case std::alignment_of<std::max_align_t>():
    std::allocator<std::max_align_t>{}.deallocate(
        reinterpret_cast<std::max_align_t*>(alloc), bytes);
    break;
  }
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
} // namespace objcache
} // namespace cachelib
} // namespace facebook
