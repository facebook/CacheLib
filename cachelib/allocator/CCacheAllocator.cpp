#include "cachelib/allocator/CCacheAllocator.h"

#include <folly/logging/xlog.h>

namespace facebook {
namespace cachelib {

CCacheAllocator::CCacheAllocator(MemoryAllocator& allocator, PoolId poolId)
    : allocator_(allocator), poolId_(poolId), currentChunksIndex_(0) {
  XDCHECK_EQ(0u, getNumChunks());
  resize();
}

CCacheAllocator::CCacheAllocator(MemoryAllocator& allocator,
                                 PoolId poolId,
                                 const SerializationType& object)
    : CCacheAllocatorBase(object.ccMetadata),
      allocator_(allocator),
      poolId_(poolId),
      currentChunksIndex_(0) {
  auto& currentChunks = chunks_[currentChunksIndex_];
  for (auto chunk : object.chunks) {
    currentChunks.push_back(allocator_.unCompress(CompressedPtr(chunk)));
  }
}

size_t CCacheAllocator::getConfiguredSize() const {
  return allocator_.getPool(poolId_).getPoolSize();
}

std::string CCacheAllocator::getName() const {
  return allocator_.getPoolName(poolId_);
}

size_t CCacheAllocator::resize() {
  auto chunks = chunks_[currentChunksIndex_];

  const size_t currChunks = chunks.size();
  const size_t curSize = currChunks * getChunkSize();

  /* Round size down to nearest even chunk_size multiple. */
  const size_t newSize = getConfiguredSize();
  const size_t numNewChunks = newSize / getChunkSize();
  const size_t newSizeWanted = numNewChunks * getChunkSize();

  if (numNewChunks < currChunks) {
    /* Shrink cache. Simply release the last N chunks. */
    while (numNewChunks < chunks.size()) {
      XDCHECK(chunks.back() != nullptr);
      release(chunks.back());
      chunks.pop_back();
    }
  } else if (numNewChunks > currChunks) {
    size_t i;
    for (i = currChunks; i < numNewChunks; i++) {
      void* chunk = allocate();
      if (chunk == nullptr) {
        break;
      }
      chunks.push_back(chunk);
    }

    if (chunks.size() != numNewChunks) {
      XLOGF(CRITICAL,
            "Unable to fully increase memory size for pool {}. Wanted to "
            "allocate {} new chunks. Allocated {} chunks increasing arena size "
            "from {} to {} bytes.",
            poolId_, numNewChunks - currChunks, i - currChunks, curSize,
            newSizeWanted);
    }
  }

  chunks_[currentChunksIndex_ ^ 1] = chunks;
  currentChunksIndex_ ^= 1;
  return chunks.size();
}

CCacheAllocator::SerializationType CCacheAllocator::saveState() {
  CCacheAllocator::SerializationType object;
  object.ccMetadata = ccType_.saveState();

  std::lock_guard<std::mutex> guard(resizeLock_);
  for (auto chunk : getCurrentChunks()) {
    object.chunks.push_back(allocator_.compress(chunk).saveState());
  }
  return object;
}

void* CCacheAllocator::allocate() {
  return allocator_.allocateZeroedSlab(poolId_);
}

void CCacheAllocator::release(void* chunk) {
  auto context = allocator_.startSlabRelease(
      poolId_,
      allocator_.getAllocationClassId(poolId_,
                                      static_cast<uint32_t>(getChunkSize())),
      Slab::kInvalidClassId, SlabReleaseMode::kResize, chunk);
  XDCHECK_EQ(1u, context.getActiveAllocations().size());
  allocator_.free(chunk);
  allocator_.completeSlabRelease(context);
}

} // namespace cachelib
} // namespace facebook
