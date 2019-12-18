#pragma once

#include "cachelib/compact_cache/allocators/CCacheAllocatorBase.h"

namespace facebook {
namespace cachelib {

/**
 * Allocator for use in CCache Tests
 */
class TestAllocator : public CCacheAllocatorBase {
 public:
  /**
   * @param configuredSize Configured Size for allocator
   * @param chunkSize Required size of chunk/slab
   */
  TestAllocator(size_t configuredSize, size_t chunkSize)
      : configuredSize_(configuredSize), chunkSize_(chunkSize) {
    resize();
  }

  TestAllocator(TestAllocator&& allocator) noexcept
      : configuredSize_(allocator.configuredSize_),
        chunkSize_(allocator.chunkSize_),
        slabs_(std::move(allocator.slabs_)) {
    allocator.configuredSize_ = 0;
  }

  ~TestAllocator() {
    configuredSize_ = 0;
    resize();
  }

  size_t resize() {
    const auto numChunks = getNumChunks();
    const auto numNewChunks = configuredSize_ / chunkSize_;

    if (numNewChunks < numChunks) {
      for (size_t i = numNewChunks; i < numChunks; i++) {
        ::operator delete(slabs_[i]);
      }
      slabs_.resize(numNewChunks);
    } else if (numNewChunks > numChunks) {
      slabs_.resize(numNewChunks);
      for (size_t i = numChunks; i < numNewChunks; i++) {
        slabs_[i] = ::operator new(chunkSize_);
        assert(slabs_[i]);
        /* It might be the case that the memory that got allocated had garbage
         * data. So, we need to zero initialize the whole chunk */
        memset(slabs_[i], 0, chunkSize_);
      }
    }
    return slabs_.size();
  }

  size_t getChunkSize() const { return chunkSize_; }

  size_t getConfiguredSize() const { return configuredSize_; }

  void* getChunk(size_t chunkNum) { return slabs_[chunkNum]; }

  size_t getNumChunks() const noexcept { return slabs_.size(); }

  std::string getName() { return ""; }

  PoolId getPoolId() { return 0; }

 private:
  size_t configuredSize_;
  const size_t chunkSize_;
  std::vector<void*> slabs_;
};

} // namespace cachelib
} // namespace facebook
