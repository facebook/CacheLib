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
