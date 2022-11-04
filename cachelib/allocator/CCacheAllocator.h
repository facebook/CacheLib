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

#include "cachelib/allocator/memory/MemoryAllocator.h"
#include "cachelib/compact_cache/allocators/CCacheAllocatorBase.h"

// This is the allocator for compact cache provided by cache allocator for one
// pool
// It helps attached compact cache allocate/release memory from the
// specified memory pool in memory allocator
namespace facebook {
namespace cachelib {

class CCacheAllocator : public CCacheAllocatorBase {
 public:
  using SerializationType = serialization::CompactCacheAllocatorObject;

  // initialize the compact cache allocator from scratch or from a previous
  // state.
  CCacheAllocator(MemoryAllocator& allocator, PoolId poolId);
  CCacheAllocator(MemoryAllocator& allocator,
                  PoolId poolId,
                  const SerializationType& object);

  CCacheAllocator(const CCacheAllocator&) = delete;
  CCacheAllocator& operator=(const CCacheAllocator&) = delete;

  // Expand or shrink the number of chunks to getConfiguredSize()/getChunkSize()
  //
  // Here we assume that compact cache implementation is doing the right thing:
  // For expandsion, ccache will rehash after calling this resize()
  // For shrink, ccache will rehash before calling this resize()
  //
  // @return the number of chunks we have resized to
  size_t resize() override;

  // Return name assocaited with this allocator. This is the name associated
  // with the pool when user has created it via CacheAllocator::addPool()
  std::string getName() const;

  // This allocator uses CacheAllocator underneath and each chunk is an
  // entire cachelib slab.
  size_t getChunkSize() const { return Slab::kSize; }

  // return the configured size of the allocator
  size_t getConfiguredSize() const override;

  // return the pointer to the chunk with index _chunkNum_
  void* getChunk(size_t chunkNum) {
    if (chunkNum >= getNumChunks()) {
      throw std::invalid_argument(folly::sformat(
          "Trying to get chunk #{} but only {} chunks are available", chunkNum,
          getNumChunks()));
    }
    return getCurrentChunks()[chunkNum];
  }

  // return number of chunks
  size_t getNumChunks() const noexcept { return getCurrentChunks().size(); }

  bool overSized() const {
    return getNumChunks() * getChunkSize() > getConfiguredSize();
  }

  // return the pool id for the compact cache
  PoolId getPoolId() const { return poolId_; }

  // store allocate state into an object for warm roll
  SerializationType saveState();

 private:
  // return the current chunks vector
  const std::vector<void*>& getCurrentChunks() const {
    return chunks_[currentChunksIndex_];
  }

  // allocate a chunk of memory with size Slab::kSize
  //
  // @ return the pointer to the new memory
  //          nullptr if we run out of memory from MemoryAllocator
  //
  // @throw std::logic_error if compact cache is not enabled
  // @throw std::invalid_argument if the poolId_ is invalid
  void* allocate();

  // release the passed in chunk back in memory allocator
  //
  // @throw std::invalid_argument if the chunk or poolId_ is invalid OR
  //                              if the chunk does not belong to any active
  //                              allocation handed out by this allocator.
  void release(void* chunk);

  // MemoryAllocator reference and corresponding memory pool id
  MemoryAllocator& allocator_;
  const PoolId poolId_;

  // two chunks vectors, one is in use while the other is spare
  // according to currentChunksIndex_
  std::vector<void*> chunks_[2];

  // index of the currently used chunks vector
  std::atomic<size_t> currentChunksIndex_{0};
};

} // namespace cachelib
} // namespace facebook
