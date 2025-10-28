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

#include "cachelib/allocator/memory/MemoryAllocator.h"

#include <folly/Format.h>

#include <algorithm>

using namespace facebook::cachelib;

namespace {
// helper function.
void checkConfig(const MemoryAllocator::Config& config) {
  if (config.allocSizes.size() > MemoryAllocator::kMaxClasses) {
    throw std::invalid_argument("Too many allocation classes");
  }
}
} // namespace

MemoryAllocator::MemoryAllocator(Config config,
                                 void* memoryStart,
                                 size_t memSize)
    : config_(std::move(config)),
      slabAllocator_(memoryStart,
                     memSize,
                     {config_.disableFullCoredump, config_.lockMemory}),
      memoryPoolManager_(slabAllocator_) {
  checkConfig(config_);
}

MemoryAllocator::MemoryAllocator(Config config, size_t memSize)
    : config_(std::move(config)),
      slabAllocator_(memSize,
                     {config_.disableFullCoredump, config_.lockMemory}),
      memoryPoolManager_(slabAllocator_) {
  checkConfig(config_);
}

MemoryAllocator::MemoryAllocator(
    const serialization::MemoryAllocatorObject& object,
    void* memoryStart,
    size_t memSize,
    bool disableCoredump)
    : config_(std::set<uint32_t>{object.allocSizes()->begin(),
                                 object.allocSizes()->end()},
              *object.enableZeroedSlabAllocs(),
              disableCoredump,
              *object.lockMemory()),
      slabAllocator_(*object.slabAllocator(),
                     memoryStart,
                     memSize,
                     {config_.disableFullCoredump, config_.lockMemory}),
      memoryPoolManager_(*object.memoryPoolManager(), slabAllocator_) {
  checkConfig(config_);
}

void* MemoryAllocator::allocate(PoolId id, uint32_t size) {
  auto& mp = memoryPoolManager_.getPoolById(id);
  return mp.allocate(size);
}

void* MemoryAllocator::allocateZeroedSlab(PoolId id) {
  if (!config_.enableZeroedSlabAllocs) {
    throw std::logic_error("Zeroed Slab allcoation is not enabled");
  }
  auto& mp = memoryPoolManager_.getPoolById(id);
  return mp.allocateZeroedSlab();
}

PoolId MemoryAllocator::addPool(folly::StringPiece name,
                                size_t size,
                                const std::set<uint32_t>& allocSizes,
                                bool ensureProvisionable) {
  const std::set<uint32_t>& poolAllocSizes =
      allocSizes.empty() ? config_.allocSizes : allocSizes;

  if (poolAllocSizes.size() > MemoryAllocator::kMaxClasses) {
    throw std::invalid_argument("Too many allocation classes");
  }

  if (ensureProvisionable &&
      cachelib::Slab::kSize * poolAllocSizes.size() > size) {
    throw std::invalid_argument(folly::sformat(
        "Pool {} cannot have at least one slab for each allocation class. "
        "{} bytes required, {} bytes given.",
        name,
        cachelib::Slab::kSize * poolAllocSizes.size(),
        size));
  }

  return memoryPoolManager_.createNewPool(name, size, poolAllocSizes);
}

PoolId MemoryAllocator::getPoolId(std::string_view name) const noexcept {
  try {
    const auto& mp = memoryPoolManager_.getPoolByName(name);
    return mp.getId();
  } catch (const std::invalid_argument&) {
    return Slab::kInvalidPoolId;
  }
}

void MemoryAllocator::free(void* memory) {
  auto& mp = getMemoryPool(memory);
  mp.free(memory);
}

MemoryPool& MemoryAllocator::getMemoryPool(const void* memory) const {
  const auto* header = slabAllocator_.getSlabHeader(memory);
  if (header == nullptr) {
    throw std::invalid_argument("not recognized by this allocator");
  }
  auto poolId = header->poolId;
  return memoryPoolManager_.getPoolById(poolId);
}

ClassId MemoryAllocator::getAllocationClassId(PoolId poolId,
                                              uint32_t size) const {
  const auto& pool = memoryPoolManager_.getPoolById(poolId);
  return pool.getAllocationClassId(size);
}

serialization::MemoryAllocatorObject MemoryAllocator::saveState() {
  serialization::MemoryAllocatorObject object;
  object.allocSizes()->insert(config_.allocSizes.begin(),
                              config_.allocSizes.end());
  *object.enableZeroedSlabAllocs() = config_.enableZeroedSlabAllocs;
  *object.lockMemory() = config_.lockMemory;
  *object.slabAllocator() = slabAllocator_.saveState();
  *object.memoryPoolManager() = memoryPoolManager_.saveState();
  return object;
}

SlabReleaseContext MemoryAllocator::startSlabRelease(
    PoolId pid,
    ClassId victim,
    ClassId receiver,
    SlabReleaseMode mode,
    const void* hint,
    SlabReleaseAbortFn shouldAbortFn) {
  auto& pool = memoryPoolManager_.getPoolById(pid);
  return pool.startSlabRelease(victim, receiver, mode, hint,
                               config_.enableZeroedSlabAllocs, shouldAbortFn);
}

bool MemoryAllocator::isAllocFreed(const SlabReleaseContext& ctx,
                                   void* memory) const {
  const auto& pool = memoryPoolManager_.getPoolById(ctx.getPoolId());
  const auto& ac = pool.getAllocationClass(ctx.getClassId());
  return ac.isAllocFreed(ctx, memory);
}

bool MemoryAllocator::allAllocsFreed(const SlabReleaseContext& ctx) const {
  const auto& pool = memoryPoolManager_.getPoolById(ctx.getPoolId());
  const auto& ac = pool.getAllocationClass(ctx.getClassId());
  return ac.allFreed(ctx.getSlab());
}

void MemoryAllocator::processAllocForRelease(
    const SlabReleaseContext& ctx,
    void* memory,
    const std::function<void(void*)>& callback) const {
  const auto& pool = memoryPoolManager_.getPoolById(ctx.getPoolId());
  const auto& ac = pool.getAllocationClass(ctx.getClassId());
  return ac.processAllocForRelease(ctx, memory, callback);
}

void MemoryAllocator::completeSlabRelease(const SlabReleaseContext& context) {
  auto pid = context.getPoolId();
  auto& pool = memoryPoolManager_.getPoolById(pid);
  pool.completeSlabRelease(context);
}

void MemoryAllocator::abortSlabRelease(const SlabReleaseContext& context) {
  auto pid = context.getPoolId();
  auto& pool = memoryPoolManager_.getPoolById(pid);
  pool.abortSlabRelease(context);
}
std::set<uint32_t> MemoryAllocator::generateAllocSizes(
    double factor,
    uint32_t maxSize,
    uint32_t minSize,
    bool reduceFragmentation) {
  if (maxSize > Slab::kSize) {
    throw std::invalid_argument(
        folly::sformat("maximum alloc size {} is more than the slab size {}",
                       maxSize, Slab::kSize));
  }

  if (factor <= 1.0) {
    throw std::invalid_argument(folly::sformat("invalid factor {}", factor));
  }

  // Returns the next chunk size. Uses the previous size and factor to select a
  // size that increases the number of chunks per slab by at least one to reduce
  // slab wastage. Also increases the chunk size to the maximum that maintains
  // the same number of chunks per slab (for example: if slabs are 1 MB then a
  // chunk size of 300 KB can be upgraded to 333 KB while maintaining 3 chunks
  // per 1 MB slab).
  auto nextSize = [=](uint32_t prevSize, double incFactor) {
    // Increment by incFactor until we have a new number of chunks per slab.
    uint32_t newSize = prevSize;
    do {
      auto tmpPrevSize = newSize;
      newSize = util::getAlignedSize(static_cast<uint32_t>(newSize * incFactor),
                                     kAlignment);
      if (newSize == tmpPrevSize) {
        throw std::invalid_argument(
            folly::sformat("invalid incFactor {}", incFactor));
      }

      if (newSize > Slab::kSize) {
        return newSize;
      }
    } while (Slab::kSize / newSize == Slab::kSize / prevSize);
    // Now make sure we're selecting the maximum chunk size while maintaining
    // the number of chunks per slab.
    return maximizeAllocSize(newSize, Slab::kSize, kAlignment);
  };

  std::set<uint32_t> allocSizes;

  auto size = minSize;
  while (size < maxSize) {
    const auto nPerSlab = Slab::kSize / size;
    // if we can not make more than one alloc per slab, we just default to the
    // max alloc size.
    if (nPerSlab <= 1) {
      break;
    }
    allocSizes.insert(size);

    if (reduceFragmentation) {
      size = nextSize(size, factor);
    } else {
      auto prevSize = size;
      size = util::getAlignedSize(static_cast<uint32_t>(size * factor),
                                  kAlignment);
      if (prevSize == size) {
        throw std::invalid_argument(
            folly::sformat("invalid incFactor {}", factor));
      }
    }
  }

  allocSizes.insert(util::getAlignedSize(maxSize, kAlignment));
  return allocSizes;
}

std::set<uint32_t> MemoryAllocator::generateOptimalAllocSizesForItemRange(
    uint32_t minItemSize, uint32_t maxItemSize) {
  if (minItemSize > maxItemSize) {
    throw std::invalid_argument("minItemSize must be <= maxItemSize");
  }
  if (maxItemSize > Slab::kSize) {
    throw std::invalid_argument(
        folly::sformat("maxItemSize must be <= {} because allocation size "
                       "must be <= {} (Slab::kSize)",
                       Slab::kSize,
                       Slab::kSize));
  }

  // Handling items smaller than Slab::kMinAllocSize as Slab::kMinAllocSize
  // to simplify the logic
  minItemSize =
      std::max(minItemSize, static_cast<uint32_t>(Slab::kMinAllocSize));
  maxItemSize =
      std::max(maxItemSize, static_cast<uint32_t>(Slab::kMinAllocSize));

  // Looks confusing but since maxItemSize >= minItemSize, the number of
  // allocations per slab is smaller when dividing slab size by maxItemSize
  uint32_t minAllocsPerSlab = Slab::kSize / maxItemSize;
  uint32_t maxAllocsPerSlab = Slab::kSize / minItemSize;

  std::set<uint32_t> allocSizes;
  for (uint32_t i = minAllocsPerSlab; i <= maxAllocsPerSlab; i++) {
    uint32_t allocSize = Slab::kSize / i;
    // We need to make sure that allocSize is aligned to kAlignment.

    // allocSize is already chosen such that it is the largest size that enables
    // i allocs per slab. If we would align by incrementing, we might end up
    // reducing the number of allocs per slab, so we try to align by
    // decrementing.

    // Example: 4MB / 62 = 67650. If we incremented to align, we would get
    // 67656. 4MB / 67656 = 61.99 (this would maximize external fragmentation
    // which we want to avoid). If we align by decrementing, we get 67648.
    // 4MB / 67648 = 62.001 (this minimizes external fragmentation)
    uint32_t alignedSize = util::getAlignedSizeDown(allocSize, kAlignment);
    if (alignedSize < minItemSize ||
        (i == minAllocsPerSlab && (maxItemSize > alignedSize))) {
      // we want to avoid adding an allocation size which is smaller than
      // minItemSize. Consider allocSize=500. This will get aligned down to
      // 496. if minItemSize is 500, we are adding a size that will probably not
      // be used. So, we align up and maximize alloc size to keep same number of
      // allocs per slab. In this case we would pick 504.
      // We also want to make sure that maxItemSize is covered by an allocation
      // class. If i==minAllocsPerSlab, then allocSize is maximal. If the
      // aligned down size is smaller than maxItemSize we align up instead.
      uint32_t alignedUp = util::getAlignedSize(allocSize, kAlignment);
      alignedSize = maximizeAllocSize(alignedUp, Slab::kSize, kAlignment);
    }
    XDCHECK(isValidAllocSize(alignedSize));
    allocSizes.insert(alignedSize);
    if (allocSizes.size() > kMaxClasses) {
      throw std::runtime_error(
          folly::sformat("Optimal number of allocations required is at least "
                         "{} which is more than the "
                         "maximum number of allocation classes allowed {}",
                         allocSizes.size(), kMaxClasses));
    }
  }
  XDCHECK_LE(maxItemSize, *allocSizes.rbegin());
  return allocSizes;
}
