#include "cachelib/allocator/memory/MemoryAllocator.h"

#include <folly/Format.h>

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
    : config_(std::set<uint32_t>{object.allocSizes.begin(),
                                 object.allocSizes.end()},
              object.enableZeroedSlabAllocs,
              disableCoredump,
              object.lockMemory),
      slabAllocator_(object.slabAllocator,
                     memoryStart,
                     memSize,
                     {config_.disableFullCoredump, config_.lockMemory}),
      memoryPoolManager_(object.memoryPoolManager, slabAllocator_) {
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

PoolId MemoryAllocator::getPoolId(const std::string& name) const noexcept {
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
  object.allocSizes.insert(config_.allocSizes.begin(),
                           config_.allocSizes.end());
  object.enableZeroedSlabAllocs = config_.enableZeroedSlabAllocs;
  object.lockMemory = config_.lockMemory;
  object.slabAllocator = slabAllocator_.saveState();
  object.memoryPoolManager = memoryPoolManager_.saveState();
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
      newSize = util::getAlignedSize(static_cast<uint32_t>(newSize * incFactor),
                                     kAlignment);
    } while (Slab::kSize / newSize == Slab::kSize / prevSize);
    // Now make sure we're selecting the maximum chunk size while maintaining
    // the number of chunks per slab.
    const uint32_t perslab = static_cast<uint32_t>(Slab::kSize) / newSize;
    const uint32_t maxChunkSize = static_cast<uint32_t>(Slab::kSize) / perslab;
    // Align down to maintain perslab
    newSize = maxChunkSize - maxChunkSize % kAlignment;
    assert(newSize % kAlignment == 0);
    assert(static_cast<uint32_t>(Slab::kSize) / newSize == perslab);
    return newSize;
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
      size = util::getAlignedSize(static_cast<uint32_t>(size * factor),
                                  kAlignment);
    }
  }

  allocSizes.insert(util::getAlignedSize(maxSize, kAlignment));
  return allocSizes;
}
