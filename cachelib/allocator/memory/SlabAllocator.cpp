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

#include "cachelib/allocator/memory/SlabAllocator.h"

#include <folly/Likely.h>
#include <folly/Random.h>
#include <folly/logging/xlog.h>
#include <folly/synchronization/SanitizeThread.h>
#include <sys/mman.h>
#include <sys/types.h>

#include <chrono>
#include <memory>
#include <stdexcept>

#include "cachelib/common/Utils.h"

/* Missing madvise(2) flags on MacOS */
#ifndef MADV_REMOVE
#define MADV_REMOVE 0
#endif
#ifndef MADV_DONTDUMP
#define MADV_DONTDUMP 0
#endif

using namespace facebook::cachelib;

namespace {
size_t roundDownToSlabSize(size_t size) { return size - (size % sizeof(Slab)); }
} // namespace

// definitions to avoid ODR violation.
using PtrType = CompressedPtr::PtrType;
constexpr uint64_t SlabAllocator::kAddressMask;
constexpr PtrType CompressedPtr::kAllocIdxMask;
constexpr unsigned int CompressedPtr::kNumAllocIdxBits;

constexpr unsigned int SlabAllocator::kLockSleepMS;
constexpr size_t SlabAllocator::kPagesPerStep;

void SlabAllocator::checkState() const {
  if (memoryStart_ == nullptr || memorySize_ <= Slab::kSize) {
    throw std::invalid_argument(
        folly::sformat("Invalid memory spec. memoryStart = {}, size = {}",
                       memoryStart_,
                       memorySize_));
  }

  if (slabMemoryStart_ == nullptr || nextSlabAllocation_ == nullptr) {
    throw std::invalid_argument(
        folly::sformat("Invalid slabMemoryStart_ {} of nextSlabAllocation_ {}",
                       slabMemoryStart_,
                       nextSlabAllocation_));
  }

  // nextSlabAllocation_ should be valid.
  if (nextSlabAllocation_ > getSlabMemoryEnd()) {
    throw std::invalid_argument(
        folly::sformat("Invalid nextSlabAllocation_ {}, with SlabMemoryEnd {}",
                       nextSlabAllocation_,
                       getSlabMemoryEnd()));
  }

  for (const auto slab : freeSlabs_) {
    if (!isValidSlab(slab)) {
      throw std::invalid_argument(folly::sformat("Invalid free slab {}", slab));
    }
  }
}

SlabAllocator::~SlabAllocator() {
  stopMemoryLocker();

  if (ownsMemory_) {
    munmap(memoryStart_, memorySize_);
  }
}

void SlabAllocator::stopMemoryLocker() {
  if (memoryLocker_.joinable()) {
    stopLocking_ = true;
    memoryLocker_.join();
  }
}

SlabAllocator::SlabAllocator(size_t size, const Config& config)
    : SlabAllocator(util::mmapAlignedZeroedMemory(sizeof(Slab), size),
                    size,
                    true,
                    config) {
  XDCHECK(!isRestorable());
}

SlabAllocator::SlabAllocator(void* memoryStart,
                             size_t memorySize,
                             const Config& config)
    : SlabAllocator(memoryStart, memorySize, false, config) {
  XDCHECK(isRestorable());
}

SlabAllocator::SlabAllocator(void* memoryStart,
                             size_t memorySize,
                             bool ownsMemory,
                             const Config& config)
    : memoryStart_(memoryStart),
      memorySize_(roundDownToSlabSize(memorySize)),
      slabMemoryStart_(computeSlabMemoryStart(memoryStart_, memorySize_)),
      nextSlabAllocation_(slabMemoryStart_),
      ownsMemory_(ownsMemory) {
  checkState();

  static_assert(!(sizeof(Slab) & (sizeof(Slab) - 1)),
                "slab size must be power of two");

  if (config.excludeFromCoredump) {
    excludeMemoryFromCoredump();
  }

  if (config.lockMemory) {
    memoryLocker_ = std::thread{[this]() { lockMemoryAsync(); }};
  }

  XDCHECK_EQ(0u, reinterpret_cast<uintptr_t>(memoryStart_) % sizeof(Slab));
  XDCHECK_EQ(0u, memorySize_ % sizeof(Slab));
  XDCHECK(nextSlabAllocation_ != nullptr);
  XDCHECK_EQ(reinterpret_cast<uintptr_t>(nextSlabAllocation_),
             reinterpret_cast<uintptr_t>(slabMemoryStart_));
}

SlabAllocator::SlabAllocator(const serialization::SlabAllocatorObject& object,
                             void* memoryStart,
                             size_t memSize,
                             const Config& config)
    : memoryStart_(memoryStart),
      memorySize_(*object.memorySize()),
      slabMemoryStart_(computeSlabMemoryStart(memoryStart_, memorySize_)),
      nextSlabAllocation_(getSlabForIdx(*object.nextSlabIdx())),
      canAllocate_(*object.canAllocate()),
      ownsMemory_(false) {
  if (Slab::kSize != *object.slabSize()) {
    throw std::invalid_argument(folly::sformat(
        "current slab size {} does not match the previous one {}",
        Slab::kSize,
        *object.slabSize()));
  }

  if (CompressedPtr::getMinAllocSize() != *object.minAllocSize()) {
    throw std::invalid_argument(folly::sformat(
        "current min alloc size {} does not match the previous one {}",
        CompressedPtr::getMinAllocSize(),
        *object.minAllocSize()));
  }

  XDCHECK(isRestorable());

  const size_t currSize = roundDownToSlabSize(memSize);
  if (memorySize_ != currSize) {
    throw std::invalid_argument(folly::sformat(
        "Memory size {} does not match the saved state's size {}",
        currSize,
        memorySize_));
  }

  if (config.excludeFromCoredump) {
    excludeMemoryFromCoredump();
  }

  for (const auto& pair : *object.memoryPoolSize()) {
    const PoolId id = pair.first;
    if (id >= static_cast<PoolId>(memoryPoolSize_.size())) {
      throw std::invalid_argument(
          folly::sformat("Invalid class id {}. Max Class Id {}",
                         id,
                         memoryPoolSize_.size() - 1));
    }
    memoryPoolSize_[id] = pair.second;
  }

  for (auto freeSlabIdx : *object.freeSlabIdxs()) {
    freeSlabs_.push_back(getSlabForIdx(freeSlabIdx));
  }

  for (auto advisedSlabIdx : *object.advisedSlabIdxs()) {
    // The slab headers in previous release did not have advised flag
    // set in the slab header. To avoid memory locking from touching
    // advised slab pages, we'd have to cold roll. To avoid cold roll
    // explicitly set the advised bit here.
    auto header = getSlabHeader(advisedSlabIdx);
    XDCHECK(header != nullptr);
    header->setAdvised(true);
    advisedSlabs_.push_back(getSlabForIdx(advisedSlabIdx));
  }

  if (config.lockMemory) {
    memoryLocker_ = std::thread{[this]() { lockMemoryAsync(); }};
  }

  checkState();
}

void SlabAllocator::lockMemoryAsync() noexcept {
  try {
    // memory start is always page aligned since it is aligned to slab size.
    auto* mem = reinterpret_cast<const uint8_t* const>(memoryStart_);
    XDCHECK(util::isPageAlignedAddr(mem));

    const size_t numPages = util::getNumPages(memorySize_);
    const size_t pageSize = util::getPageSize();

    size_t pageOffset = 0;
    size_t numAdvisedAwayPages = 0;

    while (pageOffset < numPages) {
      if (stopLocking_) {
        return;
      }

      auto pageAddr = mem + pageOffset * pageSize;
      // Avoid touching advised away pages.
      const auto header = getSlabHeader(pageAddr);
      if (header && header->isAdvised()) {
        ++numAdvisedAwayPages;
      } else {
        // this relies on the fact that the pages used with the allocator are
        // shared memory pages. For memory that is not shared, touching the
        // memory won't page them in until the page gets written to. We default
        // to mlock for that and require the caller to set the appropriate
        // rlimits. Use volatile to fool the compiler to not optimize this away
        // in opt mode.
        volatile const uint8_t val = *pageAddr;
        (void)val;
      }

      ++pageOffset;

      if (pageOffset % kPagesPerStep == 0) {
        /* sleep override */
        std::this_thread::sleep_for(std::chrono::milliseconds(kLockSleepMS));
      }
    }

    // verify everything got paged in. If it doesn't, then we'll end up locking
    // advised away pages, which means we'll start off with some unusable
    // cache memory.
    const auto numInCore = util::getNumResidentPages(memoryStart_, memorySize_);
    if (numInCore != numPages - numAdvisedAwayPages) {
      XLOGF(ERR,
            "could not page in all memory. numPages = {}, numInCore = {}. "
            "Trying to mlock.",
            numPages - numAdvisedAwayPages, numInCore);
      // try mlock to see if that helps.
      const int rv = mlock(memoryStart_, memorySize_);
      if (rv != 0) {
        XLOGF(ERR, "could not mlock. errno = {}", errno);
      }
    }
  } catch (const std::exception& e) {
    XLOGF(ERR, "Exception during locking memory {}", e.what());
  }
}

namespace {
unsigned int numSlabs(size_t memorySize) noexcept {
  return static_cast<unsigned int>(memorySize / sizeof(Slab));
}
unsigned int numSlabsForHeaders(size_t memorySize) noexcept {
  const size_t headerSpace = sizeof(SlabHeader) * numSlabs(memorySize);
  return static_cast<unsigned int>((headerSpace + sizeof(Slab) - 1) /
                                   sizeof(Slab));
}
} // namespace

unsigned int SlabAllocator::getNumUsableSlabs(size_t memorySize) noexcept {
  return numSlabs(memorySize) - numSlabsForHeaders(memorySize);
}

unsigned int SlabAllocator::getNumUsableSlabs() const noexcept {
  return getNumUsableAndAdvisedSlabs() -
         static_cast<unsigned int>(numSlabsReclaimable());
}

unsigned int SlabAllocator::getNumUsableAndAdvisedSlabs() const noexcept {
  return static_cast<unsigned int>(getSlabMemoryEnd() - slabMemoryStart_);
}

Slab* SlabAllocator::computeSlabMemoryStart(void* memoryStart,
                                            size_t memorySize) {
  // compute the number of slabs we can have.
  const auto numHeaderSlabs = numSlabsForHeaders(memorySize);
  if (numSlabs(memorySize) <= numHeaderSlabs) {
    throw std::invalid_argument("not enough memory for slabs");
  }

  if (memoryStart == nullptr ||
      reinterpret_cast<uintptr_t>(memoryStart) % sizeof(Slab)) {
    throw std::invalid_argument(
        folly::sformat("Invalid memory start {}", memoryStart));
  }

  // reserve the first numHeaderSlabs for storing the header info for all the
  // slabs.
  return reinterpret_cast<Slab*>(memoryStart) + numHeaderSlabs;
}

Slab* SlabAllocator::makeNewSlabImpl() {
  // early return without any locks.
  if (!canAllocate_) {
    return nullptr;
  }

  LockHolder l(lock_);
  // grab a free slab if it exists.
  if (!freeSlabs_.empty()) {
    auto slab = freeSlabs_.back();
    freeSlabs_.pop_back();
    return slab;
  }

  XDCHECK_EQ(0u,
             reinterpret_cast<uintptr_t>(nextSlabAllocation_) % sizeof(Slab));

  // check if we have any more memory left.
  if (allMemorySlabbed()) {
    // free list is empty and we have slabbed all the memory.
    canAllocate_ = false;
    return nullptr;
  }

  // allocate a new slab.
  return nextSlabAllocation_++;
}

// This does not hold the lock since the expectation is that its used with
// new/free/advised away slabs which are not in active use.
void SlabAllocator::initializeHeader(Slab* slab, PoolId id) {
  auto* header = getSlabHeader(slab);
  XDCHECK(header != nullptr);
  header = new (header) SlabHeader(id);
}

Slab* SlabAllocator::makeNewSlab(PoolId id) {
  Slab* slab = makeNewSlabImpl();
  if (slab == nullptr) {
    return nullptr;
  }

  memoryPoolSize_[id] += sizeof(Slab);
  // initialize the header for the slab.
  initializeHeader(slab, id);
  return slab;
}

void SlabAllocator::freeSlab(Slab* slab) {
  // find the header for the slab.
  auto* header = getSlabHeader(slab);
  XDCHECK(header != nullptr);
  if (header == nullptr) {
    throw std::runtime_error(folly::sformat("Invalid Slab {}", slab));
  }

  memoryPoolSize_[header->poolId] -= sizeof(Slab);
  // grab the lock
  LockHolder l(lock_);
  freeSlabs_.push_back(slab);
  canAllocate_ = true;
  header->resetAllocInfo();
}

bool SlabAllocator::adviseSlab(Slab* slab) {
  // find the header for the slab.
  auto* header = getSlabHeader(slab);
  if (header == nullptr) {
    throw std::runtime_error(folly::sformat("Invalid Slab {}", slab));
  }
  // Mark slab as advised in header prior to advising to avoid it from being
  // touched during memory locking.
  header->setAdvised(true);
  // madvise kernel to release this slab. Do this while not holding the
  // lock since the MADV_REMOVE happens inline.
  auto ret = madvise((void*)slab->memoryAtOffset(0), Slab::kSize, MADV_REMOVE);
  if (!ret || pretendMadvise_) {
    LockHolder l(lock_);
    advisedSlabs_.push_back(slab);
    // This doesn't reset flags
    header->resetAllocInfo();
    return true;
  }
  // Unset the flag since we failed to advise this slab away
  header->setAdvised(false);
  return false;
}

Slab* FOLLY_NULLABLE SlabAllocator::reclaimSlab(PoolId id) {
  Slab* slab = nullptr;
  {
    LockHolder l(lock_);
    if (!advisedSlabs_.empty()) {
      auto it = advisedSlabs_.begin();
      slab = *it;
      advisedSlabs_.erase(it);
    }
  }

  if (!slab) {
    return nullptr;
  }

  const size_t numPages = util::getNumPages(sizeof(Slab));
  const size_t pageSize = util::getPageSize();
  auto* mem = reinterpret_cast<const uint8_t* const>(slab->memoryAtOffset(0));
  XDCHECK(util::isPageAlignedAddr(mem));

  for (size_t pageOffset = 0; pageOffset < numPages; pageOffset++) {
    // Use volatile to fool the compiler to not optimize this away in opt
    // mode.
    volatile const uint8_t val = *(mem + pageOffset * pageSize);
    (void)val;
  }
  memoryPoolSize_[id] += sizeof(Slab);
  // initialize the header for the slab.
  initializeHeader(slab, id);
  return slab;
}

SlabHeader* SlabAllocator::getSlabHeader(
    const Slab* const slab) const noexcept {
  if ([&] {
        // TODO(T79149875): Fix data race exposed by TSAN.
        folly::annotate_ignore_thread_sanitizer_guard g(__FILE__, __LINE__);
        return isValidSlab(slab);
      }()) {
    return [&] {
      // TODO(T79149875): Fix data race exposed by TSAN.
      folly::annotate_ignore_thread_sanitizer_guard g(__FILE__, __LINE__);
      return getSlabHeader(slabIdx(slab));
    }();
  }
  return nullptr;
}

bool SlabAllocator::isMemoryInSlab(const void* ptr,
                                   const Slab* slab) const noexcept {
  if (!isValidSlab(slab)) {
    return false;
  }
  return getSlabForMemory(ptr) == slab;
}

const void* SlabAllocator::getRandomAlloc() const noexcept {
  // disregard the space we use for slab header.
  const auto validMaxOffset =
      memorySize_ - (reinterpret_cast<uintptr_t>(slabMemoryStart_) -
                     reinterpret_cast<uintptr_t>(memoryStart_));

  // pick a random location in the memory.
  const auto offset = folly::Random::rand64(0, validMaxOffset);
  const auto* memory = reinterpret_cast<void*>(
      reinterpret_cast<uintptr_t>(slabMemoryStart_) + offset);

  const auto* slab = getSlabForMemory(memory);
  const auto* header = getSlabHeader(slab);
  if (header == nullptr) {
    return nullptr;
  }

  XDCHECK_GE(reinterpret_cast<uintptr_t>(memory),
             reinterpret_cast<uintptr_t>(slab));

  const auto allocSize = header->allocSize;
  if (allocSize == 0) {
    return nullptr;
  }

  const auto maxAllocIdx = Slab::kSize / allocSize - 1;
  auto allocIdx = (reinterpret_cast<uintptr_t>(memory) -
                   reinterpret_cast<uintptr_t>(slab)) /
                  allocSize;
  allocIdx = allocIdx > maxAllocIdx ? maxAllocIdx : allocIdx;
  return reinterpret_cast<const void*>(reinterpret_cast<uintptr_t>(slab) +
                                       allocSize * allocIdx);
}

serialization::SlabAllocatorObject SlabAllocator::saveState() {
  if (!isRestorable()) {
    throw std::logic_error("Can not save state when memory is mmaped");
  }

  // stop async thread that is paging in memory if it is still running.
  stopMemoryLocker();

  serialization::SlabAllocatorObject object;
  *object.memorySize() = memorySize_;
  *object.nextSlabIdx() = slabIdx(nextSlabAllocation_);
  *object.canAllocate() = canAllocate_;

  for (PoolId id = 0; id < static_cast<PoolId>(memoryPoolSize_.size()); ++id) {
    object.memoryPoolSize()[id] = memoryPoolSize_[id];
  }

  for (auto slab : freeSlabs_) {
    object.freeSlabIdxs()->push_back(slabIdx(slab));
  }
  for (auto slab : advisedSlabs_) {
    object.advisedSlabIdxs()->push_back(slabIdx(slab));
  }

  *object.slabSize() = Slab::kSize;
  *object.minAllocSize() = CompressedPtr::getMinAllocSize();
  return object;
}

// for benchmarking purposes.
const unsigned int kMarkerBits = 6;
CompressedPtr SlabAllocator::compressAlt(const void* ptr) const {
  if (ptr == nullptr) {
    return CompressedPtr{};
  }

  ptrdiff_t delta = reinterpret_cast<const uint8_t*>(ptr) -
                    reinterpret_cast<const uint8_t*>(slabMemoryStart_);
  return CompressedPtr{
      static_cast<CompressedPtr::PtrType>(delta >> kMarkerBits)};
}

void* SlabAllocator::unCompressAlt(const CompressedPtr cPtr) const {
  if (cPtr.isNull()) {
    return nullptr;
  }

  const auto markerOffset = cPtr.getRaw() << kMarkerBits;
  const void* markerPtr =
      reinterpret_cast<const uint8_t*>(slabMemoryStart_) + markerOffset;

  const auto* header = getSlabHeader(markerPtr);
  const auto allocSize = header->allocSize;

  XDCHECK_GE(allocSize, 1u << kMarkerBits);

  auto slab = getSlabForMemory(markerPtr);

  auto slabOffset = reinterpret_cast<uintptr_t>(markerPtr) -
                    reinterpret_cast<uintptr_t>(slab);
  XDCHECK_LT(slabOffset, Slab::kSize);
  /*
   * Since the marker is to the left of the desired allocation, now
   * we want to find the alloc boundary to the right of this marker.
   * But we start off by finding the distance to the alloc
   * boundary on our left, which we call delta.
   * Then the distance to the right is allocSize - delta:
   *
   *      I                   M                       I
   *      <-- delta ---------><-- allocSize - delta -->
   *
   * Since allocs start at the beginning of the slab, and are all allocSize
   * bytes big, delta is just slabOffset % allocSize.  If delta is 0, then the
   * marker is already at an alloc boundary.
   */
  const auto delta = slabOffset % allocSize;
  if (delta) {
    slabOffset += (allocSize - delta);
  }
  return slab->memoryAtOffset(slabOffset);
}

void SlabAllocator::excludeMemoryFromCoredump() const {
  // dump the headers always. Very useful for debugging when we have
  // pointers and need to find information. slab headers are only few slabs
  // and in the order of 4-8MB
  auto slabMemStartPtr = reinterpret_cast<uint8_t*>(slabMemoryStart_);
  const size_t headerBytes =
      slabMemStartPtr - reinterpret_cast<uint8_t*>(memoryStart_);
  const size_t slabBytes = memorySize_ - headerBytes;
  XDCHECK_LT(slabBytes, memorySize_);

  if (madvise(slabMemStartPtr, slabBytes, MADV_DONTDUMP)) {
    throw std::system_error(errno, std::system_category(),
                            "madvise failed to exclude memory from coredump");
  }
}
