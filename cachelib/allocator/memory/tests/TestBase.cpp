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

#include "cachelib/allocator/memory/tests/TestBase.h"

#include "cachelib/allocator/memory/AllocationClass.h"
#include "cachelib/allocator/memory/MemoryAllocator.h"

namespace facebook {
namespace cachelib {
namespace tests {
/* static */
bool AllocTestBase::isSameSlabList(const std::vector<Slab*>& slabs1,
                                   const SlabAllocator& a1,
                                   const std::vector<Slab*>& slabs2,
                                   const SlabAllocator& a2) {
  return (slabs1.size() == slabs2.size() &&
          std::equal(slabs1.begin(), slabs1.end(), slabs2.begin(),
                     [&](const auto& slab1, const auto& slab2) {
                       return a1.slabIdx(slab1) == a2.slabIdx(slab2);
                     }));
}

/* static */
bool AllocTestBase::isSameSlabAllocator(const SlabAllocator& a1,
                                        const SlabAllocator& a2) {
  return a1.isRestorable() && a2.isRestorable() && // must be both restorable.
         a1.memoryPoolSize_ == a2.memoryPoolSize_ &&
         isSameSlabList(a1.freeSlabs_, a1, a2.freeSlabs_, a2) &&
         a1.slabIdx(a1.nextSlabAllocation_) ==
             a2.slabIdx(a2.nextSlabAllocation_) &&
         a1.canAllocate_ == a2.canAllocate_;
}

/* static */
bool AllocTestBase::isSameAllocationClass(const AllocationClass& ac1,
                                          const AllocationClass& ac2) {
  return ac1.classId_ == ac2.classId_ &&
         ac1.allocationSize_ == ac2.allocationSize_ &&
         ac1.slabAlloc_.slabIdx(ac1.currSlab_) ==
             ac2.slabAlloc_.slabIdx(ac2.currSlab_) &&
         ac1.currOffset_ == ac2.currOffset_ &&
         ac1.canAllocate_ == ac2.canAllocate_ &&
         isSameSlabList(ac1.allocatedSlabs_, ac1.slabAlloc_,
                        ac2.allocatedSlabs_, ac2.slabAlloc_) &&
         isSameSlabList(ac1.freeSlabs_, ac1.slabAlloc_, ac2.freeSlabs_,
                        ac2.slabAlloc_) &&
         ac1.freedAllocations_ == ac2.freedAllocations_;
}

/* static */
bool AllocTestBase::isSameMemoryPool(const MemoryPool& mp1,
                                     const MemoryPool& mp2) {
  if (mp1.id_ != mp2.id_ || mp1.maxSize_ != mp2.maxSize_ ||
      mp1.currSlabAllocSize_ != mp2.currSlabAllocSize_ ||
      mp1.currAllocSize_ != mp2.currAllocSize_ ||
      !isSameSlabList(mp1.freeSlabs_, mp1.slabAllocator_, mp2.freeSlabs_,
                      mp2.slabAllocator_) ||
      mp1.acSizes_ != mp2.acSizes_) {
    return false;
  }

  if (mp1.ac_.size() != mp2.ac_.size() ||
      !std::equal(mp1.ac_.begin(), mp1.ac_.end(), mp2.ac_.begin(),
                  [](const std::unique_ptr<AllocationClass>& ac1,
                     const std::unique_ptr<AllocationClass>& ac2) {
                    return isSameAllocationClass(*ac1, *ac2);
                  })) {
    return false;
  }

  return true;
}

/* static */
bool AllocTestBase::isSameMemoryPoolManager(const MemoryPoolManager& m1,
                                            const MemoryPoolManager& m2) {
  if (m1.nextPoolId_ != m2.nextPoolId_) {
    return false;
  }

  if (m1.pools_.size() != m2.pools_.size() ||
      !std::equal(m1.pools_.begin(),
                  m1.pools_.begin() + m1.nextPoolId_,
                  m2.pools_.begin(),
                  [](const std::unique_ptr<MemoryPool>& p1,
                     const std::unique_ptr<MemoryPool>& p2) {
                    return isSameMemoryPool(*p1, *p2);
                  })) {
    return false;
  }

  return m1.poolsByName_ == m2.poolsByName_;
}

/* static */
bool AllocTestBase::isSameMemoryAllocator(const MemoryAllocator& m1,
                                          const MemoryAllocator& m2) {
  return m1.config_.allocSizes == m2.config_.allocSizes &&
         isSameSlabAllocator(m1.slabAllocator_, m2.slabAllocator_) &&
         isSameMemoryPoolManager(m1.memoryPoolManager_, m2.memoryPoolManager_);
}

uint32_t getRandomAllocSize() {
  // Reserve some space for the intrusive list's hook
  constexpr uint32_t kReservedSize = 8;
  assert(Slab::kSize >= kReservedSize);
  return folly::Random::rand32() % (Slab::kSize - kReservedSize + 1) +
         kReservedSize;
}

std::set<uint32_t> getRandomPow2AllocSizes(unsigned int n) {
  unsigned int numBits = static_cast<unsigned int>(log2(Slab::kSize));
  // at least 128 bytes
  const unsigned int minNumBits = 7;
  if (n + minNumBits > numBits) {
    std::invalid_argument("not enough bits");
  }

  std::set<uint32_t> s;
  while (s.size() != n) {
    const unsigned int randBit =
        folly::Random::rand32() % (numBits - minNumBits) + minNumBits;
    s.insert(1 << randBit);
  }
  return s;
}

// get n random allocation sizes which are at least minSize bytes long.
std::set<uint32_t> getRandomAllocSizes(unsigned int n, size_t minSize) {
  std::set<uint32_t> s;
  while (s.size() != n) {
    auto size = getRandomAllocSize();
    if (size >= minSize) {
      s.insert(size);
    }
  }
  return s;
}

} // namespace tests
} // namespace cachelib
} // namespace facebook
