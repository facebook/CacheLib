#pragma once
#include <string>

#include "cachelib/allocator/memory/MemoryAllocator.h"
#include "cachelib/allocator/memory/MemoryPool.h"
#include "cachelib/allocator/memory/Slab.h"
#include "cachelib/allocator/memory/SlabAllocator.h"
#include "common/files/FileUtil.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#include <folly/Random.h>
#pragma GCC diagnostic pop

#include <gtest/gtest.h>

namespace facebook {
namespace cachelib {

class AllocationClass;
class MemoryPool;
class MemoryPoolManager;
class MemoryAllocator;

namespace tests {

// base class for all the tests that deal with memory to be allocated.
class AllocTestBase : public testing::Test {
 public:
  // returns memory of size, aligned to Slab::kSize
  void* allocate(size_t size) {
    void* memory = nullptr;
    auto ret = posix_memalign(&memory, Slab::kSize, size);
    if (memory == nullptr || ret != 0) {
      throw std::bad_alloc();
    }
    memset(memory, 0, size);
    toFree_.push_back(memory);
    return memory;
  }

  ~AllocTestBase() override {
    for (auto m : toFree_) {
      free(m);
    }
  }

  static bool isSameSlabList(const std::vector<Slab*>& slabs1,
                             const SlabAllocator& a1,
                             const std::vector<Slab*>& slabs2,
                             const SlabAllocator& a2);

  static bool isSameSlabAllocator(const SlabAllocator& a1,
                                  const SlabAllocator& a2);

  static bool isSameAllocationClass(const AllocationClass& ac1,
                                    const AllocationClass& ac2);

  static bool isSameMemoryPool(const MemoryPool& mp1, const MemoryPool& mp2);

  static bool isSameMemoryPoolManager(const MemoryPoolManager& m1,
                                      const MemoryPoolManager& m2);

  static bool isSameMemoryAllocator(const MemoryAllocator& m1,
                                    const MemoryAllocator& m2);

  static unsigned int getSlabAllocatorLockSleepMs() noexcept {
    return SlabAllocator::kLockSleepMS;
  }

  void pretendMadvise(SlabAllocator* alloc) { alloc->pretendMadvise_ = true; }

  size_t getCurrentSlabAllocSize(const MemoryPool& pool) const {
    return pool.currSlabAllocSize_;
  }

 private:
  std::vector<void*> toFree_{};
};

// base class for all tests that require a slab allocator
class SlabAllocatorTestBase : public AllocTestBase {
 public:
  std::unique_ptr<SlabAllocator> createSlabAllocator(unsigned int numSlabs) {
    // 2 slabs for the headers.
    const auto size = (numSlabs + 2) * Slab::kSize;
    auto memory = allocate(size);
    auto allocator = std::unique_ptr<SlabAllocator>(
        new SlabAllocator(memory, size, SlabAllocator::Config{}));
    if (allocator == nullptr) {
      throw std::bad_alloc();
    }
    pretendMadvise(allocator.get());
    return allocator;
  }

  static uint32_t getRandomAllocSize();

  // generates a random string of random length up 50 chars and at least 10
  // chars;
  static std::string getRandomStr();
  static std::string getRandomStr(unsigned int len);

  // generate n random allocation sizes that are powers of two.
  static std::set<uint32_t> getRandomPow2AllocSizes(unsigned int n);
  // generate n random allocation sizes.
  static std::set<uint32_t> getRandomAllocSizes(
      unsigned int n, size_t minSize = Slab::kMinAllocSize);
};

}
}
}
