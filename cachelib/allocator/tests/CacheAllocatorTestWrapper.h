// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#pragma once

#include "cachelib/allocator/CacheAllocator.h"

namespace facebook::cachelib::tests {

// For internal testing ONLY
// This class expose some private testing APIs from CacheAllocator
class CacheAllocatorTestWrapper {
 public:
  template <class Allocator, typename... Params>
  static bool removeFromRamForTesting(Allocator* alloc, Params&&... args) {
    return alloc->removeFromRamForTesting(std::forward<Params>(args)...);
  }

  template <class Allocator, typename... Params>
  static void removeFromNvmForTesting(Allocator* alloc, Params&&... args) {
    alloc->removeFromNvmForTesting(std::forward<Params>(args)...);
  }

  template <class Allocator, typename... Params>
  static void evictForTesting(Allocator* alloc, Params&&... args) {
    alloc->evictForTesting(std::forward<Params>(args)...);
  }

  template <class Allocator, typename... Params>
  static bool pushToNvmCacheFromRamForTesting(Allocator* alloc,
                                              Params&&... args) {
    return alloc->pushToNvmCacheFromRamForTesting(
        std::forward<Params>(args)...);
  }

  template <class Allocator>
  static typename Allocator::Item& getParentItem(
      Allocator* alloc, const typename Allocator::Item& child) {
    return child.asChainedItem().getParentItem(alloc->compressor_);
  }

  template <class Allocator>
  static MemoryAllocator& getMemoryAllocator(Allocator* alloc) {
    return *alloc->allocator_;
  }

  template <class Allocator>
  static typename Allocator::ItemHandle createHandle(
      Allocator* alloc, typename Allocator::Item& item) {
    return alloc->acquire(&item);
  }
};

} // namespace facebook::cachelib::tests
