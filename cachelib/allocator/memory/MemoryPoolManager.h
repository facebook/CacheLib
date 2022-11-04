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

#include <folly/SharedMutex.h>

#include <array>
#include <memory>
#include <unordered_map>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#include <folly/container/F14Map.h>
#pragma GCC diagnostic pop

#include "cachelib/allocator/memory/MemoryPool.h"
#include "cachelib/allocator/memory/Slab.h"
#include "cachelib/allocator/memory/SlabAllocator.h"
#include "cachelib/allocator/memory/serialize/gen-cpp2/objects_types.h"

namespace facebook {
namespace cachelib {

struct PoolAdviseReclaimData {
  std::unordered_map<PoolId, uint64_t> poolAdviseReclaimMap;
  bool advise; // true for advise, false for reclaim
};

// used to organize the available memory into pools and identify them by a
// string name or pool id.
class MemoryPoolManager {
 public:
  // maximum number of pools that we support.
  static constexpr unsigned int kMaxPools = 64;

  // creates a memory pool manager for this slabAllocator.
  // @param slabAlloc  the slab allocator to be used for the memory pools.
  explicit MemoryPoolManager(SlabAllocator& slabAlloc);

  // creates a memory pool manager by restoring it from a serialized buffer.
  //
  // @param object    Object that contains the data to restore MemoryPoolManger
  // @param slabAlloc the slab allocator for fetching the header info.
  //
  // @throw  std::logic_error if the slab allocator is not restorable.
  MemoryPoolManager(const serialization::MemoryPoolManagerObject& object,
                    SlabAllocator& slabAlloc);

  MemoryPoolManager(const MemoryPoolManager&) = delete;
  MemoryPoolManager& operator=(const MemoryPoolManager&) = delete;

  // adding a pool
  // @param name   the string name representing the pool.
  // @param size   the size of the memory pool.
  // @param allocSizes  set of allocation sizes sorted in increasing
  //                    order. This will be used to create the corresponding
  //                    AllocationClasses.
  //
  // @return on success, returns id of the new memory pool.
  // @throw  std::invalid_argument if the name/size/allcoSizes are invalid or
  //         std::logic_error if we have run out the allowed number of pools.
  PoolId createNewPool(folly::StringPiece name,
                       size_t size,
                       const std::set<uint32_t>& allocSizes);

  // shrink the existing pool by _bytes_ .
  // @param bytes  the number of bytes to be taken away from the pool
  // @return  true if the operation succeeded. false if the size of the pool is
  //          smaller than _bytes_
  // @throw   std::invalid_argument if the poolId is invalid.
  bool shrinkPool(PoolId pid, size_t bytes);

  // grow an existing pool by _bytes_. This will fail if there is no
  // available memory across all the pools to provide for this pool
  // @param bytes  the number of bytes to be added to the pool.
  // @return    true if the pool was grown. false if the necessary number of
  //            bytes were not available.
  // @throw     std::invalid_argument if the poolId is invalid.
  bool growPool(PoolId pid, size_t bytes);

  // move bytes from one pool to another. The source pool should be at least
  // _bytes_ in size.
  //
  // @param src     the pool to be sized down and giving the memory.
  // @param dest    the pool receiving the memory.
  // @param bytes   the number of bytes to move from src to dest.
  //
  // @return  true if the resize succeeded. False if the src pool does not have
  //          enough memory to make the resize.
  // @throw std::invalid_argument if src or dest is invalid pool
  bool resizePools(PoolId src, PoolId dest, size_t bytes);

  // Fetch the list of pools that are above their current limit due to a
  // recent resize.
  //
  // @return list of pools that are over limit.
  std::set<PoolId> getPoolsOverLimit() const;

  // access the memory pool by its name and id.
  // @returns returns a valid MemoryPool.
  // @throw std::invalid_argument if the name or id is invalid.
  MemoryPool& getPoolByName(const std::string& name) const;
  MemoryPool& getPoolById(PoolId id) const;

  // returns the pool's name by its pool ID
  // @throw std::logic_error if the pool ID not existed.
  const std::string& getPoolNameById(PoolId id) const;

  // returns the current pool ids that are being used.
  std::set<PoolId> getPoolIds() const;

  // for saving the state of the memory pool manager
  //
  // precondition:  The object must have been instantiated with a restorable
  // slab allocator that does not own the memory. serialization must happen
  // without any reader or writer present. Any modification of this object
  // afterwards will result in an invalid, inconsistent state for the
  // serialized data.
  //
  // @throw std::logic_error if the object state can not be serialized
  serialization::MemoryPoolManagerObject saveState() const;

  // size in bytes of the remaining size that is not reserved for any pools.
  size_t getBytesUnReserved() const {
    folly::SharedMutex::ReadHolder l(lock_);
    return getRemainingSizeLocked();
  }

  // returns the number of slabs to be advised
  uint64_t getNumSlabsToAdvise() const { return numSlabsToAdvise_; }

  // updates the number of slabs to be advised by numSlabs. This would
  // either increment (to advise away more slabs) or decrement (to reclaim
  // some of the previously advised away slabs) the numSlabsToAdvise_ by
  // numSlabs.
  //
  // @param numSlabs   number of slabs to add-to/subtract-from numSlabToAdvise_
  //
  // @throw std::invalid_argument if numSlabs is negative and its absolute
  //                              value is more than numSlabsToAdvise_
  //                              (ie total slabs to be reclaimed cannot be
  //                               more than total slabs advised away)
  void updateNumSlabsToAdvise(int32_t numSlabs) {
    if (numSlabs < 0 && static_cast<uint64_t>(-numSlabs) > numSlabsToAdvise_) {
      throw std::invalid_argument(
          folly::sformat("Invalid numSlabs {} to update  numSlabsToAdvise {}",
                         numSlabs,
                         numSlabsToAdvise_.load()));
    }
    numSlabsToAdvise_ += numSlabs;
  }

  // return total memory currently advised away
  size_t getAdvisedMemorySize() const noexcept {
    size_t sum = 0;
    folly::SharedMutex::WriteHolder l(lock_);
    for (PoolId id = 0; id < nextPoolId_; id++) {
      sum += pools_[id]->getPoolAdvisedSize();
    }
    return sum;
  }

  // calculate the number of slabs to be advised/reclaimed in each pool
  //
  // @param poolIds    list of pools to process
  //
  // @return   vector of pairs with first value as poolId and second value
  //           the number of slabs to advise or number of slabs to reclaim
  //           (indicated by negative number)
  PoolAdviseReclaimData calcNumSlabsToAdviseReclaim(
      const std::set<PoolId>& poolIds) const;

 private:
  // obtain the remaining size in bytes that is not reserved by taking into
  // account the total available memory in the slab allocator and the size of
  // all the pools we manage.
  size_t getRemainingSizeLocked() const noexcept;

  // returns a map of poolId and target number of slabs to be advised in that
  // pool
  // @param poolIds           list of regular pool ids. Compact Cache pools
  //                          memory is not advised-away/reclaimed
  // @param totalSlabsInUse   total slabs in use across all pools
  // @param numSlabsInUse     a map of pool-id to number of slabs in use in that
  //                          pool. This is passed in (instead of obtaining
  //                          using invoking getCurrentUsedSize() everytime
  //                          because the slabs used in a pool could change
  //                          while we are determining the number of slabs to
  //                          advise
  std::unordered_map<PoolId, uint64_t> getTargetSlabsToAdvise(
      std::set<PoolId> poolIds,
      uint64_t totalSlabsInUse,
      std::unordered_map<PoolId, size_t>& numSlabsInUse) const;

  // rw lock serializing the access to poolsByName_ and pool creation.
  mutable folly::SharedMutex lock_;

  // array of pools by Id. The valid pools are up to (nextPoolId_ - 1). This
  // is to ensure that we can fetch pools by Id without holding any locks as
  // long as the pool Id is valid.
  std::array<std::unique_ptr<MemoryPool>, kMaxPools> pools_;

  // pool name -> pool Id mapping.
  folly::F14FastMap<std::string, PoolId> poolsByName_;

  // the next available pool id.
  std::atomic<PoolId> nextPoolId_{0};

  // slab allocator for the pools
  SlabAllocator& slabAlloc_;

  // Number of slabs to advise away
  // This is target number of slabs to be advised across all pools.
  // This would be same as sum of current number of advised away slabs in
  // each pool after a memory monitor iteration
  std::atomic<uint64_t> numSlabsToAdvise_{0};

  // Allow access to private members by unit tests
  friend class facebook::cachelib::tests::AllocTestBase;
};
} // namespace cachelib
} // namespace facebook
