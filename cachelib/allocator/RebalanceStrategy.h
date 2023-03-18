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

#include "cachelib/allocator/Cache.h"
#include "cachelib/allocator/RebalanceInfo.h"
#include "cachelib/allocator/memory/Slab.h"

namespace facebook {
namespace cachelib {

struct RebalanceContext {
  // Victim and Receiver must belong to the same pool
  ClassId victimClassId{Slab::kInvalidClassId};
  ClassId receiverClassId{Slab::kInvalidClassId};

  RebalanceContext() = default;
  RebalanceContext(ClassId victim, ClassId receiver)
      : victimClassId(victim), receiverClassId(receiver) {}
};

// Base class for rebalance strategy.
// Given a pool, the rebalance strategy picks a victim allocation class to
// release slabs, or picks a pair of victim and receiver. The idea here is the
// user should inspect the stats corresponding to the pool they want to
// rebalance. They will then decide if and which allocation classes need to
// release slab and how many slabs should be released back to the pool, or
// moved.
//
// The actual release operation is handled by PoolRebalancer
class RebalanceStrategy {
 public:
  enum Type {
    PickNothingOrTest,
    Random,
    MarginalHits,
    FreeMem,
    HitsPerSlab,
    LruTailAge,
    PoolResize,
    StressRebalance,
    NumTypes
  };

  struct BaseConfig {};

  RebalanceStrategy() = default;

  virtual ~RebalanceStrategy() = default;

  // Pick an victim and receiver from the same pool
  //
  // @param allocator   Cache allocator that implements CacheBase @param pid
  // Pool to rebalance
  //
  // @return RebalanceContext   contains victim and receiver
  RebalanceContext pickVictimAndReceiver(const CacheBase& cache, PoolId pid);

  // Pick only the victim irrespective of who is receiving the slab. This is
  // used mostly for pool resizing.
  ClassId pickVictimForResizing(const CacheBase& cache, PoolId pid);

  virtual void updateConfig(const BaseConfig&) {}

  Type getType() const { return type_; }

 protected:
  using PoolState = std::array<detail::Info, MemoryAllocator::kMaxClasses>;
  static const RebalanceContext kNoOpContext;

  explicit RebalanceStrategy(Type strategyType) : type_(strategyType) {}

  virtual RebalanceContext pickVictimAndReceiverImpl(const CacheBase&,
                                                     PoolId,
                                                     const PoolStats&) {
    return {};
  }

  virtual ClassId pickVictimImpl(const CacheBase&, PoolId, const PoolStats&) {
    return Slab::kInvalidClassId;
  }

  // Returns true if the state was already initialized and set up. False if we
  // state was not present.
  bool poolStatePresent(PoolId pid) const {
    return poolState_.find(pid) != poolState_.end();
  }

  PoolState& getPoolState(PoolId pid) { return poolState_.at(pid); }
  const PoolState& getPoolState(PoolId pid) const { return poolState_.at(pid); }

  // filter the candidates based on whether they have enough slabs to be a
  // victim. This is based on config_.minSlabs
  static std::set<ClassId> filterByNumEvictableSlabs(const PoolStats& stats,
                                                     std::set<ClassId> victims,
                                                     unsigned int numSlabs);

  // Filter the candidates based on whether they have allocation failures.
  static std::set<ClassId> filterByAllocFailure(const PoolStats& stats,
                                                std::set<ClassId> candidates,
                                                const PoolState& prevState);

  // filter out the candidates that are not evicting and have free memory
  // based on the previous state. The prev state is used to figure out if
  // evictions have happened recently
  static std::set<ClassId> filterByNoEvictions(const PoolStats& stats,
                                               std::set<ClassId> candidates,
                                               const PoolState& prevState);

  // helper function to filter an std::set based on a predicate. If the
  // predicate returns true, the element is filtered.
  //
  // @param input   the input set of alloc class ids @param pred    the
  // predicate to filter by. If predicate returns true, the element is removed
  // @param purpose string identifying the purpose for logging.
  static std::set<ClassId> filter(std::set<ClassId> input,
                                  std::function<bool(const ClassId& id)> pred,
                                  const std::string& purpose);

  // filter the candidates based on whether they have at least this much tail
  // age
  static std::set<ClassId> filterByMinTailAge(
      const PoolEvictionAgeStats& poolEvictionAgeStats,
      std::set<ClassId> candidates,
      unsigned int minTailAge);

  // filter the candidates based on whether they recently gained a slab and
  // are in hold off period.
  std::set<ClassId> filterVictimsByHoldOff(PoolId pid,
                                           const PoolStats& stats,
                                           std::set<ClassId> victims);

  static ClassId pickVictimByFreeMem(const std::set<ClassId>& victims,
                                     const PoolStats& poolStats,
                                     size_t threshold,
                                     const PoolState& prevState);

 private:
  // picks any of the class id ordered by the total slabs.
  ClassId pickAnyClassIdForResizing(const CacheBase& cache,
                                    PoolId pid,
                                    const PoolStats& poolStats);

  // initialize the pool's state to the current stats.
  void initPoolState(PoolId pid, const PoolStats& stats);

  // process the stats for this pool and apply them to the previous state.
  // Get deltas for evictions and hits and determine if a slab got added.
  void recordCurrentState(PoolId pid, const PoolStats& stats);

  // Pick a receiver with max alloc failures. If no alloc failures, return
  // invalid classid.
  ClassId pickReceiverWithAllocFailures(const CacheBase& cache,
                                        PoolId pid,
                                        const PoolStats& stat);

  // Ensure pool state is initialized before calling impl, and update pool
  // state after calling impl.
  //
  // @param cache       Cache allocator that implements CacheBase @param pid
  // Pool to rebalance @param impl        Reblanacing or resizing
  // implementation to call @param noOp        No operation indicator for the
  // first time encountering a pool
  template <typename T>
  T executeAndRecordCurrentState(const CacheBase& cache,
                                 PoolId pid,
                                 const std::function<T(const PoolStats&)>& impl,
                                 T noOp);

  Type type_ = PickNothingOrTest;

  // maintain the state of the previous snapshot of pool for every pool.  We
  // ll use this for processing and getting the deltas for some of these.
  std::unordered_map<PoolId, PoolState> poolState_;

  FRIEND_TEST(RebalanceStrategy, Basic);
};

} // namespace cachelib
} // namespace facebook
