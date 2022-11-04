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

#include <cachelib/common/Hash.h>
#include <cachelib/navy/common/Buffer.h>
#include <folly/Random.h>

#include "cachelib/navy/engine/Engine.h"
#include "cachelib/navy/scheduler/JobScheduler.h"

namespace facebook {
namespace cachelib {
namespace navy {

// A pair of small and large item engine.
// A driver must have at least one engine pair.
class EnginePair {
 public:
  EnginePair(std::unique_ptr<Engine> smallItemCache,
             std::unique_ptr<Engine> largeItemCache,
             uint32_t smallItemMaxSize,
             JobScheduler* scheduler);

  // Move constructor.
  EnginePair(EnginePair&& ep) noexcept
      : EnginePair(std::move(ep.smallItemCache_),
                   std::move(ep.largeItemCache_),
                   ep.smallItemMaxSize_,
                   ep.scheduler_) {}

  // Move assignment operator.
  EnginePair& operator=(EnginePair&& other) = delete;

  // Copy constructors
  EnginePair(const EnginePair& o) = delete;
  EnginePair(EnginePair& o) = delete;

  // Return true if item is considered a "large item". This is meant to be
  // a very fast check to verify a key & value pair will be considered as
  // "small" or "large" objects.
  bool isItemLarge(HashedKey key, BufferView value) const;

  // synchronous fast lookup if a key probably exists in the cache,
  // it can return a false positive result. this provides an optimization
  // to skip the heavy lookup operation when key doesn't exist in the cache.
  bool couldExist(HashedKey key) const;

  // Schedule an insert.
  void scheduleInsert(HashedKey hk, BufferView value, InsertCallback cb);

  // Perform lookup by keeping retrying until a result (Ok, NotFound, Error) is
  // reached.
  Status lookupSync(HashedKey hk, Buffer& value) const;

  // Perform remove by keeping retrying until a result (Ok, NotFound, Error) is
  // reached.
  Status removeSync(HashedKey hk);

  // Schedule a lookup.
  void scheduleLookup(HashedKey hk, LookupCallback cb);

  // Schedule a remove.
  void scheduleRemove(HashedKey hk, RemoveCallback cb);

  // flush both engines.
  void flush();

  // reset both engines.
  void reset();

  // persist the navy engines state
  void persist(RecordWriter& rw) const;

  // recover the navy engines state
  bool recover(RecordReader& rr);

  // returns the navy stats
  void getCounters(const CounterVisitor& visitor) const;

  uint64_t getUsableSize() const;

  std::pair<Status, std::string> getRandomAlloc(Buffer& value);

  void validate();

 private:
  // Update statistics for lookup
  void updateLookupStats(Status status) const;

  // Select engine to insert key/value. Returns a pair:
  //   - first: engine to insert key/value
  //   - second: the other engine to remove key
  std::pair<Engine&, Engine&> select(HashedKey key, BufferView value) const;

  // Perform lookup in a retry friendly manner.
  Status lookupInternal(HashedKey hk,
                        Buffer& value,
                        bool& skipLargeItemCache) const;

  // insert an item to one of the engine and remove it from the other.
  // An option can be specified to skip insertion on retry.
  Status insertInternal(HashedKey key, BufferView value, bool& skipInsertion);

  // Performa a remove by hashed key in a retry friendly manner.
  Status removeHashedKeyInternal(HashedKey hk, bool& skipSmallItemCache);

  const uint32_t smallItemMaxSize_{};
  // Large item cache assumed to have fast response in case entry doesn't
  // exists (check metadata only).
  std::unique_ptr<Engine> largeItemCache_;
  // Lookup small item cache only if large item cache has no entry.
  std::unique_ptr<Engine> smallItemCache_;

  JobScheduler* scheduler_;

  // These stats are bumped only once per call.
  mutable TLCounter insertCount_;
  mutable TLCounter lookupCount_;
  // This stat is bumped once per retry.
  mutable TLCounter removeCount_;

  mutable AtomicCounter succInsertCount_;
  mutable AtomicCounter succLookupCount_;
  mutable AtomicCounter succRemoveCount_;
  mutable AtomicCounter ioErrorCount_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook
