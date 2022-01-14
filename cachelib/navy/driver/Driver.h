/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

#include <memory>
#include <stdexcept>
#include <utility>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/navy/AbstractCache.h"
#include "cachelib/navy/admission_policy/AdmissionPolicy.h"
#include "cachelib/navy/common/Device.h"
#include "cachelib/navy/engine/Engine.h"
#include "cachelib/navy/scheduler/JobScheduler.h"

namespace facebook {
namespace cachelib {
namespace navy {
// The driver for Navy cache engines.
// This class provides the synchronous and asynchronous navy APIs to NvmCache.
class Driver final : public AbstractCache {
 public:
  struct Config {
    std::unique_ptr<Device> device;
    std::unique_ptr<JobScheduler> scheduler;
    std::unique_ptr<Engine> largeItemCache;
    std::unique_ptr<Engine> smallItemCache;
    std::unique_ptr<AdmissionPolicy> admissionPolicy;
    uint32_t smallItemMaxSize{};
    // Limited by scheduler parallelism (thread), this is large enough value to
    // mean "no limit".
    uint32_t maxConcurrentInserts{1'000'000};
    uint64_t maxParcelMemory{256 << 20}; // 256MB
    size_t metadataSize{};

    Config& validate();
  };

  // Contructor throws std::exception if config is invalid.
  //
  // @param config  config that was validated with Config::validate
  //
  // @throw std::invalid_argument on bad config
  explicit Driver(Config&& config);
  Driver(const Driver&) = delete;
  Driver& operator=(const Driver&) = delete;
  ~Driver() override;

  // Return true if item is considered a "large item". This is meant to be
  // a very fast check to verify a key & value pair will be considered as
  // "small" or "large" objects.
  bool isItemLarge(BufferView key, BufferView value) const override;

  // synchronous fast lookup if a key probably exists in the cache,
  // it can return a false positive result. this provides an optimization
  // to skip the heavy lookup operation when key doesn't exist in the cache.
  bool couldExist(BufferView key) override;

  // insert a key and value into the cache
  // @param key    the item key
  // @param value  the item value
  // @return a status indicates success or failure, and the reason for failure
  Status insert(BufferView key, BufferView value) override;

  // insert a key and value into the cache asynchronously.
  // @param key    the item key
  // @param value  the item value
  // @param cb     a callback function be triggered when the insertion complete
  // @return       a status indicates success or failure enqueued, and the
  //               reason for failure
  Status insertAsync(BufferView key,
                     BufferView value,
                     InsertCallback cb) override;

  // lookup a key in the cache.
  // @param key    the item key to lookup
  // @param value  the returned value for the key if found
  // @return       a status indicates success or failure, and the reason for
  //               failure
  Status lookup(BufferView key, Buffer& value) override;

  // lookup a key in the cache asynchronously.
  // @param key  the item key to lookup
  // @param cb   a callback function be triggered when the lookup complete,
  //             the result will be provided to the function.
  // @return     a status indicates success or failure enqueued, and the reason
  //             for failure
  Status lookupAsync(BufferView key, LookupCallback cb) override;

  // remove the key from cache
  // @param key  the item key to be removed
  // @return a status indicates success or failure, and the reason for failure
  Status remove(BufferView key) override;

  // remove the key from cache asynchronously.
  // @param key  the item key to be removed
  // @param cb   a callback function be triggered when the remove complete.
  // @return     a status indicates success or failure enqueued, and the reason
  //             for failure
  Status removeAsync(BufferView key, RemoveCallback cb) override;

  // ensure all pending job have been completed and data has been flush to
  // device(s).
  void flush() override;

  // Reset navy cache to the initial state.
  void reset() override;

  // persist the navy engines state
  void persist() const override;

  // recover the navy engines state
  bool recover() override;

  // returns the size of the device
  uint64_t getSize() const override;

  // returns the navy stats
  void getCounters(const CounterVisitor& visitor) const override;

  // This is a temporary API to update the maxWriteRate for
  // DynamicRandomAdissionPolicy. The long term plan is to
  // support online update to this and other cache configs.
  // Returns true if update successfully
  //         false if AdissionPolicy is not set or not DynamicRandom.
  bool updateMaxRateForDynamicRandomAP(uint64_t maxRate) override;

 private:
  struct ValidConfigTag {};

  // Assumes that @config was validated with Config::validate
  Driver(Config&& config, ValidConfigTag);
  void onEviction(BufferView key, uint32_t valueSize);

  // Select engine to insert key/value. Returns a pair:
  //   - first: engine to insert key/value
  //   - second: the other engine to remove key
  std::pair<Engine&, Engine&> select(BufferView key, BufferView value) const;
  void updateLookupStats(Status status) const;
  Status removeHashedKey(HashedKey hk, bool& skipSmallItemCache);
  bool admissionTest(HashedKey hk, BufferView value) const;

  const uint32_t smallItemMaxSize_{};
  const uint32_t maxConcurrentInserts_{};
  const uint64_t maxParcelMemory_{};
  const size_t metadataSize_{};

  std::unique_ptr<Device> device_;
  std::unique_ptr<JobScheduler> scheduler_;
  // Large item cache assumed to have fast response in case entry doesn't
  // exists (check metadata only).
  std::unique_ptr<Engine> largeItemCache_;
  // Lookup small item cache only if large item cache has no entry.
  std::unique_ptr<Engine> smallItemCache_;
  std::unique_ptr<AdmissionPolicy> admissionPolicy_;

  // thread local counters in synchronized path
  mutable TLCounter insertCount_;
  mutable TLCounter lookupCount_;
  mutable TLCounter removeCount_;
  mutable TLCounter rejectedCount_;
  mutable TLCounter rejectedConcurrentInsertsCount_;
  mutable TLCounter rejectedParcelMemoryCount_;
  mutable TLCounter rejectedBytes_;
  mutable TLCounter acceptedCount_;
  mutable TLCounter acceptedBytes_;

  // atomic counters in asynchronized path
  mutable AtomicCounter succInsertCount_;
  mutable AtomicCounter succLookupCount_;
  mutable AtomicCounter succRemoveCount_;
  mutable AtomicCounter ioErrorCount_;
  mutable AtomicCounter parcelMemory_; // In bytes
  mutable AtomicCounter concurrentInserts_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook
