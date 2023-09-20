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

// This is the only header the client includes
#pragma once

#include <folly/Function.h>
#include <folly/Range.h>

#include <functional>
#include <memory>
#include <stdexcept>

#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/common/Hash.h"
#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/serialization/RecordIO.h"

namespace facebook {
namespace cachelib {
namespace navy {
// Inser/lookup/remove callback declared as folly function because key, value
// (if present) assumed to be valid for the whole async call. User most likely
// captures them in the callback. And this capture can be unique_ptr which is
// not possible with std::function.

using InsertCallback = folly::Function<void(Status status, HashedKey key)>;

using LookupCallback =
    folly::Function<void(Status status, HashedKey key, Buffer value)>;

using RemoveCallback = folly::Function<void(Status status, HashedKey key)>;

// Generic cache interface.
// All functions are synchronous, unless stated the opposite.
class AbstractCache {
 public:
  virtual ~AbstractCache() = default;

  // Return true if item is considered a "large item". This is meant to be
  // a very fast check to verify a key & value pair will be considered as
  // "small" or "large" objects.
  virtual bool isItemLarge(HashedKey key, BufferView value) const = 0;

  // Checks if the key could exist in the cache. This can be used as a
  // pre-check to optimize cache lookups to avoid calling lookup in an async IO
  // environment.
  // Returns: false if the key definitely does not exist and true if it could.
  virtual bool couldExist(HashedKey key) = 0;

  // Inserts entry into cache.
  // Returns: Ok, Rejected, DeviceError
  virtual Status insert(HashedKey key, BufferView value) = 0;

  // Asynchronously inserts entry into the cache.
  // Invokes callback when done on a worker thread. Callback is optional.
  //
  // No assumptions about @key and @value lifetime: all data copied before
  // async job added to the queue.
  //
  // Returns: Ok, Rejected
  virtual Status insertAsync(HashedKey key,
                             BufferView value,
                             InsertCallback cb) = 0;

  // Looks up value. Returns non-null buffer if found.
  // Returns: Ok, NotFound, DeviceError
  virtual Status lookup(HashedKey key, Buffer& value) = 0;

  // Asynchronously looks up value.
  // Invokes callback when done on a worker thread.
  //
  // @key must be valid till delayed async job execution, no copy is made. It
  // is user responsibility to make a copy if needed (capture in callback).
  virtual void lookupAsync(HashedKey key, LookupCallback cb) = 0;

  // Removes from the index, space reused after reclamation.
  // Returns: Ok, NotFound
  virtual Status remove(HashedKey key) = 0;

  // Asynchronously removes key from the index, space reused after reclamation.
  // Callback is optional.
  //
  // See @lookupAsync about @key lifetime.
  virtual void removeAsync(HashedKey key, RemoveCallback cb) = 0;

  // Executes all queued operations and makes sure result is reflected on the
  // device.
  virtual void flush() = 0;

  // Reset cache to the initial state.
  virtual void reset() = 0;

  // Stores information needed to restart the cache to device
  virtual void persist() const = 0;

  // Recovers cache information. Returns true on success.
  virtual bool recover() = 0;

  // Reports counters for monitoring/alerting purposes. Invokes @visitor for
  // every counter with key name and value.
  virtual void getCounters(const CounterVisitor& visitor) const = 0;

  // Return how big the cache size is in bytes (device size)
  virtual uint64_t getSize() const = 0;

  // Return the size of space used for caching
  virtual uint64_t getUsableSize() const = 0;

  // This is a temporary API to update the maxWriteRate for
  // DynamicRandomAdissionPolicy. The long term plan is to
  // support online update to this and other cache configs.
  // Returns true if update successfully
  //         false if AdissionPolicy is not set or not DynamicRandom.
  virtual bool updateMaxRateForDynamicRandomAP(uint64_t) = 0;

  // Get key and Buffer for a random sample
  virtual std::pair<Status, std::string /* key */> getRandomAlloc(
      Buffer& value) = 0;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook
