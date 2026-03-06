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

#include <folly/Optional.h>
#include <folly/logging/xlog.h>

#include <magic_enum/magic_enum.hpp>

#include "cachelib/allocator/KAllocation.h"

namespace facebook {
namespace cachelib {

// Enum that contains a list of events that correspond to external API calls
// on a CacheAllocator instance that we can track through event interface .
enum class AllocatorApiEvent : uint8_t {
  INVALID = 0,
  FIND = 1,
  FIND_FAST = 2,
  ALLOCATE = 3,
  INSERT = 4,
  INSERT_FROM_NVM = 5,
  INSERT_OR_REPLACE = 6,
  REMOVE = 7,
  ALLOCATE_CHAINED = 8,
  ADD_CHAINED = 9,
  POP_CHAINED = 10,
  DRAM_EVICT = 11,
  NVM_REMOVE = 12,
  NVM_EVICT = 13,
  PEEK = 14,
  NVM_INSERT = 15,
  NVM_FIND = 16,
  NVM_FIND_FAST = 17,
  NVM_ADMIT = 18,
  NVM_REINSERT = 19
};

// Enum to describe possible outcomes of Allocator API calls.
enum class AllocatorApiResult : uint8_t {
  FAILED = 0,              // Hard failure.
  FOUND = 1,               // Found an item in a 'find' call.
  NOT_FOUND = 2,           // Item was not fund in a 'find' call.
  NOT_FOUND_IN_MEMORY = 3, // Item was not found in memory with NVM enabled.
  ALLOCATED = 4,           // Successfully allocated a new item.
  INSERTED = 5,            // Inserted a new item in the map.
  REPLACED = 6,            // Replaced an item in a map.
  REMOVED = 7,             // Removed an item.
  EVICTED = 8,             // Evicted an item.
  EXPIRED = 9,             // An item has expired.
  REINSERTED = 10,         // Reinserted an item.
  NVM_ADMITTED = 11,       // Admit item to NVM
  CORRUPTED = 12,          // An item is corrupted.
  ABORTED = 13,            // The event operation aborted.
  ACCEPTED = 14,   // Positive result for admission and reinsertion policies
  REJECTED = 15,   // Negative result for admission and reinsertion policies
  INVALIDATED = 16 // Item invalidated (newer item for the key exist in NVM.)
};

namespace EventInterfaceTypes {
using SizeT = folly::Optional<uint32_t>;
using TtlT = uint32_t;
} // namespace EventInterfaceTypes

// This class defines the interface for recording events inside Cache Library.
template <typename Key>
class EventInterface {
 public:
  virtual ~EventInterface() {}

  // Method that, possibly, samples and records events.
  // @param event Enum describing the event.
  // @param key Key on which the operation was performed.
  // @param resul Result of the API call.
  // @param valueSize value size, if known
  virtual void record(AllocatorApiEvent event,
                      Key key,
                      AllocatorApiResult result,
                      EventInterfaceTypes::SizeT valueSize = folly::none,
                      EventInterfaceTypes::TtlT ttlSecs = 0) = 0;

  // Method that extracts stats from the event logger
  // @param statsMap A map of string to a stat value.
  virtual void getStats(
      std::unordered_map<std::string, uint64_t>& statsMap) const = 0;
};

using LegacyEventTracker = EventInterface<KAllocation::Key>;

} // namespace cachelib
} // namespace facebook
