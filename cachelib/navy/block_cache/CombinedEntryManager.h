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

#include "cachelib/navy/block_cache/CombinedEntryBlock.h"
#include "cachelib/shm/ShmManager.h"

namespace facebook {
namespace cachelib {
namespace navy {

// Callback to write the active combined entry block to the region when it's
// filled out
using WriteCebCallback =
    std::function<Status(uint64_t, const CombinedEntryBlock&)>;

// Callback to read the active combined entry block from the region
using ReadCebCallback = std::function<Status(uint32_t, uint32_t, Buffer&)>;

// This class will manage things related to maintaining combined entry blocks
//
// - There will be multiple streams (configured by 'numCombinedEntryStreams' in
// constructor) and each stream will maintain an active combined entry block at
// any moment.
// - Inserting an entry to each stream will add the entry to the active combined
// entry block (CEB) for the stream.
// - For persistence, CombinedEntryManager will manage the buffer for each
// active CEB and is responsible for persisting/recovering those buffers with
// shutdown/start cycles. (TODO)
// - Once active CEB is full, it will be flushed to the flash. (TODO)
//
// *** Access for each stream is NOT thread safe within CombinedEntryManager.
// It's assumed that caller is responsible for protecting any concurrent access
// issue per each stream. CombinedEntryManager does NOT handle/lock any access
// to be thread safe.
//
class CombinedEntryManager {
 public:
  static constexpr std::string_view kShmCebManagerName =
      "shm_combined_entry_manager";

  CombinedEntryManager() = delete;

  CombinedEntryManager(uint64_t numCombinedEntryStreams,
                       uint32_t CombinedEntryBlockSize,
                       ShmManager* shmManager,
                       const std::string& name,
                       WriteCebCallback writeCebCb,
                       ReadCebCallback readCebCb)
      : numCebStreams_{numCombinedEntryStreams},
        cebSize_{CombinedEntryBlockSize},
        shmManager_{shmManager},
        name_{name},
        writeCebCb_{std::move(writeCebCb)},
        readCebCb_{std::move(readCebCb)},
        cebStreams_{numCebStreams_} {}

  // Resets all the combined entry block buffers
  void reset();
  // Persist the content of the combined entry block buffers
  void persist() const;
  // Recover the persisted content of the combined entry block buffers
  void recover();

  // Will return the combined entry block for the given stream
  CombinedEntryBlock* getCombinedEntryBlock(uint64_t stream);

  // Adding a index entry to currently active combined entry block for the
  // stream
  CombinedEntryStatus addIndexEntryToStream(uint64_t stream,
                                            uint64_t bid,
                                            uint64_t key,
                                            const EntryRecord& record);

  // Peek active combined entry block(s) for the stream to check if we have a
  // valid entry for the given key
  bool peekIndexEntryFromStream(uint64_t stream, uint64_t key);

  // Get the index entry for the given key from the active CEB(s) for the stream
  folly::Expected<EntryRecord, CombinedEntryStatus> getIndexEntryFromStream(
      uint64_t stream, uint64_t key);

  // Remove an index entry for the given key from the active CEB(s) for the
  // stream
  CombinedEntryStatus removeIndexEntryFromStream(uint64_t stream, uint64_t key);

  size_t getTotalCombinedEntryBlocks() const { return totalCebs_; }

 private:
  size_t getRequiredPreallocSize() const;

  uint64_t numCebStreams_{0};
  uint32_t cebSize_{0};
  ShmManager* shmManager_{};
  std::string name_;
  uint8_t* cebBuffers_{};

  const WriteCebCallback writeCebCb_;
  const ReadCebCallback readCebCb_;

  // Total number of CombinedEntryBlocks whether it's in memory or flash
  std::atomic<size_t> totalCebs_{0};
  // Active CombinedEntryBlocks for each streams
  std::vector<std::unique_ptr<CombinedEntryBlock>> cebStreams_;
};

} // namespace navy
} // namespace cachelib
} // namespace facebook
