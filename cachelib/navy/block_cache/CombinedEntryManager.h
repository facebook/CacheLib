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

#include "cachelib/shm/ShmManager.h"

namespace facebook {
namespace cachelib {
namespace navy {

// This class will manages things related to maintain combined entry blocks
//
class CombinedEntryManager {
 public:
  static constexpr std::string_view kShmCebManagerName =
      "shm_combined_entry_manager";

  CombinedEntryManager(uint64_t numCombinedEntryStreams,
                       uint32_t CombinedEntryBlockSize,
                       ShmManager* shmManager,
                       const std::string& name)
      : numCebStreams_{numCombinedEntryStreams},
        cebSize_{CombinedEntryBlockSize},
        shmManager_{shmManager},
        name_{name} {}

  // Resets all the combined entry block buffers
  void reset();
  // Persist the content of the combined entry block buffers
  void persist() const;
  // Recover the persisted content of the combined entry block buffers
  void recover();

 private:
  size_t getRequiredPreallocSize() const;

  uint64_t numCebStreams_{0};
  uint32_t cebSize_{0};
  ShmManager* shmManager_{};
  std::string name_;
  uint8_t* cebBuffers_{};
};

} // namespace navy
} // namespace cachelib
} // namespace facebook
