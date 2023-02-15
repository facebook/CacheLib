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

#include "cachelib/allocator/PrivateMemoryManager.h"

#include <folly/ScopeGuard.h>

namespace facebook {
namespace cachelib {

PrivateMemoryManager::~PrivateMemoryManager() {
  for (auto& entry : mappings) {
    util::munmapMemory(entry.first, entry.second);
  }
}

void* PrivateMemoryManager::createMapping(size_t size,
                                          PrivateSegmentOpts opts) {
  void* addr = util::mmapAlignedZeroedMemory(opts.alignment, size);
  auto guard = folly::makeGuard([&]() {
    util::munmapMemory(addr, size);
    mappings.erase(addr);
  });

  XDCHECK_EQ(reinterpret_cast<uint64_t>(addr) & (opts.alignment - 1), 0ULL);

  if (!opts.memBindNumaNodes.empty()) {
    util::mbindMemory(addr, size, MPOL_BIND, opts.memBindNumaNodes, 0);
  }

  mappings.emplace(addr, size);

  guard.dismiss();
  return addr;
}
} // namespace cachelib
} // namespace facebook