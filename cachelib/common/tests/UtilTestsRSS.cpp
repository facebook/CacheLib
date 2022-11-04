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

#include <folly/Random.h>
#include <folly/logging/xlog.h>
#include <gtest/gtest.h>
#include <sys/mman.h>

#include <atomic>
#include <thread>

#include "cachelib/common/Utils.h"

namespace facebook {
namespace cachelib {
namespace tests {

TEST(Util, MemRSS) {
  for (int i = 0; i < 10; i++) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    auto val = util::getRSSBytes();
    EXPECT_GT(val, 0);
    const size_t len = 16 * 1024 * 1024;
    void* ptr = ::mmap(nullptr, len, PROT_WRITE | PROT_READ,
                       MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    EXPECT_NE(MAP_FAILED, ptr);
    SCOPE_EXIT { ::munmap(ptr, len); };
    std::memset(reinterpret_cast<char*>(ptr), 5, len);
    // sleep to let the stat catch up.
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    auto newVal = util::getRSSBytes();
    EXPECT_GT(newVal, val) << folly::sformat("newVal = {}, val = {}", newVal,
                                             val);
    XLOG(INFO) << folly::sformat("newVal = {}, val = {}, len = {}", newVal, val,
                                 len);
    if (newVal - val >= len) {
      return;
    }

    // At this point we know something else in the system had most likely freed
    // memory to cause the before/after memory stats to be incorrect. We will
    // retry up to 10 times. If there's something wrong with how we collect
    // RSS stats we will fail all attempts, but it should be unlikely for
    // spurious things to mess up our test 10 times in a row.
  }
  EXPECT_TRUE(false) << "MemRSS test failed after 10 attempts";
}

} // namespace tests
} // namespace cachelib
} // namespace facebook
