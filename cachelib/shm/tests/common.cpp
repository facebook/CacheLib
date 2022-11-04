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

#include "cachelib/shm/tests/common.h"

#include <folly/Random.h>

#include "cachelib/shm/ShmCommon.h"

namespace facebook {
namespace cachelib {
namespace tests {

std::atomic<unsigned int> ShmTestBase::index_{0};

/* static */
size_t ShmTestBase::getRandomSize() {
  const size_t ret = folly::Random::rand32(kMemMin, kMemMax);
  using facebook::cachelib::detail::getPageSize;
  const size_t pg_sz = getPageSize();
  assert(pg_sz > 0);
  return (ret == 0 || ret % pg_sz) ? ret + pg_sz - (ret % pg_sz) : ret;
}

/* static */
void ShmTestBase::checkMemory(void* addr,
                              size_t size,
                              const unsigned char value) {
  // check that the bytes from addr upto size are all of the same
  // magic byte
  const unsigned char* ptr = (const unsigned char*)addr;
  for (size_t i = 0; i < size; i++) {
    ASSERT_EQ(*(ptr + i), value)
        << "Found different value at pos " << i << " value " << *(ptr + i);
  }
}

/* static */
void ShmTestBase::writeToMemory(void* addr,
                                size_t size,
                                const unsigned char value) {
  memset(addr, value, size);
}
} // namespace tests
} // namespace cachelib
} // namespace facebook
