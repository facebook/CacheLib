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
#include <gtest/gtest.h>

#include <functional>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

#include "cachelib/common/BytesEqual.h"

namespace facebook {
namespace cachelib {

static const int kMaxKeySize = 2048;
static std::mt19937 gen(folly::Random::rand32());

std::vector<char> generateRandomString() {
  static std::uniform_int_distribution<> genChar(0, 255);
  std::vector<char> ret;
  for (int i = 0; i < kMaxKeySize; i++) {
    ret.push_back(static_cast<char>(genChar(gen)));
  }
  return ret;
}

void checkCorrectness(void* a, void* b, unsigned int len) {
  // a has been pushed by i bytes and b has been pushed by j bytes from
  // their default 1 byte aligned locations.
  bool res1 = memcmp(a, b, len) == 0;
  bool res2 = bytesEqual(a, b, len);
  ASSERT_TRUE(res1 == res2);
}

void testAllAlignment(bool equal) {
  auto d = generateRandomString();
  auto a = d;
  a.reserve(kMaxKeySize + 8);
  // ensure we checked all possible alignments
  uint64_t byte_align = 0;
  // we care about 0 - 8 byte alignment.
  for (int i = 0; i < 8; i++) {
    // push a's alignment by this many bytes by padding in the front.
    if (i != 0) {
      a.insert(a.begin(), 'X');
    }
    const int aAlign = ((uintptr_t)(void*)&a[i]) % 8;
    auto b = equal ? d : generateRandomString();
    b.reserve(kMaxKeySize + 8);
    for (int j = 0; j < 8; j++) {
      // push a's alignment by this many bytes by padding in the front.
      if (j != 0) {
        b.insert(b.begin(), 'X');
      }
      const int bAlign = ((uintptr_t)(void*)&b[j]) % 8;
      byte_align |= (1ULL << (aAlign * 8 + bAlign));
      for (int len = 0; len < kMaxKeySize; len++) {
        // a has been pushed by i bytes and b has been pushed by j bytes from
        // their default 1 byte aligned locations.
        checkCorrectness(&a[i], &b[j], len);
      }
    }
  }
  ASSERT_EQ(byte_align, std::numeric_limits<uint64_t>::max());
}

TEST(bytesEqual, alignment_equal) { testAllAlignment(true); }

TEST(bytesEqual, alignment_notequal) { testAllAlignment(false); }

TEST(bytesEqual, randomCmp) {
#ifdef FOLLY_SANITIZE_ADDRESS
  const auto kRandomCmps = 100;
#else
  const auto kRandomCmps = 1000;
#endif
  for (int i = 0; i < kRandomCmps; i++) {
    auto a = generateRandomString();
    auto b = generateRandomString();
    for (size_t j = 0; j < a.size(); j++) {
      for (size_t len = 0; len < a.size() - j; len++) {
        checkCorrectness(&a[j], &b[j], len);
      }
    }
  }
}

} // namespace cachelib
} // namespace facebook
