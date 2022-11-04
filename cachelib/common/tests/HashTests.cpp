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

#include <limits>
#include <string>
#include <unordered_map>

#include "cachelib/common/Hash.h"

namespace facebook {
namespace cachelib {

static const int kMaxKeySize = 2048;
static std::mt19937 gen(folly::Random::rand32());

std::string generateRandomString() {
  static std::uniform_int_distribution<> genChar(0, 255);
  std::string ret;
  for (int i = 0; i < kMaxKeySize; i++) {
    ret.push_back(static_cast<char>(genChar(gen)));
  }
  return ret;
}

TEST(Hash, Murmur2) {
  int numTests = 10000;
  facebook::cachelib::MurmurHash2 murmur;

  for (int i = 0; i < numTests; i++) {
    auto str = generateRandomString();
    uint32_t hash = murmur(reinterpret_cast<void*>(str.data()), str.size());
    EXPECT_EQ(hash, murmur(reinterpret_cast<void*>(str.data()), str.size()));
  }
}

TEST(Hash, Murmur2Other) {
  int numTests = 10000;
  facebook::cachelib::MurmurHash2 murmur;
  facebook::cachelib::MurmurHash2 murmurOther;

  for (int i = 0; i < numTests; i++) {
    auto str = generateRandomString();
    uint32_t hash = murmur(reinterpret_cast<void*>(str.data()), str.size());
    EXPECT_EQ(hash,
              murmurOther(reinterpret_cast<void*>(str.data()), str.size()));
  }
}

TEST(Hash, Furc) {
  int numTests = 10000;

  uint32_t range = folly::Random::rand32(2, 100000);

  for (int i = 0; i < numTests; i++) {
    auto str = generateRandomString();
    uint32_t hash = facebook::cachelib::furcHash(
        reinterpret_cast<void*>(str.data()), str.size(), range);
    EXPECT_LT(hash, range);
    EXPECT_EQ(hash,
              facebook::cachelib::furcHash(
                  reinterpret_cast<void*>(str.data()), str.size(), range));
  }
}

// rehash to a lower range and ensure that only the keys mapping to removed
// buckets are rehashed.
TEST(Hash, FurcRehashLow) {
  int numTests = 10000;

  uint32_t range = folly::Random::rand32(2, 100000);

  // previous results
  std::unordered_map<std::string, uint32_t> res;

  for (int i = 0; i < numTests; i++) {
    auto str = generateRandomString();
    uint32_t hash = facebook::cachelib::furcHash(
        reinterpret_cast<void*>(str.data()), str.size(), range);
    EXPECT_LE(hash, range);
    res[str] = hash;
  }

  uint32_t newRange = range - folly::Random::rand32(0, range);

  // ensure the re-distribution preserves the old elements that were already
  // mapped to 0 - range.
  for (const auto& kv : res) {
    auto oldHash = kv.second;
    auto newHash = facebook::cachelib::furcHash(
        reinterpret_cast<const void*>(kv.first.data()),
        kv.first.size(),
        newRange);
    EXPECT_LE(newHash, newRange - 1);
    if (oldHash < newRange - 1) {
      // hash value should not change for keys that mapped to previous range
      // values
      EXPECT_EQ(newHash, oldHash);
    }
  }
}

// ensure that when the range expands, old buckets only hash to new ones and
// not to other old ones.
TEST(Hash, FurcRehashHigh) {
  int numTests = 10000;

  uint32_t range = folly::Random::rand32(2, 100000);

  // previous results
  std::unordered_map<std::string, uint32_t> res;

  for (int i = 0; i < numTests; i++) {
    auto str = generateRandomString();
    uint32_t hash = facebook::cachelib::furcHash(
        reinterpret_cast<void*>(str.data()), str.size(), range);
    EXPECT_LE(hash, range);
    res[str] = hash;
  }

  uint32_t newRange = range + folly::Random::rand32(2, range);

  for (const auto& kv : res) {
    auto oldHash = kv.second;
    auto newHash = facebook::cachelib::furcHash(
        reinterpret_cast<const void*>(kv.first.data()),
        kv.first.size(),
        newRange);
    EXPECT_LE(newHash, newRange - 1);
    if (oldHash < range - 1) {
      EXPECT_TRUE(oldHash == newHash || newHash >= range);
    }
  }
}

} // namespace cachelib
} // namespace facebook
