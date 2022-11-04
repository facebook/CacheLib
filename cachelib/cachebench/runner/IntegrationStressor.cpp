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

#include "cachelib/cachebench/runner/IntegrationStressor.h"

#include <folly/logging/xlog.h>

#include <iostream>

namespace facebook {
namespace cachelib {
namespace cachebench {

const uint32_t HighRefcountStressor::kNumThreads;

HighRefcountStressor::HighRefcountStressor(const CacheConfig& cacheConfig,
                                           uint64_t numOps)
    : numOpsPerThread_{numOps},
      cache_{std::make_unique<Cache<LruAllocator>>(cacheConfig)} {}

void HighRefcountStressor::start() {
  startTime_ = std::chrono::system_clock::now();
  testThread_ = std::thread([this] {
    std::cout << folly::sformat("Total {:.2f}M ops to be run",
                                kNumThreads * numOpsPerThread_ / 1e6)
              << std::endl;

    std::vector<std::thread> workers;
    for (size_t i = 0; i < kNumThreads; i++) {
      workers.push_back(std::thread([this] {
        for (uint64_t j = 0; j < numOpsPerThread_; j++) {
          testLoop();
          ops_.fetch_add(1, std::memory_order_relaxed);
          if (shouldTestStop()) {
            break;
          }
        }
      }));
    }
    for (auto& worker : workers) {
      worker.join();
    }

    endTime_ = std::chrono::system_clock::now();
  });
}

void HighRefcountStressor::testLoop() {
  try {
    auto it = cache_->find("high_refcount");
    if (!it) {
      // Item was likely evicted during slab release. Try to allocate a new
      // one. It's fine to fail as the small alloc class may not have any
      // slab left.
      auto newIt =
          cache_->allocate(PoolId{0} /* pid */, "high_refcount", 1 /* size */);
      if (newIt) {
        cache_->insertOrReplace(newIt);
      }
    }
    uint32_t delay = 1 + folly::Random::rand32(10);
    std::this_thread::sleep_for(std::chrono::milliseconds{delay});
  } catch (const exception::RefcountOverflow& e) {
    XLOG_EVERY_MS(INFO, 600'000) << folly::sformat(
        "Detected refcount overflow in the last 10 minutes: {}", e.what());
  }
}

const uint32_t CachelibMapStressor::kMapSizeUpperbound;
const uint32_t CachelibMapStressor::kMapDeletionRate;
const uint32_t CachelibMapStressor::kMapShrinkSize;
const uint32_t CachelibMapStressor::kMapEntryValueSizeMin;
const uint32_t CachelibMapStressor::kMapEntryValueSizeMax;
const uint32_t CachelibMapStressor::kMapInsertionBatchMin;
const uint32_t CachelibMapStressor::kMapInsertionBatchMax;

CachelibMapStressor::CachelibMapStressor(const CacheConfig& cacheConfig,
                                         uint64_t numOps)
    : numOpsPerThread_{numOps} {
  struct CachelibMapTestCaseSync : public CacheType::SyncObj {
    CachelibMapStressor& stressor;
    std::string key;

    CachelibMapTestCaseSync(CachelibMapStressor& c, std::string itemKey)
        : stressor(c), key(std::move(itemKey)) {
      stressor.lockExclusive(key);
    }
    ~CachelibMapTestCaseSync() override { stressor.unlockExclusive(key); }
  };
  auto movingSync = [this](CacheType::Key key) {
    return std::make_unique<CachelibMapTestCaseSync>(*this, key.str());
  };

  cache_ = std::make_unique<CacheType>(cacheConfig, movingSync);
}

void CachelibMapStressor::start() {
  startTime_ = std::chrono::system_clock::now();

  for (size_t i = 0; i < keys_.size(); i++) {
    keys_[i] = folly::sformat("map_key_{}", i);
  }

  testThread_ = std::thread([this] {
    const size_t numThreads = std::thread::hardware_concurrency();
    std::cout << folly::sformat("Total {:.2f}M ops to be run",
                                numThreads * numOpsPerThread_ / 1e6)
              << std::endl;

    std::vector<std::thread> workers;
    for (size_t i = 0; i < numThreads; i++) {
      workers.push_back(std::thread([this] {
        for (uint64_t j = 0; j < numOpsPerThread_; j++) {
          testLoop();
          ops_.fetch_add(1, std::memory_order_relaxed);
          if (shouldTestStop()) {
            break;
          }
        }
      }));
    }
    for (auto& worker : workers) {
      worker.join();
    }

    endTime_ = std::chrono::system_clock::now();
  });
}

void CachelibMapStressor::testLoop() {
  const auto& key = keys_[folly::Random::rand32(keys_.size())];

  auto it = cache_->find(key);
  try {
    if (it) {
      folly::SharedMutex::ReadHolder r{getLock(key)};
      auto map =
          TestMap::fromWriteHandle(*cache_, std::move(it).toWriteHandle());
      if (map.size() > kMapSizeUpperbound &&
          folly::Random::oneIn(kMapDeletionRate)) {
        cache_->remove(map.viewWriteHandle());
      } else if (map.size() > kMapSizeUpperbound) {
        r.unlock();
        folly::SharedMutex::WriteHolder w{getLock(key)};
        pokeHoles(map);
      } else {
        readEntries(map);
        r.unlock();
        folly::SharedMutex::WriteHolder w{getLock(key)};
        populate(map);
      }
      return;
    }
    folly::SharedMutex::WriteHolder w{getLock(key)};
    auto map = TestMap::create(*cache_, 0, key);
    populate(map);
    cache_->insertOrReplace(map.viewWriteHandle());
  } catch (const std::bad_alloc& e) {
    XLOG_EVERY_MS(INFO, 600'000) << folly::sformat(
        "Detected allocation failure in the last 10 minutes: {}", e.what());
  }
}

void CachelibMapStressor::populate(TestMap& map) {
  // Insert 1000 - 2000 new entries into the map
  uint32_t curSize = map.size();
  uint32_t batchSize =
      folly::Random::rand32(kMapInsertionBatchMin, kMapInsertionBatchMax);
  for (uint32_t i = curSize; i < curSize + batchSize; i++) {
    auto val = detail::TestValue::create(
        i, folly::Random::rand32(kMapEntryValueSizeMin, kMapEntryValueSizeMax));
    map.insertOrReplace(i, *val);
  }
}

void CachelibMapStressor::pokeHoles(TestMap& map) {
  // Scan through all the entries in the map and remove up to 100 keys.
  uint32_t rate = map.size() / kMapShrinkSize;
  std::vector<uint64_t> keys;
  for (const auto& kv : map) {
    if (folly::Random::oneIn(rate)) {
      keys.push_back(kv.key);
    }
  }
  for (auto k : keys) {
    if (!map.erase(k)) {
      throw std::runtime_error(
          folly::sformat("Bug: Key '{}' couldn't be removed from map", k));
    }
  }
  map.compact();
}

void CachelibMapStressor::readEntries(TestMap& map) {
  // Scan through all the entries in the map and look up them by key
  std::vector<uint64_t> keys;
  for (const auto& kv : map) {
    keys.push_back(kv.key);
  }
  for (auto k : keys) {
    if (map.find(k) == nullptr) {
      throw std::runtime_error(
          folly::sformat("Bug: Key '{}' disappeared from map", k));
    }
  }
}

const uint32_t CachelibRangeMapStressor::kMapSizeUpperbound;
const uint32_t CachelibRangeMapStressor::kMapDeletionRate;
const uint32_t CachelibRangeMapStressor::kMapShrinkSize;
const uint32_t CachelibRangeMapStressor::kMapEntryValueSizeMin;
const uint32_t CachelibRangeMapStressor::kMapEntryValueSizeMax;
const uint32_t CachelibRangeMapStressor::kMapInsertionBatchMin;
const uint32_t CachelibRangeMapStressor::kMapInsertionBatchMax;

CachelibRangeMapStressor::CachelibRangeMapStressor(
    const CacheConfig& cacheConfig, uint64_t numOps)
    : numOpsPerThread_{numOps} {
  struct CachelibMapTestCaseSync : public CacheType::SyncObj {
    CachelibRangeMapStressor& stressor;
    std::string key;

    CachelibMapTestCaseSync(CachelibRangeMapStressor& c, std::string itemKey)
        : stressor(c), key(std::move(itemKey)) {
      stressor.lockExclusive(key);
    }
    ~CachelibMapTestCaseSync() override { stressor.unlockExclusive(key); }
  };
  auto movingSync = [this](LruAllocator::Key key) {
    return std::make_unique<CachelibMapTestCaseSync>(*this, key.str());
  };

  cache_ = std::make_unique<CacheType>(cacheConfig, movingSync);
}

void CachelibRangeMapStressor::start() {
  startTime_ = std::chrono::system_clock::now();

  for (size_t i = 0; i < keys_.size(); i++) {
    keys_[i] = folly::sformat("map_key_{}", i);
  }

  testThread_ = std::thread([this] {
    const size_t numThreads = std::thread::hardware_concurrency();
    std::cout << folly::sformat("Total {:.2f}M ops to be run",
                                numThreads * numOpsPerThread_ / 1e6)
              << std::endl;

    std::vector<std::thread> workers;
    for (size_t i = 0; i < numThreads; i++) {
      workers.push_back(std::thread([this] {
        for (uint64_t j = 0; j < numOpsPerThread_; j++) {
          testLoop();
          ops_.fetch_add(1, std::memory_order_relaxed);
          if (shouldTestStop()) {
            break;
          }
        }
      }));
    }
    for (auto& worker : workers) {
      worker.join();
    }

    endTime_ = std::chrono::system_clock::now();
  });
}

void CachelibRangeMapStressor::testLoop() {
  const auto& key = keys_[folly::Random::rand32(keys_.size())];

  auto it = cache_->find(key);
  try {
    if (it) {
      folly::SharedMutex::ReadHolder r{getLock(key)};
      auto map =
          TestMap::fromWriteHandle(*cache_, std::move(it).toWriteHandle());
      if (map.size() > kMapSizeUpperbound &&
          folly::Random::oneIn(kMapDeletionRate)) {
        cache_->remove(map.viewWriteHandle());
      } else if (map.size() > kMapSizeUpperbound) {
        r.unlock();
        folly::SharedMutex::WriteHolder w{getLock(key)};
        pokeHoles(map);
      } else {
        readEntries(map);
        r.unlock();
        folly::SharedMutex::WriteHolder w{getLock(key)};
        populate(map);
      }
      return;
    }
    folly::SharedMutex::WriteHolder w{getLock(key)};
    auto map = TestMap::create(*cache_, 0, key);
    populate(map);
    cache_->insertOrReplace(map.viewWriteHandle());
  } catch (const std::bad_alloc& e) {
    XLOG_EVERY_MS(INFO, 600'000) << folly::sformat(
        "Detected allocation failure in the last 10 minutes: {}", e.what());
  }
}

void CachelibRangeMapStressor::populate(TestMap& map) {
  // Insert 1000 - 2000 new entries into the map
  uint32_t curSize = map.size();
  uint32_t batchSize =
      folly::Random::rand32(kMapInsertionBatchMin, kMapInsertionBatchMax);
  for (uint32_t i = curSize; i < curSize + batchSize; i++) {
    auto val = detail::TestValue::create(
        i, folly::Random::rand32(kMapEntryValueSizeMin, kMapEntryValueSizeMax));
    map.insertOrReplace(i, *val);
  }
}

void CachelibRangeMapStressor::pokeHoles(TestMap& map) {
  // Scan through all the entries in the map and remove up to 100 keys.
  uint32_t rate = map.size() / kMapShrinkSize;
  std::vector<uint64_t> keys;
  for (const auto& kv : map) {
    if (folly::Random::oneIn(rate)) {
      keys.push_back(kv.key);
    }
  }
  for (auto k : keys) {
    if (!map.remove(k)) {
      throw std::runtime_error(
          folly::sformat("Bug: Key '{}' couldn't be removed from map", k));
    }
  }
  map.compact();
}

void CachelibRangeMapStressor::readEntries(TestMap& map) {
  // Scan through all the entries in the map and look up them by key
  std::vector<uint64_t> keys;
  for (const auto& kv : map) {
    keys.push_back(kv.key);
  }
  for (auto k : keys) {
    if (map.lookup(k) == map.end()) {
      throw std::runtime_error(
          folly::sformat("Bug: Key '{}' disappeared from map", k));
    }
  }

  auto key1 = folly::Random::rand32(keys.size() / 100);
  auto key2 = key1 + folly::Random::rand32(keys.size() - key1);
  auto range = map.rangeLookupApproximate(key1, key2);
  for (const auto& kv : range) {
    auto res = map.lookup(kv.key);
    if (res == map.end()) {
      throw std::runtime_error(folly::sformat(
          "Bug: Key '{}' in a range disappeared from map", kv.key));
    }
  }
}
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
