#pragma once

#include "cachelib/allocator/tests/TestBase.h"

#include <algorithm>
#include <chrono>
#include <ctime>
#include <future>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

#include <folly/Random.h>

#include "cachelib/allocator/CCacheAllocator.h"
#include "cachelib/allocator/FreeMemStrategy.h"
#include "cachelib/allocator/LruTailAgeStrategy.h"
#include "cachelib/allocator/MarginalHitsOptimizeStrategy.h"
#include "cachelib/allocator/MarginalHitsStrategy.h"
#include "cachelib/allocator/PoolRebalancer.h"
#include "cachelib/allocator/Util.h"

#include "cachelib/compact_cache/CCacheCreator.h"

#include "cachelib/allocator/EventInterface.h"

namespace facebook {
namespace cachelib {

std::ostream& operator<<(std::ostream& str, AllocatorApiEvent ev) {
  return str << toString(ev);
}

std::ostream& operator<<(std::ostream& str, AllocatorApiResult result) {
  return str << toString(result);
}

namespace tests {

using EventInterfaceTypes::SizeT;
using EventInterfaceTypes::TtlT;

// Implementation of EventInterface used for testing.
template <typename Key>
class TestEventInterface : public facebook::cachelib::EventInterface<Key> {
 public:
  void record(AllocatorApiEvent event,
              Key key,
              AllocatorApiResult result,
              SizeT valueSize = folly::none,
              TtlT ttl = folly::none) override {
    lastEvent = event;
    lastKey = key;
    lastResult = result;
    lastValueSize = valueSize;
    lastTtl = ttl;
  }

  void getStats(std::unordered_map<std::string, uint64_t>&) const {}

  void check(AllocatorApiEvent event,
             Key key,
             AllocatorApiResult result,
             SizeT valueSize,
             TtlT ttl) {
    ASSERT_EQ(event, lastEvent);
    ASSERT_EQ(key, lastKey);
    ASSERT_EQ(result, lastResult);
    ASSERT_EQ(valueSize, lastValueSize);
    ASSERT_EQ(ttl, lastTtl);
  }

 private:
  AllocatorApiEvent lastEvent{AllocatorApiEvent::INVALID};
  Key lastKey{};
  AllocatorApiResult lastResult{AllocatorApiResult::FAILED};
  SizeT lastValueSize{folly::none};
  TtlT lastTtl{folly::none};
};

template <typename AllocatorT>
class EventInterfaceTest : public AllocatorTest<AllocatorT> {
 private:
  const size_t kShmInfoSize = 10 * 1024 * 1024; // 10 MB
  using AllocatorTest<AllocatorT>::testShmIsNotRemoved;
  using AllocatorTest<AllocatorT>::testShmIsRemoved;
  using AllocatorTest<AllocatorT>::testInfoShmIsRemoved;

 public:
  // test events related to item allocation
  void testAllocateAndInsertEvents() {
    typename AllocatorT::Config config;
    // create an allocator worth 100 slabs.
    config.setCacheSize(100 * Slab::kSize);
    auto eventTracker =
        std::make_shared<TestEventInterface<typename AllocatorT::Key>>();
    config.setEventTracker(eventTracker);
    AllocatorT alloc(config);

    auto eventTrackerPtr = eventTracker.get();

    std::set<uint32_t> allocSizes{100, 1000, 2000, 5000};
    auto pid = alloc.addPool("default", alloc.getCacheMemoryStats().cacheSize,
                             allocSizes);

    const unsigned int keyLen = 100;
    const uint32_t valueSize = 1024;
    const uint32_t ttl = 101;
    const auto key = this->getRandomNewKey(alloc, keyLen);

    {
      auto handle = alloc.allocate(pid, key, valueSize, ttl);
      ASSERT_NE(handle, nullptr);
      eventTrackerPtr->check(AllocatorApiEvent::ALLOCATE, key,
                             AllocatorApiResult::ALLOCATED, valueSize, ttl);

      ASSERT_TRUE(alloc.insert(handle));
      eventTrackerPtr->check(AllocatorApiEvent::INSERT, key,
                             AllocatorApiResult::INSERTED, valueSize, ttl);
    }

    {
      auto handle = alloc.allocate(pid, key, valueSize, ttl);
      ASSERT_NE(handle, nullptr);
      eventTrackerPtr->check(AllocatorApiEvent::ALLOCATE, key,
                             AllocatorApiResult::ALLOCATED, valueSize, ttl);

      // An attempt to insert a handle for a key that already exists fails.
      ASSERT_FALSE(alloc.insert(handle));
      eventTrackerPtr->check(AllocatorApiEvent::INSERT, key,
                             AllocatorApiResult::FAILED, valueSize, ttl);
      // Insert or replace succeeds with replace outcome.
      ASSERT_NE(alloc.insertOrReplace(handle), nullptr);
      eventTrackerPtr->check(AllocatorApiEvent::INSERT_OR_REPLACE, key,
                             AllocatorApiResult::REPLACED, valueSize, ttl);
    }

    {
      const auto key2 = this->getRandomNewKey(alloc, keyLen);
      auto handle = alloc.allocate(pid, key2, valueSize, ttl);
      ASSERT_NE(handle, nullptr);
      eventTrackerPtr->check(AllocatorApiEvent::ALLOCATE, key2,
                             AllocatorApiResult::ALLOCATED, valueSize, ttl);

      // Insert or replace with a new key succeeds with 'INSERT' result.
      ASSERT_EQ(alloc.insertOrReplace(handle), nullptr);
      eventTrackerPtr->check(AllocatorApiEvent::INSERT_OR_REPLACE, key2,
                             AllocatorApiResult::INSERTED, valueSize, ttl);
    }
  }

  // make some allocations without evictions and ensure that we are able to
  // fetch them.
  void testFindEvents() {
    typename AllocatorT::Config config;
    // create an allocator worth 100 slabs.
    config.setCacheSize(100 * Slab::kSize);

    auto eventTracker =
        std::make_shared<TestEventInterface<typename AllocatorT::Key>>();
    auto eventTrackerPtr = eventTracker.get();

    config.setEventTracker(eventTracker);

    AllocatorT alloc(config);

    std::set<uint32_t> allocSizes{100, 1000, 2000, 5000};
    auto pid = alloc.addPool("default", alloc.getCacheMemoryStats().cacheSize,
                             allocSizes);

    const unsigned int keyLen = 100;
    const uint32_t valueSize = 1024;
    const uint32_t ttl = 102;
    const auto key = this->getRandomNewKey(alloc, keyLen);

    // Try to find non-existing key.
    EXPECT_EQ(alloc.find(key), nullptr);
    eventTrackerPtr->check(AllocatorApiEvent::FIND, key,
                           AllocatorApiResult::NOT_FOUND, folly::none,
                           folly::none);

    // Allocate and insert.
    util::allocateAccessible(alloc, pid, key, valueSize, ttl);

    // Find Again
    EXPECT_NE(alloc.find(key), nullptr);
    eventTrackerPtr->check(AllocatorApiEvent::FIND, key,
                           AllocatorApiResult::FOUND, valueSize, ttl);
  }

  // make some allocations without evictions, remove them and ensure that they
  // cannot be accessed through find.
  void testRemoveEvents() {
    typename AllocatorT::Config config;
    // create an allocator worth 100 slabs.
    config.setCacheSize(100 * Slab::kSize);
    auto eventTracker =
        std::make_shared<TestEventInterface<typename AllocatorT::Key>>();
    auto eventTrackerPtr = eventTracker.get();

    config.setEventTracker(eventTracker);

    AllocatorT alloc(config);

    std::set<uint32_t> allocSizes{100, 1000, 2000, 5000};
    auto pid = alloc.addPool("default", alloc.getCacheMemoryStats().cacheSize,
                             allocSizes);

    const unsigned int keyLen = 100;
    const uint32_t valueSize = 1024;
    const uint32_t ttl = 103;
    const auto key = this->getRandomNewKey(alloc, keyLen);

    // Try to remove non-existing key.
    EXPECT_EQ(alloc.remove(key), AllocatorT::RemoveRes::kNotFoundInRam);
    eventTrackerPtr->check(AllocatorApiEvent::REMOVE, key,
                           AllocatorApiResult::NOT_FOUND, folly::none,
                           folly::none);

    // Allocate and insert.
    util::allocateAccessible(alloc, pid, key, valueSize, ttl);

    // Find Again
    EXPECT_EQ(alloc.remove(key), AllocatorT::RemoveRes::kSuccess);
    eventTrackerPtr->check(AllocatorApiEvent::REMOVE, key,
                           AllocatorApiResult::REMOVED, valueSize, ttl);
  }
};
} // namespace tests
} // namespace cachelib
} // namespace facebook
