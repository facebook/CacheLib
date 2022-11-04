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

/**
 * @file CCacheTests.h
 * This file contains the unit tests for compact cache instances.
 */

#pragma once

#include <folly/Random.h>
#include <gtest/gtest.h>

#include <memory>

#include "cachelib/compact_cache/CCacheCreator.h"

#define BUCKETS_PER_CHUNK 64

/****************************************************************************/
/* Utility functions and classes */

namespace facebook {
namespace cachelib {
namespace tests {

using CCacheReturn = facebook::cachelib::CCacheReturn;

/**
 * Useful wrappers around the compact cache's remove and replace callbacks that
 * keep track of the amount of times the callbacks were called and the values
 * that were passed to them.
 * This makes it easy to create tests with callbacks without having to create
 * global variables and flags for checking if the callbacks were triggered
 * properly.
 *
 * Usage:
 *
 *   1) Create the wrappers:
 *      RemoveCbWrapper<CC> removeCb;
 *      ReplaceCbWrapper<CC> replaceCb;
 *
 *   2) Create a compact cache with the wrappers
 *      CC ccache(arena, removeCb.getCallable(),
 *                          replaceCb.getCallable());
 *
 *   3) Do some tests, and check that the callbacks were called as expected:
 *      ASSERT_EQ(removeCb.numCalls, 0)
 *      ASSERT_EQ(replaceCb.numCalls, 1)
 *      ASSERT_EQ(replaceCb.key, epectedKey);
 *      ASSERT_EQ(replaceCb.oldVal, expectedOldVal);
 *      ASSERT_EQ(replaceCb.newVal, expectedNewVal);
 */
using namespace std::placeholders;
template <typename CC, typename Sfinae = void>
struct RemoveCbWrapper {
  int numCalls;
  typename CC::Value val;
  typename CC::Key key;
  RemoveCbWrapper() : numCalls(0) {}
  void cb(typename CC::Key k) {
    memcpy(&key, &k, sizeof(k));
    numCalls++;
  }
  /** Return a callable object that can be used in a compact cache */
  typename CC::RemoveCb getCallable() {
    return std::bind(&RemoveCbWrapper<CC>::cb, this, _1);
  }
};
template <typename CC>
struct RemoveCbWrapper<CC, typename std::enable_if<CC::kHasValues>::type> {
  int numCalls;
  typename CC::Value val;
  typename CC::Key key;
  RemoveCbWrapper() : numCalls(0) {}
  void cb(typename CC::Key k, typename CC::Value const* v) {
    memcpy(&key, &k, sizeof(k));
    memcpy(&val, v, sizeof(*v));
    numCalls++;
  }
  typename CC::RemoveCb getCallable() {
    return std::bind(&RemoveCbWrapper<CC>::cb, this, _1, _2);
  }
};
template <typename CC, typename Sfinae = void>
struct ReplaceCbWrapper {
  int numCalls;
  typename CC::Value oldVal;
  typename CC::Value newVal;
  typename CC::Key key;
  ReplaceCbWrapper() : numCalls(0) {}
  void cb(typename CC::Key k) {
    numCalls++;
    memcpy(&key, &k, sizeof(k));
  }
  typename CC::ReplaceCb getCallable() {
    return std::bind(&ReplaceCbWrapper<CC>::cb, this, _1);
  }
};
template <typename CC>
struct ReplaceCbWrapper<CC, typename std::enable_if<CC::kHasValues>::type> {
  int numCalls;
  typename CC::Value oldVal;
  typename CC::Value newVal;
  typename CC::Key key;
  ReplaceCbWrapper() : numCalls(0) {}
  void cb(typename CC::Key k,
          const typename CC::Value* oldV,
          const typename CC::Value* newV) {
    memcpy(&key, &k, sizeof(k));
    memcpy(&oldVal, oldV, sizeof(*oldV));
    memcpy(&newVal, newV, sizeof(*newV));
    numCalls++;
  }
  typename CC::ReplaceCb getCallable() {
    return std::bind(&ReplaceCbWrapper<CC>::cb, this, _1, _2, _3);
  }
};

template <typename CC>
class CCWrapper : public CC {
 public:
  std::atomic<size_t>& numChunks() { return CC::numChunks_; }

  std::atomic<size_t>& pendingNumChunks() { return CC::pendingNumChunks_; }
};

/**
 * Handle setup and teardown for a test
 *
 * Usage:
 *
 *  TestSetup<CC> setup(2);
 *  // Do some tests here, if a test fails, ~TestSetup will cleanup.
 */
template <typename CC>
class TestSetup {
 public:
  /* Set up a compact cache with nbuckets buckets.
   * We reuse the same cache repeatedly. */
  explicit TestSetup(int nbuckets,
                     bool allowPromotions = true,
                     typename CC::RemoveCb removeCb = 0,
                     typename CC::ReplaceCb replaceCb = 0)
      : numChunks_((nbuckets + BUCKETS_PER_CHUNK - 1) / BUCKETS_PER_CHUNK),
        chunkSize_(std::min(nbuckets, BUCKETS_PER_CHUNK) *
                   sizeof(typename CC::Bucket)),
        allocator_(numChunks_ * chunkSize_ /* configuredSize */, chunkSize_),
        ccache_(new CC(allocator_,
                       removeCb,
                       replaceCb,
                       0 /* validCb */,
                       allowPromotions)) {
    setup();
  }

  ~TestSetup() {
    try {
      ccache_.reset();
    } catch (const std::exception& e) {
    }
  }

  CCWrapper<CC>* getCache() {
    return static_cast<CCWrapper<CC>*>(ccache_.get());
  }

 private:
  const size_t numChunks_;
  const size_t chunkSize_;
  typename CC::Allocator allocator_;
  std::unique_ptr<CC> ccache_;

  void setup() { ccache_->resize(); }
};

/****************************************************************************/
/* Tests */

template <typename CC>
static void testSet(bool allowPromotions) {
  typename CC::Value dummyValue(0xAA);

  TestSetup<CC> setup(2, allowPromotions);
  auto ccache = setup.getCache();

  // This should be a miss
  ASSERT_EQ(CCacheReturn::NOTFOUND, ccache->set(1, &dummyValue));
  // This should be a hit
  ASSERT_EQ(CCacheReturn::FOUND, ccache->set(1, &dummyValue));
}

template <typename CC>
static void testGet(bool allowPromotions, bool shouldPromote) {
  typename CC::Value dummyValue(0xCA);
  typename CC::Value out;

  TestSetup<CC> setup(2, allowPromotions);
  auto ccache = setup.getCache();

  // This should be a miss
  ASSERT_EQ(CCacheReturn::NOTFOUND,
            ccache->get(1, &out, nullptr, shouldPromote));
  // out should have been left untouched
  ASSERT_TRUE(out.isEmpty());

  // This should be a miss
  ASSERT_EQ(CCacheReturn::NOTFOUND, ccache->set(1, &dummyValue));
  // This should be a hit
  ASSERT_EQ(CCacheReturn::FOUND, ccache->get(1, &out, nullptr, shouldPromote));
  // Get should return the value we inserted.
  ASSERT_EQ(dummyValue, out);
}

template <typename CC>
static void testGetShouldPromoteParam(bool allowPromotions,
                                      bool shouldPromote) {
  constexpr int nbEntries = CC::BucketDescriptor::kEntriesPerBucket;

  RemoveCbWrapper<CC> removeCb;
  TestSetup<CC> setup(1, allowPromotions, removeCb.getCallable());
  auto ccache = setup.getCache();

  typename CC::Value val;

  /** Insert nbEntries elements to fill the bucket. */
  for (unsigned int i = 1; i <= nbEntries; ++i) {
    val = i;
    ASSERT_EQ(CCacheReturn::NOTFOUND, ccache->set(i, &val));
  }

  typename CC::Key firstKey(1);

  /** Get first entry*/
  ASSERT_EQ(CCacheReturn::FOUND,
            ccache->get(firstKey, &val, nullptr, shouldPromote));

  /** Add another item. This must evict an entry */
  val = nbEntries + 1;
  ASSERT_EQ(CCacheReturn::NOTFOUND, ccache->set(nbEntries + 1, &val));

  /** Check that an entry was removed from ccache*/
  ASSERT_EQ(1, removeCb.numCalls);

  if (!allowPromotions || !shouldPromote) {
    ASSERT_EQ(firstKey, removeCb.key);
  } else {
    ASSERT_NE(firstKey, removeCb.key);
  }
}

template <typename CC>
static void testDel(bool allowPromotions) {
  typename CC::Value dummyValue(0xFA);
  typename CC::Value out;

  TestSetup<CC> setup(2, allowPromotions);
  auto ccache = setup.getCache();

  // This should be a miss
  ASSERT_EQ(CCacheReturn::NOTFOUND, ccache->del(1, &out));
  // out should have been left untouched
  ASSERT_TRUE(out.isEmpty());

  // This should be a miss
  ASSERT_EQ(CCacheReturn::NOTFOUND, ccache->set(1, &dummyValue));
  // This should be a hit
  ASSERT_EQ(CCacheReturn::FOUND, ccache->del(1, &out));
  // Del should return the value we inserted.
  ASSERT_EQ(dummyValue, out);
  // This should be a miss
  ASSERT_EQ(CCacheReturn::NOTFOUND, ccache->get(1, &out));
  ASSERT_EQ(dummyValue, out);
}

template <typename CC>
static void testResizeSmaller(bool allowPromotions) {
  typename CC::Value dummyValue(0xFA);
  typename CC::Value out;

  const int wantSlabs = 15;
  const int invertBuckets = wantSlabs * BUCKETS_PER_CHUNK;
  TestSetup<CC> setup(invertBuckets, allowPromotions);
  auto ccache = setup.getCache();

  ASSERT_EQ(ccache->numChunks(), wantSlabs);
  ccache->pendingNumChunks() = 1;
  for (int i = 1; i < 100; i++) {
    ASSERT_EQ(CCacheReturn::NOTFOUND, ccache->set(i, &dummyValue));
    ASSERT_EQ(CCacheReturn::FOUND, ccache->get(i, &out));
  }

  // also find them in the new chunk from the double writing
  ccache->numChunks() = ccache->pendingNumChunks().load();
  for (int i = 1; i < 100; i++) {
    ASSERT_EQ(CCacheReturn::FOUND, ccache->get(i, &out));
  }
}

template <typename CC>
static void testResizeLarger(bool allowPromotions) {
  typename CC::Value dummyValue(0xFA);
  typename CC::Value out;

  const int wantSlabs = 15;
  const int invertBuckets = wantSlabs * BUCKETS_PER_CHUNK;
  TestSetup<CC> setup(invertBuckets, allowPromotions);
  auto ccache = setup.getCache();

  ASSERT_EQ(ccache->numChunks(), wantSlabs);
  ccache->pendingNumChunks() = ccache->numChunks().load();
  ccache->numChunks() = 1;
  for (int i = 1; i < 100; i++) {
    ASSERT_EQ(CCacheReturn::NOTFOUND, ccache->set(i, &dummyValue));
    ASSERT_EQ(CCacheReturn::FOUND, ccache->get(i, &out));
  }

  // also find them in the new chunk from the double writing
  ccache->numChunks() = ccache->pendingNumChunks().load();
  for (int i = 1; i < 100; i++) {
    ASSERT_EQ(CCacheReturn::FOUND, ccache->get(i, &out));
  }
}

template <typename CC>
static void testRehashSmaller(bool allowPromotions) {
  typename CC::Value dummyValue(0xFA);
  typename CC::Value out;

  const int wantSlabs = 15;
  const int invertBuckets = wantSlabs * BUCKETS_PER_CHUNK;
  TestSetup<CC> setup(invertBuckets, allowPromotions);
  auto ccache = setup.getCache();

  ASSERT_EQ(ccache->numChunks(), wantSlabs);

  for (int i = 1; i < 100; i++) {
    ASSERT_EQ(CCacheReturn::NOTFOUND, ccache->set(i, &dummyValue));
    ASSERT_EQ(CCacheReturn::FOUND, ccache->get(i, &out));
  }

  // also find them in new chunk after rehashing
  size_t curChunks = ccache->numChunks();

  ccache->numChunks() = curChunks / 2;

  int hits = 0;

  for (int i = 1; i < 100; i++) {
    if (ccache->get(i, &out) == CCacheReturn::FOUND) {
      hits++;
    }
  }
  // expect about half to still hit
  ASSERT_TRUE(hits < 60);
  ASSERT_TRUE(hits > 40);

  ccache->tableRehash(
      curChunks, curChunks / 2, facebook::cachelib::RehashOperation::COPY);

  for (int i = 1; i < 100; i++) {
    ASSERT_EQ(CCacheReturn::FOUND, ccache->get(i, &out));
  }
}

template <typename CC>
static void testRehashLarger(bool allowPromotions) {
  typename CC::Value dummyValue(0xFA);
  typename CC::Value out;

  const int wantSlabs = 15;
  const int invertBuckets = wantSlabs * BUCKETS_PER_CHUNK;
  TestSetup<CC> setup(invertBuckets, allowPromotions);
  auto ccache = setup.getCache();

  ASSERT_EQ(ccache->numChunks(), wantSlabs);

  // insert into smaller table
  size_t curChunks = ccache->numChunks();

  ccache->numChunks() = curChunks / 2;

  for (int i = 1; i < 100; i++) {
    ASSERT_EQ(CCacheReturn::NOTFOUND, ccache->set(i, &dummyValue));
    ASSERT_EQ(CCacheReturn::FOUND, ccache->get(i, &out));
  }

  ccache->numChunks() = curChunks;

  int hits = 0;

  for (int i = 1; i < 100; i++) {
    if (ccache->get(i, &out) == CCacheReturn::FOUND) {
      hits++;
    }
  }
  // expect about half to still hit
  ASSERT_TRUE(hits < 60);
  ASSERT_TRUE(hits > 40);

  ccache->tableRehash(
      curChunks / 2, curChunks, facebook::cachelib::RehashOperation::COPY);

  for (int i = 1; i < 100; i++) {
    ASSERT_EQ(CCacheReturn::FOUND, ccache->get(i, &out));
  }
}

template <typename CC>
static void testPurgeCallback(bool allowPromotions) {
  int nAdded = 0;
  typename CC::Value val;

  RemoveCbWrapper<CC> removeCb;
  ReplaceCbWrapper<CC> replaceCb;
  TestSetup<CC> setup(
      2, allowPromotions, removeCb.getCallable(), replaceCb.getCallable());
  auto ccache = setup.getCache();

  while (removeCb.numCalls * 2 <= nAdded) {
    // run this test while we have more less than half of entries evicted
    for (int i = 0; i < 10; ++i) {
      if (ccache->set(folly::Random::rand64(), &val) ==
          CCacheReturn::NOTFOUND) {
        ++nAdded;
      }
    }

    int cbCalled = 0;
    ASSERT_TRUE(ccache->purge(
        [&](const typename CC::Key&, const typename CC::Value* const) {
          ++cbCalled;
          return CC::PurgeFilterResult::SKIP;
        }));
    ASSERT_EQ(nAdded - removeCb.numCalls, cbCalled);
  }
}

template <typename CC>
static void testEvict(bool allowPromotions) {
  constexpr int nbEntries = CC::BucketDescriptor::kEntriesPerBucket;
  RemoveCbWrapper<CC> removeCb;
  ReplaceCbWrapper<CC> replaceCb;

  /** It is important to have only one bucket here. */
  TestSetup<CC> setup(
      1, allowPromotions, removeCb.getCallable(), replaceCb.getCallable());
  auto ccache = setup.getCache();
  typename CC::Value val;

  /** Insert nb_entries elements to fill all the buckets. */
  for (unsigned int i = 1; i <= nbEntries; ++i) {
    val = i;
    ASSERT_EQ(CCacheReturn::NOTFOUND, ccache->set(i, &val));
  }

  /** Add another item, this should evict the first one inserted */
  val = nbEntries + 1;
  ASSERT_EQ(CCacheReturn::NOTFOUND, ccache->set(nbEntries + 1, &val));

  /** Check that the callbacks were called properly */
  ASSERT_EQ(1, removeCb.numCalls);
  ASSERT_EQ(0, replaceCb.numCalls);
  ASSERT_TRUE(removeCb.key == 1);
  if (CC::kHasValues) {
    ASSERT_TRUE(removeCb.val == 1);
  }

  /** Check that the first item we inserted does not exist anymore and out is
   * left unchanged. */
  typename CC::Value out;
  ASSERT_EQ(CCacheReturn::NOTFOUND, ccache->get(1, &out));
  ASSERT_TRUE(out.isEmpty());

  /** Check that all the other entries are still there with the expected
   * value. */
  for (int i = 2; i <= nbEntries + 1; ++i) {
    ASSERT_EQ(CCacheReturn::FOUND, ccache->get(i, &out));
    ASSERT_TRUE(out == i);
  }
}

template <typename CC>
static void testCallbacks(bool allowPromotions) {
  /*
   * Starting the tests.
   */
  RemoveCbWrapper<CC> removeCb;
  ReplaceCbWrapper<CC> replaceCb;
  TestSetup<CC> setup(
      5000, allowPromotions, removeCb.getCallable(), replaceCb.getCallable());
  auto ccache = setup.getCache();

  unsigned int numExpectedRemoved = 0;
  unsigned int numExpectedReplaced = 0;

  for (int i = 1; i <= CC::BucketDescriptor::kEntriesPerBucket + 1; i++) {
    std::array<typename CC::Value, 2> values = {{i, i + 1}};

    /* First call expects none of the callbacks to be triggered */
    ASSERT_EQ(CCacheReturn::NOTFOUND, ccache->set(i, &values[0]));
    ASSERT_EQ(numExpectedRemoved, removeCb.numCalls);
    ASSERT_EQ(numExpectedReplaced, replaceCb.numCalls);

    /* Second call with the same key should trigger the replace callback. */
    ASSERT_EQ(CCacheReturn::FOUND, ccache->set(i, &values[1]));
    ++numExpectedReplaced;
    ASSERT_EQ(numExpectedRemoved, removeCb.numCalls);
    ASSERT_EQ(numExpectedReplaced, replaceCb.numCalls);
    ASSERT_EQ(replaceCb.key, i);
    if (CC::kHasValues) {
      ASSERT_EQ(values[0], replaceCb.oldVal);
      ASSERT_EQ(values[1], replaceCb.newVal);
    }
  }

  /* Delete key and expect remove callback to be called */
  ASSERT_EQ(CCacheReturn::FOUND, ccache->del(2, nullptr));
  ++numExpectedRemoved;
  ASSERT_EQ(numExpectedRemoved, removeCb.numCalls);
  ASSERT_EQ(numExpectedReplaced, replaceCb.numCalls);
  ASSERT_TRUE(removeCb.key == 2);
  if (CC::kHasValues) {
    ASSERT_TRUE(removeCb.val == 3);
  }
}

template <typename CC>
static void testPromotionMode(bool allowPromotions) {
  constexpr int nbEntries = CC::BucketDescriptor::kEntriesPerBucket;

  RemoveCbWrapper<CC> removeCb;
  TestSetup<CC> setup(1, allowPromotions, removeCb.getCallable());
  auto ccache = setup.getCache();

  typename CC::Value val;

  /** Insert nbEntries elements to fill the bucket. */
  for (unsigned int i = 1; i <= nbEntries; ++i) {
    val = i;
    ASSERT_EQ(CCacheReturn::NOTFOUND, ccache->set(i, &val));
  }

  typename CC::Key firstKey(1);

  /** Get first entry to give it opportunity to get promoted */
  ASSERT_EQ(CCacheReturn::FOUND, ccache->get(firstKey, &val));

  /** Add another item. This must evict an entry */
  val = nbEntries + 1;
  ASSERT_EQ(CCacheReturn::NOTFOUND, ccache->set(nbEntries + 1, &val));

  /** Check that an entry was removed from ccache*/
  ASSERT_EQ(1, removeCb.numCalls);

  /** Test the correctness of promotion mode */
  if (allowPromotions) {
    ASSERT_NE(firstKey, removeCb.key);
  } else {
    ASSERT_EQ(firstKey, removeCb.key);
  }
}

template <typename CC>
static void testTailHits(bool allowPromotions) {
  constexpr int numEntries = CC::BucketDescriptor::kEntriesPerBucket;
  RemoveCbWrapper<CC> removeCb;
  TestSetup<CC> setup(1, allowPromotions, removeCb.getCallable());
  auto ccache = setup.getCache();

  typename CC::Value val;
  typename CC::Value out;

  /** fill the bucket */
  for (unsigned int i = 1; i <= numEntries; i++) {
    val = i;
    ASSERT_EQ(CCacheReturn::NOTFOUND, ccache->set(i, &val));
  }

  /** access in the same order */
  for (unsigned int i = 1; i <= numEntries; i++) {
    ASSERT_EQ(CCacheReturn::FOUND,
              ccache->get(i, &out, nullptr, allowPromotions));
  }
  auto expectedTailHits = allowPromotions ? numEntries : 1;
  EXPECT_EQ(expectedTailHits, ccache->getStats().tailHits);

  /** access in reverse order */
  for (unsigned int i = numEntries; i > 0; i--) {
    ASSERT_EQ(CCacheReturn::FOUND,
              ccache->get(i, &out, nullptr, allowPromotions));
  }
  expectedTailHits += 1;
  EXPECT_EQ(expectedTailHits, ccache->getStats().tailHits);
}

/****************************************************************************/
/** Main testing functions */

/**
 * Util function to call all tests common to both modes (default mode and
 * promotions disabled mode)
 * @param   allowPromotions    Whether the tests run with promotions enabled
 *                             (default) or disabled.
 */
template <typename CC>
static void CompactCacheRunBasicTestsUtil(bool allowPromotions) {
  // Test general correctness of Get in all cases
  testGet<CC>(allowPromotions, true /* shouldPromote */);
  testGet<CC>(allowPromotions, false /*shouldPromote */);

  // Test correctness of shouldPromote param of Get
  testGetShouldPromoteParam<CC>(allowPromotions, true /* shouldPromote */);
  testGetShouldPromoteParam<CC>(allowPromotions, false /* shouldPromote */);

  testSet<CC>(allowPromotions);
  testDel<CC>(allowPromotions);
  testResizeSmaller<CC>(allowPromotions);
  testResizeLarger<CC>(allowPromotions);
  testRehashSmaller<CC>(allowPromotions);
  testRehashLarger<CC>(allowPromotions);
  testEvict<CC>(allowPromotions);
  testCallbacks<CC>(allowPromotions);
  testPurgeCallback<CC>(allowPromotions);
  testPromotionMode<CC>(allowPromotions);
  testTailHits<CC>(allowPromotions);
}

/**
 * Call all tests common to all types of compact caches.
 */
template <typename CC>
static void CompactCacheRunBasicTests() {
  // Run common tests in Default mode
  CompactCacheRunBasicTestsUtil<CC>(true /* allowPromotions */);

  // Run common tests in Promotions Disabled Mode
  CompactCacheRunBasicTestsUtil<CC>(false /* allowPromotions */);
}
} // namespace tests
} // namespace cachelib
} // namespace facebook
