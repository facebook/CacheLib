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

#include <folly/small_vector.h>

#include "cachelib/allocator/tests/TestBase.h"
#include "cachelib/datatype/FixedSizeArray.h"
#include "cachelib/datatype/tests/DataTypeTest.h"

namespace facebook {
namespace cachelib {
namespace tests {
template <typename AllocatorT>
class FixedSizeArrayTest : public ::testing::Test {
 public:
  using Array = FixedSizeArray<int, AllocatorT>;

  void testBasic() {
    auto cache = DataTypeTest::createCache<AllocatorT>();
    const auto pid = cache->getPoolId(DataTypeTest::kDefaultPool);

    // Allocate a new array and populate some data and insert
    {
      auto array = Array{
          cache->allocate(pid, "array",
                          Array::computeStorageSize(100 /* numElements */)),
          100 /* numElements */};
      ASSERT_FALSE(array.isNullWriteHandle());
      for (uint32_t i = 0; i < array.size(); ++i) {
        array[i] = i;
      }
      cache->insertOrReplace(array.viewWriteHandle());
    }

    // Look up for this array and verify data is the same
    {
      auto array =
          Array::fromWriteHandle(cache->findImpl("array", AccessMode::kRead));
      ASSERT_FALSE(array.isNullWriteHandle());
      for (uint32_t i = 0; i < array.size(); ++i) {
        ASSERT_EQ(array[i], i);
      }
    }

    // Test out of range access
    {
      auto array =
          Array::fromWriteHandle(cache->findImpl("array", AccessMode::kRead));
      ASSERT_FALSE(array.isNullWriteHandle());

      // operator[] does not do bounds cheecking
      ASSERT_NO_THROW(array[array.size()]);

      // at() throws if out of bound
      ASSERT_THROW(array.at(array.size()), std::out_of_range);

      EXPECT_NE(std::move(array).resetToWriteHandle(), nullptr);
      EXPECT_TRUE(array.isNullWriteHandle());
    }
  }

  void testIterator() {
    auto cache = DataTypeTest::createCache<AllocatorT>();
    const auto pid = cache->getPoolId(DataTypeTest::kDefaultPool);

    auto array =
        Array{cache->allocate(pid, "array",
                              Array::computeStorageSize(100 /* numElements */)),
              100 /* numElements */};
    ASSERT_FALSE(array.isNullWriteHandle());

    // Use iterator to populate the array
    {
      int i = 0;
      for (auto& element : array) {
        element = i++;
      }
    }

    // Use const iterator to read the array
    {
      int i = 0;
      for (const auto& element : array) {
        ASSERT_EQ(element, i++);
      }
    }

    // Test moving out of bounds
    {
      auto itr = array.begin();
      auto end = array.end();
      while (itr != end) {
        ++itr;
      }
      ASSERT_THROW(++itr, std::out_of_range);
    }
    {
      auto itr = array.cbegin();
      auto end = array.cend();
      while (itr != end) {
        ++itr;
      }
      ASSERT_THROW(++itr, std::out_of_range);
    }
  }

  void testMovingConstruction() {
    auto cache = DataTypeTest::createCache<AllocatorT>();
    const auto pid = cache->getPoolId(DataTypeTest::kDefaultPool);

    auto array1 =
        Array{cache->allocate(pid, "array1",
                              Array::computeStorageSize(100 /* numElements */)),
              100 /* numElements */};
    ASSERT_FALSE(array1.isNullWriteHandle());

    auto array2 =
        Array{cache->allocate(pid, "array2",
                              Array::computeStorageSize(100 /* numElements */)),
              100 /* numElements */};
    ASSERT_FALSE(array2.isNullWriteHandle());

    ASSERT_EQ("array1", array1.viewWriteHandle().getKey());
    ASSERT_EQ("array2", array2.viewWriteHandle().getKey());

    // Move array2 into array1
    array1 = std::move(array2);
    ASSERT_TRUE(array2.isNullWriteHandle());
    ASSERT_EQ("array2", array1.viewWriteHandle().getKey());
  }

  void testEquality() {
    auto cache = DataTypeTest::createCache<AllocatorT>();
    const auto pid = cache->getPoolId(DataTypeTest::kDefaultPool);

    auto array1 =
        Array{cache->allocate(pid, "array1",
                              Array::computeStorageSize(100 /* numElements */)),
              100 /* numElements */};
    ASSERT_FALSE(array1.isNullWriteHandle());
    for (auto& element : array1) {
      element = 0;
    }

    auto array2 =
        Array{cache->allocate(pid, "array2",
                              Array::computeStorageSize(100 /* numElements */)),
              100 /* numElements */};
    ASSERT_FALSE(array2.isNullWriteHandle());
    for (auto& element : array2) {
      element = 1;
    }

    auto array3 =
        Array{cache->allocate(pid, "array3",
                              Array::computeStorageSize(100 /* numElements */)),
              100 /* numElements */};
    ASSERT_FALSE(array3.isNullWriteHandle());
    for (auto& element : array3) {
      element = 0;
    }
    ASSERT_NE(array1, array2);
    ASSERT_EQ(array1, array3);

    std::array<int, 100> anotherArray1;
    for (auto& element : anotherArray1) {
      element = 0;
    }
    std::array<int, 100> anotherArray2;
    for (auto& element : anotherArray2) {
      element = 1;
    }
    std::array<int, 99> anotherArray3;
    for (auto& element : anotherArray3) {
      element = 0;
    }
    ASSERT_TRUE(isEqual(array1, anotherArray1));
    ASSERT_FALSE(isEqual(array1, anotherArray2));
    ASSERT_FALSE(isEqual(array1, anotherArray3));
  }

  void testStdContainers() {
    auto cache = DataTypeTest::createCache<AllocatorT>();
    const auto pid = cache->getPoolId(DataTypeTest::kDefaultPool);

    // Test copying to std containers
    {
      auto array = Array{
          cache->allocate(pid, "array",
                          Array::computeStorageSize(100 /* numElements */)),
          100 /* numElements */};

      // Insert into a vector by back_insert_iterator
      std::vector<int> anotherVector;
      array.copyTo(std::back_inserter(anotherVector));
      ASSERT_TRUE(isEqual(array, anotherVector));

      // Test by std::copy
      std::vector<int> yetAnotherVector(100);
      array.copyTo(std::begin(yetAnotherVector));
      ASSERT_TRUE(isEqual(array, yetAnotherVector));

      // Insert into an array by forward iterator
      std::array<int, 100> anotherArray;
      array.copyTo(std::begin(anotherArray));
      ASSERT_TRUE(isEqual(array, anotherArray));

      // Insert into a set by insert_iterator
      const auto uniqueElements = 10;
      for (uint32_t i = 0; i < array.size(); ++i) {
        array[i] = i % uniqueElements;
      }
      std::set<int> dedupeSet;
      array.copyTo(std::inserter(dedupeSet, std::begin(dedupeSet)));
      ASSERT_EQ(uniqueElements, dedupeSet.size());
    }

    // Test copying from std containers
    {
      std::array<int, 100> anotherArray;
      for (size_t i = 0; i < anotherArray.size(); ++i) {
        anotherArray[i] = i;
      }

      auto array = Array{
          cache->allocate(pid, "array",
                          Array::computeStorageSize(100 /* numElements */)),
          100 /* numElements */};
      for (auto& element : array) {
        element = 0;
      }
      ASSERT_FALSE(isEqual(array, anotherArray));

      array.copyFrom(anotherArray.begin(), anotherArray.end());
      ASSERT_TRUE(isEqual(array, anotherArray));
    }
  }

  void testStdAlgorithms() {
    auto cache = DataTypeTest::createCache<AllocatorT>();
    const auto pid = cache->getPoolId(DataTypeTest::kDefaultPool);
    auto array =
        Array{cache->allocate(pid, "array",
                              Array::computeStorageSize(100 /* numElements */)),
              100 /* numElements */};

    std::generate(array.begin(), array.end(),
                  [n = 0]() mutable { return n++; });
    for (uint32_t i = 0; i < array.size(); ++i) {
      ASSERT_EQ(i, array[i]);
    }

    auto itr = std::find(array.begin(), array.end(), 5);
    ASSERT_NE(itr, array.end());
  }

  void testFollyContainers() {
    auto cache = DataTypeTest::createCache<AllocatorT>();
    const auto pid = cache->getPoolId(DataTypeTest::kDefaultPool);
    auto array =
        Array{cache->allocate(pid, "array",
                              Array::computeStorageSize(100 /* numElements */)),
              100 /* numElements */};
    std::generate(array.begin(), array.end(),
                  [n = 0]() mutable { return n++; });

    folly::small_vector<int> smallVec{array.begin(), array.end()};
    for (uint32_t i = 0; i < array.size(); ++i) {
      ASSERT_EQ(array[i], smallVec[i]);
    }
  }
};

TYPED_TEST_CASE(FixedSizeArrayTest, AllocatorTypes);
TYPED_TEST(FixedSizeArrayTest, Basic) { this->testBasic(); }
TYPED_TEST(FixedSizeArrayTest, Equality) { this->testEquality(); }
TYPED_TEST(FixedSizeArrayTest, Iterator) { this->testIterator(); }
TYPED_TEST(FixedSizeArrayTest, StdContainers) { this->testStdContainers(); }
TYPED_TEST(FixedSizeArrayTest, StdAlgorithms) { this->testStdAlgorithms(); }
TYPED_TEST(FixedSizeArrayTest, testFollyContainers) {
  this->testFollyContainers();
}
} // namespace tests
} // namespace cachelib
} // namespace facebook
