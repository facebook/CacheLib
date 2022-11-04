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

#include "cachelib/allocator/Util.h"
#include "cachelib/allocator/tests/TestBase.h"
#include "cachelib/datatype/Buffer.h"
#include "cachelib/datatype/Map.h"
#include "cachelib/datatype/MapView.h"
#include "cachelib/datatype/tests/DataTypeTest.h"

namespace facebook {
namespace cachelib {
namespace tests {
template <typename AllocatorT>
class MapViewTest : public ::testing::Test {
 public:
  void testBasic() {
    auto cache = DataTypeTest::createCache<AllocatorT>();
    const auto pid = cache->getPoolId(DataTypeTest::kDefaultPool);

    using BasicMap = cachelib::Map<int, int, AllocatorT>;
    using BasicMapView = cachelib::MapView<int, int, AllocatorT>;

    auto map = BasicMap::create(*cache, pid, "my_map");
    ASSERT_TRUE(map.insert(123, 100));
    auto& parent = map.viewWriteHandle();
    auto allocs = cache->viewAsChainedAllocs(parent);

    BasicMapView mapView{*parent, allocs.getChain()};
    ASSERT_TRUE(mapView.find(123));
    ASSERT_FALSE(mapView.find(456));
    const int* val = mapView.find(123);
    ASSERT_EQ(*val, 100);
    ASSERT_EQ(mapView.size(), map.size());
    ASSERT_EQ(mapView.sizeInBytes(), map.sizeInBytes());
  }
  void testMapToMapView() {
    auto cache = DataTypeTest::createCache<AllocatorT>();
    const auto pid = cache->getPoolId(DataTypeTest::kDefaultPool);

    using BasicMap = cachelib::Map<int, int, AllocatorT>;
    using BasicMapView = cachelib::MapView<int, int, AllocatorT>;

    auto map = BasicMap::create(*cache, pid, "my_map");
    ASSERT_TRUE(map.insert(123, 100));

    BasicMapView mapView = map.toView();
    ASSERT_TRUE(mapView.find(123));
    ASSERT_FALSE(mapView.find(456));

    ASSERT_EQ(*mapView.find(123), 100);
    ASSERT_EQ(mapView.size(), map.size());
    ASSERT_EQ(mapView.sizeInBytes(), map.sizeInBytes());

    // mutate the map and create another MapView
    ASSERT_TRUE(map.insert(456, 200));
    BasicMapView mapView2 = map.toView();
    ASSERT_TRUE(mapView2.find(123));
    ASSERT_TRUE(mapView2.find(456));
    ASSERT_EQ(*mapView2.find(123), 100);
    ASSERT_EQ(*mapView2.find(456), 200);
    ASSERT_EQ(mapView2.size(), 2);
    ASSERT_EQ(mapView2.sizeInBytes(), map.sizeInBytes());
  }

  void testEmptyIterator() {
    auto cache = DataTypeTest::createCache<AllocatorT>();
    const auto pid = cache->getPoolId(DataTypeTest::kDefaultPool);

    using BasicMap = cachelib::Map<int, int, AllocatorT>;
    using BasicMapView = cachelib::MapView<int, int, AllocatorT>;

    auto map = BasicMap::create(*cache, pid, "my_map");
    auto& parent = map.viewWriteHandle();
    auto allocs = cache->viewAsChainedAllocs(parent);

    BasicMapView mapView{*parent, allocs.getChain()};
    ASSERT_EQ(mapView.begin(), mapView.end());

    auto itr = mapView.begin();
    ASSERT_THROW(++itr, std::out_of_range);
  }

  void testIterator() {
    auto cache = DataTypeTest::createCache<AllocatorT>();
    const auto pid = cache->getPoolId(DataTypeTest::kDefaultPool);

    using BasicMap = cachelib::Map<int, int, AllocatorT>;
    using BasicMapView = cachelib::MapView<int, int, AllocatorT>;

    auto map = BasicMap::create(*cache, pid, "my_map");
    std::vector<int> keys = {123, 456, 789};
    std::vector<int> values = {100, 200, 300};
    for (unsigned int i = 0; i < keys.size(); ++i) {
      ASSERT_TRUE(map.insert(keys.at(i), values.at(i)));
    }
    auto& parent = map.viewWriteHandle();
    auto allocs = cache->viewAsChainedAllocs(parent);

    BasicMapView mapView{*parent, allocs.getChain()};
    unsigned int i = 0;
    for (auto& kv : mapView) {
      ASSERT_EQ(kv.key, keys.at(i));
      ASSERT_EQ(kv.value, values.at(i));
      i++;
    }
    ASSERT_EQ(i, mapView.size());
  }

  void testConstructReadOnlyMap() {
    auto cache = DataTypeTest::createCache<AllocatorT>();
    const auto pid = cache->getPoolId(DataTypeTest::kDefaultPool);

    using BasicMap = cachelib::Map<int, int, AllocatorT>;
    using BasicReadOnlyMap = cachelib::ReadOnlyMap<int, int, AllocatorT>;

    auto map = BasicMap::create(*cache, pid, "my_map");
    std::vector<int> keys = {123, 456, 789};
    std::vector<int> values = {100, 200, 300};
    for (unsigned int i = 0; i < keys.size(); ++i) {
      ASSERT_TRUE(map.insert(keys.at(i), values.at(i)));
    }
    cache->insert(map.viewWriteHandle());
    {
      auto handle = cache->find("your_map");
      ASSERT_EQ(nullptr, handle);
      auto readOnlyMap =
          BasicReadOnlyMap::fromReadHandle(*cache, std::move(handle));
      ASSERT_TRUE(readOnlyMap.isNullReadHandle());
    }
    {
      auto handle = cache->find("my_map");
      ASSERT_NE(nullptr, handle);
      auto readOnlyMap =
          BasicReadOnlyMap::fromReadHandle(*cache, std::move(handle));
      ASSERT_FALSE(readOnlyMap.isNullReadHandle());

      unsigned int i = 0;
      for (auto& kv : readOnlyMap) {
        ASSERT_EQ(kv.key, keys.at(i));
        ASSERT_EQ(kv.value, values.at(i));
        i++;
      }
      ASSERT_EQ(i, readOnlyMap.size());

      auto readOnlyMap2 = std::move(readOnlyMap);
      ASSERT_FALSE(readOnlyMap2.isNullReadHandle());

      i = 0;
      for (auto& kv : readOnlyMap2) {
        ASSERT_EQ(kv.key, keys.at(i));
        ASSERT_EQ(kv.value, values.at(i));
        i++;
      }
      ASSERT_EQ(i, readOnlyMap.size());
    }
  }
};
TYPED_TEST_CASE(MapViewTest, AllocatorTypes);
TYPED_TEST(MapViewTest, Basic) { this->testBasic(); }
TYPED_TEST(MapViewTest, MapToMapView) { this->testMapToMapView(); }
TYPED_TEST(MapViewTest, EmptyIterator) { this->testEmptyIterator(); }
TYPED_TEST(MapViewTest, Iterator) { this->testIterator(); }
TYPED_TEST(MapViewTest, ConstructReadOnlyMap) {
  this->testConstructReadOnlyMap();
}
} // namespace tests
} // namespace cachelib
} // namespace facebook
