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
    auto& parent = map.viewItemHandle();
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
    auto& parent = map.viewItemHandle();
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
    auto& parent = map.viewItemHandle();
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
};
TYPED_TEST_CASE(MapViewTest, AllocatorTypes);
TYPED_TEST(MapViewTest, Basic) { this->testBasic(); }
TYPED_TEST(MapViewTest, MapToMapView) { this->testMapToMapView(); }
TYPED_TEST(MapViewTest, EmptyIterator) { this->testEmptyIterator(); }
TYPED_TEST(MapViewTest, Iterator) { this->testIterator(); }
} // namespace tests
} // namespace cachelib
} // namespace facebook
