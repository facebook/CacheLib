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

#include <gtest/gtest.h>

#include <vector>

#include "cachelib/experimental/objcache/Allocator.h"
#include "cachelib/experimental/objcache/tests/Common.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include "cachelib/experimental/objcache/tests/gen-cpp2/ThriftCustomAllocator_types.h"
#pragma GCC diagnostic pop

namespace facebook {
namespace cachelib {
namespace objcache {
namespace test {
TEST(ThriftCustomAllocator, Simple) {
  {
    UseSimpleCustomAllocator useSimple1{TestAllocatorResource{"custom_alloc"}};

    // Just an integer. We don't need allocator.
    useSimple1.m2() = 12345;

    // First create an entry with key "key-1" with default value.
    // Then, we std::move() "100" into value via move-assignment.
    //
    // However, move-assignment will FALL-BACK to copying, because
    // allocator "operator==" compares false.
    useSimple1.m().value()["key-1"] = "100";

    // Move-construct a new entry with key "key-2" and value "200".
    // However, move-construct will fail because allocator does not
    // compare equal. And we will force a copy instead.
    useSimple1.m().value().insert(
        std::make_pair(TestString{"key-2"}, TestString{"200"}));
  }

  {
    TestAllocatorResource myCustomAlloc{"another_custom_alloc"};
    UseSimpleCustomAllocator useSimple2{myCustomAlloc};

    // First create an entry with key "key-1" with default value.
    // Then, we std::move() "100" into value via move-assignment.
    //
    // Move works because allocator compares equal.
    useSimple2.m().value()["key-1"] = TestString{"100", myCustomAlloc};

    // Same here. Move works because allocator compares equal.
    useSimple2.m().value().insert(std::make_pair(
        TestString{"key-2", myCustomAlloc}, TestString{"200", myCustomAlloc}));
  }
}

TEST(ThriftCustomAllocator, Propagation) {
  auto createStr = [](const char* prefix) {
    return TestString{
        folly::sformat("{}_{}", prefix, "this is too long to be inlined")
            .c_str()};
  };

  TestAllocatorResource alloc1{"alloc-1"};
  TestAllocatorResource alloc2{"alloc-2"};
  EXPECT_FALSE(alloc1.isEqual(alloc2));

  UseSimpleCustomAllocator useSimple1{alloc1};
  EXPECT_EQ(0, alloc1.getNumAllocs());
  EXPECT_EQ(0, alloc2.getNumAllocs());

  useSimple1.m().value()[createStr("key-1")];
  // One construction for the map entry, and a second one for the key.
  EXPECT_EQ(2, alloc1.getNumAllocs());

  // Move assignment with incompatible allocator
  useSimple1.m().value()[createStr("key-1")] =
      TestString{createStr("100"), alloc2};
  // Verify copying occured. One construction for the value and a second
  // construction to copy-assign into the entry.
  EXPECT_EQ(3, alloc1.getNumAllocs());
  EXPECT_EQ(1, alloc2.getNumAllocs());

  // Move assignment with compatible allocator
  useSimple1.m().value()[createStr("key-2")] =
      TestString{createStr("200"), alloc1};
  // Verifying moving occured One construction for the map entry, second one
  // for the key, and a third one for the value which is moved in.
  EXPECT_EQ(6, alloc1.getNumAllocs());
  EXPECT_EQ(1, alloc2.getNumAllocs());

  // Move construction with incompatible allocator
  useSimple1.m().value().insert(
      std::make_pair(TestString{createStr("key-3"), alloc2},
                     TestString{createStr("300"), alloc2}));
  // Verify copying occured. Three constructions from alloc1, one is for
  // creating the map entry, and two allocations are for copying the key and
  // value. Two allocations from alloc2 are for creating the key-value pair.
  EXPECT_EQ(9, alloc1.getNumAllocs());
  EXPECT_EQ(3, alloc2.getNumAllocs());

  // Move construction with compatible allocator
  useSimple1.m().value().insert(
      std::make_pair(TestString{createStr("key-4"), alloc1},
                     TestString{createStr("400"), alloc1}));
  // Verify moving occured. Three constructions from alloc1, one is for
  // creating the map entry, and two allocations are for copying the key and
  // value. No allocations are made from alloc 2.
  EXPECT_EQ(12, alloc1.getNumAllocs());
  EXPECT_EQ(3, alloc2.getNumAllocs());

  for (auto itr : useSimple1.m().value()) {
    EXPECT_TRUE(
        itr.first.get_allocator().getAllocatorResource().isEqual(alloc1));
    EXPECT_TRUE(
        itr.second.get_allocator().getAllocatorResource().isEqual(alloc1));
  }

  UseSimpleCustomAllocator useSimple2{alloc2};
  useSimple2.m().value()[createStr("key-100")];
  EXPECT_EQ(5, alloc2.getNumAllocs());

  // Copy-assignment will not propagate the allocator
  useSimple1 = useSimple2;
  EXPECT_TRUE(
      useSimple1.get_allocator().getAllocatorResource().isEqual(alloc1));

  // Copy construction will propagate the allocator
  useSimple1.~UseSimpleCustomAllocator();
  new (&useSimple1) UseSimpleCustomAllocator(useSimple2);
  EXPECT_TRUE(
      useSimple1.get_allocator().getAllocatorResource().isEqual(alloc2));
}

TEST(ThriftCustomAllocator, Deserialization) {
  uint64_t numAllocs = 0;
  std::unique_ptr<folly::IOBuf> iobuf;
  {
    TestAllocatorResource alloc{"alloc"};
    UseSimpleCustomAllocator useSimple1{alloc};
    useSimple1.m2() = 12345;
    useSimple1.m().value()["key-1"] = "100";
    useSimple1.m().value().insert(
        std::make_pair(TestString{"key-2"}, TestString{"200"}));
    numAllocs = alloc.getNumAllocs();
    iobuf = Serializer::serializeToIOBuf(useSimple1);
  }

  {
    TestAllocatorResource alloc{"alloc"};
    UseSimpleCustomAllocator useSimple1{alloc};
    Deserializer deserializer{iobuf->data(), iobuf->data() + iobuf->length()};
    deserializer.deserialize(useSimple1);
    EXPECT_EQ(numAllocs, alloc.getNumAllocs());
  }
}

TEST(ThriftCustomAllocator, UnionSimple) {
  {
    UnionWithCustomAllocator someUnion;
    someUnion.m1_ref() = {{1, "value-1"}, {2, "value-2"}};
    EXPECT_EQ("value-1", someUnion.get_m1().find(1)->second);
    someUnion.m2_ref() = "some string";
    EXPECT_EQ("some string", someUnion.get_m2());
    someUnion.m3_ref() = 123;
    EXPECT_EQ(123, someUnion.get_m3());
  }

  {
    TestAllocatorResource alloc{"alloc"};
    UnionWithCustomAllocator someUnion;
    TestMap<int32_t, TestString> myMap{alloc};
    myMap[1] = "value-1 too long to be inlined";
    myMap[2] = "value-2 too long to be inlined";
    // 2 allocations for constructing the two map entries, and
    // 2 allocators for the two values
    EXPECT_EQ(4, alloc.getNumAllocs());
    // Copied over. So 4 more allocations were made
    someUnion.m1_ref() = myMap;
    EXPECT_EQ(myMap.get_allocator(), someUnion.get_m1().get_allocator());
    EXPECT_EQ(8, alloc.getNumAllocs());
  }

  {
    TestAllocatorResource alloc{"alloc"};
    UnionWithCustomAllocator someUnion;
    TestMap<int32_t, TestString> myMap{alloc};
    myMap[1] = "value-1 too long to be inlined";
    myMap[2] = "value-2 too long to be inlined";
    // 2 allocations for constructing the two map entries, and
    // 2 allocators for the two values
    EXPECT_EQ(4, alloc.getNumAllocs());
    // Moved over. So no extra allocations were made
    someUnion.m1_ref() = std::move(myMap);
    EXPECT_EQ(myMap.get_allocator(), someUnion.get_m1().get_allocator());
    EXPECT_EQ(4, alloc.getNumAllocs());
  }

  {
    TestAllocatorResource alloc{"alloc"};
    UnionWithCustomAllocator someUnion;
    TestMap<int32_t, TestString> myMap{alloc};
    myMap[1] = "value-1 too long to be inlined";
    myMap[2] = "value-2 too long to be inlined";
    // 2 allocations for constructing the two map entries, and
    // 2 allocators for the two values
    EXPECT_EQ(4, alloc.getNumAllocs());
    // Moved over. So no extra allocations were made
    someUnion.m1_ref() = std::move(myMap);
    EXPECT_EQ(myMap.get_allocator(), someUnion.get_m1().get_allocator());
    EXPECT_EQ(4, alloc.getNumAllocs());

    UnionWithCustomAllocator anotherUnion;
    // Copied over. So 4 more allocations are made
    anotherUnion = someUnion;
    EXPECT_EQ(myMap.get_allocator(), anotherUnion.get_m1().get_allocator());
    EXPECT_EQ(8, alloc.getNumAllocs());

    UnionWithCustomAllocator anotherUnion2;
    // Moved over. So no extra allocations were made
    anotherUnion2 = std::move(someUnion);
    EXPECT_EQ(myMap.get_allocator(), anotherUnion2.get_m1().get_allocator());
    EXPECT_EQ(8, alloc.getNumAllocs());
  }
}

TEST(ThriftCustomAllocator, TwoF14Maps) {
  {
    TestAllocatorResource myAlloc{"my alloc"};
    UseTwoF14Maps f14{
        TestF14TemplateAllocator<std::pair<const int32_t, int32_t>>{myAlloc}};
    EXPECT_EQ(0, myAlloc.getNumAllocs());
    auto allocs = myAlloc.getNumAllocs();

    f14.m1().value().insert(std::make_pair(123, 123));
    EXPECT_LT(allocs, myAlloc.getNumAllocs());
    allocs = myAlloc.getNumAllocs();

    f14.m2().value().insert(std::make_pair(123, 123.123));
    EXPECT_LT(allocs, myAlloc.getNumAllocs());
  }

  {
    TestAllocatorResource myAlloc{"my alloc 2"};
    UseTwoF14Maps f14{TestF14TemplateAllocator<char>{myAlloc}};
    EXPECT_EQ(0, myAlloc.getNumAllocs());
    auto allocs = myAlloc.getNumAllocs();

    f14.m1().value().insert(std::make_pair(123, 123));
    EXPECT_LT(allocs, myAlloc.getNumAllocs());
    allocs = myAlloc.getNumAllocs();

    f14.m2().value().insert(std::make_pair(123, 123.123));
    EXPECT_LT(allocs, myAlloc.getNumAllocs());
  }
}
} // namespace test
} // namespace objcache
} // namespace cachelib
} // namespace facebook
