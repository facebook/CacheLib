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
    useSimple1.m2_ref() = 12345;

    // First create an entry with key "key-1" with default value.
    // Then, we std::move() "100" into value via move-assignment.
    //
    // However, move-assignment will FALL-BACK to copying, because
    // allocator "operator==" compares false.
    useSimple1.m_ref().value()["key-1"] = "100";

    // Move-construct a new entry with key "key-2" and value "200".
    // However, move-construct will fail because allocator does not
    // compare equal. And we will force a copy instead.
    useSimple1.m_ref().value().insert(
        std::make_pair(TestString{"key-2"}, TestString{"200"}));
  }

  {
    TestAllocatorResource myCustomAlloc{"another_custom_alloc"};
    UseSimpleCustomAllocator useSimple2{myCustomAlloc};

    // First create an entry with key "key-1" with default value.
    // Then, we std::move() "100" into value via move-assignment.
    //
    // Move works because allocator compares equal.
    useSimple2.m_ref().value()["key-1"] = TestString{"100", myCustomAlloc};

    // Same here. Move works because allocator compares equal.
    useSimple2.m_ref().value().insert(std::make_pair(
        TestString{"key-2", myCustomAlloc}, TestString{"200", myCustomAlloc}));
  }
}

TEST(ThriftCustomAllocator, TwoF14Maps) {
  {
    TestAllocatorResource myAlloc{"my alloc"};
    UseTwoF14Maps f14{
        TestF14TemplateAllocator<std::pair<const int32_t, int32_t>>{myAlloc}};
    EXPECT_EQ(0, myAlloc.getNumAllocs());
    auto allocs = myAlloc.getNumAllocs();

    f14.m1_ref().value().insert(std::make_pair(123, 123));
    EXPECT_LT(allocs, myAlloc.getNumAllocs());
    allocs = myAlloc.getNumAllocs();

    f14.m2_ref().value().insert(std::make_pair(123, 123.123));
    EXPECT_LT(allocs, myAlloc.getNumAllocs());
  }

  {
    TestAllocatorResource myAlloc{"my alloc 2"};
    UseTwoF14Maps f14{TestF14TemplateAllocator<char>{myAlloc}};
    EXPECT_EQ(0, myAlloc.getNumAllocs());
    auto allocs = myAlloc.getNumAllocs();

    f14.m1_ref().value().insert(std::make_pair(123, 123));
    EXPECT_LT(allocs, myAlloc.getNumAllocs());
    allocs = myAlloc.getNumAllocs();

    f14.m2_ref().value().insert(std::make_pair(123, 123.123));
    EXPECT_LT(allocs, myAlloc.getNumAllocs());
  }
}
} // namespace test
} // namespace objcache
} // namespace cachelib
} // namespace facebook
