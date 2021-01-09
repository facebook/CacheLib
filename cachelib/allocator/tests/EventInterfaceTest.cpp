#include "cachelib/allocator/tests/EventInterfaceTest.h"

#include "cachelib/allocator/tests/TestBase.h"

namespace facebook {
namespace cachelib {
namespace tests {

TYPED_TEST_CASE(EventInterfaceTest, AllocatorTypes);

// Test event recording during allocating and inserting items
TYPED_TEST(EventInterfaceTest, AllocateAndInsertEvents) {
  this->testAllocateAndInsertEvents();
}

// Test event recording during find API calls.
TYPED_TEST(EventInterfaceTest, FindEvents) { this->testFindEvents(); }

// Test event recording during remove API calls.
TYPED_TEST(EventInterfaceTest, RemoveEvents) { this->testRemoveEvents(); }

} // end of namespace tests
} // end of namespace cachelib
} // end of namespace facebook
