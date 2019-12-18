#include "AllocatorResizeTest.h"
#include "TestBase.h"

namespace facebook {
namespace cachelib {
namespace tests {

TYPED_TEST_CASE(AllocatorResizeTest, AllocatorTypes);

TYPED_TEST(AllocatorResizeTest, ShrinkWithFreeMem) {
  this->testShrinkWithFreeMem();
}

TYPED_TEST(AllocatorResizeTest, GrowWithFreeMem) {
  this->testGrowWithFreeMem();
}

TYPED_TEST(AllocatorResizeTest, BasicResize) { this->testBasicResize(); }
TYPED_TEST(AllocatorResizeTest, ResizeWithFreeSlabs) {
  this->testResizingWithFreeSlabs();
}

TYPED_TEST(AllocatorResizeTest, BasicResizeWithSharedMem) {
  this->testBasicResizeWithSharedMem();
}

TYPED_TEST(AllocatorResizeTest, ResizeAndMemMonitorTests) {
  this->testResizeMemMonitor();
  this->testMemMonitorNoResize();
  this->testMemMonitorCompactCache();
  this->testMemMonitorEmptySlabs();
  this->testMemoryAdviseWithSaveRestore();
  this->testMemoryMonitorPerIterationAdviseReclaim();
}

TYPED_TEST(AllocatorResizeTest, ShrinkGrowthAdviseRaceCondition) {
  this->testShrinkGrowthAdviseRaceCondition();
}

} // end of namespace tests
} // end of namespace cachelib
} // end of namespace facebook
