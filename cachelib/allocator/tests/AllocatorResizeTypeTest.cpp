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

#include "cachelib/allocator/tests/AllocatorResizeTest.h"
#include "cachelib/allocator/tests/TestBase.h"

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
