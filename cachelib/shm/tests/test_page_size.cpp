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

#include "cachelib/shm/Shm.h"
#include "cachelib/shm/ShmCommon.h"
#include "cachelib/shm/tests/common.h"

namespace facebook {
namespace cachelib {
namespace tests {

void ShmTest::testPageSize(size_t p, bool posix) {
  ShmSegmentOpts opts{PageSize(p)};
  const auto& ps = opts.pageSize;
  size_t size = ps.getPageAlignedSize(4096);
  ASSERT_TRUE(ps.isPageAlignedSize(size));

  // create with unaligned size
  ASSERT_NO_THROW({
    ShmSegment s(ShmNew, segmentName, size, posix, opts);
    ASSERT_TRUE(s.mapAddress(nullptr));
    ASSERT_EQ(ps.getPageSize(),
              ps.getPageSizeInSMap(s.getCurrentMapping().addr));
  });

  ASSERT_NO_THROW({
    ShmSegment s2(ShmAttach, segmentName, posix, opts);
    ASSERT_TRUE(s2.mapAddress(nullptr));
    ASSERT_EQ(ps.getPageSize(),
              ps.getPageSizeInSMap(s2.getCurrentMapping().addr));
  });
}

// The following tests will fail on sandcastle. THP requires sysctls to be set
// up as root and even after that, the posix api support for THP is not
// complete yet. See https://fburl.com/f0umrcwq . We will re-enable these
// tests on sandcastle when these get fixed.

TEST_F(ShmTestPosix, PageSizesNormal) {
  testPageSize(PageSize::kNormalPageSize, true);
}

TEST_F(ShmTestPosix, PageSizesTwoMB) {
  testPageSize(PageSize::kHugePageSize2MB, true);
}

TEST_F(ShmTestSysV, PageSizesNormal) {
  testPageSize(PageSize::kNormalPageSize, false);
}

TEST_F(ShmTestSysV, PageSizesTwoMB) {
  testPageSize(PageSize::kHugePageSize2MB, false);
}

} // namespace tests
} // namespace cachelib
} // namespace facebook
