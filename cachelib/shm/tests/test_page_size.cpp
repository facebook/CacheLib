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

#include <folly/Conv.h>
#include <folly/FileUtil.h>
#include <folly/String.h>

#include <cstdlib>

#include "cachelib/shm/Shm.h"
#include "cachelib/shm/ShmCommon.h"
#include "cachelib/shm/tests/common.h"

namespace facebook {
namespace cachelib {
namespace tests {

namespace {
// Cachelib requires an explicit hugetlbfs mount for POSIX huge pages. CI hosts
// (sandcastle) have neither a reserved huge-page pool nor a mounted hugetlbfs
// and can't create either (no root), so the POSIX huge-page end-to-end test
// only runs on a host provisioned out-of-band with a pool + hugetlbfs mount
// whose path is supplied here.
constexpr const char* kHugetlbfsMountEnv = "CACHELIB_TEST_HUGETLBFS_MOUNT";

std::string hugetlbfsMountFromEnv() {
  const char* dir = std::getenv(kHugetlbfsMountEnv);
  return dir != nullptr ? std::string{dir} : std::string{};
}

// Number of free huge pages of the given size, from sysfs.
size_t freeHugePages(size_t pageSize) {
  const auto path =
      fmt::format("/sys/kernel/mm/hugepages/hugepages-{}kB/free_hugepages",
                  pageSize / 1024);
  std::string content;
  if (!folly::readFile(path.c_str(), content)) {
    return 0;
  }
  return folly::tryTo<size_t>(folly::trimWhitespace(content)).value_or(0);
}

// Huge pages of pageSize can actually be allocated on this host: the kernel
// supports the size, there are free pages in the pool, and (for POSIX) a
// hugetlbfs mount was supplied via kHugetlbfsMountEnv. Gates the HugeTLB
// end-to-end tests so they skip on unprovisioned hosts instead of failing.
bool hugePagesUsable(size_t pageSize, bool posix) {
  if (!PageSize::supportedHugePageSizes().contains(pageSize) ||
      freeHugePages(pageSize) == 0) {
    return false;
  }
  return !posix || !hugetlbfsMountFromEnv().empty();
}
} // namespace

void ShmTest::testPageSize(size_t p, bool posix) {
  ShmSegmentOpts opts{PageSize(p)};
  const auto& ps = opts.pageSize;
  size_t size = ps.getPageAlignedSize(4096);
  ASSERT_TRUE(ps.isPageAlignedSize(size));
  // POSIX huge-page segments need the hugetlbfs mount passed to the segment.
  const std::string mountDir =
      (posix && ps.isHugePage()) ? hugetlbfsMountFromEnv() : std::string{};

  // create with unaligned size
  ASSERT_NO_THROW({
    ShmSegment s(ShmNew, segmentName, size, posix, opts, mountDir);
    ASSERT_TRUE(s.mapAddress(nullptr));
    ASSERT_EQ(ps.getPageSize(),
              ps.getPageSizeInSMap(s.getCurrentMapping().addr));
  });

  ASSERT_NO_THROW({
    ShmSegment s2(ShmAttach, segmentName, posix, opts, mountDir);
    ASSERT_TRUE(s2.mapAddress(nullptr));
    ASSERT_EQ(ps.getPageSize(),
              ps.getPageSizeInSMap(s2.getCurrentMapping().addr));
  });
}

TEST_F(ShmTestPosix, HugePageRequiresMount) {
  ShmSegmentOpts opts{PageSize(PageSize::kHugePageSize2MB)};
  const size_t size = opts.pageSize.getPageAlignedSize(4096);
  EXPECT_THROW(ShmSegment(ShmNew, segmentName, size, /*usePosix=*/true, opts,
                          /*hugePageMountDir=*/""),
               std::system_error);
}

// The HugeTLB end-to-end tests require a provisioned huge-page pool (kernel
// cmdline / sysctl) and, for POSIX, a hugetlbfs mount via kHugetlbfsMountEnv.
// They skip when those are absent (e.g. sandcastle) rather than fail.

TEST_F(ShmTestPosix, PageSizesNormal) {
  testPageSize(PageSize::kNormalPageSize, true);
}

TEST_F(ShmTestPosix, PageSizesTwoMB) {
  if (!hugePagesUsable(PageSize::kHugePageSize2MB, true)) {
    GTEST_SKIP() << "2MB HugeTLB pool / hugetlbfs mount not available";
  }
  testPageSize(PageSize::kHugePageSize2MB, true);
}

TEST_F(ShmTestSysV, PageSizesNormal) {
  testPageSize(PageSize::kNormalPageSize, false);
}

TEST_F(ShmTestSysV, PageSizesTwoMB) {
  if (!hugePagesUsable(PageSize::kHugePageSize2MB, false)) {
    GTEST_SKIP() << "2MB HugeTLB pool not available";
  }
  testPageSize(PageSize::kHugePageSize2MB, false);
}

} // namespace tests
} // namespace cachelib
} // namespace facebook
