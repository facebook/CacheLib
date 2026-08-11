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
#include <sys/mman.h>
#include <unistd.h>

#include "cachelib/shm/ShmCommon.h"

namespace facebook {
namespace cachelib {

TEST(HugePageHelpers, EffectivePageSize) {
  EXPECT_EQ(static_cast<size_t>(sysconf(_SC_PAGESIZE)),
            PageSize::systemPageSize());
  EXPECT_EQ(PageSize::systemPageSize(), PageSize().getPageSize());
  EXPECT_EQ(PageSize::kHugePageSize2MB,
            PageSize(PageSize::kHugePageSize2MB).getPageSize());
  EXPECT_EQ(PageSize::kHugePageSize1GB,
            PageSize(PageSize::kHugePageSize1GB).getPageSize());
}

TEST(HugePageHelpers, IsHugePage) {
  const auto base = static_cast<size_t>(sysconf(_SC_PAGESIZE));
  EXPECT_FALSE(PageSize(PageSize::kNormalPageSize).isHugePage());
  EXPECT_FALSE(PageSize(base).isHugePage());
  EXPECT_TRUE(PageSize(PageSize::kHugePageSize2MB).isHugePage());
  EXPECT_TRUE(PageSize(PageSize::kHugePageSize1GB).isHugePage());
}

TEST(HugePageHelpers, SizeToShift) {
  // Validate that the public flag helpers correctly encode log2(pageSize)
  // hugePageMmapFlags()  == MAP_HUGETLB | (log2(size) << MAP_HUGE_SHIFT)
  // hugePageShmgetFlags() == SHM_HUGETLB | (log2(size) << SHM_HUGE_SHIFT)
  auto validate = [](size_t sz, uint32_t expectedShift) {
    PageSize ps(sz);

    const int mmapFlags = ps.hugePageMmapFlags();
#if MAP_HUGETLB != 0
    EXPECT_NE(0, mmapFlags & MAP_HUGETLB);
    if (MAP_HUGE_SHIFT != 0) {
      EXPECT_EQ(static_cast<int>(expectedShift << MAP_HUGE_SHIFT),
                mmapFlags & (0x3f << MAP_HUGE_SHIFT));
    }
#else
    EXPECT_EQ(0, mmapFlags);
#endif

    const int shmFlags = ps.hugePageShmgetFlags();
#if SHM_HUGETLB != 0
    EXPECT_NE(0, shmFlags & SHM_HUGETLB);
    if (SHM_HUGE_SHIFT != 0) {
      EXPECT_EQ(static_cast<int>(expectedShift << SHM_HUGE_SHIFT),
                shmFlags & (0x3f << SHM_HUGE_SHIFT));
    }
#else
    EXPECT_EQ(0, shmFlags);
#endif
  };

  validate(PageSize::kHugePageSize2MB, 21u);
  validate(PageSize::kHugePageSize32MB, 25u);
  validate(PageSize::kHugePageSize512MB, 29u);
  validate(PageSize::kHugePageSize1GB, 30u);
  validate(PageSize::kHugePageSize64GB, 36u);
}

TEST(HugePageHelpers, FlagsZeroForNormalPageSize) {
  EXPECT_EQ(0, PageSize().hugePageMmapFlags());
  EXPECT_EQ(0, PageSize().hugePageShmgetFlags());
}

#if defined(MAP_HUGETLB) && MAP_HUGETLB != 0
TEST(HugePageHelpers, MmapFlagsEncodeSizeForHuge) {
  const int flags = PageSize(PageSize::kHugePageSize2MB).hugePageMmapFlags();
  EXPECT_NE(0, flags & MAP_HUGETLB);
  // the size field encodes log2(2 MiB) == 21
  EXPECT_EQ(static_cast<int>(21u << MAP_HUGE_SHIFT),
            flags & (0x3f << MAP_HUGE_SHIFT));
}
#endif

TEST(HugePageHelpers, SupportedSizesAreHugeAndPowerOfTwo) {
  // Pool need not be provisioned; the /sys entries exist whenever the kernel
  // supports the size. Whatever is reported must be a valid huge-page size.
  for (const size_t s : PageSize::supportedHugePageSizes()) {
    EXPECT_TRUE(PageSize(s).isHugePage()) << "not a huge page: " << s;
    EXPECT_EQ(0u, s & (s - 1)) << "not a power of two: " << s;
  }
}

} // namespace cachelib
} // namespace facebook
