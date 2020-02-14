#include "cachelib/shm/PosixShmSegment.h"
#include "cachelib/shm/Shm.h"
#include "cachelib/shm/ShmCommon.h"
#include "cachelib/shm/SysVShmSegment.h"
#include "cachelib/shm/tests/common.h"

using facebook::cachelib::detail::getPageAlignedSize;
using facebook::cachelib::detail::getPageSizeInSMap;
using facebook::cachelib::detail::isPageAlignedSize;

namespace facebook {
namespace cachelib {
namespace tests {

void ShmTest::testPageSize(PageSizeT p, bool posix) {
  ShmSegmentOpts opts{p};
  size_t size = getPageAlignedSize(4096, p);
  ASSERT_TRUE(isPageAlignedSize(size, p));

  // create with unaligned size
  ASSERT_NO_THROW({
    ShmSegment s(ShmNew, segmentName, size, posix, opts);
    ASSERT_TRUE(s.mapAddress(nullptr));
    ASSERT_EQ(p, getPageSizeInSMap(s.getCurrentMapping().addr));
  });

  ASSERT_NO_THROW({
    ShmSegment s2(ShmAttach, segmentName, posix, opts);
    ASSERT_TRUE(s2.mapAddress(nullptr));
    ASSERT_EQ(p, getPageSizeInSMap(s2.getCurrentMapping().addr));
  });
}

// The following tests will fail on sandcastle. THP requires sysctls to be set
// up as root and even after that, the posix api support for THP is not
// complete yet. See https://fburl.com/f0umrcwq . We will re-enable these
// tests on sandcastle when these get fixed.

TEST_F(ShmTestPosix, PageSizesNormal) { testPageSize(PageSizeT::NORMAL, true); }

TEST_F(ShmTestPosix, PageSizesTwoMB) { testPageSize(PageSizeT::TWO_MB, true); }

TEST_F(ShmTestSysV, PageSizesNormal) { testPageSize(PageSizeT::NORMAL, false); }

TEST_F(ShmTestSysV, PageSizesTwoMB) { testPageSize(PageSizeT::TWO_MB, false); }

} // namespace tests
} // namespace cachelib
} // namespace facebook
