#include "cachelib/shm/PosixShmSegment.h"
#include "cachelib/shm/Shm.h"
#include "cachelib/shm/ShmCommon.h"
#include "cachelib/shm/SysVShmSegment.h"
#include "cachelib/shm/tests/common.h"

using namespace facebook::cachelib::tests;

using facebook::cachelib::detail::getPageSize;
using facebook::cachelib::detail::getPageSizeInSMap;
using facebook::cachelib::detail::isPageAlignedSize;

void ShmTest::testCreateAttach(bool posix) {
  const unsigned char magicVal = 'd';
  {
    // create with 0 size should round up to page size
    ShmSegment s(ShmNew, segmentName, 0, posix);
    ASSERT_EQ(getPageSize(), s.getSize());
    s.markForRemoval();
  }

  {
    // create with unaligned size
    ASSERT_TRUE(isPageAlignedSize(shmSize));
    ShmSegment s(ShmNew, segmentName, shmSize + 500, posix);
    ASSERT_EQ(shmSize + getPageSize(), s.getSize());
    s.markForRemoval();
  }
  auto addr = getNewUnmappedAddr();

  {
    ShmSegment s(ShmNew, segmentName, shmSize, posix);
    ASSERT_EQ(s.getSize(), shmSize);
    ASSERT_FALSE(s.isMapped());
    ASSERT_TRUE(s.mapAddress(addr));

    ASSERT_EQ(PageSizeT::NORMAL, getPageSizeInSMap(addr));

    ASSERT_TRUE(s.isMapped());
    checkMemory(addr, s.getSize(), 0);
    writeToMemory(addr, s.getSize(), magicVal);
    ASSERT_THROW(ShmSegment(ShmNew, segmentName, shmSize, posix),
                 std::system_error);
    const auto m = s.getCurrentMapping();
    ASSERT_EQ(m.size, shmSize);
  }

  ASSERT_NO_THROW({
    ShmSegment s2(ShmAttach, segmentName, posix);
    ASSERT_EQ(s2.getSize(), shmSize);
    ASSERT_TRUE(s2.mapAddress(addr));
    checkMemory(addr, s2.getSize(), magicVal);
    s2.detachCurrentMapping();
    ASSERT_FALSE(s2.isMapped());
  });
}

TEST_F(ShmTestPosix, CreateAttach) { testCreateAttach(true); }

TEST_F(ShmTestSysV, CreateAttach) { testCreateAttach(false); }

void ShmTest::testAttachReadOnly(bool posix) {
  unsigned char magicVal = 'd';
  ShmSegmentOpts ropts{PageSizeT::NORMAL, true /* read Only */};
  ShmSegmentOpts rwopts{PageSizeT::NORMAL, false /* read Only */};

  {
    // attaching to something that does not exist should fail in read only
    // mode.
    ASSERT_TRUE(isPageAlignedSize(shmSize));
    ASSERT_THROW(ShmSegment(ShmAttach, segmentName, posix, ropts),
                 std::system_error);
  }

  // create a new segment
  {
    ShmSegment s(ShmNew, segmentName, shmSize, posix, rwopts);
    ASSERT_EQ(s.getSize(), shmSize);
    ASSERT_TRUE(s.mapAddress(nullptr));
    ASSERT_TRUE(s.isMapped());
    void* addr = s.getCurrentMapping().addr;
    writeToMemory(addr, s.getSize(), magicVal);
    checkMemory(addr, s.getSize(), magicVal);
  }

  ASSERT_NO_THROW({
    ShmSegment s(ShmAttach, segmentName, posix, rwopts);
    ASSERT_EQ(s.getSize(), shmSize);
    ASSERT_TRUE(s.mapAddress(nullptr));
    void* addr = s.getCurrentMapping().addr;
    checkMemory(addr, s.getSize(), magicVal);
    magicVal = 'z';
    writeToMemory(addr, s.getSize(), magicVal);
    s.detachCurrentMapping();
    ASSERT_FALSE(s.isMapped());
  });

  // reading in read only mode should work fine. while another one is
  // attached.
  ASSERT_NO_THROW({
    ShmSegment s(ShmAttach, segmentName, posix, ropts);
    ShmSegment s2(ShmAttach, segmentName, posix, rwopts);
    ASSERT_EQ(s.getSize(), shmSize);
    ASSERT_TRUE(s.mapAddress(nullptr));
    void* addr = s.getCurrentMapping().addr;
    checkMemory(addr, s.getSize(), magicVal);

    ASSERT_TRUE(s2.mapAddress(nullptr));

    // write and the other mapping should observe this.
    magicVal = 'h';
    writeToMemory(s2.getCurrentMapping().addr, s.getSize(), magicVal);
    checkMemory(addr, s.getSize(), magicVal);

    s.detachCurrentMapping();
    s2.detachCurrentMapping();
    ASSERT_FALSE(s.isMapped());
    ASSERT_FALSE(s2.isMapped());
  });

  // writing to a read only mapping should abort and mapping should be
  // detached. segment should be present after it.
  ASSERT_DEATH(
      {
        ShmSegment s(ShmAttach, segmentName, posix, ropts);
        ASSERT_EQ(s.getSize(), shmSize);
        ASSERT_TRUE(s.mapAddress(nullptr));
        void* addr = s.getCurrentMapping().addr;
        checkMemory(addr, s.getSize(), magicVal);
        // writing should abort since we are ronly mapped at this address.
        writeToMemory(addr, s.getSize(), magicVal);
        s.detachCurrentMapping();
        ASSERT_FALSE(s.isMapped());
      },
      ".*");

  ASSERT_NO_THROW(ShmSegment s(ShmAttach, segmentName, posix, ropts));
}

TEST_F(ShmTestPosix, AttachReadOnlyDeathTest) { testAttachReadOnly(true); }

TEST_F(ShmTestSysV, AttachReadOnlyDeathTest) { testAttachReadOnly(false); }

void ShmTest::testMapping(bool posix) {
  const unsigned char magicVal = 'z';
  auto addr = getNewUnmappedAddr();
  { // create a segment
    ShmSegment s(ShmNew, segmentName, shmSize, posix);
    ASSERT_TRUE(s.mapAddress(addr));
    ASSERT_TRUE(s.isMapped());
    // creating another mapping should fail
    ASSERT_FALSE(s.mapAddress(getNewUnmappedAddr()));
    const auto m = s.getCurrentMapping();
    ASSERT_EQ(m.addr, addr);
    ASSERT_EQ(m.size, shmSize);
    writeToMemory(m.addr, m.size, magicVal);
    checkMemory(m.addr, m.size, magicVal);
  }

  // map with nullptr
  {
    ShmSegment s(ShmAttach, segmentName, posix);
    ASSERT_TRUE(s.mapAddress(nullptr));
    ASSERT_TRUE(s.isMapped());
    const auto m = s.getCurrentMapping();
    ASSERT_NE(m.addr, nullptr);
    // previous values still exist on unmap and map again
    checkMemory(m.addr, m.size, magicVal);
    s.detachCurrentMapping();
    ASSERT_FALSE(s.isMapped());
  }

  {
    ShmSegment s(ShmAttach, segmentName, posix);
    // can map again.
    ASSERT_TRUE(s.mapAddress(addr));
    ASSERT_TRUE(s.isMapped());
    const auto m = s.getCurrentMapping();
    // previous values still exist on unmap and map again
    checkMemory(m.addr, m.size, magicVal);
    s.detachCurrentMapping();
    ASSERT_FALSE(s.isMapped());

    // mapping again to the same address should work fine.
    ASSERT_TRUE(s.mapAddress(addr));
    ASSERT_TRUE(s.isMapped());
    const auto m1 = s.getCurrentMapping();
    // previous values still exist on unmap and map again
    checkMemory(m1.addr, m1.size, magicVal);
    s.detachCurrentMapping();
    ASSERT_FALSE(s.isMapped());

    // Map to an used address will replace it
    void* ret = mmap(
        addr, getPageSize(), PROT_NONE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
    ASSERT_NE(ret, MAP_FAILED);
    ASSERT_NO_THROW(s.mapAddress(addr));
    ASSERT_NO_THROW(s.mapAddress(nullptr));
    s.detachCurrentMapping();
    ASSERT_FALSE(s.isMapped());

    // creating another mapping at different address should succeed
    ASSERT_TRUE(s.mapAddress(getNewUnmappedAddr()));
    ASSERT_TRUE(s.isMapped());
    const auto m2 = s.getCurrentMapping();
    checkMemory(m2.addr, m2.size, magicVal);
    // detach the mapping and remove.
    s.detachCurrentMapping();
    s.markForRemoval();
    // mapping now should fail.
    ASSERT_FALSE(s.mapAddress(addr));
  }
}

TEST_F(ShmTestPosix, Mapping) { testMapping(true); }

TEST_F(ShmTestSysV, Mapping) { testMapping(false); }

void ShmTest::testLifetime(bool posix) {
  const size_t safeSize = getRandomSize();
  const char magicVal = 'x';
  ASSERT_NO_THROW({
    {
      // create a segment, map and write to it. Remove the segment, detach
      // from address space. this should not actually delete the segment and
      // we should be able to map it back as long as the object is within the
      // scope.
      ShmSegment s(ShmNew, segmentName, safeSize, posix);
      s.mapAddress(nullptr);
      auto m = s.getCurrentMapping();
      writeToMemory(m.addr, m.size, magicVal);
      s.markForRemoval();
      ASSERT_TRUE(s.isMarkedForRemoval());
      s.detachCurrentMapping();
      s.mapAddress(nullptr);
      auto newM = s.getCurrentMapping();
      checkMemory(newM.addr, newM.size, magicVal);
      ASSERT_TRUE(s.isMarkedForRemoval());
    }
    {
      // should be able to create  a new segment with same segmentName after the
      // previous scope exit destroys the segment.
      const size_t newSize = getRandomSize();
      ShmSegment s(ShmNew, segmentName, newSize, posix);
      s.mapAddress(nullptr);
      auto m = s.getCurrentMapping();
      checkMemory(m.addr, m.size, 0);
      writeToMemory(m.addr, m.size, magicVal);
    }
    // attaching should have the same behavior.
    ShmSegment s(ShmAttach, segmentName, posix);
    s.mapAddress(nullptr);
    s.markForRemoval();
    ASSERT_TRUE(s.isMarkedForRemoval());
    s.detachCurrentMapping();
    s.mapAddress(nullptr);
    auto m = s.getCurrentMapping();
    checkMemory(m.addr, m.size, magicVal);
  });
}

TEST_F(ShmTestPosix, Lifetime) { testLifetime(true); }
TEST_F(ShmTestSysV, Lifetime) { testLifetime(false); }
