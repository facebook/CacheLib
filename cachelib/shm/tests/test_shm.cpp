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

#include <folly/Random.h>

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

void ShmTest::testMappingAlignment(bool posix) {
  { // create a segment
    ShmSegment s(ShmNew, segmentName, shmSize, posix);

    // 0 alignment is wrong.
    ASSERT_FALSE(s.mapAddress(nullptr, 0));
    // alignment that is not a power of two is wrong.
    ASSERT_FALSE(
        s.mapAddress(nullptr, folly::Random::rand32(1 << 20, 1 << 21)));
    size_t alignment = 1ULL << (folly::Random::rand32(0, 22));
    ASSERT_TRUE(s.mapAddress(nullptr, alignment));
    ASSERT_TRUE(s.isMapped());
    auto m = s.getCurrentMapping();
    ASSERT_EQ(reinterpret_cast<uint64_t>(m.addr) & (alignment - 1), 0);
    ASSERT_EQ(m.size, shmSize);
    writeToMemory(m.addr, m.size, 'k');
  }
}

TEST_F(ShmTestPosix, MappingAlignment) { testMappingAlignment(true); }

TEST_F(ShmTestSysV, MappingAlignment) { testMappingAlignment(false); }

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
