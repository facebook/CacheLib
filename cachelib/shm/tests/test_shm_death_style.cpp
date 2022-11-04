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

using facebook::cachelib::detail::isPageAlignedSize;

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

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  // Use thread-safe mode for all tests that use ASSERT_DEATH
  ::testing::GTEST_FLAG(death_test_style) = "threadsafe";
  return RUN_ALL_TESTS();
}
