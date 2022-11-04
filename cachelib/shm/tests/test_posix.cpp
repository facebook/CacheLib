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

#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/time.h>

#include "cachelib/shm/PosixShmSegment.h"
#include "cachelib/shm/tests/common.h"

static const std::string namePrefix = "posix-shm-test-";

using namespace facebook::cachelib::tests;

using facebook::cachelib::PosixShmSegment;
using facebook::cachelib::ShmAttach;
using facebook::cachelib::ShmNew;

class PosixShmTest : public ShmTestBase {
 public:
  PosixShmTest() : segmentName(namePrefix + std::to_string((int)::getpid())) {}
  // use a different name for each test since they could be run in
  // parallel by fbmake runtests.
  const std::string segmentName{};

 protected:
  void SetUp() override {
    struct rlimit lim;
    getrlimit(RLIMIT_MEMLOCK, &lim);
    ASSERT_TRUE(lim.rlim_cur >= 2 * kMemMax);
  }
  void TearDown() override {
    // we use the same segment for all tests. always make sure that
    // the segment is unlinked so that we dont leak this across
    // tests.. since deleting segment can fail and throw an exception,
    // do this in a separate teardown and not in the destructor
    try {
      PosixShmSegment::removeByName(segmentName);
    } catch (const std::system_error& e) {
      if (e.code().value() != ENOENT) {
        throw;
      }
    }
  }
};

TEST_F(PosixShmTest, CreateInitialSize) {
  const size_t initialSize = getRandomSize();
  PosixShmSegment s(ShmNew, segmentName, initialSize);
  ASSERT_TRUE(s.isActive());
  ASSERT_EQ(s.getSize(), initialSize) << "Creating segment with size failed";
}

TEST_F(PosixShmTest, Attach) {
  const size_t initialSize = getRandomSize();
  {
    // create a new segment
    PosixShmSegment tmp(ShmNew, segmentName, initialSize);
  }

  PosixShmSegment s(ShmAttach, segmentName);
  ASSERT_TRUE(s.isActive());
  ASSERT_EQ(initialSize, s.getSize()) << "attach with initial size failed";
}

TEST_F(PosixShmTest, ReCreate) {
  const auto size = getRandomSize();
  {
    // create a new segment
    PosixShmSegment tmp(ShmNew, segmentName, size);
  }

  PosixShmSegment s(ShmAttach, segmentName);
  ASSERT_TRUE(s.isActive());
  ASSERT_TRUE(s.getSize() == size);
  const unsigned char magicVal = 'c';
  auto addr = getNewUnmappedAddr();
  s.mapAddress(addr);
  writeToMemory(addr, size, magicVal);
  checkMemory(addr, size, magicVal);
  s.unMap(addr);

  // create new one with same name and verify that it is actually a new
  // segment.
  PosixShmSegment::removeByName(segmentName);
  PosixShmSegment newS(ShmNew, segmentName, size);
  addr = getNewUnmappedAddr();
  newS.mapAddress(addr);
  checkMemory(addr, size, 0);
}

TEST_F(PosixShmTest, MapAddress) {
  using facebook::cachelib::detail::getPageSize;
  const auto size = getRandomSize() + getPageSize();
  {
    // create a new segment
    PosixShmSegment tmp(ShmNew, segmentName, size);
  }

  PosixShmSegment s(ShmAttach, segmentName);
  ASSERT_TRUE(s.isActive());
  ASSERT_TRUE(s.getSize() == size);
  const unsigned char magicVal = 'c';
  auto addr = getNewUnmappedAddr();
  s.mapAddress(addr);
  writeToMemory(addr, size, magicVal);
  checkMemory(addr, size, magicVal);
  s.unMap(addr);

  // map using nullptr
  addr = s.mapAddress(nullptr);
  ASSERT_NE(addr, nullptr);
  checkMemory(addr, size, magicVal);
  s.unMap(addr);
}

// attach to a segment that has not been created and ensure that it fails.
TEST_F(PosixShmTest, AttachToInvalidSegment) {
  // attach with no size
  ASSERT_THROW(PosixShmSegment(ShmAttach, segmentName), std::system_error);
}

TEST_F(PosixShmTest, RemoveWithMMap) {
  using facebook::cachelib::detail::getPageSize;
  const size_t size = getRandomSize();
  const size_t fullSize = size + getPageSize();
  PosixShmSegment s(ShmNew, segmentName, size);
  const unsigned char magicVal = 'c';
  auto addr = getNewUnmappedAddr();
  s.mapAddress(addr);
  writeToMemory(addr, size, magicVal);
  checkMemory(addr, size, magicVal);

  s.markForRemoval();
  ASSERT_TRUE(s.isMarkedForRemoval());
  // get size should work even after it is marked to be removed.
  ASSERT_EQ(s.getSize(), size);

  // memory that is already mapped can be accessed
  checkMemory(addr, size, magicVal);
  const unsigned char newMagicVal = 'd';
  writeToMemory(addr, size, newMagicVal);
  checkMemory(addr, size, newMagicVal);

  ASSERT_THROW(PosixShmSegment(ShmAttach, segmentName), std::system_error);

  // creating a new segment with same name should succeed while the previous
  // segment is still mapped.
  { PosixShmSegment newS(ShmNew, segmentName, size); }

  // the memory currently mapped is still valid even though we created a new
  // segment with similar name.
  checkMemory(addr, size, newMagicVal);

  // try to unmap the memory manually. This will release the old segment's
  // memory
  const int ret = munmap(addr, fullSize);
  ASSERT_TRUE(ret == 0) << "Failed munmap errno = " << errno;
  PosixShmSegment newS(ShmAttach, segmentName);
  ASSERT_EQ(size, newS.getSize());
  newS.mapAddress(addr);
  checkMemory(addr, size, 0);
}
