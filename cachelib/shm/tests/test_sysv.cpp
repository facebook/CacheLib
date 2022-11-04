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

#include <sys/resource.h>

#include "cachelib/shm/SysVShmSegment.h"
#include "cachelib/shm/tests/common.h"

using namespace facebook::cachelib::tests;
using facebook::cachelib::ShmAttach;
using facebook::cachelib::ShmNew;
using facebook::cachelib::SysVShmSegment;
using facebook::cachelib::detail::getPageSize;

class SysVShmTest : public ShmTestBase {
 public:
  SysVShmTest() : shmSize(getRandomSize()) {}
  void* getMapAddr() { return addr_; }

  // use a different name for each test since they could be run in
  // parallel by fbmake runtests.
  static std::string getName() { return std::to_string(getpid()); }

  const size_t shmSize{0};

 protected:
  void SetUp() override {
    struct rlimit lim;
    getrlimit(RLIMIT_MEMLOCK, &lim);
    ASSERT_TRUE(lim.rlim_cur >= 2 * kMemMax);
    addr_ = getNewUnmappedAddr();
  }

  void TearDown() override {
    // we use the same segment for all tests. always make sure that
    // the segment is unlinked so that we dont leak this across
    // tests.. since deleting segment can fail and throw an exception,
    // do this in a separate teardown and not in the destructor
    try {
      SysVShmSegment::removeByName(getName());
    } catch (const std::system_error& e) {
      if (e.code().value() != ENOENT) {
        throw;
      }
    }
  }

 private:
  void* addr_{nullptr};
};

TEST_F(SysVShmTest, Create) {
  SysVShmSegment s(ShmNew, getName(), shmSize);
  ASSERT_TRUE(s.isActive());
  ASSERT_EQ(s.getSize(), shmSize);
}

TEST_F(SysVShmTest, CreateZeroSize) {
  SysVShmSegment s(ShmNew, getName(), 0);
  ASSERT_EQ(getPageSize(), s.getSize());
}

TEST_F(SysVShmTest, Attach) {
  ASSERT_NO_THROW(SysVShmSegment(ShmNew, getName(), shmSize));
  {
    SysVShmSegment s(ShmAttach, getName());
    ASSERT_TRUE(s.isActive());
    ASSERT_EQ(s.getSize(), shmSize);
  }

  ASSERT_THROW(SysVShmSegment(ShmNew, getName(), shmSize), std::system_error);
}

TEST_F(SysVShmTest, MapAddress) {
  const unsigned char magicVal = 'v';
  ASSERT_NO_THROW({
    SysVShmSegment tmp(ShmNew, getName(), shmSize);
    auto addr = getMapAddr();
    tmp.mapAddress(addr);
    writeToMemory(addr, shmSize, magicVal);
    tmp.unMap(addr);
  });

  // map using nullptr
  SysVShmSegment s(ShmAttach, getName());
  ASSERT_EQ(shmSize, s.getSize());
  void* addr = s.mapAddress(nullptr);
  ASSERT_NE(addr, nullptr);
  checkMemory(addr, shmSize, magicVal);
  s.unMap(addr);
}

TEST_F(SysVShmTest, AttachDifferentSize) {
  const unsigned char magicVal = 'v';
  ASSERT_NO_THROW({
    SysVShmSegment tmp(ShmNew, getName(), shmSize);
    auto addr = getMapAddr();
    tmp.mapAddress(addr);
    writeToMemory(addr, shmSize, magicVal);
    tmp.unMap(addr);
  });

  SysVShmSegment s(ShmAttach, getName());
  auto addr = getMapAddr();
  ASSERT_EQ(shmSize, s.getSize());
  s.mapAddress(addr);
  checkMemory(addr, shmSize, magicVal);
  s.unMap(addr);
}

TEST_F(SysVShmTest, MemoryRetention) {
  SysVShmSegment s(ShmNew, getName(), shmSize);
  auto addr = getMapAddr();
  s.mapAddress(addr);
  const unsigned char magicVal = 'a';
  writeToMemory(addr, shmSize, magicVal);
  checkMemory(addr, shmSize, magicVal);
}
