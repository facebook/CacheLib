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

#pragma once
#include <gtest/gtest.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/time.h>

#include <atomic>
#include <cstddef>
#include <cstdint>

#include "cachelib/shm/PosixShmSegment.h"
#include "cachelib/shm/Shm.h"
#include "cachelib/shm/ShmCommon.h"
#include "cachelib/shm/SysVShmSegment.h"

namespace facebook {
namespace cachelib {
namespace tests {

constexpr size_t kMemMin = 1; // 1 byte
// 10k since rlimit under non-priveldged is set to 64K for mlocking the pages
// We check for this in the setup of every test.
constexpr size_t kMemMax = 10 * (1 << 10);

class ShmTestBase : public ::testing::Test {
 public:
  // when tests run in the same binary, we want to ensure that each test
  // gets its own address range to map into.
  void* getNewUnmappedAddr() {
    const auto index = ++index_;
    return reinterpret_cast<void*>(kAddr + index * kAddrChunk);
  }

  static size_t getRandomSize();
  static void checkMemory(void* addr, size_t size, const unsigned char value);
  static void writeToMemory(void* addr, size_t size, const unsigned char value);

 private:
  // index for the address range
  static std::atomic<unsigned int> index_;
  // address range for each test
  static constexpr unsigned int kAddrChunk = 1 << 20; // 1MB chunk
  static_assert(kMemMax <= kAddrChunk, "address ranges will overlap");
  static constexpr intptr_t kAddr = 0x7e0000000000;
};

class ShmTest : public ShmTestBase {
 public:
  ShmTest()
      : segmentName("shm-tests" + std::to_string(::getpid())),
        shmSize(getRandomSize()) {}
  // use a different name for each test since they could be run in
  // parallel by fbmake runtests.
  const std::string segmentName{};
  const size_t shmSize{0};

 protected:
  void SetUp() final {
    struct rlimit lim;
    getrlimit(RLIMIT_MEMLOCK, &lim);
    ASSERT_TRUE(lim.rlim_cur >= 2 * kMemMax);
  }

  void TearDown() final {
    // make sure that the segment is unlinked so that we dont leak this across
    // tests.. since deleting segment can fail and throw an exception, do this
    // in a separate teardown and not in the destructor
    clearSegment();
  }

  virtual void clearSegment() = 0;

  // common tests
  void testCreateAttach(bool posix);
  void testAttachReadOnly(bool posix);
  void testMapping(bool posix);
  void testMappingAlignment(bool posix);
  void testLifetime(bool posix);
  void testPageSize(PageSizeT, bool posix);
};

class ShmTestPosix : public ShmTest {
 public:
  ShmTestPosix() {}

 private:
  void clearSegment() override {
    try {
      PosixShmSegment::removeByName(segmentName);
    } catch (const std::system_error& e) {
      if (e.code().value() != ENOENT) {
        throw;
      }
    }
  }
};

class ShmTestSysV : public ShmTest {
 public:
  ShmTestSysV() {}

 private:
  void clearSegment() override {
    try {
      SysVShmSegment::removeByName(segmentName);
    } catch (const std::system_error& e) {
      if (e.code().value() != ENOENT) {
        throw;
      }
    }
  }
};
} // namespace tests
} // namespace cachelib
} // namespace facebook
