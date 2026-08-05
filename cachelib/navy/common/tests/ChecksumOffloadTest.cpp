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

#include <folly/fibers/FiberManagerMap.h>
#include <folly/io/async/EventBase.h>
#include <gtest/gtest.h>

#include <cstring>
#include <random>
#include <vector>

#include "cachelib/navy/common/ChecksumOffload.h"
#include "cachelib/navy/common/Hash.h"

namespace facebook::cachelib::navy::tests {
namespace {
std::vector<uint8_t> randomBytes(size_t size, uint32_t seed) {
  std::mt19937 gen{seed};
  std::vector<uint8_t> bytes(size);
  for (auto& b : bytes) {
    b = static_cast<uint8_t>(gen());
  }
  return bytes;
}
} // namespace

// Sizes spanning below and above typical offload gates (DTO_MIN_BYTES and
// navy-level thresholds), so both the DSA path and internal CPU fallbacks
// are exercised when built with DTO support.
const size_t kSizes[] = {1, 13, 175, 4096, 16 * 1024, 64 * 1024, 1024 * 1024};

TEST(ChecksumOffload, CopyAndChecksumMatchesSoftware) {
  uint32_t seed = 1;
  for (auto size : kSizes) {
    auto src = randomBytes(size, seed++);
    std::vector<uint8_t> dst(size, 0xAA);
    const BufferView view{src.size(), src.data()};

    const uint32_t cs = copyAndChecksum(dst.data(), view, [] {});
    EXPECT_EQ(checksum(view), cs) << "size=" << size;
    EXPECT_EQ(0, std::memcmp(src.data(), dst.data(), size)) << "size=" << size;
  }
}

TEST(ChecksumOffload, ChecksumWithOverlapMatchesSoftware) {
  uint32_t seed = 100;
  for (auto size : kSizes) {
    auto src = randomBytes(size, seed++);
    const BufferView view{src.size(), src.data()};
    EXPECT_EQ(checksum(view), checksumWithOverlap(view, [] {}))
        << "size=" << size;
  }
}

TEST(ChecksumOffload, OverlapRunsExactlyOnce) {
  for (auto size : {175UL, 1024UL * 1024UL}) {
    auto src = randomBytes(size, 7);
    std::vector<uint8_t> dst(size, 0);
    const BufferView view{src.size(), src.data()};

    int copyOverlapRuns = 0;
    copyAndChecksum(dst.data(), view, [&] { copyOverlapRuns++; });
    EXPECT_EQ(1, copyOverlapRuns) << "size=" << size;

    int crcOverlapRuns = 0;
    checksumWithOverlap(view, [&] { crcOverlapRuns++; });
    EXPECT_EQ(1, crcOverlapRuns) << "size=" << size;
  }
}

// The overlap callback is allowed to read the source range concurrently with
// the operation (e.g. BigHash rebuilds its bloom filter from the bucket while
// DSA checksums it).
TEST(ChecksumOffload, OverlapMayReadSource) {
  const size_t size = 64 * 1024;
  auto src = randomBytes(size, 11);
  const BufferView view{src.size(), src.data()};

  uint64_t sum = 0;
  const uint32_t cs = checksumWithOverlap(view, [&] {
    for (auto b : src) {
      sum += b;
    }
  });
  EXPECT_EQ(checksum(view), cs);
  EXPECT_NE(0ULL, sum);
}

// Multiple async operations in flight from one thread, out-of-order waits.
TEST(ChecksumOffload, AsyncOpsInFlight) {
  const size_t size = 64 * 1024;
  auto src1 = randomBytes(size, 21);
  auto src2 = randomBytes(size, 22);
  std::vector<uint8_t> dst1(size, 0);
  const BufferView v1{src1.size(), src1.data()};
  const BufferView v2{src2.size(), src2.data()};

  AsyncChecksumOp op1;
  AsyncChecksumOp op2;
  op1.submitCopyAndChecksum(dst1.data(), v1, true /* cacheControl */);
  op2.submitChecksum(v2);

  // Complete in reverse submission order.
  EXPECT_EQ(checksum(v2), op2.wait());
  EXPECT_EQ(checksum(v1), op1.wait());
  EXPECT_EQ(0, std::memcmp(src1.data(), dst1.data(), size));

  // An op object is reusable after wait().
  op1.submitChecksum(v2);
  EXPECT_EQ(checksum(v2), op1.wait());
}

// wait() on a fiber takes the yield path: other fibers run on the thread
// while the operation is in flight.
TEST(ChecksumOffload, AsyncOpOnFiber) {
  const size_t size = 1024 * 1024;
  auto src = randomBytes(size, 27);
  std::vector<uint8_t> dst(size, 0);
  const BufferView view{src.size(), src.data()};

  folly::EventBase evb;
  auto& fm = folly::fibers::getFiberManager(evb);
  uint32_t got = 0;
  bool otherFiberRan = false;
  fm.addTask([&] {
    AsyncChecksumOp op;
    op.submitCopyAndChecksum(dst.data(), view, true /* cacheControl */);
    got = op.wait();
  });
  fm.addTask([&] { otherFiberRan = true; });
  evb.loop();

  EXPECT_EQ(checksum(view), got);
  EXPECT_TRUE(otherFiberRan);
  EXPECT_EQ(0, std::memcmp(src.data(), dst.data(), size));
}

// Destructor drains an in-flight op (does not leave the device writing to
// freed stack memory).
TEST(ChecksumOffload, AsyncOpDrainOnDestroy) {
  const size_t size = 256 * 1024;
  auto src = randomBytes(size, 33);
  std::vector<uint8_t> dst(size, 0);
  {
    AsyncChecksumOp op;
    op.submitCopyAndChecksum(dst.data(), BufferView{src.size(), src.data()},
                             false /* cacheControl */);
    // no wait(): destructor must drain
  }
  EXPECT_EQ(0, std::memcmp(src.data(), dst.data(), size));
}

TEST(ChecksumOffload, SelfCheck) {
  if (checksumOffloadSupported()) {
    // Built with DTO: DSA (or DTO's internal CPU fallback) must agree with
    // navy::checksum. A failure here means offloaded checksums would not
    // verify against CPU-computed ones on this machine.
    EXPECT_TRUE(checksumOffloadSelfCheck());
  } else {
    EXPECT_FALSE(checksumOffloadSelfCheck());
  }
}
} // namespace facebook::cachelib::navy::tests
