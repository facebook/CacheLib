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

#include "cachelib/navy/block_cache/Region.h"

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {
TEST(Region, ReadAndBlock) {
  Region r{RegionId(0), 1024};

  auto desc = r.openForRead();
  EXPECT_EQ(desc.status(), OpenStatus::Ready);

  EXPECT_FALSE(r.readyForReclaim());
  // Once readyForReclaim has been attempted, all future accesses will be
  // blocked.
  EXPECT_EQ(r.openForRead().status(), OpenStatus::Retry);
  r.close(std::move(desc));
  EXPECT_TRUE(r.readyForReclaim());

  r.reset();
  EXPECT_EQ(r.openForRead().status(), OpenStatus::Ready);
}

TEST(Region, WriteAndBlock) {
  Region r{RegionId(0), 1024};

  auto [desc1, addr1] = r.openAndAllocate(1025);
  EXPECT_EQ(desc1.status(), OpenStatus::Error);

  auto [desc2, addr2] = r.openAndAllocate(100);
  EXPECT_EQ(desc2.status(), OpenStatus::Ready);
  EXPECT_FALSE(r.readyForReclaim());
  r.close(std::move(desc2));
  EXPECT_TRUE(r.readyForReclaim());

  r.reset();
  auto [desc3, addr3] = r.openAndAllocate(1024);
  EXPECT_EQ(desc3.status(), OpenStatus::Ready);
}

TEST(Region, BufferAttachDetach) {
  auto b = std::make_unique<Buffer>(1024);
  Region r{RegionId(0), 1024};
  r.attachBuffer(std::move(b));
  EXPECT_TRUE(r.hasBuffer());
  Buffer writeBuf(1024);
  memset(writeBuf.data(), 'A', 1024);
  Buffer readBuf(1024);
  r.writeToBuffer(0, writeBuf.view());
  r.readFromBuffer(0, readBuf.mutableView());
  EXPECT_TRUE(writeBuf.view() == readBuf.view());
  b = r.detachBuffer();
  EXPECT_FALSE(r.hasBuffer());
}

TEST(Region, BufferFlush) {
  auto b = std::make_unique<Buffer>(1024);
  Region r{RegionId(0), 1024};
  r.attachBuffer(std::move(b));
  EXPECT_TRUE(r.hasBuffer());

  auto [desc2, addr2] = r.openAndAllocate(100);
  EXPECT_EQ(desc2.status(), OpenStatus::Ready);

  EXPECT_EQ(Region::FlushRes::kRetryPendingWrites,
            r.flushBuffer([](auto, auto) { return true; }));

  r.close(std::move(desc2));
  EXPECT_EQ(Region::FlushRes::kRetryDeviceFailure,
            r.flushBuffer([](auto, auto) { return false; }));
  EXPECT_EQ(Region::FlushRes::kSuccess,
            r.flushBuffer([](auto, auto) { return true; }));
}
} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook
