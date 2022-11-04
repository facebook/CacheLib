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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <string>
#include <thread>

#include "cachelib/navy/common/Utils.h"
#include "cachelib/navy/testing/SeqPoints.h"

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {
TEST(SeqPoints, Basic) {
  SeqPoints sp;
  std::string str;

  std::thread t1{[&sp, &str] {
    str.append("abc");
    sp.reached(0);
  }};

  std::thread t2{[&sp, &str] {
    sp.wait(1);
    str.append("123");
    sp.reached(2);
  }};

  std::thread t3{[&sp, &str] {
    sp.wait(0);
    str.append("-def-");
    sp.reached(1);
  }};

  sp.wait(2);
  EXPECT_EQ("abc-def-123", str);
  t1.join();
  t2.join();
  t3.join();
}

TEST(SeqPoints, DefaultLogging) {
  SeqPoints sp{SeqPoints::defaultLogger()};
  sp.setName(0, "print something");
  sp.reached(0);
  sp.wait(0);
}

TEST(SeqPoints, Names) {
  struct MockHelper {
    MOCK_METHOD3(call,
                 void(SeqPoints::Event event, uint32_t, const std::string&));
  };

  MockHelper helper;
  auto logger = bindThis(&MockHelper::call, helper);
  EXPECT_CALL(helper, call(SeqPoints::Event::Wait, 0, "action A"));
  EXPECT_CALL(helper, call(SeqPoints::Event::Reached, 0, "action A"));
  EXPECT_CALL(helper, call(SeqPoints::Event::Wait, 1, "action B"));
  EXPECT_CALL(helper, call(SeqPoints::Event::Reached, 1, "action B"));

  SeqPoints sp{logger};

  sp.setName(0, "action A");
  sp.setName(1, "action B");

  std::thread t1{[&sp] { sp.reached(0); }};

  std::thread t2{[&sp] {
    sp.wait(0);
    sp.reached(1);
  }};

  sp.wait(1);
  t1.join();
  t2.join();
}

TEST(SeqPoints, WaitFor) {
  SeqPoints sp{SeqPoints::defaultLogger()};
  std::atomic<bool> stop{false};
  std::thread t{[&sp, &stop] {
    while (!stop.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    sp.reached(0);
  }};

  auto start = getSteadyClock();
  auto waitTime = std::chrono::milliseconds{1'000};
  EXPECT_FALSE(sp.waitFor(0, waitTime));
  EXPECT_LE(waitTime, getSteadyClock() - start);
  stop.store(true, std::memory_order_release);

  sp.wait(0);
  t.join();
}
} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook
