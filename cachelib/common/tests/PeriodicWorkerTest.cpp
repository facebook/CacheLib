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

#include <map>
#include <thread>

#include "cachelib/common/PeriodicWorker.h"
#include "cachelib/common/Time.h"
using facebook::cachelib::PeriodicWorker;

bool eventuallyTrue(std::function<bool()> test,
                    unsigned int total_wait_sec = 10,
                    unsigned int tries_per_sec = 5) {
  const unsigned long long int NANOS_PER_SEC = 1e9;
  const int64_t sleepTime = NANOS_PER_SEC / tries_per_sec;
  int num_tries = total_wait_sec * tries_per_sec;
  for (int i = 0; i < num_tries; i++) {
    const int64_t startTime = facebook::cachelib::util::getCurrentTimeNs();
    const bool last_time = (i == num_tries - 1);
    const auto res = test();
    if (res || last_time) {
      return res;
    }
    const int64_t endTime = facebook::cachelib::util::getCurrentTimeNs();
    const int64_t timeLeft = sleepTime - (endTime - startTime);
    if (timeLeft > 0) {
      std::this_thread::sleep_for(std::chrono::nanoseconds(timeLeft));
    }
  }

  return false;
}

const unsigned int LONG_WORK_TIME = 5;
const unsigned int SHORT_WORK_TIME = 0;

class IncWork : public PeriodicWorker {
 public:
  explicit IncWork(unsigned int to) : timeout(to) {}
  ~IncWork() override { stop(std::chrono::milliseconds(0)); }

  void inc_and_sleep(unsigned int) {
    pthread_mutex_lock(&lock_x);
    state++;
    pthread_mutex_unlock(&lock_x);
    /* use select so that the signal delivery does not interfere
     * with the sleep */
    timeval t;
    fd_set fds;
    FD_ZERO(&fds);
    t.tv_sec = timeout;
    t.tv_usec = 10;
    std::cout << "Performing Work";
    select(FD_SETSIZE, &fds, &fds, &fds, &t);
    std::cout << "Finished Work";
  }

  void work(void) final { inc_and_sleep(timeout); }

  int read_state() {
    int ret;
    pthread_mutex_lock(&lock_x);
    ret = state;
    pthread_mutex_unlock(&lock_x);
    return ret;
  }

  bool state_has_changed() {
    int prev_ = prev_state;
    prev_state = read_state();
    return prev_ != prev_state;
  }

  bool state_has_not_changed() {
    int prev_ = prev_state;
    prev_state = read_state();
    return prev_ == prev_state;
  }

  void update_prev() { prev_state = read_state(); }

 private:
  unsigned int timeout = SHORT_WORK_TIME;
  int state = 0;
  int prev_state = 0;
  pthread_mutex_t lock_x = PTHREAD_MUTEX_INITIALIZER;
};

TEST(periodic_worker, basic) {
  IncWork w(SHORT_WORK_TIME);
  auto didSomeWorkSinceLastCheck = std::bind(&IncWork::state_has_changed, &w);
  auto noWorkSinceLastCheck = std::bind(&IncWork::state_has_not_changed, &w);
  const std::chrono::milliseconds sleep_interval(10);
  w.update_prev();

  /* Start and check that the state has changed after a small period of time
   * indicating the thread performed the work */
  w.start(sleep_interval);
  EXPECT_TRUE(eventuallyTrue(didSomeWorkSinceLastCheck, SHORT_WORK_TIME + 1));

  /* Wait for more time to see that the thread does the sleep and increments
   * it. This is a bit racy so all we can check is to make sure that there
   * was atleast one iteration after the last time  */
  EXPECT_TRUE(eventuallyTrue(didSomeWorkSinceLastCheck, 1));

  /* try to stop with no timeout and check if it stopped */
  bool success = w.stop(std::chrono::seconds(0));
  EXPECT_TRUE(success);

  /* make sure that we actually stopped */
  w.update_prev();
  /* sleep override */
  std::this_thread::sleep_for(sleep_interval);
  EXPECT_TRUE(eventuallyTrue(noWorkSinceLastCheck));

  /* trying to stop while the worker thread is sleeping between the
   * execution */
  w.start(sleep_interval);
  EXPECT_TRUE(eventuallyTrue(didSomeWorkSinceLastCheck, SHORT_WORK_TIME + 1));
  success = w.stop(std::chrono::seconds(1));
  EXPECT_TRUE(success);
  EXPECT_TRUE(eventuallyTrue(noWorkSinceLastCheck));
}

TEST(periodic_worker, stop_timeout) {
  IncWork w(LONG_WORK_TIME);
  auto didSomeWorkSinceLastCheck = std::bind(&IncWork::state_has_changed, &w);
  auto noWorkSinceLastCheck = std::bind(&IncWork::state_has_not_changed, &w);
  const std::chrono::seconds sleep_interval(1);

  w.update_prev();

  /* start a work which takes a considerable amount of time per iteration */
  w.start(sleep_interval);
  EXPECT_TRUE(eventuallyTrue(didSomeWorkSinceLastCheck, 1));

  /* try to stop it with a short timeout. Since it will be performing the
   * work, the stop should fail */
  EXPECT_TRUE(eventuallyTrue(noWorkSinceLastCheck, 1));
  sleep(1);
  EXPECT_TRUE(eventuallyTrue(noWorkSinceLastCheck, 1));

  bool success = w.stop(std::chrono::seconds(1));
  EXPECT_FALSE(success);

  /* try a longer timeout and see that this time it stops the work  */
  success = w.stop(std::chrono::seconds(LONG_WORK_TIME + 1));
  EXPECT_TRUE(success);

  w.update_prev();
  EXPECT_TRUE(eventuallyTrue(noWorkSinceLastCheck, LONG_WORK_TIME));
  sleep(1);
  EXPECT_TRUE(eventuallyTrue(noWorkSinceLastCheck, LONG_WORK_TIME));
}

TEST(periodic_worker, stop) {
  /* Test calling stop first and then trying to sttart */
  {
    IncWork w(SHORT_WORK_TIME);
    bool success = w.stop(std::chrono::seconds(0));
    EXPECT_TRUE(success);

    /* Start with 0 interval should fail */
    success = w.start(std::chrono::seconds(0));
    EXPECT_FALSE(success);

    /* Start should succeed */
    success = w.start(std::chrono::seconds(3));
    EXPECT_TRUE(success);

    /* Another start before stop just updates interval */
    success = w.start(std::chrono::seconds(4));
    EXPECT_TRUE(success);

    success = w.stop(std::chrono::seconds(0));
    EXPECT_TRUE(success);
  }

  /* Test calling stop and start and ensuring no work happens when stopped */
  {
    IncWork w(LONG_WORK_TIME);
    w.update_prev();

    auto didSomeWorkSinceLastCheck = std::bind(&IncWork::state_has_changed, &w);
    auto noWorkSinceLastCheck = std::bind(&IncWork::state_has_not_changed, &w);

    const std::chrono::seconds sleep_interval(1);
    bool success = w.start(sleep_interval);
    EXPECT_TRUE(success);
    EXPECT_TRUE(eventuallyTrue(didSomeWorkSinceLastCheck, 1));

    success = w.stop(std::chrono::seconds(0));
    EXPECT_TRUE(success);

    success = w.start(std::chrono::seconds(1));
    EXPECT_TRUE(success);

    success = w.stop(std::chrono::seconds(0));
    EXPECT_TRUE(success);
    w.update_prev();
    sleep(2);
    EXPECT_TRUE(eventuallyTrue(noWorkSinceLastCheck, 1));

    success = w.start(std::chrono::seconds(1));
    EXPECT_TRUE(success);

    success = w.stop(std::chrono::seconds(0));
    EXPECT_TRUE(success);
  }
}

TEST(periodic_worker, wakeUp) {
  IncWork w(SHORT_WORK_TIME);
  w.update_prev();

  auto didSomeWorkSinceLastCheck = std::bind(&IncWork::state_has_changed, &w);

  const std::chrono::seconds sleep_interval(30);
  bool success = w.start(sleep_interval);
  EXPECT_TRUE(success);
  EXPECT_TRUE(eventuallyTrue(didSomeWorkSinceLastCheck, 1));

  w.wakeUp();
  EXPECT_TRUE(eventuallyTrue(didSomeWorkSinceLastCheck, 1));

  success = w.stop(std::chrono::seconds(0));
  EXPECT_TRUE(success);
}

enum class Stage : int8_t {
  NONE,
  PREWORK_COMPLETE,
  PREWORK_ERROR,
  WORK_DONE_ONCE,
  WORKING_ERROR
};

const std::map<Stage, const char*> StageString = {
    {Stage::NONE, "NONE"},
    {Stage::PREWORK_COMPLETE, "PREWORK_COMPLETE"},
    {Stage::PREWORK_ERROR, "PREWORK_ERROR"},
    {Stage::WORK_DONE_ONCE, "WORK_DONE_ONCE"},
    {Stage::WORKING_ERROR, "WORKING_ERROR"}};

inline std::ostream& operator<<(std::ostream& os, const Stage& stage) {
  os << StageString.at(stage);
  return os;
}

class Work : public PeriodicWorker {
 public:
  Work() : stage(Stage::NONE) {}
  ~Work() override { stop(std::chrono::seconds(0)); }

  bool checkStage(Stage expectedStage) { return stage == expectedStage; }

 private:
  void preWork() override {
    if (stage == Stage::NONE) {
      stage = Stage::PREWORK_COMPLETE;
    } else {
      stage = Stage::PREWORK_ERROR;
    }
  }

  void work() override {
    if (stage == Stage::PREWORK_COMPLETE) {
      /* this means we executed after preWORK. change our
         stage to mark that */
      stage = Stage::WORK_DONE_ONCE;
    } else if (stage != Stage::WORK_DONE_ONCE) {
      /* we can come into this state when our current state is
       * already registered to have worked once. Any other form of
       * entry into this method is incorrect */
      stage = Stage::WORKING_ERROR;
    }
  }

  Stage stage;
};

/* Make sure that the prework is executed before starting the work & the post
 * work is executed before stop is completed */
TEST(periodic_worker, work) {
  Work w;
  w.start(std::chrono::seconds(1));
  EXPECT_TRUE(
      eventuallyTrue(std::bind(&Work::checkStage, &w, Stage::WORK_DONE_ONCE)));
  bool success = w.stop(std::chrono::seconds(0));
  EXPECT_TRUE(success);
  EXPECT_TRUE(
      eventuallyTrue(std::bind(&Work::checkStage, &w, Stage::WORK_DONE_ONCE)));
}

TEST(periodic_worker, work_milliseconds) {
  Work w;
  w.start(std::chrono::milliseconds(1));
  EXPECT_TRUE(
      eventuallyTrue(std::bind(&Work::checkStage, &w, Stage::WORK_DONE_ONCE)));
  bool success = w.stop(std::chrono::seconds(0));
  EXPECT_TRUE(success);
  EXPECT_TRUE(
      eventuallyTrue(std::bind(&Work::checkStage, &w, Stage::WORK_DONE_ONCE)));
}
