#include <atomic>

#include <sys/mman.h>

#include <folly/Random.h>
#include <gtest/gtest.h>

#include "cachelib/common/FastStats.h"
#include "cachelib/common/Utils.h"

using facebook::cachelib::util::FastStats;
using facebook::cachelib::util::SysctlSetting;
using facebook::cachelib::util::toString;

namespace facebook {
namespace cachelib {
namespace tests {

TEST(Util, ToString) {
  ASSERT_EQ(toString(std::chrono::seconds(5)), "5.00s");
  ASSERT_EQ(toString(std::chrono::seconds(4000)), "4000.00s");
  ASSERT_EQ(toString(std::chrono::milliseconds(3)), "3.00ms");
  ASSERT_EQ(toString(std::chrono::milliseconds(3300)), "3.30s");
  ASSERT_EQ(toString(std::chrono::microseconds(10)), "10.00us");
  ASSERT_EQ(toString(std::chrono::microseconds(10530)), "10.53ms");
  ASSERT_EQ(toString(std::chrono::nanoseconds(14)), "14ns");
}

TEST(Util, FastStats) {
  struct TData {
    uint64_t a{0};
    uint64_t b{0};
    TData& operator+=(const TData& o) {
      a += o.a;
      b += o.b;
      return *this;
    }
  };

  std::atomic<bool> shutDown{false};
  std::atomic<unsigned int> completed{0};
  std::atomic<unsigned int> initDone{0};
  unsigned int nBumps = folly::Random::rand32(1e4, 1e5);
  FastStats<TData> stats;
  auto doThreadWork = [&]() {
    std::ignore = stats.tlStats();
    ++initDone;
    for (unsigned int i = 0; i < nBumps; i++) {
      auto& s = stats.tlStats();
      s.a++;
      s.b += 2;
    }

    ++completed;
    // wait until we are told to shutDown
    while (!shutDown) {
      /* sleep override */
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  };

  const unsigned int nThreads = folly::Random::rand32(5, 30);
  std::vector<std::thread> threads;

  for (unsigned int i = 0; i < nThreads; i++) {
    threads.emplace_back([&]() { doThreadWork(); });
  }

  // give some time for the threads to initialize
  while (initDone != threads.size()) {
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  ASSERT_EQ(threads.size(), stats.getActiveThreadCount());

  // wait for threads to complete the loops
  while (completed != threads.size()) {
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  ASSERT_EQ(threads.size(), stats.getActiveThreadCount());

  TData accum;
  auto visitFn = [&accum](const TData& d) { accum += d; };
  stats.forEach(visitFn);

  ASSERT_EQ(nBumps * threads.size(), accum.a);
  ASSERT_EQ(nBumps * threads.size() * 2, accum.b);

  // shut down and make sure that the snap shot reflects all the bumps.
  shutDown = true;
  for (auto& t : threads) {
    if (t.joinable()) {
      t.join();
    }
  }

  ASSERT_EQ(0, stats.getActiveThreadCount());
  auto snapShot = stats.getSnapshot();
  ASSERT_EQ(nBumps * nThreads, snapShot.a);
  ASSERT_EQ(nBumps * nThreads * 2, snapShot.b);
}

/* This test ensures that we destroy the TLD of this current thread before the
 * parent into which we need to accumulate.
 *
 */
TEST(Util, FastStatsDestructionOrder) {
  struct TData {
    bool is_alive{true};

    /* Doing this check inside the += operator overload gives an error:
     * "cannot bind to a temporary of type 'void'".
     * Not sure why, but this works too.
     */
    void checkIsAlive() { ASSERT_TRUE(is_alive); }
    TData& operator+=(const TData&) {
      /* Check that the variable we initialized at construction time is still
       * set to "true". If not, the destructor has already been called on this
       * object and so somebody holds a reference to it that may no longer be
       * valid
       */
      checkIsAlive();
      return *this;
    }

    ~TData() {
      /* On destruction, this will be our canary indicating the destructor was
       * called */
      is_alive = false;
    }
  };

  /* Allocate a tiny TData in scope. When this goes out of scope, it will
   * accumulate */
  FastStats<TData> stats;
  /* Call to initialize the tld in this thread */
  stats.tlStats();
}

TEST(Util, SysctlTests) {
  auto ruid = geteuid();
  std::string settingName("vm.overcommit_memory");
  // cannot expect this to succeed as non-root
  if (ruid == 0) {
    std::string oldValue, afterValue;
    oldValue = SysctlSetting::get(settingName);
    EXPECT_NO_THROW(SysctlSetting setting(settingName, "1"));
    afterValue = SysctlSetting::get(settingName);
    EXPECT_EQ(oldValue, afterValue);
  } else {
    EXPECT_THROW(SysctlSetting setting(settingName, "1"), std::runtime_error);
  }
}

TEST(Util, MemAvailable) { EXPECT_GT(util::getMemAvailable(), 0); }

TEST(Util, MemRSS) {
  auto val = util::getRSSBytes();
  EXPECT_GT(val, 0);
  size_t len = 500 * 1024 * 1024;
  void* ptr = ::mmap(nullptr, len, PROT_WRITE | PROT_READ,
                     MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  EXPECT_NE(MAP_FAILED, ptr);
  SCOPE_EXIT { ::munmap(ptr, len); };
  memset(ptr, 5, len);
  EXPECT_GT(util::getRSSBytes(), val);
  EXPECT_GE(util::getRSSBytes() - val, len);
}

} // namespace tests
} // namespace cachelib
} // namespace facebook
