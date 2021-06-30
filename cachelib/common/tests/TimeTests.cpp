#include <folly/Random.h>
#include <gtest/gtest.h>

#include <thread>

#include "cachelib/common/Time.h"

using facebook::cachelib::util::Timer;

namespace facebook {
namespace cachelib {
namespace tests {

TEST(Util, TimerTest) {
  {
    auto rnd = folly::Random::rand32(100, 2000);

    Timer timer;
    timer.startOrResume();
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::milliseconds(rnd));
    timer.pause();

    ASSERT_EQ(timer.getDurationMs(), rnd);
    ASSERT_EQ(timer.getDurationSec(), rnd / 1000);

    {
      auto t = timer.scopedStartOrResume();
      /* sleep override */
      std::this_thread::sleep_for(std::chrono::milliseconds(rnd));
    }
    ASSERT_EQ(timer.getDurationMs(), rnd * 2);
  }
}

} // namespace tests
} // namespace cachelib
} // namespace facebook
