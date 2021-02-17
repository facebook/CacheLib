#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "cachelib/cachebench/consistency/ShortThreadId.h"

namespace facebook {
namespace cachelib {
namespace cachebench {
namespace tests {
TEST(ShortThreadId, Basic) {
  EXPECT_EQ(ShortThreadId{10}, ShortThreadId{10});
  EXPECT_NE(ShortThreadId{10}, ShortThreadId{11});

  ShortThreadIdMap smap;

  EXPECT_EQ(0, smap.getShort(std::this_thread::get_id()));
  EXPECT_EQ(0, smap.getShort(std::this_thread::get_id()));

  std::vector<std::thread> threads;
  std::vector<ShortThreadId> shortIds(16);
  // Spawn several threads at the same time to check synchronization: each
  // thread must get its own unique, sequential id.
  for (auto& sid : shortIds) {
    threads.emplace_back(std::thread{[&smap, &sid] {
      const auto tid = std::this_thread::get_id();
      sid = smap.getShort(tid);
      ASSERT_EQ(sid, smap.getShort(tid));
    }});
  }
  for (auto& t : threads) {
    t.join();
  }

  EXPECT_EQ(0, smap.getShort(std::this_thread::get_id()));
  std::sort(shortIds.begin(), shortIds.end());
  for (size_t i = 0; i < shortIds.size(); i++) {
    EXPECT_EQ(i + 1, shortIds[i]);
  }
}
} // namespace tests
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
