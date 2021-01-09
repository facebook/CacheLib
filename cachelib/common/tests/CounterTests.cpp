#include <gtest/gtest.h>

#include "cachelib/common/AtomicCounter.h"

namespace facebook {
namespace cachelib {
namespace tests {

template <typename Counter>
void testAddSubIncDecBasic(Counter& a) {
  EXPECT_EQ(0, a.get());
  a.add(1);
  EXPECT_EQ(1, a.get());
  a.add(5);
  EXPECT_EQ(6, a.get());
  a.sub(1);
  EXPECT_EQ(5, a.get());
  a.inc();
  EXPECT_EQ(6, a.get());
  a.dec();
  EXPECT_EQ(5, a.get());
}

TEST(AtomicCounter, AddSubIncDecBasic) {
  AtomicCounter a{0};
  testAddSubIncDecBasic(a);
}

TEST(TLCounter, AddSubIncDecBasic) {
  TLCounter a{0};
  testAddSubIncDecBasic(a);
}

void runInThreads(int nThreads, const std::function<void()>& f) {
  std::vector<std::thread> threads;
  for (int i = 0; i < nThreads; i++) {
    threads.push_back(std::thread(f));
  }

  for (auto& t : threads) {
    t.join();
  }
}

template <typename Counter>
void testAddSubIncDecMT(Counter& a) {
  int ops = 100;
  int numThreads = 10;
  auto f = [&]() {
    for (int i = 0; i < ops; i++) {
      a.add(1);
      a.inc();
      a.add(5);
      a.sub(1);
      a.dec();
    }
  };

  runInThreads(numThreads, f);
  EXPECT_EQ(5 * numThreads * ops, a.get());
}

TEST(AtomicCounter, AddSubIncDecMT) {
  AtomicCounter a;
  testAddSubIncDecMT(a);
}

TEST(TLCounter, AddSubIncDecMT) {
  TLCounter a;
  testAddSubIncDecMT(a);
}

TEST(AtomicCounter, FetchAddSub) {
  AtomicCounter a{0};
  EXPECT_EQ(1, a.add_fetch(1));
  EXPECT_EQ(6, a.add_fetch(5));
  EXPECT_EQ(1, a.sub_fetch(5));
  EXPECT_EQ(0, a.sub_fetch(1));
}

} // namespace tests
} // namespace cachelib
} // namespace facebook
