#include <folly/Random.h>
#include <gtest/gtest.h>

#include <thread>
#include <vector>

#include "cachelib/allocator/nvmcache/TombStones.h"

namespace facebook {
namespace cachelib {
namespace tests {

void runInThreads(const std::function<void(int index)>& f,
                  unsigned int nThreads) {
  std::vector<std::thread> threads;
  for (unsigned int i = 0; i < nThreads; i++) {
    threads.push_back(std::thread{f, i});
  }

  for (auto& t : threads) {
    if (t.joinable()) {
      t.join();
    }
  }
}

TEST(TombStoneTest, ConcurrentAddRemove) {
  TombStones t;
  const unsigned int nThreads = 10;
  const unsigned int nHashes = 100;
  std::vector<std::string> hashes;
  for (unsigned int i = 0; i < nHashes; i++) {
    hashes.push_back(std::to_string(i));
  }

  std::vector<std::vector<std::unique_ptr<TombStones::Guard>>> guards;
  for (unsigned int i = 0; i < nThreads; i++) {
    guards.push_back({});
  }

  auto addFunc = [&t, hashes, &guards](int index) mutable {
    std::random_shuffle(hashes.begin(), hashes.end());
    for (auto hash : hashes) {
      guards[index].push_back(std::make_unique<TombStones::Guard>(t.add(hash)));
    }
  };

  runInThreads(addFunc, nThreads);
  for (auto hash : hashes) {
    ASSERT_TRUE(t.isPresent(hash));
  }

  auto removeFunc = [&guards](int index) mutable {
    std::random_shuffle(guards[index].begin(), guards[index].end());
    for (auto& guard : guards[index]) {
      // destroy the guard
      guard.reset();
    }
  };

  runInThreads(removeFunc, nThreads);
  for (auto hash : hashes) {
    ASSERT_FALSE(t.isPresent(hash));
  }
}

TEST(TombStoneTest, MultipleGuard) {
  TombStones t;
  int nGuards = 10;
  std::string key = "12325";
  std::vector<std::unique_ptr<TombStones::Guard>> guards(nGuards);
  for (int i = 0; i < nGuards; i++) {
    guards[i] = std::make_unique<TombStones::Guard>(t.add(key));
  }

  ASSERT_TRUE(t.isPresent(key));

  for (int i = 0; i < nGuards - 1; i++) {
    guards[i].reset();
    ASSERT_TRUE(t.isPresent(key));
  }
  guards[nGuards - 1].reset();
  ASSERT_FALSE(t.isPresent(key));
}

TEST(TombStoneTest, Move) {
  TombStones t;
  std::string key = "12325";
  {
    auto guard = t.add(key);
    ASSERT_TRUE(t.isPresent(key));
    auto moveGuard = std::move(guard);
    ASSERT_TRUE(t.isPresent(key));
    TombStones::Guard moveAssignGuard;
    moveAssignGuard = std::move(moveGuard);
    ASSERT_TRUE(t.isPresent(key));
  }
  ASSERT_FALSE(t.isPresent(key));
}

} // namespace tests
} // namespace cachelib
} // namespace facebook
