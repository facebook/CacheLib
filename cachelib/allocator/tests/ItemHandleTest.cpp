#include "TestBase.h"

#include <algorithm>
#include <future>
#include <mutex>
#include <thread>

#include <gmock/gmock.h>

#include <folly/io/async/EventBase.h>
#include <folly/synchronization/Baton.h>

#include "cachelib/allocator/CacheItem.h"
#include "cachelib/common/FastStats.h"

namespace facebook {
namespace cachelib {

namespace {

struct TestAllocator;

struct TestItem {
  using CacheT = TestAllocator;
  using Item = int;
  using ChainedItem = int;

  void reset() {}
};

struct TestNvmCache;

using TestItemHandle = detail::HandleImpl<TestItem>;

struct TestAllocator {
  using Item = int;
  using ChainedItem = int;
  using NvmCacheT = TestNvmCache;

  TestAllocator() {
    ON_CALL(*this, release(testing::_, testing::_))
        .WillByDefault(testing::Invoke(
            [this](TestItem*, bool) { tlRef_.tlStats() -= 1; }));
  }

  MOCK_METHOD2(release, void(TestItem*, bool));

  TestItemHandle acquire(TestItem* it) {
    tlRef_.tlStats() += 1;
    return TestItemHandle{it, *this};
  }

  TestItemHandle getHandle(TestItem* it = nullptr) {
    return it != nullptr ? TestItemHandle{it, *this} : TestItemHandle{*this};
  }

  void setHandle(const TestItemHandle& hdl, TestItem* k) {
    hdl.getItemWaitContext()->set(acquire(k));
  }

  void setHandle(const TestItemHandle& hdl, TestItemHandle h) {
    hdl.getItemWaitContext()->set(std::move(h));
  }

  void markExpired(TestItemHandle& hdl) { hdl.markExpired(); }

  void adjustHandleCountForThread(int i) { tlRef_.tlStats() += i; }

  util::FastStats<int> tlRef_;
};
} // namespace

TEST(ItemHandleTest, WaitContext_set) {
  testing::NiceMock<TestAllocator> t;
  TestItem k;
  auto hdl = t.getHandle();
  EXPECT_FALSE(hdl.isReady());
  t.setHandle(hdl, &k);
  EXPECT_TRUE(hdl.isReady());
  EXPECT_EQ(&k, hdl.get());
}

TEST(ItemHandleTest, WaitContext_set_wait) {
  testing::NiceMock<TestAllocator> t;
  TestItem k;
  auto hdl = t.getHandle();

  folly::Baton<> run;
  auto thr = std::thread([&]() {
    run.wait();
    t.setHandle(hdl, &k);
  });

  EXPECT_FALSE(hdl.isReady());
  run.post();
  hdl.wait();
  EXPECT_TRUE(hdl.isReady());
  EXPECT_EQ(&k, hdl.get());
  thr.join();
}

TEST(ItemHandleTest, WaitContext_set_waitSemiFuture) {
  testing::NiceMock<TestAllocator> t;
  TestItem k;
  auto hdl = t.getHandle();

  folly::Baton<> run;
  folly::Baton<> refCountChecked;
  auto thr = std::thread([&]() {
    run.wait();
    t.setHandle(hdl, &k);
    refCountChecked.wait();
  });

  folly::EventBase evb;
  bool called = false;
  auto future = std::move(hdl)
                    .toSemiFuture()
                    .deferValue([&](TestItemHandle h) {
                      called = true;
                      EXPECT_TRUE(h.isReady());
                      EXPECT_EQ(&k, h.get());
                      return h;
                    })
                    .via(&evb);

  EXPECT_FALSE(called);
  run.post();
  EXPECT_FALSE(called);
  hdl = future.getVia(&evb);
  EXPECT_TRUE(called);
  hdl.reset();

  EXPECT_EQ(0, t.tlRef_.getSnapshot());
  t.tlRef_.forEach([](const auto& tlref) { EXPECT_EQ(0, tlref); });

  refCountChecked.post();
  thr.join();
}

TEST(ItemHandleTest, WaitContext_set_waitSemiFuture_ready) {
  testing::NiceMock<TestAllocator> t;
  TestItem k;
  auto hdl = t.getHandle();
  t.setHandle(hdl, &k);
  EXPECT_TRUE(hdl.isReady());

  folly::EventBase evb;
  bool called = false;
  auto future = std::move(hdl)
                    .toSemiFuture()
                    .deferValue([&](decltype(hdl)&& h) {
                      called = true;
                      EXPECT_TRUE(h.isReady());
                      EXPECT_EQ(&k, h.get());
                    })
                    .via(&evb);
  EXPECT_FALSE(called);
  future.getVia(&evb);
  EXPECT_TRUE(called);
}

TEST(ItemHandleTest, WaitContext_readycb) {
  testing::NiceMock<TestAllocator> t;
  TestItem k;
  bool cbFired = false;

  auto cb = [&](TestItemHandle it) {
    EXPECT_EQ(&k, it.get());
    cbFired = true;
  };
  auto hdl = t.getHandle();
  auto retCallback = hdl.onReady(std::move(cb));
  // cb expected to consumed. So retCallback should be empty function
  EXPECT_FALSE(retCallback);

  auto thr =
      std::thread([&t, &k, copy = std::move(hdl)]() { t.setHandle(copy, &k); });
  thr.join();

  EXPECT_TRUE(cbFired);
}

TEST(ItemHandleTest, WaitContext_ready_immediate) {
  testing::NiceMock<TestAllocator> t;
  TestItem k;
  bool cbFired = false;

  auto hdl = t.getHandle();
  t.setHandle(hdl, &k);
  EXPECT_TRUE(hdl.isReady());
  EXPECT_FALSE(cbFired);

  // attaching onReady to ready handle just invokes it inline
  auto retCallback = hdl.onReady([&](TestItemHandle it) {
    EXPECT_EQ(&k, it.get());
    cbFired = true;
  });
  // cb not expected to be consumed. So retCallback should be same function
  EXPECT_TRUE(retCallback);
  EXPECT_FALSE(cbFired);
  retCallback(std::move(hdl));
  EXPECT_TRUE(cbFired);
}

TEST(ItemHandleTest, Release) {
  TestAllocator t;
  TestItem k;

  EXPECT_CALL(t, release(testing::_, testing::_)).Times(0);

  auto hdl = t.getHandle(&k);
  EXPECT_EQ(&k, hdl.release());
  EXPECT_EQ(nullptr, hdl.get());
  EXPECT_EQ(nullptr, hdl.release());
}

TEST(ItemHandleTest, ReleaseWithWaitContext) {
  // Try several times to possibly get a race
  for (uint32_t i = 0; i < 100; i++) {
    TestAllocator t;
    TestItem k;

    EXPECT_CALL(t, release(&k, false)).WillOnce(testing::Return());

    auto hdl = t.getHandle();
    folly::Baton<> setThreadStarted;
    std::thread setThread{[&] {
      setThreadStarted.post();
      std::this_thread::yield();
      t.setHandle(hdl, &k);
    }};

    setThreadStarted.wait();
    auto p = hdl.release();
    EXPECT_EQ(&k, p);

    EXPECT_EQ(nullptr, hdl.get());
    EXPECT_EQ(nullptr, hdl.release());
    auto hdlDup = t.getHandle(p);
    setThread.join();
  }
}

TEST(ItemHandleTest, onReadyWithNoWaitContext) {
  testing::NiceMock<TestAllocator> t;
  TestItem k;
  TestItemHandle hdl = t.acquire(&k);
  EXPECT_TRUE(hdl.isReady());
  bool cbFired = false;
  auto myCallback = [&](TestItemHandle it) {
    EXPECT_EQ(&k, it.get());
    cbFired = true;
  };
  auto retCallback = hdl.onReady(std::move(myCallback));
  EXPECT_FALSE(cbFired);
  // cb not expected to be consumed. So retCallback should be same function
  EXPECT_TRUE(retCallback);
  retCallback(std::move(hdl));
  EXPECT_TRUE(cbFired);
}

TEST(ItemHandleTest, handleState) {
  testing::NiceMock<TestAllocator> t;

  {
    TestItemHandle hdl{};
    EXPECT_FALSE(hdl.wentToNvm());
    EXPECT_FALSE(hdl.wasExpired());

    t.markExpired(hdl);
    EXPECT_TRUE(hdl.wasExpired());
  }

  {
    auto flashHdl = t.getHandle();
    EXPECT_FALSE(flashHdl.isReady());

    TestItemHandle toSetHdl{};
    t.setHandle(flashHdl, std::move(toSetHdl));

    EXPECT_TRUE(flashHdl.isReady());
    EXPECT_FALSE(flashHdl.wasExpired());
  }

  {
    auto flashHdl = t.getHandle();
    EXPECT_FALSE(flashHdl.isReady());

    TestItemHandle toSetHdl{};
    t.markExpired(toSetHdl);
    t.setHandle(flashHdl, std::move(toSetHdl));

    EXPECT_TRUE(flashHdl.isReady());
    EXPECT_TRUE(flashHdl.wasExpired());
  }
}
} // namespace cachelib
} // namespace facebook
