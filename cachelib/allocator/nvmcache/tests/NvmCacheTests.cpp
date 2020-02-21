#include <set>
#include <thread>

#include <folly/Random.h>
#include <gtest/gtest.h>

#include "cachelib/allocator/nvmcache/tests/NvmTestBase.h"

namespace facebook {
namespace cachelib {
namespace tests {

typedef ::testing::Types<NavyDipper> DipperBackendTypes;
TYPED_TEST_CASE(NvmCacheTest, DipperBackendTypes);

namespace {
std::string genRandomStr(size_t len) {
  std::string text;
  text.reserve(len);
  for (unsigned int i = 0; i < len; i++) {
    text += static_cast<char>(folly::Random::rand32());
  }
  return text;
}
} // namespace

TYPED_TEST(NvmCacheTest, BasicGet) {
  auto& nvm = this->cache();
  auto pid = this->poolId();

  std::string key = "blah";

  ASSERT_FALSE(this->checkKeyExists(key, false /* ramOnly */));

  {
    auto it = nvm.allocate(pid, key, 100);
    nvm.insertOrReplace(it);
  }

  ASSERT_TRUE(this->checkKeyExists(key, true /* ramOnly */));
  ASSERT_TRUE(this->checkKeyExists(key, false /* ramOnly */));
}

TYPED_TEST(NvmCacheTest, EvictToDipperGet) {
  auto& nvm = this->cache();
  auto pid = this->poolId();

  const auto evictBefore = this->evictionCount();
  const int nKeys = 1024;

  for (unsigned int i = 0; i < nKeys; i++) {
    auto key = folly::sformat("key{}", i);
    auto it = nvm.allocate(pid, key, 15 * 1024);
    ASSERT_NE(nullptr, it);
    nvm.insertOrReplace(it);
  }

  const auto nEvictions = this->evictionCount() - evictBefore;
  ASSERT_LT(0, nEvictions);

  // read from reverse to no cause evictions to dipper
  for (unsigned int i = nKeys + 100; i-- > 0;) {
    unsigned int index = i - 1;
    auto key = folly::sformat("key{}", index);
    auto hdl = this->fetch(key, false /* ramOnly */);
    hdl.wait();
    if (index < nKeys) {
      ASSERT_NE(nullptr, hdl);
      // First nEvictions keys should have nvm clean bit set since
      // we load it from nvm
      const auto isClean = hdl->isNvmClean();
      if (index < nEvictions) {
        ASSERT_TRUE(isClean);
        ASSERT_TRUE(hdl.wentToNvm());
      } else {
        ASSERT_FALSE(isClean);
      }
    } else {
      ASSERT_EQ(nullptr, hdl);
    }
  }
}

TYPED_TEST(NvmCacheTest, EvictToDipperGetCheckCtime) {
  auto& nvm = this->cache();
  auto pid = this->poolId();

  const auto evictBefore = this->evictionCount();
  const int nKeys = 1024;

  std::unordered_map<std::string, uint32_t> keyToCtime;
  for (unsigned int i = 0; i < nKeys; i++) {
    auto key = std::string("blah") + folly::to<std::string>(i);
    auto it = nvm.allocate(pid, key, 15 * 1024);
    ASSERT_NE(nullptr, it);
    nvm.insertOrReplace(it);
    keyToCtime.insert({key, it->getCreationTime()});
  }

  const auto nEvictions = this->evictionCount() - evictBefore;
  ASSERT_LT(0, nEvictions);

  /* sleep override */ std::this_thread::sleep_for(std::chrono::seconds(5));

  // read from reverse to no cause evictions to dipper
  for (unsigned int i = nKeys - 1; i > 0; i--) {
    auto key = std::string("blah") + folly::to<std::string>(i - 1);
    auto hdl = this->fetch(key, false /* ramOnly */);
    hdl.wait();
    XDCHECK(hdl);
    ASSERT_EQ(hdl->getCreationTime(), keyToCtime[key]);
  }
}

TYPED_TEST(NvmCacheTest, EvictToDipperExpired) {
  auto& nvm = this->cache();
  auto pid = this->poolId();

  const uint32_t ttl = 5; // 5 second ttl
  std::string key = "blah";
  {
    auto it = nvm.allocate(pid, key, 15 * 1024, ttl);
    ASSERT_NE(nullptr, it);
    nvm.insertOrReplace(it);

    /* sleep override */ std::this_thread::sleep_for(
        std::chrono::seconds(ttl + 1));
    ASSERT_TRUE(it->isExpired());
  }

  this->pushToNvmCacheFromRamForTesting(key);
  this->removeFromRamForTesting(key);
  // should not be present in RAM since we pushed it to nvmcache
  ASSERT_FALSE(this->checkKeyExists(key, true /*ram Only*/));

  // should not have been in nvmcache since it expired.
  ASSERT_FALSE(this->checkKeyExists(key, false /* ram Only */));
}

TYPED_TEST(NvmCacheTest, FilterCb) {
  auto& config = this->getConfig();
  std::string failKey = "failure";
  std::string successKey = "success";

  std::string unEvictableKey = "unEvictable";

  unsigned int filterSeen = 0;
  unsigned int unEvictableSeen = 0;
  config.setNvmCacheFilterCallback(
      [&](const typename AllocatorT::Item& it,
          folly::Range<typename AllocatorT::ChainedItemIter>) {
        ++filterSeen;

        if (it.getKey() == unEvictableKey) {
          ++unEvictableSeen;
        }
        return it.getKey() != successKey;
      });

  std::set<std::string> evictedKeys;
  config.setRemoveCallback([&](typename AllocatorT::RemoveCbData c) {
    evictedKeys.insert(c.item.getKey().str());
  });

  auto& nvm = this->makeCache();
  auto pid = this->poolId();

  size_t allocSize = 15 * 1024;
  {
    auto it = nvm.allocate(pid, successKey, allocSize);
    auto it2 = nvm.allocate(pid, failKey, allocSize);
    auto it3 = nvm.allocatePermanent(pid, unEvictableKey, allocSize);
    ASSERT_NE(nullptr, it2);
    ASSERT_NE(nullptr, it);
    ASSERT_NE(nullptr, it3);
    nvm.insertOrReplace(it);
    nvm.insertOrReplace(it2);
    nvm.insertOrReplace(it3);
  }

  auto keysFound = [&]() {
    return evictedKeys.find(successKey) != evictedKeys.end() &&
           evictedKeys.find(failKey) != evictedKeys.end();
  };

  int i = 0;
  while (!keysFound()) {
    auto it = nvm.allocate(pid, std::to_string(i++), allocSize);
    nvm.insertOrReplace(it);
  }

  EXPECT_EQ(2, filterSeen);
  EXPECT_EQ(0, unEvictableSeen);

  // should not be present in RAM since we evicted it
  EXPECT_FALSE(this->checkKeyExists(successKey, true /*ram Only*/));
  EXPECT_FALSE(this->checkKeyExists(failKey, true /*ram Only*/));
  EXPECT_TRUE(this->checkKeyExists(unEvictableKey, true /*ram Only*/));

  // success one should be in nvmcache, failure one should not
  EXPECT_FALSE(this->checkKeyExists(failKey, false /* ram Only */));
  EXPECT_TRUE(this->checkKeyExists(successKey, false /* ram Only */));
  EXPECT_TRUE(this->checkKeyExists(unEvictableKey, false /*ram Only*/));
}

// Unevictable items should be kept in both dram and nvm and maintain the
// state on dram accordingly
TYPED_TEST(NvmCacheTest, UnEvictable) {
  auto& nvm = this->makeCache();
  auto pid = this->poolId();
  auto key = "foo";

  size_t allocSize = 15 * 1024;
  auto it = nvm.allocatePermanent(pid, key, allocSize);
  ASSERT_NE(nullptr, it);
  nvm.insertOrReplace(it);
  ASSERT_TRUE(it->isNvmClean());
  auto res = this->inspectCache(key);
  EXPECT_NE(nullptr, res.first);
  EXPECT_NE(nullptr, res.second);
}

TYPED_TEST(NvmCacheTest, ReadFromDipperExpired) {
  auto& nvm = this->cache();
  auto pid = this->poolId();

  const uint32_t ttl = 5; // 5 second ttl
  std::string key = "blah";
  {
    auto it = nvm.allocate(pid, key, 15 * 1024, ttl);
    ASSERT_NE(nullptr, it);
    nvm.insertOrReplace(it);
    ASSERT_FALSE(it->isExpired());
  }

  ASSERT_TRUE(this->checkKeyExists(key, true /*ram Only*/));

  this->pushToNvmCacheFromRamForTesting(key);
  this->removeFromRamForTesting(key);
  // should not be present in RAM since we pushed it to nvmcache
  ASSERT_FALSE(this->checkKeyExists(key, true /*ram Only*/));

  /* sleep override */ std::this_thread::sleep_for(
      std::chrono::seconds(ttl + 1));

  // reading an expired object from nvmcache must not insert it into cache.
  ASSERT_FALSE(this->checkKeyExists(key, false /* ram only */));
  {
    auto it = this->fetch(key, false /* ram Only */);
    ASSERT_EQ(nullptr, it);
    ASSERT_TRUE(it.wentToNvm());
    ASSERT_TRUE(it.wasExpired());
  }
}

TYPED_TEST(NvmCacheTest, Delete) {
  auto& nvm = this->cache();
  auto pid = this->poolId();

  const int nKeys = 1024;

  for (unsigned int i = 0; i < nKeys; i++) {
    auto key = std::string("blah") + folly::to<std::string>(i);
    auto it = nvm.allocate(pid, key, 15 * 1024);
    ASSERT_NE(nullptr, it);
    nvm.insertOrReplace(it);
  }

  // fetch all of them
  for (unsigned int i = 0; i < nKeys; i++) {
    auto key = std::string("blah") + folly::to<std::string>(i);
    ASSERT_TRUE(this->checkKeyExists(key, false /* ramOnly */));
  }

  for (unsigned int i = 0; i < nKeys; i++) {
    auto key = std::string("blah") + folly::to<std::string>(i);
    nvm.remove(key);
  }

  // fetch should fail for all
  for (unsigned int i = 0; i < nKeys; i++) {
    auto key = std::string("blah") + folly::to<std::string>(i);
    ASSERT_FALSE(this->checkKeyExists(key, false /* ramOnly */));
  }
}

TYPED_TEST(NvmCacheTest, InsertOrReplace) {
  auto& nvm = this->cache();
  auto pid = this->poolId();

  std::string key = "blah";

  {
    auto it = nvm.allocate(pid, key, 100);
    ASSERT_NE(nullptr, it);
    *(int*)it->getMemory() = 0xdeadbeef;
    nvm.insertOrReplace(it);
  }

  // verify contents after fetching

  {
    auto it = this->fetch(key, false);
    auto val = *(int*)it->getMemory();
    ASSERT_EQ(0xdeadbeef, val);
  }

  // replace with new content

  {
    auto it = nvm.allocate(pid, key, 100);
    ASSERT_NE(nullptr, it);
    *(int*)it->getMemory() = 0x5a5a5a5a;
    nvm.insertOrReplace(it);
  }

  // verify contents after fetching

  {
    auto it = this->fetch(key, false);
    auto val = *(int*)it->getMemory();
    ASSERT_EQ(0x5a5a5a5a, val);
  }
}

TYPED_TEST(NvmCacheTest, Permanent) {
  auto& nvm = this->cache();
  auto pid = this->poolId();

  std::string key = "foobar";
  {
    auto hdl = nvm.allocatePermanent(pid, key, 100);
    ASSERT_TRUE(hdl->isUnevictable());
    nvm.insertOrReplace(hdl);
  }

  ASSERT_TRUE(nvm.find(key)->isUnevictable());

  this->removeFromRamForTesting(key);
  {
    auto hdl = nvm.find(key);
    hdl.wait();
    ASSERT_TRUE(hdl);
    ASSERT_TRUE(hdl->isUnevictable());
  }

  this->removeFromNvmForTesting(key);
  {
    auto hdl = nvm.find(key);
    ASSERT_TRUE(hdl);
    ASSERT_TRUE(hdl->isUnevictable());
  }

  this->removeFromRamForTesting(key);
  ASSERT_FALSE(nvm.find(key));
}

TYPED_TEST(NvmCacheTest, ConcurrentFills) {
  auto& nvm = this->cache();
  auto pid = this->poolId();

  const int nKeys = 1024;

  for (unsigned int i = 0; i < nKeys; i++) {
    auto key = std::string("blah") + folly::to<std::string>(i);
    auto it = nvm.allocate(pid, key, 15 * 1024);
    ASSERT_NE(nullptr, it);
    *((int*)it->getMemory()) = i;
    nvm.insertOrReplace(it);
  }

  auto doConcurrentFetch = [&](int id) {
    auto key = std::string("blah") + folly::to<std::string>(id);
    std::vector<std::thread> thr;
    for (unsigned int j = 0; j < 50; j++) {
      thr.push_back(std::thread([&]() {
        auto hdl = nvm.find(key, AccessMode::kRead);
        hdl.wait();
        ASSERT_NE(hdl, nullptr);
        ASSERT_EQ(id, *(int*)hdl->getMemory());
      }));
    }
    for (unsigned int j = 0; j < 50; j++) {
      thr[j].join();
    }
  };

  for (unsigned int i = 0; i < 10; i++) {
    doConcurrentFetch(i);
  }
}

TYPED_TEST(NvmCacheTest, NvmClean) {
  auto& nvm = this->cache();
  auto pid = this->poolId();

  auto evictBefore = this->evictionCount();
  auto putsBefore = this->getStats().numNvmPuts;
  const int nKeys = 1024;
  const uint32_t allocSize = 15 * 1024;

  for (unsigned int i = 0; i < nKeys; i++) {
    auto key = std::string("blah") + folly::to<std::string>(i);
    auto it = nvm.allocate(pid, key, allocSize);
    ASSERT_NE(nullptr, it);
    nvm.insertOrReplace(it);
  }

  auto nEvictions = this->evictionCount() - evictBefore;
  auto nPuts = this->getStats().numNvmPuts - putsBefore;
  ASSERT_LT(0, nEvictions);
  ASSERT_EQ(nPuts, nEvictions);
  evictBefore = this->evictionCount();
  putsBefore = this->getStats().numNvmPuts;

  // read everything again. This should churn and cause the current ones to be
  // evicted to nvmcache.
  size_t numClean = 0;
  for (unsigned int i = nKeys - 1; i > 0; i--) {
    auto key = std::string("blah") + folly::to<std::string>(i - 1);
    bool missInRam = !this->checkKeyExists(key, true /* ramOnly */);
    auto hdl = this->fetch(key, false /* ramOnly */);
    hdl.wait();
    ASSERT_TRUE(hdl);
    if (missInRam) {
      ++numClean;
      ASSERT_TRUE(hdl->isNvmClean());
    }
  }

  ASSERT_LT(0, numClean);

  // we must have done evictions from ram to dipper
  nEvictions = this->evictionCount() - evictBefore;
  nPuts = this->getStats().numNvmPuts - putsBefore;
  ASSERT_LT(0, nEvictions);
  ASSERT_EQ(nKeys - numClean, nPuts);

  putsBefore = this->getStats().numNvmPuts;
  evictBefore = this->evictionCount();

  // read everything again. This should cause everything to be clean
  for (unsigned int i = 0; i < nKeys; i++) {
    auto key = std::string("blah") + folly::to<std::string>(i);
    auto hdl = this->fetch(key, false /* ramOnly */);
    hdl.wait();
    XDCHECK(hdl);
    ASSERT_TRUE(hdl->isNvmClean());
  }
  ASSERT_EQ(0, this->getStats().numNvmEvictions);

  // we must have done evictions from ram to dipper
  nEvictions = this->evictionCount() - evictBefore;
  nPuts = this->getStats().numNvmPuts - putsBefore;
  ASSERT_LT(0, nEvictions);
  ASSERT_EQ(0, nPuts);
}

// put nvmclean entries in cache and then mark them as nvmRewrite. this should
// write them to nvmcache.
TYPED_TEST(NvmCacheTest, NvmEvicted) {
  auto& nvm = this->cache();
  auto pid = this->poolId();

  const int nKeys = 1024;
  const uint32_t allocSize = 15 * 1024;

  for (unsigned int i = 0; i < nKeys; i++) {
    auto key = std::string("blah") + folly::to<std::string>(i);
    auto it = nvm.allocate(pid, key, allocSize);
    ASSERT_NE(nullptr, it);
    nvm.insertOrReplace(it);
  }

  // read everything again. This should churn and cause the current ones to be
  // evicted to nvmcache.
  for (unsigned int i = nKeys - 1; i > 0; i--) {
    auto key = std::string("blah") + folly::to<std::string>(i - 1);
    auto hdl = this->fetch(key, false /* ramOnly */);
    hdl.wait();
    ASSERT_TRUE(hdl);
  }

  // read everything again. This should cause everything to be clean
  for (unsigned int i = 0; i < nKeys; i++) {
    auto key = std::string("blah") + folly::to<std::string>(i);
    auto hdl = this->fetch(key, false /* ramOnly */);
    hdl.wait();
    XDCHECK(hdl);
    ASSERT_TRUE(hdl->isNvmClean());
  }

  auto putsBefore = this->getStats().numNvmPuts;
  auto evictBefore = this->evictionCount();
  for (unsigned int i = 0; i < nKeys; i++) {
    auto key = std::string("blah") + folly::to<std::string>(i);
    auto hdl = this->fetch(key, false /* ramOnly */);
    hdl.wait();
    XDCHECK(hdl);
    ASSERT_TRUE(hdl->isNvmClean());
  }

  // nothing should be put since everything is nvmclean
  ASSERT_EQ(this->getStats().numNvmPuts, putsBefore);
  ASSERT_EQ(this->evictionCount() - evictBefore, nKeys);

  putsBefore = this->getStats().numNvmPuts;
  evictBefore = this->evictionCount();

  for (unsigned int i = 0; i < nKeys; i++) {
    auto key = std::string("blah") + folly::to<std::string>(i);
    auto hdl = this->fetch(key, false /* ramOnly */);
    hdl.wait();
    XDCHECK(hdl);
    ASSERT_TRUE(hdl->isNvmClean());
    hdl->markNvmEvicted();
  }

  for (unsigned int i = 0; i < nKeys; i++) {
    auto key = std::string("blah") + folly::to<std::string>(i);
    auto hdl = this->fetch(key, false /* ramOnly */);
    hdl.wait();
    XDCHECK(hdl);
    ASSERT_TRUE(hdl->isNvmClean());
  }

  // we must have done evictions from ram to dipper
  ASSERT_EQ(this->evictionCount() - evictBefore, 2 * nKeys);
  ASSERT_EQ(nKeys, this->getStats().numNvmPuts - putsBefore);
  ASSERT_EQ(nKeys, this->getStats().numNvmPutFromClean);
}

TYPED_TEST(NvmCacheTest, InspectCache) {
  auto& cache = this->cache();
  auto pid = this->poolId();

  std::string key = "blah";
  std::string val = "foobar";
  {
    auto it = cache.allocate(pid, key, val.length());
    ASSERT_NE(nullptr, it);
    cache.insertOrReplace(it);
    ::memcpy(it->getMemory(), val.data(), val.length());
  }

  // item is only in RAM
  {
    auto res = this->inspectCache(key);
    // must exist in RAM
    ASSERT_NE(nullptr, res.first);
    ASSERT_EQ(::memcmp(res.first->getMemory(), val.data(), val.length()), 0);

    // must not be in nvmcache
    ASSERT_EQ(nullptr, res.second);
  }

  this->pushToNvmCacheFromRamForTesting(key);
  this->removeFromRamForTesting(key);

  {
    auto res = this->inspectCache(key);
    // must not exist in RAM
    ASSERT_EQ(nullptr, res.first);

    // must be in nvmcache
    ASSERT_NE(nullptr, res.second);
    ASSERT_EQ(::memcmp(res.second->getMemory(), val.data(), val.length()), 0);

    // we should not have brought anything into RAM.
    ASSERT_EQ(nullptr, this->inspectCache(key).first);
  }

  // remove from NVM
  this->removeFromNvmForTesting(key);
  {
    auto res = this->inspectCache(key);
    // must not exist in RAM
    ASSERT_EQ(nullptr, res.first);
    // must not be in nvmcache
    ASSERT_EQ(nullptr, res.second);
  }
}

// same as above, but uses large items using chained items
TYPED_TEST(NvmCacheTest, InspectCacheLarge) {
  auto& config = this->getConfig();
  config.configureChainedItems();
  auto& cache = this->makeCache();
  auto pid = this->poolId();

  std::string key = "blah";
  const size_t allocSize = 19 * 1024;
  int nChained = 100;
  std::string val = genRandomStr(allocSize);
  {
    auto it = cache.allocate(pid, key, val.length());
    ASSERT_NE(nullptr, it);
    cache.insertOrReplace(it);
    ::memcpy(it->getMemory(), val.data(), val.length());
    for (int i = 0; i < nChained; i++) {
      auto chainedIt = cache.allocateChainedItem(it, val.length());
      ASSERT_TRUE(chainedIt);
      ::memcpy(chainedIt->getMemory(), val.data(), val.length());
      cache.addChainedItem(it, std::move(chainedIt));
    }
  }

  // item is only in RAM
  {
    auto res = this->inspectCache(key);
    // must exist in RAM
    ASSERT_NE(nullptr, res.first);
    ASSERT_EQ(::memcmp(res.first->getMemory(), val.data(), val.length()), 0);

    // must not be in nvmcache
    ASSERT_EQ(nullptr, res.second);
  }

  this->pushToNvmCacheFromRamForTesting(key);
  this->removeFromRamForTesting(key);

  {
    auto res = this->inspectCache(key);
    // must not exist in RAM
    ASSERT_EQ(nullptr, res.first);

    // must be in nvmcache
    ASSERT_NE(nullptr, res.second);
    ASSERT_EQ(::memcmp(res.second->getMemory(), val.data(), val.length()), 0);

    {
      auto allocs = cache.viewAsChainedAllocs(res.second);
      for (const auto& curr : allocs.getChain()) {
        ASSERT_EQ(0, ::memcmp(curr.getMemory(), val.data(), val.length()));
      }
    }

    // we should not have brought anything into RAM.
    ASSERT_EQ(nullptr, this->inspectCache(key).first);
  }

  // remove from NVM
  this->removeFromNvmForTesting(key);
  {
    auto res = this->inspectCache(key);
    // must not exist in RAM
    ASSERT_EQ(nullptr, res.first);
    // must not be in nvmcache
    ASSERT_EQ(nullptr, res.second);
  }
}

TYPED_TEST(NvmCacheTest, WarmRoll) {
  this->convertToShmCache();
  std::string key = "blah";
  {
    auto& nvm = this->cache();
    auto pid = this->poolId();

    {
      auto it = nvm.allocate(pid, key, 100);
      nvm.insertOrReplace(it);
    }

    ASSERT_TRUE(this->checkKeyExists(key, true /* ramOnly */));
    ASSERT_TRUE(this->checkKeyExists(key, false /* ramOnly */));

    ASSERT_TRUE(this->pushToNvmCacheFromRamForTesting(key));
    this->removeFromRamForTesting(key);
    ASSERT_FALSE(this->checkKeyExists(key, true /* ramOnly */));
  }

  this->warmRoll();
  {
    auto res = this->inspectCache(key);
    // key was removed from ram before we warm rolled
    ASSERT_FALSE(res.first);

    // key is present in nvmcache
    ASSERT_TRUE(res.second);

    // fetch from nvmcache on warm roll
    ASSERT_TRUE(this->checkKeyExists(key, false /* ramOnly */));
  }
}

TYPED_TEST(NvmCacheTest, ColdRoll) {
  this->convertToShmCache();
  std::string key = "blah";
  {
    auto& nvm = this->cache();
    auto pid = this->poolId();

    {
      auto it = nvm.allocate(pid, key, 100);
      nvm.insertOrReplace(it);
    }

    ASSERT_TRUE(this->checkKeyExists(key, true /* ramOnly */));
    ASSERT_TRUE(this->checkKeyExists(key, false /* ramOnly */));

    ASSERT_TRUE(this->pushToNvmCacheFromRamForTesting(key));
    ASSERT_TRUE(this->checkKeyExists(key, true /* ramOnly */));
  }

  this->coldRoll();
  {
    // we cold rolled
    ASSERT_FALSE(this->checkKeyExists(key, true /* ramOnly */));

    // fetch from nvmcache should succeed.
    ASSERT_TRUE(this->checkKeyExists(key, false /* ramOnly */));
  }
}

TYPED_TEST(NvmCacheTest, ColdRollDropNvmCache) {
  this->getConfig().setDropNvmCacheOnShmNew(true);
  this->convertToShmCache();
  std::string key = "blah";
  {
    auto& nvm = this->cache();
    auto pid = this->poolId();

    {
      auto it = nvm.allocate(pid, key, 100);
      nvm.insertOrReplace(it);
    }

    ASSERT_TRUE(this->checkKeyExists(key, true /* ramOnly */));
    ASSERT_TRUE(this->checkKeyExists(key, false /* ramOnly */));

    ASSERT_TRUE(this->pushToNvmCacheFromRamForTesting(key));
    ASSERT_TRUE(this->checkKeyExists(key, true /* ramOnly */));
  }

  this->coldRoll();
  {
    // we cold rolled
    ASSERT_FALSE(this->checkKeyExists(key, true /* ramOnly */));

    // fetch from nvmcache should also fail
    ASSERT_FALSE(this->checkKeyExists(key, false /* ramOnly */));
  }
}

TYPED_TEST(NvmCacheTest, IceRoll) {
  this->convertToShmCache();
  std::string key1 = "blah1";
  std::string key2 = "blah2";
  {
    auto& nvm = this->cache();
    auto pid = this->poolId();

    {
      auto it1 = nvm.allocate(pid, key1, 100);
      nvm.insertOrReplace(it1);
      auto it2 = nvm.allocate(pid, key2, 100);
      nvm.insertOrReplace(it2);
    }

    ASSERT_TRUE(this->checkKeyExists(key1, true /* ramOnly */));
    ASSERT_TRUE(this->checkKeyExists(key2, true /* ramOnly */));
    ASSERT_TRUE(this->checkKeyExists(key1, false /* ramOnly */));
    ASSERT_TRUE(this->checkKeyExists(key2, false /* ramOnly */));

    ASSERT_TRUE(this->pushToNvmCacheFromRamForTesting(key1));
    ASSERT_TRUE(this->pushToNvmCacheFromRamForTesting(key2));

    this->removeFromRamForTesting(key1);

    ASSERT_FALSE(this->checkKeyExists(key1, true /* ramOnly */));
    ASSERT_TRUE(this->checkKeyExists(key2, true /* ramOnly */));
  }

  this->iceRoll();
  {
    // we preserved memory but key1 was removed from ram.
    ASSERT_FALSE(this->checkKeyExists(key1, true /* ramOnly */));

    // key2 was still in ram.
    ASSERT_TRUE(this->checkKeyExists(key2, true /* ramOnly */));
    this->removeFromRamForTesting(key2);

    // fetch from nvmcache should fail for both
    ASSERT_FALSE(this->checkKeyExists(key2, false /* ramOnly */));
    ASSERT_FALSE(this->checkKeyExists(key2, false /* ramOnly */));
  }
}

TYPED_TEST(NvmCacheTest, IceColdRoll) {
  this->convertToShmCache();
  std::string key1 = "blah1";
  std::string key2 = "blah2";
  {
    auto& nvm = this->cache();
    auto pid = this->poolId();

    {
      auto it1 = nvm.allocate(pid, key1, 100);
      nvm.insertOrReplace(it1);
      auto it2 = nvm.allocate(pid, key2, 100);
      nvm.insertOrReplace(it2);
    }

    ASSERT_TRUE(this->checkKeyExists(key1, true /* ramOnly */));
    ASSERT_TRUE(this->checkKeyExists(key2, true /* ramOnly */));
    ASSERT_TRUE(this->checkKeyExists(key1, false /* ramOnly */));
    ASSERT_TRUE(this->checkKeyExists(key2, false /* ramOnly */));

    ASSERT_TRUE(this->pushToNvmCacheFromRamForTesting(key1));
    ASSERT_TRUE(this->pushToNvmCacheFromRamForTesting(key2));

    this->removeFromRamForTesting(key1);

    ASSERT_FALSE(this->checkKeyExists(key1, true /* ramOnly */));
    ASSERT_TRUE(this->checkKeyExists(key2, true /* ramOnly */));
  }

  this->iceColdRoll();

  {
    // we lost memory
    ASSERT_FALSE(this->checkKeyExists(key1, true /* ramOnly */));
    ASSERT_FALSE(this->checkKeyExists(key2, true /* ramOnly */));

    // fetch from nvmcache should fail as well since we did ice-cold
    ASSERT_FALSE(this->checkKeyExists(key2, true /* ramOnly */));
    ASSERT_FALSE(this->checkKeyExists(key1, true /* ramOnly */));
  }
}

// this test assumes that by default, the config we use does not move on slab
// release.
TYPED_TEST(NvmCacheTest, EvictSlabRelease) {
  auto& cache = this->cache();
  auto pid = this->poolId();

  // insert allocations into cache. Release some slabs corresponding to the
  // keys  and make sure that the keys are evicted to nvmcache.
  std::vector<std::string> keys;
  int nKeys = 100;
  // we have only one alloc size that is 20K in the config.
  uint32_t size = 1024 * 19;
  for (int i = 0; i < nKeys; i++) {
    std::string key = "key" + std::to_string(i);
    std::string val = "val" + std::to_string(i);
    auto handle = cache.allocate(pid, key, size);
    cache.insertOrReplace(handle);
    if (handle) {
      std::memcpy(handle->getMemory(), val.data(), val.size());
      keys.push_back(std::move(key));
    }
  }

  // everything must be in ram and nothing in nvmcache
  for (const auto& key : keys) {
    auto res = cache.inspectCache(key);
    ASSERT_NE(res.first, nullptr);
    ASSERT_EQ(res.second, nullptr);
  }

  ASSERT_EQ(0, this->evictionCount());
  // pick a key and see if it exists in RAM
  for (const auto& key : keys) {
    auto handle = this->fetch(key, true /* ramOnly */);
    if (handle) {
      void* mem = handle.get();
      handle.reset();
      this->releaseSlabFor(mem);
      ASSERT_FALSE(this->checkKeyExists(key, true /* ramOnly */));
    }
  }

  // everything must be in nvmcache.
  for (const auto& key : keys) {
    auto res = cache.inspectCache(key);
    ASSERT_EQ(res.first, nullptr);
    ASSERT_NE(res.second, nullptr);
  }
}

// allocate an item that has extra bytes in the end that we use and make sure
// that we save and restore that
TYPED_TEST(NvmCacheTest, TrailingAllocSize) {
  auto& cache = this->cache();
  auto pid = this->poolId();

  const uint32_t allocSize = 15 * 1024 - 5;
  std::string key = "foobar";
  std::string text;
  {
    auto it = cache.allocate(pid, key, allocSize);
    ASSERT_NE(nullptr, it);
    const size_t extraBytes = cache.getUsableSize(*it) - allocSize;
    // scribble some random stuff into the trailing space
    ASSERT_GT(extraBytes, text.size());
    text = genRandomStr(extraBytes);
    std::memcpy(reinterpret_cast<char*>(it->getMemory()) + allocSize,
                text.data(),
                text.size());

    cache.insertOrReplace(it);
  }

  this->pushToNvmCacheFromRamForTesting(key);
  this->removeFromRamForTesting(key);

  auto it = this->fetch(key, false /* ramOnly */);
  ASSERT_EQ(
      0,
      std::memcmp(reinterpret_cast<const char*>(it->getMemory()) + allocSize,
                  text.data(),
                  text.size()));
}

TYPED_TEST(NvmCacheTest, ChainedItems) {
  auto& config = this->getConfig();
  config.configureChainedItems();
  auto& cache = this->makeCache();
  auto pid = this->poolId();

  const uint32_t allocSize = 15 * 1024 - 5;
  const uint32_t nChained = folly::Random::rand32(2, 20);
  // int nChained = 2;
  std::string key = "foobar";
  std::vector<std::string> vals;
  {
    auto it = cache.allocate(pid, key, allocSize);
    ASSERT_NE(nullptr, it);

    auto fillItem = [&](Item& item) {
      size_t fullSize = cache.getUsableSize(item);
      const auto text = genRandomStr(fullSize);
      vals.push_back(text);
      std::memcpy(
          reinterpret_cast<char*>(item.getMemory()), text.data(), text.size());
    };

    fillItem(*it);

    for (unsigned int i = 0; i < nChained; i++) {
      auto chainedIt =
          cache.allocateChainedItem(it, folly::Random::rand32(100, allocSize));
      ASSERT_TRUE(chainedIt);
      fillItem(*chainedIt);
      cache.addChainedItem(it, std::move(chainedIt));
    }

    ASSERT_EQ(vals.size(), nChained + 1);
    cache.insertOrReplace(it);
  }

  auto verifyItem = [&](const Item& item, const std::string& text) {
    ASSERT_EQ(cache.getUsableSize(item), text.size()) << item.toString();
    ASSERT_EQ(0, std::memcmp(item.getMemory(), text.data(), text.size()))
        << item.toString();
  };

  auto verifyChainedAllcos = [&](const ItemHandle& hdl) {
    auto allocs = cache.viewAsChainedAllocs(hdl);
    verifyItem(allocs.getParentItem(), vals[0]);

    int index = 0;
    for (const auto& c : allocs.getChain()) {
      verifyItem(c, vals[nChained - index++]);
    }
  };

  {
    auto it = this->fetch(key, true /* ramOnly*/);
    verifyChainedAllcos(it);
  }

  this->pushToNvmCacheFromRamForTesting(key);
  this->removeFromRamForTesting(key);

  auto it = this->fetch(key, false /* ramOnly */);
  ASSERT_TRUE(it);
  verifyChainedAllcos(it);
}

TYPED_TEST(NvmCacheTest, ChainedItemsModifyAccessible) {
  auto& config = this->getConfig();
  config.configureChainedItems();
  auto& cache = this->makeCache();
  auto pid = this->poolId();

  const uint32_t allocSize = 15 * 1024 - 5;
  std::string key = "foobar";
  std::vector<std::string> vals;
  {
    auto it = cache.allocate(pid, key, allocSize);
    ASSERT_NE(nullptr, it);

    auto fillItem = [&](Item& item) {
      size_t fullSize = cache.getUsableSize(item);
      const auto text = genRandomStr(fullSize);
      vals.push_back(text);
      std::memcpy(
          reinterpret_cast<char*>(item.getMemory()), text.data(), text.size());
    };

    fillItem(*it);
    cache.insertOrReplace(it);
    {
      auto chainedIt =
          cache.allocateChainedItem(it, folly::Random::rand32(100, allocSize));
      ASSERT_TRUE(chainedIt);
      fillItem(*chainedIt);
      cache.addChainedItem(it, std::move(chainedIt));
    }
    {
      this->pushToNvmCacheFromRamForTesting(key);
      this->removeFromRamForTesting(key);
    }
    // Read everything again
    {
      auto hdl = this->fetch(key, false /* ramOnly*/);
      hdl.wait();
      ASSERT_TRUE(hdl->isNvmClean());
      {
        auto chainedIt = cache.allocateChainedItem(
            hdl, folly::Random::rand32(100, allocSize));
        ASSERT_TRUE(chainedIt);
        fillItem(*chainedIt);
        cache.addChainedItem(hdl, std::move(chainedIt));
      }
      ASSERT_EQ(vals.size(), 3);
    }
    auto verifyItem = [&](const Item& item, const std::string& text) {
      ASSERT_EQ(cache.getUsableSize(item), text.size()) << item.toString();
      ASSERT_EQ(0, std::memcmp(item.getMemory(), text.data(), text.size()))
          << item.toString();
    };

    auto verifyChainedAllcos = [&](const ItemHandle& hdl, uint32_t nChained) {
      auto allocs = cache.viewAsChainedAllocs(hdl);
      verifyItem(allocs.getParentItem(), vals[0]);

      int index = 0;
      for (const auto& c : allocs.getChain()) {
        verifyItem(c, vals[nChained - index++]);
      }
    };
    {
      auto res = this->inspectCache(key);
      EXPECT_NE(nullptr, res.first);
      verifyChainedAllcos(res.first, 2);
      if (nullptr != res.second) {
        verifyChainedAllcos(res.second, 2);
      }
    }

    // popChained Item test
    {
      this->pushToNvmCacheFromRamForTesting(key);
      this->removeFromRamForTesting(key);
    }
    // Read everything again
    {
      auto hdl = this->fetch(key, false /* ramOnly*/);
      hdl.wait();
      ASSERT_TRUE(hdl->isNvmClean());
      {
        auto chainedIt = cache.popChainedItem(hdl);
        ASSERT_TRUE(chainedIt);
        vals.pop_back();
      }
      ASSERT_EQ(vals.size(), 2);
    }

    {
      auto res = this->inspectCache(key);
      EXPECT_NE(nullptr, res.first);
      verifyChainedAllcos(res.first, 1);
      if (nullptr != res.second) {
        verifyChainedAllcos(res.second, 1);
      }
    }

    // replaceChained Item test
    {
      this->pushToNvmCacheFromRamForTesting(key);
      this->removeFromRamForTesting(key);
    }

    // Read everything again
    {
      auto hdl = this->fetch(key, false /* ramOnly*/);
      hdl.wait();
      ASSERT_TRUE(hdl->isNvmClean());

      vals.pop_back();
      auto newItemHandle =
          cache.allocateChainedItem(hdl, folly::Random::rand32(100, allocSize));
      ASSERT_TRUE(newItemHandle);
      fillItem(*newItemHandle);

      {
        auto* firstChainedItem =
            cache.viewAsChainedAllocs(hdl).getNthInChain(0);
        Item& oldItem = *firstChainedItem;
        auto oldHandle =
            cache.replaceChainedItem(oldItem, std::move(newItemHandle), *hdl);
        ASSERT_TRUE(oldHandle);
      }
      ASSERT_EQ(vals.size(), 2);
    }
    {
      auto res = this->inspectCache(key);
      EXPECT_NE(nullptr, res.first);
      verifyChainedAllcos(res.first, 1);
      if (nullptr != res.second) {
        verifyChainedAllcos(res.second, 1);
      }
    }
  }
}

TYPED_TEST(NvmCacheTest, EncodeDecode) {
  auto& config = this->getConfig();
  config.configureChainedItems();
  std::unordered_map<std::string, int> callbacks;
  std::string failKey = "failure";
  config.setNvmCacheEncodeCallback(
      [&](typename AllocatorT::NvmCacheT::EncodeDecodeContext ctx) mutable {
        auto& it = ctx.item;
        auto& cnt = callbacks[it.getKey().str()];
        ++cnt;

        if (it.getKey() == failKey) {
          return false;
        }

        for (Item& item : ctx.chainedItemRange) {
          (void)item;
          ++cnt;
        }
        return true;
      });
  config.setNvmCacheDecodeCallback(
      [&](typename AllocatorT::NvmCacheT::EncodeDecodeContext ctx) mutable {
        auto& it = ctx.item;
        auto& cnt = callbacks[it.getKey().str()];
        --cnt;

        for (Item& item : ctx.chainedItemRange) {
          (void)item;
          --cnt;
        }
      });

  auto& cache = this->makeCache();
  auto pid = this->poolId();

  const uint32_t allocSize = 15 * 1024 - 5;
  // chained item
  {
    const uint32_t nChained = folly::Random::rand32(2, 20);
    std::string key = "chained";
    {
      auto it = cache.allocate(pid, key, allocSize);
      ASSERT_NE(nullptr, it);

      for (unsigned int i = 0; i < nChained; i++) {
        auto chainedIt = cache.allocateChainedItem(
            it, folly::Random::rand32(100, allocSize));
        *reinterpret_cast<int*>(chainedIt->getMemory()) = i;
        ASSERT_TRUE(chainedIt);
        cache.addChainedItem(it, std::move(chainedIt));
      }

      cache.insertOrReplace(it);
    }

    this->pushToNvmCacheFromRamForTesting(key);
    ASSERT_EQ(nChained + 1, callbacks[key]);
    this->removeFromRamForTesting(key);

    {
      auto it = this->fetch(key, true /* ramOnly */);
      ASSERT_TRUE(!it);
      ASSERT_EQ(nChained + 1, callbacks[key]);
    }
    {
      auto it = this->fetch(key, false /* ramOnly */);
      ASSERT_TRUE(it);
      ASSERT_EQ(0, callbacks[key]);
      auto allocs = cache.viewAsChainedAllocs(it);

      // verify the order and content of chain
      int i = nChained;
      for (const auto& c : allocs.getChain()) {
        ASSERT_EQ(i - 1, *reinterpret_cast<const int*>(c.getMemory()));
        i--;
      }
    }
  }

  // regular non chained item
  {
    std::string key = "regular";
    {
      auto it = cache.allocate(pid, key, allocSize);
      ASSERT_NE(nullptr, it);
      cache.insertOrReplace(it);
    }

    this->pushToNvmCacheFromRamForTesting(key);
    ASSERT_EQ(1, callbacks[key]);
    this->removeFromRamForTesting(key);

    {
      auto it = this->fetch(key, true /* ramOnly */);
      ASSERT_TRUE(!it);
      ASSERT_EQ(1, callbacks[key]);
    }
    {
      auto it = this->fetch(key, false /* ramOnly */);
      ASSERT_TRUE(it);
      ASSERT_EQ(0, callbacks[key]);
    }
  }

  // failure to encode a regular item
  {
    {
      auto it = cache.allocate(pid, failKey, allocSize);
      ASSERT_NE(nullptr, it);
      cache.insertOrReplace(it);
    }

    this->pushToNvmCacheFromRamForTesting(failKey);
    ASSERT_EQ(1, callbacks[failKey]);
    this->removeFromRamForTesting(failKey);

    {
      auto it = this->fetch(failKey, true /* ramOnly */);
      ASSERT_TRUE(!it);
      ASSERT_EQ(1, callbacks[failKey]);
    }
    {
      auto it = this->fetch(failKey, false /* ramOnly */);
      ASSERT_TRUE(!it);
      ASSERT_EQ(1, callbacks[failKey]);
    }
  }

  // failure to encode a chained item
  {
    {
      auto it = cache.allocate(pid, failKey, allocSize);
      ASSERT_NE(nullptr, it);
      int nChained = 10;
      for (int i = 0; i < nChained; i++) {
        auto chainedIt = cache.allocateChainedItem(
            it, folly::Random::rand32(100, allocSize));
        ASSERT_TRUE(chainedIt);
        cache.addChainedItem(it, std::move(chainedIt));
      }
      cache.insertOrReplace(it);
    }

    this->pushToNvmCacheFromRamForTesting(failKey);
    ASSERT_EQ(2, callbacks[failKey]);
    this->removeFromRamForTesting(failKey);

    {
      auto it = this->fetch(failKey, true /* ramOnly */);
      ASSERT_TRUE(!it);
      ASSERT_EQ(2, callbacks[failKey]);
    }
    {
      auto it = this->fetch(failKey, false /* ramOnly */);
      ASSERT_TRUE(!it);
      ASSERT_EQ(2, callbacks[failKey]);
    }
  }
}

TYPED_TEST(NvmCacheTest, Encryption) {
  auto& config = this->getConfig();
  config.configureChainedItems();

  const folly::StringPiece text = "helloworld";
  config.enableNvmCacheEncryption(
      [&](folly::ByteRange buf) -> std::unique_ptr<folly::IOBuf> {
        if (reinterpret_cast<const DipperItem*>(buf.data())->poolId() == 1) {
          return {};
        }
        auto iobuf = folly::IOBuf::create(buf.size() + text.size());
        std::memcpy(iobuf->writableBuffer(), buf.data(), buf.size());
        std::memcpy(
            iobuf->writableBuffer() + buf.size(), text.data(), text.size());
        iobuf->append(buf.size() + text.size());
        return iobuf;
      },
      [&](folly::ByteRange buf) -> std::unique_ptr<folly::IOBuf> {
        folly::StringPiece textCopy{reinterpret_cast<const char*>(buf.data()) +
                                        buf.size() - text.size(),
                                    text.size()};
        EXPECT_EQ(textCopy, text);

        if (reinterpret_cast<const DipperItem*>(buf.data())->poolId() == 2) {
          return {};
        }

        auto iobuf = folly::IOBuf::create(buf.size() - text.size());
        std::memcpy(
            iobuf->writableBuffer(), buf.data(), buf.size() - text.size());
        iobuf->append(buf.size() - text.size());
        return iobuf;
      });

  auto& cache = this->makeCache();
  auto pid = this->poolId();

  const auto encryptFailPid = cache.addPool("encrypt_fail", 4 * 1024 * 1024);
  ASSERT_EQ(1, encryptFailPid);
  const auto decryptFailPid = cache.addPool("decrypt_fail", 4 * 1024 * 1024);
  ASSERT_EQ(2, decryptFailPid);

  const uint32_t allocSize = 15 * 1024 - 5;
  // chained item
  {
    const uint32_t nChained = folly::Random::rand32(2, 20);
    std::string key = "chained";
    {
      auto it = cache.allocate(pid, key, allocSize);
      ASSERT_NE(nullptr, it);

      for (unsigned int i = 0; i < nChained; i++) {
        auto chainedIt = cache.allocateChainedItem(
            it, folly::Random::rand32(100, allocSize));
        *reinterpret_cast<int*>(chainedIt->getMemory()) = i;
        ASSERT_TRUE(chainedIt);
        cache.addChainedItem(it, std::move(chainedIt));
      }

      cache.insertOrReplace(it);
    }

    this->pushToNvmCacheFromRamForTesting(key);
    this->removeFromRamForTesting(key);

    {
      auto it = this->fetch(key, true /* ramOnly */);
      ASSERT_TRUE(!it);
    }
    {
      auto it = this->fetch(key, false /* ramOnly */);
      ASSERT_TRUE(it);
      auto allocs = cache.viewAsChainedAllocs(it);

      // verify the order and content of chain
      int i = nChained;
      for (const auto& c : allocs.getChain()) {
        ASSERT_EQ(i - 1, *reinterpret_cast<const int*>(c.getMemory()));
        i--;
      }
    }
  }

  // regular non chained item
  {
    std::string key = "regular";
    {
      auto it = cache.allocate(pid, key, allocSize);
      ASSERT_NE(nullptr, it);
      cache.insertOrReplace(it);
    }

    this->pushToNvmCacheFromRamForTesting(key);
    this->removeFromRamForTesting(key);

    {
      auto it = this->fetch(key, true /* ramOnly */);
      ASSERT_TRUE(!it);
    }
    {
      auto it = this->fetch(key, false /* ramOnly */);
      ASSERT_TRUE(it);
    }
  }

  // encrypt failure
  {
    std::string key = "regular_encrypt_failure";
    {
      auto it = cache.allocate(encryptFailPid, key, allocSize);
      ASSERT_NE(nullptr, it);
      cache.insertOrReplace(it);
    }

    this->pushToNvmCacheFromRamForTesting(key);
    this->removeFromRamForTesting(key);

    {
      auto it = this->fetch(key, true /* ramOnly */);
      ASSERT_TRUE(!it);
    }
    {
      auto it = this->fetch(key, false /* ramOnly */);
      ASSERT_TRUE(!it);
    }
    auto globalStats = cache.getGlobalCacheStats();
    EXPECT_EQ(1, globalStats.numNvmEncryptionErrors);
    EXPECT_EQ(0, globalStats.numNvmDecryptionErrors);
  }

  // decrypt failure
  {
    std::string key = "regular_decrypt_failure";
    {
      auto it = cache.allocate(decryptFailPid, key, allocSize);
      ASSERT_NE(nullptr, it);
      cache.insertOrReplace(it);
    }

    this->pushToNvmCacheFromRamForTesting(key);
    this->removeFromRamForTesting(key);

    {
      auto it = this->fetch(key, true /* ramOnly */);
      ASSERT_TRUE(!it);
    }
    {
      auto it = this->fetch(key, false /* ramOnly */);
      ASSERT_TRUE(!it);
    }
    auto globalStats = cache.getGlobalCacheStats();
    EXPECT_EQ(1, globalStats.numNvmEncryptionErrors);
    EXPECT_EQ(1, globalStats.numNvmDecryptionErrors);
  }
}

TYPED_TEST(NvmCacheTest, NvmUptime) {
  unsigned int time = 6;
  this->convertToShmCache();
  {
    /* sleep override */ std::this_thread::sleep_for(
        std::chrono::seconds(time));
    ASSERT_GE(this->getStats().nvmUpTime, time);
  }

  this->warmRoll();
  {
    // uptime must  be preserved
    /* sleep override */ std::this_thread::sleep_for(
        std::chrono::seconds(time));
    ASSERT_GE(this->getStats().nvmUpTime, 2 * time);
  }

  this->coldRoll();
  {
    // uptime must  be preserved
    /* sleep override */ std::this_thread::sleep_for(
        std::chrono::seconds(time));
    ASSERT_GE(this->getStats().nvmUpTime, 3 * time);
  }

  this->iceColdRoll();
  {
    // uptime must  be reset
    ASSERT_LE(this->getStats().nvmUpTime, time);
    /* sleep override */ std::this_thread::sleep_for(
        std::chrono::seconds(time));
    ASSERT_GE(this->getStats().nvmUpTime, time);
    ASSERT_LE(this->getStats().nvmUpTime, 2 * time);
  }

  {
    auto& config = this->getConfig();
    // empty cache dir means no persistency
    config.enableCachePersistence("");
    this->makeCache();
    /* sleep override */ std::this_thread::sleep_for(
        std::chrono::seconds(time));
    ASSERT_GE(this->getStats().nvmUpTime, time);
  }
}

TYPED_TEST(NvmCacheTest, FullAllocSize) {
  // Test truncated alloc sizes
  auto& config = this->getConfig();
  config.nvmConfig->truncateItemToOriginalAllocSizeInNvm = false;
  this->poolAllocsizes_ = {64};
  auto& cache = this->makeCache();
  auto pid = this->poolId();

  // Allocate a small item but use its extra bytes
  uint32_t totalSize = 0;
  {
    auto it = cache.allocate(pid, "test", 1);
    ASSERT_NE(nullptr, it);
    ASSERT_LT(it->getSize(), cache.getUsableSize(*it));

    totalSize = cache.getUsableSize(*it);
    for (uint32_t i = 0; i < totalSize; ++i) {
      it->template getMemoryAs<char>()[i] = static_cast<char>(i);
    }

    cache.insertOrReplace(it);
    this->pushToNvmCacheFromRamForTesting("test");
    this->removeFromRamForTesting("test");
  }
  {
    // Make sure we end up with a different item in free list
    auto it = cache.allocate(pid, "placeholder", 1);
    for (uint32_t i = 0; i < totalSize; ++i) {
      it->template getMemoryAs<char>()[i] = 0;
    }
  }

  {
    auto it = cache.find("test");
    ASSERT_NE(nullptr, it);
    ASSERT_EQ(totalSize, cache.getUsableSize(*it));
    EXPECT_EQ(0, it->template getMemoryAs<char>()[0]);
    for (uint32_t i = 1; i < totalSize; ++i) {
      EXPECT_EQ(static_cast<char>(i), it->template getMemoryAs<char>()[i])
          << "i: " << i;
    }
  }
}

TYPED_TEST(NvmCacheTest, TruncatedAllocSize) {
  // Test truncated alloc sizes
  auto& config = this->getConfig();
  config.nvmConfig->truncateItemToOriginalAllocSizeInNvm = true;
  this->poolAllocsizes_ = {64};
  auto& cache = this->makeCache();
  auto pid = this->poolId();

  // Allocate a small item but use its extra bytes
  uint32_t totalSize = 0;
  {
    auto it = cache.allocate(pid, "test", 1);
    ASSERT_NE(nullptr, it);
    ASSERT_LT(it->getSize(), cache.getUsableSize(*it));

    totalSize = cache.getUsableSize(*it);
    for (uint32_t i = 0; i < totalSize; ++i) {
      it->template getMemoryAs<char>()[i] = static_cast<char>(i);
    }

    cache.insertOrReplace(it);
    this->pushToNvmCacheFromRamForTesting("test");
    this->removeFromRamForTesting("test");
  }
  {
    // Make sure we end up with a different item in free list
    auto it = cache.allocate(pid, "placeholder", 1);
    for (uint32_t i = 0; i < totalSize; ++i) {
      it->template getMemoryAs<char>()[i] = 0;
    }
  }

  {
    auto it = cache.find("test");
    ASSERT_NE(nullptr, it);
    ASSERT_EQ(totalSize, cache.getUsableSize(*it));
    EXPECT_EQ(0, it->template getMemoryAs<char>()[0]);
    for (uint32_t i = 1; i < totalSize; ++i) {
      EXPECT_NE(static_cast<char>(i), it->template getMemoryAs<char>()[i])
          << "i: " << i;
    }
  }
}
} // namespace tests
} // namespace cachelib
} // namespace facebook
