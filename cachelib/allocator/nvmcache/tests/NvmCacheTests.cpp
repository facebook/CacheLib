#include <set>
#include <thread>

#include <folly/Random.h>
#include <gtest/gtest.h>

#include "cachelib/allocator/nvmcache/tests/NvmTestBase.h"

namespace facebook {
namespace cachelib {
namespace tests {

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

TEST_F(NvmCacheTest, Config) {
  struct MockEncryptor : public navy::DeviceEncryptor {
   public:
    uint32_t encryptionBlockSize() const override { return 5555; }
    bool encrypt(folly::MutableByteRange, uint64_t) override { return true; }
    bool decrypt(folly::MutableByteRange, uint64_t) override { return true; }
  };

  auto config = *this->getConfig().nvmConfig;
  ASSERT_NO_THROW(config.validate());

  config.dipperOptions["dipper_navy_block_size"] = 5555;
  config.dipperOptions["dipper_navy_bighash_bucket_size"] = 5555;
  config.deviceEncryptor = std::make_shared<MockEncryptor>();
  ASSERT_NO_THROW(config.validate());

  config.dipperOptions["dipper_navy_block_size"] = 4444;
  ASSERT_THROW(config.validate(), std::invalid_argument);

  config.dipperOptions["dipper_navy_block_size"] = 5555;
  config.dipperOptions["dipper_navy_bighash_bucket_size"] = 4444;
  ASSERT_THROW(config.validate(), std::invalid_argument);
}

namespace {
template <typename T>
struct MockNvmAdmissionPolicy : public NvmAdmissionPolicy<T> {
  MockNvmAdmissionPolicy() = default;
  using Item = typename T::Item;
  using ChainedItemIter = typename T::ChainedItemIter;

 protected:
  virtual bool acceptImpl(const Item&, folly::Range<ChainedItemIter>) override {
    return true;
  }
  virtual std::unordered_map<std::string, double> getCountersImpl() override {
    std::unordered_map<std::string, double> ret;
    ret["nvm_mock_policy"] = 1;
    return ret;
  }
};
} // namespace

TEST_F(NvmCacheTest, APConfig) {
  auto policy = std::make_shared<MockNvmAdmissionPolicy<AllocatorT>>();
  {
    auto& config = getConfig();
    config.enableRejectFirstAPForNvm(10, 10, 1, true);
    auto& nvm = makeCache();
    auto ctrs = nvm.getNvmCacheStatsMap();
    EXPECT_NE(ctrs.find("ap.reject_first_keys_tracked"), ctrs.end());
  }

  {
    auto& config = getConfig();
    config.setNvmCacheAdmissionPolicy(policy);
    ASSERT_NO_THROW(config.validate());
    auto& nvm = makeCache();
    auto ctrs = nvm.getNvmCacheStatsMap();
    EXPECT_NE(ctrs.find("nvm_mock_policy"), ctrs.end());
  }

  // setting both reject first and custom ap will give custom ap the higher
  // priority
  {
    auto& config = getConfig();
    config.enableRejectFirstAPForNvm(10, 10, 1, true);
    auto& nvm = makeCache();
    auto ctrs = nvm.getNvmCacheStatsMap();
    EXPECT_NE(ctrs.find("nvm_mock_policy"), ctrs.end());
  }

  {
    auto& config = getConfig();
    EXPECT_THROW(config.setNvmCacheAdmissionPolicy(nullptr),
                 std::invalid_argument);
  }
}

TEST_F(NvmCacheTest, BasicGet) {
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

TEST_F(NvmCacheTest, EvictToDipperGet) {
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

  // read from reverse to no cause evictions to navy
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

TEST_F(NvmCacheTest, EvictToDipperGetCheckCtime) {
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

  // read from reverse to no cause evictions to navy
  for (unsigned int i = nKeys - 1; i > 0; i--) {
    auto key = std::string("blah") + folly::to<std::string>(i - 1);
    auto hdl = this->fetch(key, false /* ramOnly */);
    hdl.wait();
    XDCHECK(hdl);
    ASSERT_EQ(hdl->getCreationTime(), keyToCtime[key]);
  }
}

TEST_F(NvmCacheTest, EvictToDipperExpired) {
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

TEST_F(NvmCacheTest, FilterCb) {
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
    auto it3 = nvm.allocatePermanent_deprecated(pid, unEvictableKey, allocSize);
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

// Unevictable items should be kept in DRAM
TEST_F(NvmCacheTest, UnEvictable) {
  auto& nvm = this->makeCache();
  auto pid = this->poolId();
  auto key = "foo";

  size_t allocSize = 15 * 1024;
  auto it = nvm.allocatePermanent_deprecated(pid, key, allocSize);
  ASSERT_NE(nullptr, it);
  nvm.insertOrReplace(it);
  auto res = this->inspectCache(key);
  EXPECT_NE(nullptr, res.first);
  EXPECT_EQ(nullptr, res.second);
}

TEST_F(NvmCacheTest, ReadFromDipperExpired) {
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

TEST_F(NvmCacheTest, Delete) {
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

TEST_F(NvmCacheTest, InsertOrReplace) {
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

TEST_F(NvmCacheTest, ConcurrentFills) {
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

TEST_F(NvmCacheTest, NvmClean) {
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

  // we must have done evictions from ram to navy
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

  // we must have done evictions from ram to navy
  nEvictions = this->evictionCount() - evictBefore;
  nPuts = this->getStats().numNvmPuts - putsBefore;
  ASSERT_LT(0, nEvictions);
  ASSERT_EQ(0, nPuts);
}

// put nvmclean entries in cache and then mark them as nvmRewrite. this should
// write them to nvmcache.
TEST_F(NvmCacheTest, NvmEvicted) {
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

  // we must have done evictions from ram to navy
  ASSERT_EQ(this->evictionCount() - evictBefore, 2 * nKeys);
  ASSERT_EQ(nKeys, this->getStats().numNvmPuts - putsBefore);
  ASSERT_EQ(nKeys, this->getStats().numNvmPutFromClean);
}

TEST_F(NvmCacheTest, InspectCache) {
  auto& cache = this->cache();
  auto pid = this->poolId();

  std::string key = "blah";
  std::string val = "foobar";
  {
    auto it = cache.allocate(pid, key, val.length());
    ASSERT_NE(nullptr, it);
    cache.insertOrReplace(it);
    ::memcpy(it->getWritableMemory(), val.data(), val.length());
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
TEST_F(NvmCacheTest, InspectCacheLarge) {
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
    ::memcpy(it->getWritableMemory(), val.data(), val.length());
    for (int i = 0; i < nChained; i++) {
      auto chainedIt = cache.allocateChainedItem(it, val.length());
      ASSERT_TRUE(chainedIt);
      ::memcpy(chainedIt->getWritableMemory(), val.data(), val.length());
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

TEST_F(NvmCacheTest, WarmRoll) {
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

TEST_F(NvmCacheTest, ColdRoll) {
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

TEST_F(NvmCacheTest, ColdRollDropNvmCache) {
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

TEST_F(NvmCacheTest, IceRoll) {
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

TEST_F(NvmCacheTest, IceColdRoll) {
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
TEST_F(NvmCacheTest, EvictSlabRelease) {
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
      std::memcpy(handle->getWritableMemory(), val.data(), val.size());
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
TEST_F(NvmCacheTest, TrailingAllocSize) {
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

TEST_F(NvmCacheTest, ChainedItems) {
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

TEST_F(NvmCacheTest, ChainedItemsModifyAccessible) {
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

TEST_F(NvmCacheTest, EncodeDecode) {
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

TEST_F(NvmCacheTest, NvmUptime) {
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

TEST_F(NvmCacheTest, FullAllocSize) {
  // Test truncated alloc sizes
  auto& config = this->getConfig();
  config.nvmConfig->truncateItemToOriginalAllocSizeInNvm = false;
  this->poolAllocsizes_ = {200};
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

TEST_F(NvmCacheTest, TruncatedAllocSize) {
  // Test truncated alloc sizes
  auto& config = this->getConfig();
  config.nvmConfig->truncateItemToOriginalAllocSizeInNvm = true;
  this->poolAllocsizes_ = {200};
  auto& cache = this->makeCache();
  auto pid = this->poolId();

  // We use 101 bytes for value size because it's just bigger than
  // the small item threshold (100 bytes) we set up for NvmCache.
  // This ensures we won't evict anything prematurely in flash device.
  const uint32_t valSize = 101;

  // Allocate a small item but use its extra bytes
  uint32_t totalSize = 0;
  {
    auto it = cache.allocate(pid, "test", valSize);
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
    auto it = cache.allocate(pid, "placeholder", valSize);
    for (uint32_t i = 0; i < totalSize; ++i) {
      it->template getMemoryAs<char>()[i] = 0;
    }
  }

  {
    auto it = cache.find("test");
    ASSERT_NE(nullptr, it);
    ASSERT_EQ(totalSize, cache.getUsableSize(*it));
    EXPECT_EQ(0, it->template getMemoryAs<char>()[0]);
    for (uint32_t i = valSize; i < totalSize; ++i) {
      EXPECT_NE(static_cast<char>(i), it->template getMemoryAs<char>()[i])
          << "i: " << i;
    }
  }
}

TEST_F(NvmCacheTest, NavyStats) {
  // Ensure we export all the stats we expect
  // Everytime we add a new stat, make sure to update this test accordingly
  auto nvmStats = this->cache().getNvmCacheStatsMap();

  auto cs = [&nvmStats](const std::string& name) mutable {
    if (nvmStats.end() != nvmStats.find(name)) {
      nvmStats.erase(name);
      return true;
    }
    return false;
  };

  // navy::Driver
  EXPECT_TRUE(cs("navy_inserts"));
  EXPECT_TRUE(cs("navy_succ_inserts"));
  EXPECT_TRUE(cs("navy_lookups"));
  EXPECT_TRUE(cs("navy_succ_lookups"));
  EXPECT_TRUE(cs("navy_removes"));
  EXPECT_TRUE(cs("navy_succ_removes"));
  EXPECT_TRUE(cs("navy_rejected"));
  EXPECT_TRUE(cs("navy_rejected_concurrent_inserts"));
  EXPECT_TRUE(cs("navy_rejected_parcel_memory"));
  EXPECT_TRUE(cs("navy_rejected_bytes"));
  EXPECT_TRUE(cs("navy_io_errors"));
  EXPECT_TRUE(cs("navy_parcel_memory"));
  EXPECT_TRUE(cs("navy_concurrent_inserts"));

  // navy::OrderedThreadPoolJobScheduler
  EXPECT_TRUE(cs("navy_reader_pool_max_queue_len"));
  EXPECT_TRUE(cs("navy_reader_pool_reschedules"));
  EXPECT_TRUE(cs("navy_reader_pool_jobs_high_reschedule"));
  EXPECT_TRUE(cs("navy_reader_pool_jobs_done"));
  EXPECT_TRUE(cs("navy_max_reader_pool_pending_jobs"));
  EXPECT_TRUE(cs("navy_writer_pool_max_queue_len"));
  EXPECT_TRUE(cs("navy_writer_pool_reschedules"));
  EXPECT_TRUE(cs("navy_writer_pool_jobs_high_reschedule"));
  EXPECT_TRUE(cs("navy_writer_pool_jobs_done"));
  EXPECT_TRUE(cs("navy_max_writer_pool_pending_jobs"));
  EXPECT_TRUE(cs("navy_req_order_spooled"));
  EXPECT_TRUE(cs("navy_req_order_curr_spool_size"));

  // navy::BlockCache
  EXPECT_TRUE(cs("navy_bc_item_removed_with_no_access"));
  EXPECT_TRUE(cs("navy_bc_item_hits_avg"));
  EXPECT_TRUE(cs("navy_bc_item_hits_min"));
  EXPECT_TRUE(cs("navy_bc_item_hits_p5"));
  EXPECT_TRUE(cs("navy_bc_item_hits_p10"));
  EXPECT_TRUE(cs("navy_bc_item_hits_p25"));
  EXPECT_TRUE(cs("navy_bc_item_hits_p50"));
  EXPECT_TRUE(cs("navy_bc_item_hits_p75"));
  EXPECT_TRUE(cs("navy_bc_item_hits_p90"));
  EXPECT_TRUE(cs("navy_bc_item_hits_p95"));
  EXPECT_TRUE(cs("navy_bc_item_hits_p99"));
  EXPECT_TRUE(cs("navy_bc_item_hits_p999"));
  EXPECT_TRUE(cs("navy_bc_item_hits_p9999"));
  EXPECT_TRUE(cs("navy_bc_item_hits_p99999"));
  EXPECT_TRUE(cs("navy_bc_item_hits_p999999"));
  EXPECT_TRUE(cs("navy_bc_item_hits_max"));
  EXPECT_TRUE(cs("navy_bc_items"));
  EXPECT_TRUE(cs("navy_bc_inserts"));
  EXPECT_TRUE(cs("navy_bc_insert_hash_collisions"));
  EXPECT_TRUE(cs("navy_bc_succ_inserts"));
  EXPECT_TRUE(cs("navy_bc_lookups"));
  EXPECT_TRUE(cs("navy_bc_lookup_false_positives"));
  EXPECT_TRUE(cs("navy_bc_lookup_entry_header_checksum_errors"));
  EXPECT_TRUE(cs("navy_bc_lookup_value_checksum_errors"));
  EXPECT_TRUE(cs("navy_bc_succ_lookups"));
  EXPECT_TRUE(cs("navy_bc_removes"));
  EXPECT_TRUE(cs("navy_bc_num_regions"));
  EXPECT_TRUE(cs("navy_bc_num_clean_regions"));
  EXPECT_TRUE(cs("navy_bc_succ_removes"));
  EXPECT_TRUE(cs("navy_bc_eviction_lookup_misses"));
  EXPECT_TRUE(cs("navy_bc_alloc_errors"));
  EXPECT_TRUE(cs("navy_bc_logical_written"));
  EXPECT_TRUE(cs("navy_bc_hole_count"));
  EXPECT_TRUE(cs("navy_bc_hole_bytes"));
  EXPECT_TRUE(cs("navy_bc_reinsertions"));
  EXPECT_TRUE(cs("navy_bc_reinsertion_bytes"));
  EXPECT_TRUE(cs("navy_bc_reinsertion_errors"));
  EXPECT_TRUE(cs("navy_bc_reclaim_entry_header_checksum_errors"));
  EXPECT_TRUE(cs("navy_bc_reclaim_value_checksum_errors"));
  for (int size = 64;;
       size = std::min(4 * 1024 * 1024, static_cast<int>(size * 1.25))) {
    EXPECT_TRUE(cs(folly::sformat("navy_bc_approx_bytes_in_size_{}", size)));
    if (size == 4 * 1024 * 1024) {
      break;
    }
  }

  // navy::RegionManager
  EXPECT_TRUE(cs("navy_bc_reclaim"));
  EXPECT_TRUE(cs("navy_bc_reclaim_time"));
  EXPECT_TRUE(cs("navy_bc_region_reclaim_errors"));
  EXPECT_TRUE(cs("navy_bc_evicted"));
  EXPECT_TRUE(cs("navy_bc_pinned_regions"));
  EXPECT_TRUE(cs("navy_bc_physical_written"));
  EXPECT_TRUE(cs("navy_bc_external_fragmentation"));

  // navy::LruPolicy
  EXPECT_TRUE(cs("navy_bc_lru_secs_since_insertion_avg"));
  EXPECT_TRUE(cs("navy_bc_lru_secs_since_insertion_min"));
  EXPECT_TRUE(cs("navy_bc_lru_secs_since_insertion_p5"));
  EXPECT_TRUE(cs("navy_bc_lru_secs_since_insertion_p10"));
  EXPECT_TRUE(cs("navy_bc_lru_secs_since_insertion_p25"));
  EXPECT_TRUE(cs("navy_bc_lru_secs_since_insertion_p50"));
  EXPECT_TRUE(cs("navy_bc_lru_secs_since_insertion_p75"));
  EXPECT_TRUE(cs("navy_bc_lru_secs_since_insertion_p90"));
  EXPECT_TRUE(cs("navy_bc_lru_secs_since_insertion_p95"));
  EXPECT_TRUE(cs("navy_bc_lru_secs_since_insertion_p99"));
  EXPECT_TRUE(cs("navy_bc_lru_secs_since_insertion_p999"));
  EXPECT_TRUE(cs("navy_bc_lru_secs_since_insertion_p9999"));
  EXPECT_TRUE(cs("navy_bc_lru_secs_since_insertion_p99999"));
  EXPECT_TRUE(cs("navy_bc_lru_secs_since_insertion_p999999"));
  EXPECT_TRUE(cs("navy_bc_lru_secs_since_insertion_max"));
  EXPECT_TRUE(cs("navy_bc_lru_secs_since_access_avg"));
  EXPECT_TRUE(cs("navy_bc_lru_secs_since_access_min"));
  EXPECT_TRUE(cs("navy_bc_lru_secs_since_access_p5"));
  EXPECT_TRUE(cs("navy_bc_lru_secs_since_access_p10"));
  EXPECT_TRUE(cs("navy_bc_lru_secs_since_access_p25"));
  EXPECT_TRUE(cs("navy_bc_lru_secs_since_access_p50"));
  EXPECT_TRUE(cs("navy_bc_lru_secs_since_access_p75"));
  EXPECT_TRUE(cs("navy_bc_lru_secs_since_access_p90"));
  EXPECT_TRUE(cs("navy_bc_lru_secs_since_access_p95"));
  EXPECT_TRUE(cs("navy_bc_lru_secs_since_access_p99"));
  EXPECT_TRUE(cs("navy_bc_lru_secs_since_access_p999"));
  EXPECT_TRUE(cs("navy_bc_lru_secs_since_access_p9999"));
  EXPECT_TRUE(cs("navy_bc_lru_secs_since_access_p99999"));
  EXPECT_TRUE(cs("navy_bc_lru_secs_since_access_p999999"));
  EXPECT_TRUE(cs("navy_bc_lru_secs_since_access_max"));
  EXPECT_TRUE(cs("navy_bc_lru_region_hits_estimate_avg"));
  EXPECT_TRUE(cs("navy_bc_lru_region_hits_estimate_min"));
  EXPECT_TRUE(cs("navy_bc_lru_region_hits_estimate_p5"));
  EXPECT_TRUE(cs("navy_bc_lru_region_hits_estimate_p10"));
  EXPECT_TRUE(cs("navy_bc_lru_region_hits_estimate_p25"));
  EXPECT_TRUE(cs("navy_bc_lru_region_hits_estimate_p50"));
  EXPECT_TRUE(cs("navy_bc_lru_region_hits_estimate_p75"));
  EXPECT_TRUE(cs("navy_bc_lru_region_hits_estimate_p90"));
  EXPECT_TRUE(cs("navy_bc_lru_region_hits_estimate_p95"));
  EXPECT_TRUE(cs("navy_bc_lru_region_hits_estimate_p99"));
  EXPECT_TRUE(cs("navy_bc_lru_region_hits_estimate_p999"));
  EXPECT_TRUE(cs("navy_bc_lru_region_hits_estimate_p9999"));
  EXPECT_TRUE(cs("navy_bc_lru_region_hits_estimate_p99999"));
  EXPECT_TRUE(cs("navy_bc_lru_region_hits_estimate_p999999"));
  EXPECT_TRUE(cs("navy_bc_lru_region_hits_estimate_max"));
  EXPECT_TRUE(cs("navy_bc_inmem_waiting_flush"));
  EXPECT_TRUE(cs("navy_bc_inmem_active"));

  // navy::BigHash
  EXPECT_TRUE(cs("navy_bh_items"));
  EXPECT_TRUE(cs("navy_bh_inserts"));
  EXPECT_TRUE(cs("navy_bh_succ_inserts"));
  EXPECT_TRUE(cs("navy_bh_lookups"));
  EXPECT_TRUE(cs("navy_bh_succ_lookups"));
  EXPECT_TRUE(cs("navy_bh_removes"));
  EXPECT_TRUE(cs("navy_bh_succ_removes"));
  EXPECT_TRUE(cs("navy_bh_evictions"));
  EXPECT_TRUE(cs("navy_bh_logical_written"));
  EXPECT_TRUE(cs("navy_bh_physical_written"));
  EXPECT_TRUE(cs("navy_bh_io_errors"));
  EXPECT_TRUE(cs("navy_bh_bf_false_positive_pct"));
  EXPECT_TRUE(cs("navy_bh_bf_lookups"));
  EXPECT_TRUE(cs("navy_bh_bf_rebuilds"));
  EXPECT_TRUE(cs("navy_bh_checksum_errors"));
  EXPECT_TRUE(cs("navy_bh_used_size_bytes"));
  for (int size = 64;; size = std::min(1024, static_cast<int>(size * 1.25))) {
    EXPECT_TRUE(cs(folly::sformat("navy_bh_approx_bytes_in_size_{}", size)));
    if (size == 1024) {
      break;
    }
  }

  // navy::Device
  EXPECT_TRUE(cs("navy_device_bytes_written"));
  EXPECT_TRUE(cs("navy_device_read_errors"));
  EXPECT_TRUE(cs("navy_device_write_errors"));
  EXPECT_TRUE(cs("navy_device_read_latency_us_avg"));
  EXPECT_TRUE(cs("navy_device_read_latency_us_min"));
  EXPECT_TRUE(cs("navy_device_read_latency_us_p5"));
  EXPECT_TRUE(cs("navy_device_read_latency_us_p10"));
  EXPECT_TRUE(cs("navy_device_read_latency_us_p25"));
  EXPECT_TRUE(cs("navy_device_read_latency_us_p50"));
  EXPECT_TRUE(cs("navy_device_read_latency_us_p75"));
  EXPECT_TRUE(cs("navy_device_read_latency_us_p90"));
  EXPECT_TRUE(cs("navy_device_read_latency_us_p95"));
  EXPECT_TRUE(cs("navy_device_read_latency_us_p99"));
  EXPECT_TRUE(cs("navy_device_read_latency_us_p999"));
  EXPECT_TRUE(cs("navy_device_read_latency_us_p9999"));
  EXPECT_TRUE(cs("navy_device_read_latency_us_p99999"));
  EXPECT_TRUE(cs("navy_device_read_latency_us_p999999"));
  EXPECT_TRUE(cs("navy_device_read_latency_us_max"));
  EXPECT_TRUE(cs("navy_device_write_latency_us_avg"));
  EXPECT_TRUE(cs("navy_device_write_latency_us_min"));
  EXPECT_TRUE(cs("navy_device_write_latency_us_p5"));
  EXPECT_TRUE(cs("navy_device_write_latency_us_p10"));
  EXPECT_TRUE(cs("navy_device_write_latency_us_p25"));
  EXPECT_TRUE(cs("navy_device_write_latency_us_p50"));
  EXPECT_TRUE(cs("navy_device_write_latency_us_p75"));
  EXPECT_TRUE(cs("navy_device_write_latency_us_p90"));
  EXPECT_TRUE(cs("navy_device_write_latency_us_p95"));
  EXPECT_TRUE(cs("navy_device_write_latency_us_p99"));
  EXPECT_TRUE(cs("navy_device_write_latency_us_p999"));
  EXPECT_TRUE(cs("navy_device_write_latency_us_p9999"));
  EXPECT_TRUE(cs("navy_device_write_latency_us_p99999"));
  EXPECT_TRUE(cs("navy_device_write_latency_us_p999999"));
  EXPECT_TRUE(cs("navy_device_encryption_errors"));
  EXPECT_TRUE(cs("navy_device_decryption_errors"));
  EXPECT_TRUE(cs("navy_device_write_latency_us_max"));

  // there should be no additional stats
  if (nvmStats.size()) {
    for (auto kv : nvmStats) {
      XLOG(ERR) << kv.first << ", " << kv.second;
    }
  }
  EXPECT_EQ(0, nvmStats.size());
}

TEST_F(NvmCacheTest, Raid0Basic) {
  auto& config = getConfig();
  auto& options = config.nvmConfig->dipperOptions;
  auto filePath = folly::sformat("/tmp/nvmcache-navy-raid0/{}", ::getpid());
  util::makeDir(filePath);
  SCOPE_EXIT { util::removePath(filePath); };
  options["dipper_navy_file_name"] = "";
  options["dipper_navy_raid_paths"] =
      folly::dynamic::array(filePath + "/CACHE0",
                            filePath + "/CACHE1",
                            filePath + "/CACHE2",
                            filePath + "/CACHE3");

  options["dipper_navy_file_size"] = 10 * 1024 * 1024;

  this->convertToShmCache();
  auto& nvm = this->cache();
  auto pid = this->poolId();
  std::string key = "blah";
  std::string val = "foobar";
  {
    auto it = nvm.allocate(pid, key, val.length());
    ASSERT_NE(nullptr, it);
    ::memcpy(it->getMemory(), val.data(), val.length());
    nvm.insertOrReplace(it);
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

  // recovery should find the key/val
  this->warmRoll();
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
}

TEST_F(NvmCacheTest, IncorrectRaid) {
  auto& config = getConfig();
  auto& options = config.nvmConfig->dipperOptions;
  auto filePath = folly::sformat("/tmp/nvmcache-navy-raid0/{}", ::getpid());
  util::makeDir(filePath);
  SCOPE_EXIT { util::removePath(filePath); };

  // specifying just a single file path for raid should fail.
  options["dipper_navy_file_name"] = "";
  options["dipper_navy_raid_paths"] =
      folly::dynamic::array(filePath + "/CACHE0");

  options["dipper_navy_file_size"] = 10 * 1024 * 1024;

  ASSERT_THROW(this->makeCache(), std::invalid_argument);

  // specify both file name and raid path and creation should fail.
  options["dipper_navy_file_name"] = "/tmp/nvmcache-navy-raid0/foo";
  options["dipper_navy_raid_paths"] =
      folly::dynamic::array(filePath + "/CACHE0");

  options["dipper_navy_file_size"] = 10 * 1024 * 1024;
}

TEST_F(NvmCacheTest, Raid0OrderChange) {
  auto& config = getConfig();
  auto& options = config.nvmConfig->dipperOptions;
  auto filePath = folly::sformat("/tmp/nvmcache-navy-raid0/{}", ::getpid());
  util::makeDir(filePath);
  SCOPE_EXIT { util::removePath(filePath); };

  options["dipper_navy_file_name"] = "";
  options["dipper_navy_raid_paths"] =
      folly::dynamic::array(filePath + "/CACHE0",
                            filePath + "/CACHE1",
                            filePath + "/CACHE2",
                            filePath + "/CACHE3");

  options["dipper_navy_file_size"] = 10 * 1024 * 1024;

  // setup a cache with some content and change the raid0 order and verify
  // that everything is correct.
  std::string val = "foobar";
  int nKeys = 100;
  auto makeKey = [&](int i) { return folly::sformat("blah-{}", i); };

  this->convertToShmCache();
  {
    auto& nvm = this->cache();
    auto pid = this->poolId();

    for (int i = 0; i < nKeys; i++) {
      auto it = nvm.allocate(pid, makeKey(i), val.length());
      ASSERT_NE(nullptr, it);
      ::memcpy(it->getMemory(), val.data(), val.length());
      nvm.insertOrReplace(it);
    }

    // item is only in RAM
    for (int i = 0; i < nKeys; i++) {
      auto res = this->inspectCache(makeKey(i));
      // must exist in RAM
      ASSERT_NE(nullptr, res.first);
      ASSERT_EQ(::memcmp(res.first->getMemory(), val.data(), val.length()), 0);

      // must not be in nvmcache
      ASSERT_EQ(nullptr, res.second);
      this->pushToNvmCacheFromRamForTesting(makeKey(i));
      this->removeFromRamForTesting(makeKey(i));
    }

    for (int i = 0; i < nKeys; i++) {
      auto res = this->inspectCache(makeKey(i));
      // must not exist in RAM
      ASSERT_EQ(nullptr, res.first);

      // must be in nvmcache
      ASSERT_NE(nullptr, res.second);
      ASSERT_EQ(::memcmp(res.second->getMemory(), val.data(), val.length()), 0);
    }
  }

  // change the order of files
  options["dipper_navy_raid_paths"] =
      folly::dynamic::array(filePath + "/CACHE3",
                            filePath + "/CACHE1",
                            filePath + "/CACHE2",
                            filePath + "/CACHE0");

  this->warmRoll();
  // recovery should succeed and we must find those item in nvmcache.
  for (int i = 0; i < nKeys; i++) {
    auto res = this->inspectCache(makeKey(i));
    // must not exist in RAM
    ASSERT_EQ(nullptr, res.first);

    // must be in nvmcache
    ASSERT_NE(nullptr, res.second);
    ASSERT_EQ(::memcmp(res.second->getMemory(), val.data(), val.length()), 0);
  }
}

TEST_F(NvmCacheTest, Raid0NumFilesChange) {
  auto& config = getConfig();
  auto& options = config.nvmConfig->dipperOptions;
  auto filePath = folly::sformat("/tmp/nvmcache-navy-raid0/{}", ::getpid());
  util::makeDir(filePath);
  SCOPE_EXIT { util::removePath(filePath); };

  options["dipper_navy_file_name"] = "";
  options["dipper_navy_raid_paths"] =
      folly::dynamic::array(filePath + "/CACHE0",
                            filePath + "/CACHE1",
                            filePath + "/CACHE2",
                            filePath + "/CACHE3");

  options["dipper_navy_file_size"] = 10 * 1024 * 1024;

  // setup a cache with some content and change the raid0 order and verify
  // that everything is correct.
  std::string val = "foobar";
  int nKeys = 100;
  auto makeKey = [&](int i) { return folly::sformat("blah-{}", i); };

  this->convertToShmCache();
  {
    auto& nvm = this->cache();
    auto pid = this->poolId();

    for (int i = 0; i < nKeys; i++) {
      auto it = nvm.allocate(pid, makeKey(i), val.length());
      ASSERT_NE(nullptr, it);
      ::memcpy(it->getMemory(), val.data(), val.length());
      nvm.insertOrReplace(it);
    }

    // item is only in RAM
    for (int i = 0; i < nKeys; i++) {
      auto res = this->inspectCache(makeKey(i));
      // must exist in RAM
      ASSERT_NE(nullptr, res.first);
      ASSERT_EQ(::memcmp(res.first->getMemory(), val.data(), val.length()), 0);

      // must not be in nvmcache
      ASSERT_EQ(nullptr, res.second);
      this->pushToNvmCacheFromRamForTesting(makeKey(i));
      this->removeFromRamForTesting(makeKey(i));
    }

    for (int i = 0; i < nKeys; i++) {
      auto res = this->inspectCache(makeKey(i));
      // must not exist in RAM
      ASSERT_EQ(nullptr, res.first);

      // must be in nvmcache
      ASSERT_NE(nullptr, res.second);
      ASSERT_EQ(::memcmp(res.second->getMemory(), val.data(), val.length()), 0);
    }
  }

  options["dipper_navy_raid_paths"] = folly::dynamic::array(
      filePath + "/CACHE0", filePath + "/CACHE2", filePath + "/CACHE3");
  this->warmRoll();
  // recovery should fail and we should lose the previous content. nvmcache
  // should still be enabled
  //
  EXPECT_TRUE(this->cache().isNvmCacheEnabled());
  for (int i = 0; i < nKeys; i++) {
    auto res = this->inspectCache(makeKey(i));
    // must not exist in RAM
    ASSERT_EQ(nullptr, res.first);

    // must not be in nvmcache since it got dropped
    ASSERT_EQ(nullptr, res.second);
  }
}

TEST_F(NvmCacheTest, Raid0SizeChange) {
  auto& config = getConfig();
  auto& options = config.nvmConfig->dipperOptions;
  auto filePath = folly::sformat("/tmp/nvmcache-navy-raid0/{}", ::getpid());
  util::makeDir(filePath);
  SCOPE_EXIT { util::removePath(filePath); };

  options["dipper_navy_file_name"] = "";
  options["dipper_navy_raid_paths"] =
      folly::dynamic::array(filePath + "/CACHE0",
                            filePath + "/CACHE1",
                            filePath + "/CACHE2",
                            filePath + "/CACHE3");

  options["dipper_navy_file_size"] = 10 * 1024 * 1024;

  // setup a cache with some content and change the raid0 order and verify
  // that everything is correct.
  std::string val = "foobar";
  int nKeys = 100;
  auto makeKey = [&](int i) { return folly::sformat("blah-{}", i); };

  this->convertToShmCache();
  {
    auto& nvm = this->cache();
    auto pid = this->poolId();

    for (int i = 0; i < nKeys; i++) {
      auto it = nvm.allocate(pid, makeKey(i), val.length());
      ASSERT_NE(nullptr, it);
      ::memcpy(it->getMemory(), val.data(), val.length());
      nvm.insertOrReplace(it);
    }

    // item is only in RAM
    for (int i = 0; i < nKeys; i++) {
      auto res = this->inspectCache(makeKey(i));
      // must exist in RAM
      ASSERT_NE(nullptr, res.first);
      ASSERT_EQ(::memcmp(res.first->getMemory(), val.data(), val.length()), 0);

      // must not be in nvmcache
      ASSERT_EQ(nullptr, res.second);
      this->pushToNvmCacheFromRamForTesting(makeKey(i));
      this->removeFromRamForTesting(makeKey(i));
    }

    for (int i = 0; i < nKeys; i++) {
      auto res = this->inspectCache(makeKey(i));
      // must not exist in RAM
      ASSERT_EQ(nullptr, res.first);

      // must be in nvmcache
      ASSERT_NE(nullptr, res.second);
      ASSERT_EQ(::memcmp(res.second->getMemory(), val.data(), val.length()), 0);
    }
  }

  // increase the size of the raid-0 files
  options["dipper_navy_file_size"] = 32 * 1024 * 1024;
  this->warmRoll();
  // recovery should fail and we should lose the previous content. nvmcache
  // should still be enabled
  //
  EXPECT_TRUE(this->cache().isNvmCacheEnabled());
  for (int i = 0; i < nKeys; i++) {
    auto res = this->inspectCache(makeKey(i));
    // must not exist in RAM
    ASSERT_EQ(nullptr, res.first);

    // must not be in nvmcache since it got dropped
    ASSERT_EQ(nullptr, res.second);
  }
}

TEST_F(NvmCacheTest, ShardHashIsNotFillMapHash) {
  auto const shardHash = getNvmShardAndHashForKey("hello world");
  ASSERT_NE(shardHash.first, shardHash.second);
}

} // namespace tests
} // namespace cachelib
} // namespace facebook
