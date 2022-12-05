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

#include "plugin/cachelib/CachelibWrapper.h"
#include "cachelib/common/Utils.h"
#include "test_util/testharness.h"

#include <folly/Random.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <pthread.h>
#include <unistd.h>

namespace facebook {
namespace rocks_secondary_cache {
using namespace rocksdb;

class CachelibWrapperTest : public ::testing::Test {
 public:
  class TestItem {
   public:
    TestItem(const char* buf, size_t size) : buf_(new char[size]), size_(size) {
      memcpy(buf_.get(), buf, size);
    }
    TestItem(TestItem&& other) noexcept {
      buf_.reset();
      buf_ = std::move(other.buf_);
      size_ = other.size_;
      other.size_ = 0;
    }
    ~TestItem() {}

    TestItem& operator=(TestItem&& other) {
      buf_.reset();
      buf_ = std::move(other.buf_);
      size_ = other.size_;
      other.size_ = 0;
      return *this;
    }

    char* Buf() { return buf_.get(); }
    size_t Size() { return size_; }

   private:
    std::unique_ptr<char[]> buf_;
    size_t size_;
  };

  CachelibWrapperTest() : fail_create_(false) {
    RocksCachelibOptions opts;

    path_ = ROCKSDB_NAMESPACE::test::TmpDir();
    opts.volatileSize = kVolatileSize;
    opts.cacheName = "CachelibWrapperTest";
    opts.fileName = path_ + "/cachelib_wrapper_test_file";
    opts.size = 64 << 20;
    opts.fb303Stats = true;

    cache_ = NewRocksCachelibWrapper(opts);
  }

 protected:
  friend Status InsertWhileCloseTestCb(void* obj,
                                       size_t offset,
                                       size_t size,
                                       void* out);
  static const uint64_t kVolatileSize = 8 << 20;

  static size_t SizeCallback(void* obj) {
    return static_cast<TestItem*>(obj)->Size();
  }

  static Status SaveToCallback(void* obj,
                               size_t offset,
                               size_t size,
                               void* out) {
    TestItem* item = reinterpret_cast<TestItem*>(obj);
    char* buf = item->Buf();
    EXPECT_EQ(size, item->Size());
    EXPECT_EQ(offset, 0);
    memcpy(out, buf, size);
    return Status::OK();
  }

  static void DeletionCallback(const Slice& /*key*/, void* obj) {
    delete reinterpret_cast<TestItem*>(obj);
  }

  static Cache::CacheItemHelper helper_;

  static Status SaveToCallbackFail(void* /*obj*/,
                                   size_t /*offset*/,
                                   size_t /*size*/,
                                   void* /*out*/) {
    return Status::NotSupported();
  }

  static Cache::CacheItemHelper helper_fail_;

  Cache::CreateCallback test_item_creator = [&](const void* buf,
                                                size_t size,
                                                void** out_obj,
                                                size_t* charge) -> Status {
    if (fail_create_) {
      return Status::NotSupported();
    }
    *out_obj = reinterpret_cast<void*>(new TestItem((char*)buf, size));
    *charge = size;
    return Status::OK();
  };

  std::string RandomString(int len) {
    std::string ret;
    ret.resize(len);
    for (int i = 0; i < len; i++) {
      ret[i] = static_cast<char>(' ' +
                                 folly::Random::secureRand64(95)); // ' ' .. '~'
    }
    return ret;
  }

  void SetFailCreate(bool fail) { fail_create_ = fail; }

  SecondaryCache* cache() { return cache_.get(); }

  const std::string& path() { return path_; }

 private:
  std::unique_ptr<SecondaryCache> cache_;
  bool fail_create_;
  std::string path_;
};

Cache::CacheItemHelper CachelibWrapperTest::helper_(
    CachelibWrapperTest::SizeCallback,
    CachelibWrapperTest::SaveToCallback,
    CachelibWrapperTest::DeletionCallback);

Cache::CacheItemHelper CachelibWrapperTest::helper_fail_(
    CachelibWrapperTest::SizeCallback,
    CachelibWrapperTest::SaveToCallbackFail,
    CachelibWrapperTest::DeletionCallback);

TEST_F(CachelibWrapperTest, BasicTest) {
  std::string str1 = RandomString(1020);
  TestItem item1(str1.data(), str1.length());
  ASSERT_EQ(cache()->Insert("k1", &item1, &CachelibWrapperTest::helper_),
            Status::OK());
  std::string str2 = RandomString(1020);
  TestItem item2(str2.data(), str2.length());
  ASSERT_EQ(cache()->Insert("k2", &item2, &CachelibWrapperTest::helper_),
            Status::OK());

  std::unique_ptr<ROCKSDB_NAMESPACE::SecondaryCacheResultHandle> handle;
  bool is_in_sec_cache{false};
  handle = cache()->Lookup("k2", test_item_creator, true
#if ROCKSDB_MAJOR > 7 || (ROCKSDB_MAJOR == 7 && ROCKSDB_MINOR >= 7)
                           ,
                           /*advise_erase=*/false
#endif
                           ,
                           is_in_sec_cache);
  ASSERT_NE(handle, nullptr);
  TestItem* val = static_cast<TestItem*>(handle->Value());
  ASSERT_NE(val, nullptr);
  ASSERT_EQ(memcmp(val->Buf(), item2.Buf(), item2.Size()), 0);
  delete val;
  handle.reset();

  handle = cache()->Lookup("k1", test_item_creator, true
#if ROCKSDB_MAJOR > 7 || (ROCKSDB_MAJOR == 7 && ROCKSDB_MINOR >= 7)
                           ,
                           /*advise_erase=*/false
#endif
                           ,
                           is_in_sec_cache);
  ASSERT_NE(handle, nullptr);
  ASSERT_NE(handle->Value(), nullptr);
  delete static_cast<TestItem*>(handle->Value());
  handle.reset();
}

TEST_F(CachelibWrapperTest, BasicFailTest) {
  std::unique_ptr<ROCKSDB_NAMESPACE::SecondaryCacheResultHandle> handle;
  bool is_in_sec_cache{false};
  handle = cache()->Lookup("k1", test_item_creator, true
#if ROCKSDB_MAJOR > 7 || (ROCKSDB_MAJOR == 7 && ROCKSDB_MINOR >= 7)
                           ,
                           /*advise_erase=*/false
#endif
                           ,
                           is_in_sec_cache);
  ASSERT_EQ(handle, nullptr);
}

TEST_F(CachelibWrapperTest, WaitAllTest) {
  // Make num_blocks larger than the volatile size by 200 in order to force
  // some items to spill into the cache file
  int num_blocks = kVolatileSize / 1020 + 200;
  std::vector<TestItem> items;
  for (int i = 0; i < num_blocks; ++i) {
    std::string str = RandomString(1020);
    items.emplace_back(str.data(), str.length());
    ASSERT_EQ(cache()->Insert("k" + std::to_string(i),
                              &items.back(),
                              &CachelibWrapperTest::helper_),
              Status::OK());
  }

  std::vector<std::unique_ptr<SecondaryCacheResultHandle>> handles;
  std::vector<SecondaryCacheResultHandle*> handle_ptrs;
  for (int i = 0; i < 100; ++i) {
    int block = i;
    bool invalid = false;
    // Add a few non-existent blocks in the middle
    if (i > 50 && i < 55) {
      block = i + num_blocks;
      invalid = true;
    }
    bool is_in_sec_cache{false};
    handles.emplace_back(cache()->Lookup("k" + std::to_string(block),
                                         test_item_creator,
                                         false,
#if ROCKSDB_MAJOR > 7 || (ROCKSDB_MAJOR == 7 && ROCKSDB_MINOR >= 7)
                                         /*advise_erase=*/false,
#endif
                                         is_in_sec_cache));
    if (invalid) {
      // Fast fail
      ASSERT_EQ(handles.back(), nullptr);
    } else {
      ASSERT_NE(handles.back(), nullptr);
    }
  }
  for (int i = 0; i < 100; ++i) {
    if (!handles[i]) {
      continue;
    }
    ASSERT_EQ(handles[i]->IsReady(), false);
    handle_ptrs.emplace_back(handles[i].get());
  }

  cache()->WaitAll(handle_ptrs);
  for (size_t i = 0; i < handles.size(); ++i) {
    if (!handles[i]) {
      continue;
    }
    ASSERT_EQ(handles[i]->IsReady(), true);
    TestItem* item = static_cast<TestItem*>(handles[i]->Value());
    ASSERT_NE(item, nullptr);
    ASSERT_EQ(memcmp(item->Buf(), items[i].Buf(), items[i].Size()), 0);
    delete item;
  }
}

TEST_F(CachelibWrapperTest, CreateFailTest) {
  std::string str1 = RandomString(1020);
  TestItem item1(str1.data(), str1.length());
  SetFailCreate(true);
  ASSERT_EQ(cache()->Insert("k1", &item1, &CachelibWrapperTest::helper_fail_),
            Status::NotSupported());
  ASSERT_EQ(cache()->Insert("k1", &item1, &CachelibWrapperTest::helper_),
            Status::OK());

  std::unique_ptr<SecondaryCacheResultHandle> handle;
  bool is_in_sec_cache{false};
  handle = cache()->Lookup("k1", test_item_creator, true
#if ROCKSDB_MAJOR > 7 || (ROCKSDB_MAJOR == 7 && ROCKSDB_MINOR >= 7)
                           ,
                           /*advise_erase=*/false
#endif
                           ,
                           is_in_sec_cache);
  ASSERT_EQ(handle, nullptr);
}

TEST_F(CachelibWrapperTest, LookupWhileCloseTest) {
  std::string str1 = RandomString(1020);
  TestItem item1(str1.data(), str1.length());
  ASSERT_EQ(cache()->Insert("k1", &item1, &CachelibWrapperTest::helper_),
            Status::OK());

  pthread_mutex_t mu;
  pthread_mutex_init(&mu, nullptr);

  pthread_cond_t cv_seq_1;
  pthread_cond_t cv_seq_2;
  pthread_cond_init(&cv_seq_1, nullptr);
  pthread_cond_init(&cv_seq_2, nullptr);
  bool is_in_sec_cache{false};

  auto lookup_fn = [&]() {
    std::unique_ptr<SecondaryCacheResultHandle> hdl =
        cache()->Lookup("k1", test_item_creator, false
#if ROCKSDB_MAJOR > 7 || (ROCKSDB_MAJOR == 7 && ROCKSDB_MINOR >= 7)
                        ,
                        /*advise_erase=*/false
#endif
                        ,
                        is_in_sec_cache);
    pthread_mutex_lock(&mu);
    pthread_cond_signal(&cv_seq_1);
    pthread_cond_wait(&cv_seq_2, &mu);
    hdl->Wait();
    TestItem* val = static_cast<TestItem*>(hdl->Value());
    EXPECT_NE(val, nullptr);
    EXPECT_EQ(memcmp(val->Buf(), item1.Buf(), item1.Size()), 0);
    delete val;
  };
  auto close_fn = [&]() {
    RocksCachelibWrapper* wrap_cache =
        static_cast<RocksCachelibWrapper*>(cache());
    wrap_cache->Close();
  };

  pthread_mutex_lock(&mu);
  std::thread lookup_thread(lookup_fn);
  pthread_cond_wait(&cv_seq_1, &mu);
  std::thread close_thread(close_fn);
  pthread_mutex_unlock(&mu);
  while (auto hdl = cache()->Lookup("k1", test_item_creator, true,
#if ROCKSDB_MAJOR > 7 || (ROCKSDB_MAJOR == 7 && ROCKSDB_MINOR >= 7)
                                    /*advise_erase=*/false,
#endif
                                    is_in_sec_cache)) {
    TestItem* item = static_cast<TestItem*>(hdl->Value());
    delete item;
    sleep(1);
  }
  pthread_mutex_lock(&mu);
  pthread_cond_signal(&cv_seq_2);
  pthread_mutex_unlock(&mu);

  lookup_thread.join();
  close_thread.join();

  // Verify that lookups fail, since the cache is closed
  ASSERT_EQ(cache()->Lookup("k1", test_item_creator, true,
#if ROCKSDB_MAJOR > 7 || (ROCKSDB_MAJOR == 7 && ROCKSDB_MINOR >= 7)
                            /*advise_erase=*/false,
#endif
                            is_in_sec_cache),
            nullptr);
  pthread_cond_destroy(&cv_seq_1);
  pthread_cond_destroy(&cv_seq_2);
  pthread_mutex_destroy(&mu);
}

class InsertWhileCloseTestItem : public CachelibWrapperTest::TestItem {
 public:
  InsertWhileCloseTestItem(const char* data,
                           size_t size,
                           pthread_mutex_t* _mu,
                           pthread_cond_t* _cv)
      : TestItem(data, size), mu_(_mu), cv_(_cv) {}

  pthread_mutex_t* mu() { return mu_; }
  pthread_cond_t* cv() { return cv_; }

 private:
  pthread_mutex_t* mu_;
  pthread_cond_t* cv_;
};

Status InsertWhileCloseTestCb(void* obj,
                              size_t offset,
                              size_t size,
                              void* out) {
  InsertWhileCloseTestItem* item = static_cast<InsertWhileCloseTestItem*>(obj);
  pthread_mutex_lock(item->mu());
  pthread_cond_wait(item->cv(), item->mu());
  return (*CachelibWrapperTest::helper_.saveto_cb)(obj, offset, size, out);
}

TEST_F(CachelibWrapperTest, InsertWhileCloseTest) {
  std::string str1 = RandomString(1020);
  TestItem item1(str1.data(), str1.length());
  ASSERT_EQ(cache()->Insert("k1", &item1, &CachelibWrapperTest::helper_),
            Status::OK());

  pthread_mutex_t mu;
  pthread_mutex_init(&mu, nullptr);

  pthread_cond_t cv_seq_1;
  pthread_cond_init(&cv_seq_1, nullptr);

  auto insert_fn = [&]() {
    std::string str = RandomString(1020);
    Cache::CacheItemHelper helper = CachelibWrapperTest::helper_;
    helper.saveto_cb = InsertWhileCloseTestCb;
    InsertWhileCloseTestItem item(str.data(), str.length(), &mu, &cv_seq_1);
    EXPECT_EQ(cache()->Insert("k2", &item, &helper), Status::OK());
  };
  auto close_fn = [&]() {
    RocksCachelibWrapper* wrap_cache =
        static_cast<RocksCachelibWrapper*>(cache());
    wrap_cache->Close();
  };

  std::thread insert_thread(insert_fn);
  std::thread close_thread(close_fn);
  bool is_in_sec_cache{false};
  while (auto hdl = cache()->Lookup("k1", test_item_creator, true,
#if ROCKSDB_MAJOR > 7 || (ROCKSDB_MAJOR == 7 && ROCKSDB_MINOR >= 7)
                                    /*advise_erase=*/false,
#endif
                                    is_in_sec_cache)) {
    TestItem* item = static_cast<TestItem*>(hdl->Value());
    delete item;
    sleep(1);
  }
  pthread_mutex_lock(&mu);
  pthread_cond_signal(&cv_seq_1);
  pthread_mutex_unlock(&mu);

  insert_thread.join();
  close_thread.join();

  // Verify that lookups fail, since the cache is closed
  ASSERT_EQ(cache()->Lookup("k1", test_item_creator, true,
#if ROCKSDB_MAJOR > 7 || (ROCKSDB_MAJOR == 7 && ROCKSDB_MINOR >= 7)
                            /*advise_erase=*/false,
#endif
                            is_in_sec_cache),
            nullptr);

  pthread_cond_destroy(&cv_seq_1);
  pthread_mutex_destroy(&mu);
}

TEST_F(CachelibWrapperTest, WaitAllWhileCloseTest) {
  // Make num_blocks larger than the volatile size by 200 in order to force
  // some items to spill into the cache file
  int num_blocks = kVolatileSize / 1020 + 200;
  std::vector<TestItem> items;
  for (int i = 0; i < num_blocks; ++i) {
    std::string str = RandomString(1020);
    items.emplace_back(str.data(), str.length());
    ASSERT_EQ(cache()->Insert("k" + std::to_string(i),
                              &items.back(),
                              &CachelibWrapperTest::helper_),
              Status::OK());
  }

  pthread_mutex_t mu;
  pthread_mutex_init(&mu, nullptr);

  pthread_cond_t cv_seq_1;
  pthread_cond_t cv_seq_2;
  pthread_cond_init(&cv_seq_1, nullptr);
  pthread_cond_init(&cv_seq_2, nullptr);
  bool is_in_sec_cache{false};

  auto lookup_fn = [&]() {
    std::vector<std::unique_ptr<SecondaryCacheResultHandle>> handles;
    std::vector<SecondaryCacheResultHandle*> handle_ptrs;
    for (int i = 0; i < 100; ++i) {
      handles.emplace_back(cache()->Lookup("k" + std::to_string(i),
                                           test_item_creator, false,
#if ROCKSDB_MAJOR > 7 || (ROCKSDB_MAJOR == 7 && ROCKSDB_MINOR >= 7)
                                           /*advise_erase=*/false,
#endif
                                           is_in_sec_cache));
      EXPECT_NE(handles.back(), nullptr);
      handle_ptrs.emplace_back(handles.back().get());
    }
    pthread_mutex_lock(&mu);
    pthread_cond_signal(&cv_seq_1);
    pthread_cond_wait(&cv_seq_2, &mu);
    cache()->WaitAll(handle_ptrs);
    for (int i = 0; i < 100; ++i) {
      TestItem* val = static_cast<TestItem*>(handle_ptrs[i]->Value());
      EXPECT_NE(val, nullptr);
      EXPECT_EQ(memcmp(val->Buf(), items[i].Buf(), items[i].Size()), 0);
      delete val;
    }
  };
  auto close_fn = [&]() {
    RocksCachelibWrapper* wrap_cache =
        static_cast<RocksCachelibWrapper*>(cache());
    wrap_cache->Close();
  };

  pthread_mutex_lock(&mu);
  std::thread lookup_thread(lookup_fn);
  pthread_cond_wait(&cv_seq_1, &mu);
  std::thread close_thread(close_fn);
  pthread_mutex_unlock(&mu);
  while (auto hdl = cache()->Lookup("k1", test_item_creator, true,
#if ROCKSDB_MAJOR > 7 || (ROCKSDB_MAJOR == 7 && ROCKSDB_MINOR >= 7)
                                    /*advise_erase=*/false,
#endif
                                    is_in_sec_cache)) {
    TestItem* item = static_cast<TestItem*>(hdl->Value());
    delete item;
    sleep(1);
  }
  pthread_mutex_lock(&mu);
  pthread_cond_signal(&cv_seq_2);
  pthread_mutex_unlock(&mu);

  lookup_thread.join();
  close_thread.join();

  // Verify that lookups fail, since the cache is closed
  ASSERT_EQ(cache()->Lookup("k1", test_item_creator, true,
#if ROCKSDB_MAJOR > 7 || (ROCKSDB_MAJOR == 7 && ROCKSDB_MINOR >= 7)
                            /*advise_erase=*/false,
#endif
                            is_in_sec_cache),
            nullptr);
  pthread_cond_destroy(&cv_seq_1);
  pthread_cond_destroy(&cv_seq_2);
  pthread_mutex_destroy(&mu);
}

#if 0
TEST_F(CachelibWrapperTest, UpdateMaxRateTest) {
  RocksCachelibOptions opts;
  opts.volatileSize = kVolatileSize;
  opts.cacheName = "CachelibWrapperTest";
  opts.fileName = path() + "/cachelib_wrapper_test_update_max_rate";
  opts.size = 64 << 20;
  opts.admPolicy = "dynamic_random";
  opts.maxWriteRate = 64 << 20;

  std::unique_ptr<SecondaryCache> sec_cache = NewRocksCachelibWrapper(opts);
  ASSERT_NE(sec_cache, nullptr);
  ASSERT_TRUE(static_cast<RocksCachelibWrapper*>(sec_cache.get())
                  ->UpdateMaxWriteRateForDynamicRandom(32 << 20));
}
#endif
  
TEST_F(CachelibWrapperTest, LargeItemTest) {
  std::string str1 = RandomString(8 << 20);
  TestItem item1(str1.data(), str1.length());
  ASSERT_EQ(cache()->Insert("k1", &item1, &CachelibWrapperTest::helper_),
            Status::InvalidArgument());

  std::unique_ptr<ROCKSDB_NAMESPACE::SecondaryCacheResultHandle> handle;
  bool is_in_sec_cache{false};
  handle = cache()->Lookup("k1", test_item_creator, true,
#if ROCKSDB_MAJOR > 7 || (ROCKSDB_MAJOR == 7 && ROCKSDB_MINOR >= 7)
                           /*advise_erase=*/false,
#endif
                           is_in_sec_cache);
  ASSERT_EQ(handle, nullptr);
  handle.reset();
}

#ifndef ROCKSDB_LITE
TEST_F(CachelibWrapperTest, CreateFromString) {
  ROCKSDB::NAMESPACE::ConfigOptions opts;
  opts.invoke_prepare_options = true;
  std::shared_ptr<ROCKSDB_NAMESPACE::SecondaryCache> scache;
  std::string props = "cachename=foo;"
		      "filename=bar;"
		      "size=1000000;" 
 		      "block_size=8192;" 
		      "region_size=4096;"
			"policy=unknown;" 
			"probablity=0.50;"
			"max_write_rate=1024;"
			"admission_write_rate=2048;"
			"volatile_size=2000000;"
			"bucket_power=48;"
    "lock_power=24;";
    
  ASSERT_EQ(ROCKSDB_NAMESPACE::SecondaryCache::CreateFromString(opts,
	 props + "id=" + RocksCachelibWrapper::kClassName() +
								&scache),
	    ROCKSDB_NAMESPACE::Status::OK());
  auto rco =  scache->GetOptions<RocksCachelibOptions>();
  ASSERT_NE(rco, nullptr);
  ASSERT_STREQ(rco->cacheName, "foo");
  ASSERT_STREQ(rco->fileName, "bar");
  ASSERT_EQ(rco->size, 1000000);
  ASSERT_EQ(rco->blockSize, 8192);
  ASSERT_EQ(rco->regionSize, 4096);
  ASSERT_EQ(rco->admProbability, 0.5);
  ASSERT_EQ(rco->admPolicy, "unknown");
  ASSERT_EQ(rco->maxWriteRate, 1024);
  ASSERT_EQ(rco->admissionWriteRate, 2048);
  ASSERT_EQ(rco->volatileSize, 2000000);
  ASSERT_EQ(rco->bktPower, 48);
  ASSERT_EQ(rco->lockPower, 24);
  
}
#endif // ROCKSDB_LITE
} // namespace rocks_secondary_cache
} // namespace facebook

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
