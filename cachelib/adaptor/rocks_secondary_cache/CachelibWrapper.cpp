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

#include "CachelibWrapper.h"

#include "folly/init/Init.h"
#include "folly/synchronization/Rcu.h"
#include "rocksdb/convenience.h"
#include "rocksdb/version.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"

namespace facebook {
namespace rocks_secondary_cache {

#define FB_CACHE_MAX_ITEM_SIZE 4 << 20

namespace {
// We use a separate RCU domain since read side critical sections can block
// on IO, and we don't want to interfere with other system activities that
// use RCU synchronization.
folly::rcu_domain& GetRcuDomain() {
  static folly::rcu_domain domain;
  return domain;
}

class RocksCachelibWrapperHandle : public rocksdb::SecondaryCacheResultHandle {
 public:
  RocksCachelibWrapperHandle(folly::SemiFuture<FbCacheReadHandle>&& future,
                             const rocksdb::Cache::CreateCallback& create_cb,
                             std::unique_lock<folly::rcu_domain>&& guard)
      : future_(std::move(future)),
        create_cb_(create_cb),
        val_(nullptr),
        charge_(0),
        is_value_ready_(false),
        guard_(std::move(guard)) {}
  ~RocksCachelibWrapperHandle() override {}

  RocksCachelibWrapperHandle(const RocksCachelibWrapperHandle&) = delete;
  RocksCachelibWrapperHandle& operator=(const RocksCachelibWrapperHandle&) =
      delete;

  bool IsReady() override {
    bool ready = true;
    if (!is_value_ready_) {
      ready = future_.isReady();
      if (ready) {
        handle_ = std::move(future_).value();
        CalcValue();
      }
    }
    return ready;
  }

  void Wait() override {
    if (!is_value_ready_) {
      future_.wait();
      handle_ = std::move(future_).value();
      CalcValue();
    }
  }

  static void WaitAll(
      std::vector<rocksdb::SecondaryCacheResultHandle*> handles) {
    std::vector<folly::SemiFuture<FbCacheReadHandle>> h_semi;
    for (auto h_ptr : handles) {
      RocksCachelibWrapperHandle* hdl =
          static_cast<RocksCachelibWrapperHandle*>(h_ptr);
      if (hdl->is_value_ready_) {
        continue;
      }
      h_semi.emplace_back(std::move(hdl->future_));
    }
    auto all_handles = folly::collectAll(std::move(h_semi));
    auto new_handles = std::move(all_handles).get();
    assert(new_handles.size() == h_semi.size());
    int result_idx = 0;
    for (size_t i = 0; i < handles.size(); ++i) {
      RocksCachelibWrapperHandle* hdl =
          static_cast<RocksCachelibWrapperHandle*>(handles[i]);
      if (hdl->is_value_ready_) {
        continue;
      }
      hdl->handle_ = std::move(new_handles[result_idx]).value();
      result_idx++;
      hdl->CalcValue();
    }
  }

  void* Value() override { return val_; }

  size_t Size() override { return charge_; }

 private:
  FbCacheReadHandle handle_;
  folly::SemiFuture<FbCacheReadHandle> future_;
  const rocksdb::Cache::CreateCallback create_cb_;
  void* val_;
  size_t charge_;
  bool is_value_ready_;
  std::unique_lock<folly::rcu_domain> guard_;

  void CalcValue() {
    is_value_ready_ = true;

    if (handle_) {
      uint32_t size = handle_->getSize();
      const void* item = handle_->getMemory();
      rocksdb::Status s;

      s = create_cb_(item, size, &val_, &charge_);
      if (!s.ok()) {
        val_ = nullptr;
      }
      handle_.reset();
    }
  }
};

static std::unordered_map<std::string, ROCKSDB_NAMESPACE::OptionTypeInfo>
rocks_cachelib_type_info = {
#ifndef ROCKSDB_LITE
  {"cachename",
   {offsetof(struct RocksCachelibOptions, cacheName), ROCKSDB_NAMESPACE::OptionType::kString}},
  {"filename",
   {offsetof(struct RocksCachelibOptions, fileName), ROCKSDB_NAMESPACE::OptionType::kString}},
  {"size",
   {offsetof(struct RocksCachelibOptions, size), ROCKSDB_NAMESPACE::OptionType::kSizeT}},
  {"block_size",
   {offsetof(struct RocksCachelibOptions, blockSize), ROCKSDB_NAMESPACE::OptionType::kSizeT}},
  {"region_size",
   {offsetof(struct RocksCachelibOptions, regionSize), ROCKSDB_NAMESPACE::OptionType::kSizeT}},
  {"policy",
   {offsetof(struct RocksCachelibOptions, admPolicy), ROCKSDB_NAMESPACE::OptionType::kString}},
  {"probability",
   {offsetof(struct RocksCachelibOptions, admProbability), ROCKSDB_NAMESPACE::OptionType::kDouble}},
  {"max_write_rate",
   {offsetof(struct RocksCachelibOptions, maxWriteRate), ROCKSDB_NAMESPACE::OptionType::kUInt64T}},
  {"admission_write_rate",
   {offsetof(struct RocksCachelibOptions, admissionWriteRate), ROCKSDB_NAMESPACE::OptionType::kUInt64T}},
  {"volatile_size",
   {offsetof(struct RocksCachelibOptions, volatileSize), ROCKSDB_NAMESPACE::OptionType::kSizeT}},
  {"bucket_power",
   {offsetof(struct RocksCachelibOptions, bktPower), ROCKSDB_NAMESPACE::OptionType::kUInt32T}},
  {"lock_power",
   {offsetof(struct RocksCachelibOptions, lockPower), ROCKSDB_NAMESPACE::OptionType::kUInt32T}},
#endif // ROCKSDB_LITE
};
} // namespace

RocksCachelibWrapper::RocksCachelibWrapper(const RocksCachelibOptions& options)
  : options_(options), cache_(nullptr) {
  RegisterOptions(options_, rocks_cachelib_type_info);
}
  
ROCKSDB_NAMESPACE::Status RocksCachelibWrapper::PrepareOptions(const ROCKSDB_NAMESPACE::ConfigOptions& opts) {
  FbCache* cache = cache_.load();

  if (!cache) {
    cachelib::PoolId defaultPool;
    FbCacheConfig config;
    NvmCacheConfig nvmConfig;
    
    nvmConfig.navyConfig.setBlockSize(options_.blockSize);
    nvmConfig.navyConfig.setSimpleFile(options_.fileName,
				       options_.size,
				       /*truncateFile=*/true);
    nvmConfig.navyConfig.blockCache().setRegionSize(options_.regionSize);
    if (options_.admPolicy == "random") {
      nvmConfig.navyConfig.enableRandomAdmPolicy().setAdmProbability(
       options_.admProbability);
    } else {
      nvmConfig.navyConfig.enableDynamicRandomAdmPolicy()
          .setMaxWriteRate(options_.maxWriteRate)
          .setAdmWriteRate(options_.admissionWriteRate);
    }
    nvmConfig.enableFastNegativeLookups = true;

    config.setCacheSize(options_.volatileSize)
      .setCacheName(options_.cacheName)
      .setAccessConfig(
          {options_.bktPower /* bucket power */, options_.lockPower /* lock power */})
      .enableNvmCache(nvmConfig)
      .validate(); // will throw if bad config
    auto new_cache = std::make_unique<FbCache>(config);
    pool_ =
      new_cache->addPool("default", new_cache->getCacheMemoryStats().cacheSize);
    cache_.store(new_cache.release());
  }
  return SecondaryCache::PrepareOptions(opts);
}
  
RocksCachelibWrapper::~RocksCachelibWrapper() { Close(); }

rocksdb::Status RocksCachelibWrapper::Insert(
    const rocksdb::Slice& key,
    void* value,
    const rocksdb::Cache::CacheItemHelper* helper) {
  FbCacheKey k(key.data(), key.size());
  size_t size;
  void* buf;
  rocksdb::Status s;
  std::scoped_lock guard(GetRcuDomain());
  FbCache* cache = cache_.load();

  if (cache) {
    size = (*helper->size_cb)(value);
    if (FbCacheItem::getRequiredSize(k, size) <= FB_CACHE_MAX_ITEM_SIZE) {
      auto handle = cache->allocate(pool_, k, size);
      if (handle) {
        buf = handle->getMemory();
        s = (*helper->saveto_cb)(value, /*offset=*/0, size, buf);
        try {
          cache->insertOrReplace(handle);
        } catch (const std::exception& ex) {
          s = rocksdb::Status::Aborted(folly::sformat(
              "Cachelib insertOrReplace exception, error:{}", ex.what()));
        }
      }
    } else {
      s = rocksdb::Status::InvalidArgument();
    }
  }
  return s;
}

std::unique_ptr<rocksdb::SecondaryCacheResultHandle>
RocksCachelibWrapper::Lookup(const rocksdb::Slice& key,
                             const rocksdb::Cache::CreateCallback& create_cb,
                             bool wait
#if ROCKSDB_MAJOR > 7 || (ROCKSDB_MAJOR == 7 && ROCKSDB_MINOR >= 7)
                             ,
                             bool /*advise_erase*/
#endif
                             ,
                             bool& is_in_sec_cache) {
  std::unique_lock guard(GetRcuDomain());
  FbCache* cache = cache_.load();
  std::unique_ptr<rocksdb::SecondaryCacheResultHandle> hdl;

  if (cache) {
    auto handle = cache->find(FbCacheKey(key.data(), key.size()));
    // We cannot dereference the handle in anyway. Any dereference will make it
    // synchronous, so get the SamiFuture right away
    // std::move the std::unique_lock<rcu_domain> (reader lock) to the
    // RocksCachelibWrapperHandle, and will be released when the handle is
    // destroyed.
    hdl = std::make_unique<RocksCachelibWrapperHandle>(
        std::move(handle).toSemiFuture(), create_cb, std::move(guard));
    if (hdl->IsReady() || wait) {
      if (!hdl->IsReady()) {
        hdl->Wait();
      }
      if (hdl->Value() == nullptr) {
        hdl.reset();
      }
    }
  }

#if ROCKSDB_MAJOR > 7 || (ROCKSDB_MAJOR == 7 && ROCKSDB_MINOR >= 2)
  is_in_sec_cache = hdl != nullptr;
#endif
  return hdl;
}

void RocksCachelibWrapper::Erase(const rocksdb::Slice& key) {
  std::scoped_lock guard(GetRcuDomain());
  FbCache* cache = cache_.load();

  if (cache) {
    cache->remove(FbCacheKey(key.data(), key.size()));
  }
}

void RocksCachelibWrapper::WaitAll(
    std::vector<rocksdb::SecondaryCacheResultHandle*> handles) {
  RocksCachelibWrapperHandle::WaitAll(handles);
}

void RocksCachelibWrapper::Close() {
  FbCache* cache = cache_.load();
  if (cache) {
    // Nullify the cache pointer, then wait for all read side critical
    // sections already started to finish, and then delete the cache
    cache_.store(nullptr);
    GetRcuDomain().synchronize();
    delete cache;
  }
}

// Global cache object and a default cache pool
std::unique_ptr<rocksdb::SecondaryCache> NewRocksCachelibWrapper(
    const RocksCachelibOptions& opts) {
  std::unique_ptr<rocksdb::SecondaryCache> secondary = std::make_unique<RocksCachelibWrapper>(opts);
  assert(secondary->PrepareOptions(ROCKSDB_NAMESPACE::ConfigOptions()).ok());
  return secondary;
}

#ifndef ROCKSDB_LITE
int register_CachelibObjects(ROCKSDB_NAMESPACE::ObjectLibrary& library, const std::string&) {
  library.AddFactory<ROCKSDB_NAMESPACE::SecondaryCache>(RocksCachelibWrapper::kClassName(), 
      [](const std::string& uri, std::unique_ptr<ROCKSDB_NAMESPACE::SecondaryCache>* guard,
         std::string* /*errmsg*/) {
	RocksCachelibOptions options;
	guard->reset(new RocksCachelibWrapper(options));
        return guard->get();
      });
  return 1;
}
#endif // ROCKSDB_LITE
} // namespace rocks_secondary_cache
} // namespace facebook


