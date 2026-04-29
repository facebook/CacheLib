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

#pragma once

#include <folly/testing/TestUtil.h>

#include "cachelib/interface/CacheComponent.h"
#include "cachelib/interface/components/FlashCacheComponent.h"
#include "cachelib/interface/components/RAMCacheComponent.h"

namespace facebook::cachelib::interface::test {

class CacheFactory {
 public:
  virtual ~CacheFactory() = default;
  virtual std::unique_ptr<CacheComponent> create() = 0;
  virtual std::unique_ptr<CacheComponent> createPersistent() = 0;
  virtual void onShutdown(CacheComponent& /* component */) {}
  virtual Result<std::unique_ptr<CacheComponent>> recover() = 0;
};

class RAMCacheFactory : public CacheFactory {
 public:
  std::unique_ptr<CacheComponent> create() override;
  std::unique_ptr<CacheComponent> createPersistent() override;
  Result<std::unique_ptr<CacheComponent>> recover() override;

 private:
  folly::test::TemporaryDirectory tmpDir_;

  Result<RAMCacheComponent> createWithPersistence(
      RAMCacheComponent::PersistenceConfig pc);
  static LruAllocatorConfig createConfig();
  static RAMCacheComponent::PoolConfig createPoolConfig();
};

class FlashCacheFactory : public CacheFactory {
 public:
  FlashCacheFactory();
  std::unique_ptr<CacheComponent> create() override;
  std::unique_ptr<CacheComponent> createPersistent() override;
  void onShutdown(CacheComponent& /* component */) override;
  Result<std::unique_ptr<CacheComponent>> recover() override;

 protected:
  static constexpr size_t kMetadataSize{/* 32KB */ 32 * 1024};
  static constexpr size_t kCacheSize{/* 256KB */ 256 * 1024};
  static constexpr size_t kDeviceSize{kMetadataSize + kCacheSize};
  std::unique_ptr<navy::Device> savedDevice_;

  virtual Result<std::unique_ptr<CacheComponent>> createWithPersistence(
      std::unique_ptr<navy::Device> device,
      FlashCacheComponent::PersistenceConfig pc);
  static std::unique_ptr<navy::Device> makeDevice(size_t size);
  navy::BlockCache::Config makeConfig();

 private:
  static constexpr size_t kRegionSize{/* 16KB */ 16 * 1024};
  static constexpr uint32_t kIOAlignSize{4096};
  std::vector<uint32_t> hits_;
};

class ConsistentFlashCacheFactory : public FlashCacheFactory {
 protected:
  Result<std::unique_ptr<CacheComponent>> createWithPersistence(
      std::unique_ptr<navy::Device> device,
      FlashCacheComponent::PersistenceConfig pc) override;

 private:
  static constexpr size_t kShardsPower{4};
};

} // namespace facebook::cachelib::interface::test
