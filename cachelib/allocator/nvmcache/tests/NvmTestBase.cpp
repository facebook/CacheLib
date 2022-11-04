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

#include "cachelib/allocator/nvmcache/tests/NvmTestBase.h"

#include <folly/synchronization/Baton.h>

#include "cachelib/allocator/NvmCacheState.h"
#include "cachelib/allocator/tests/NvmTestUtils.h"

namespace facebook {
namespace cachelib {
namespace tests {

NvmCacheTest::NvmCacheTest() {
  cacheDir_ = folly::sformat("/tmp/nvmcache-cachedir/{}", ::getpid());
  util::makeDir(cacheDir_);
  config_ = utils::getNvmTestConfig(cacheDir_);

  {
    allocConfig_.enableCachePersistence(cacheDir_);
    allocConfig_.setRemoveCallback(
        [this](const LruAllocator::RemoveCbData&) { nEvictions_++; });
    allocConfig_.setCacheSize(20 * 1024 * 1024);

    // Disable slab rebalancing
    allocConfig_.enablePoolRebalancing(nullptr, std::chrono::seconds{0});

    LruAllocator::NvmCacheConfig nvmConfig;
    nvmConfig.navyConfig = config_;
    allocConfig_.enableNvmCache(nvmConfig);
  }
  makeCache();
}

AllocatorT& NvmCacheTest::makeCache() {
  cache_.reset();
  cache_ = std::make_unique<LruAllocator>(allocConfig_);
  id_ = cache_->addPool("default", poolSize_, poolAllocsizes_);
  return *cache_;
}

NvmCacheTest::~NvmCacheTest() { util::removePath(cacheDir_); }

bool NvmCacheTest::checkKeyExists(folly::StringPiece key, bool ramOnly) {
  return ramOnly ? cache_->peek(key) != nullptr : fetch(key, false) != nullptr;
}

WriteHandle NvmCacheTest::fetch(folly::StringPiece key, bool ramOnly) {
  auto hdl = ramOnly ? cache_->findFastImpl(key, AccessMode::kRead)
                     : cache_->findImpl(key, AccessMode::kRead);
  hdl.wait();
  return hdl;
}

WriteHandle NvmCacheTest::fetchToWrite(folly::StringPiece key, bool ramOnly) {
  auto hdl = ramOnly ? cache_->findFastToWrite(key) : cache_->findToWrite(key);
  hdl.wait();
  return hdl;
}

GlobalCacheStats NvmCacheTest::getStats() const {
  return cache_->getGlobalCacheStats();
}

void NvmCacheTest::convertToShmCache() {
  cache_.reset();
  cache_ =
      std::make_unique<LruAllocator>(LruAllocator::SharedMemNew, allocConfig_);
  id_ = cache_->addPool("default", poolSize_, poolAllocsizes_);
}

void NvmCacheTest::warmRoll() {
  if (cache_->shutDown() != LruAllocator::ShutDownStatus::kSuccess) {
    throw std::runtime_error("Failed to warm roll");
  }
  cache_.reset();
  cache_ = std::make_unique<LruAllocator>(LruAllocator::SharedMemAttach,
                                          allocConfig_);
}

void NvmCacheTest::coldRoll() {
  // to simulate a cold roll, we shutdown safely and then explicitly create a
  // new one for the ram part
  if (cache_->shutDown() != LruAllocator::ShutDownStatus::kSuccess) {
    throw std::runtime_error("Failed to cold roll");
  }
  cache_ =
      std::make_unique<LruAllocator>(LruAllocator::SharedMemNew, allocConfig_);
  id_ = cache_->addPool("default", poolSize_, poolAllocsizes_);
}

void NvmCacheTest::iceRoll() {
  // shutdown with warm roll and indicatae that we want to drop navy
  if (cache_->shutDown() != LruAllocator::ShutDownStatus::kSuccess) {
    throw std::runtime_error("Failed to ice roll");
  }
  cache_.reset();

  const auto fileName = NvmCacheState::getFileForNvmCacheDrop(cacheDir_);
  {
    std::ofstream dropFile(fileName, std::ios::trunc);
    dropFile.flush();
  }

  if (!util::getStatIfExists(fileName, nullptr)) {
    throw std::runtime_error(
        folly::sformat("Failed to create drop file {}", fileName));
  }

  cache_ = std::make_unique<LruAllocator>(LruAllocator::SharedMemAttach,
                                          allocConfig_);
  if (util::getStatIfExists(fileName, nullptr)) {
    throw std::runtime_error(folly::sformat(
        "Drop file {} exists after re-initializing the cache", fileName));
  }
}

void NvmCacheTest::iceColdRoll() {
  // shutdown with cold roll and indicate that we want to drop nvm
  cache_.reset();

  const auto fileName = NvmCacheState::getFileForNvmCacheDrop(cacheDir_);
  {
    std::ofstream dropFile(fileName, std::ios::trunc);
    dropFile.flush();
  }

  if (!util::getStatIfExists(fileName, nullptr)) {
    throw std::runtime_error(
        folly::sformat("Failed to create drop file {}", fileName));
  }

  cache_ =
      std::make_unique<LruAllocator>(LruAllocator::SharedMemNew, allocConfig_);
  id_ = cache_->addPool("default", poolSize_, poolAllocsizes_);
}

} // namespace tests
} // namespace cachelib
} // namespace facebook
