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

#include "cachelib/interface/components/FlashCacheComponent.h"

using namespace facebook::cachelib::navy;

namespace facebook::cachelib::interface {

/* static */ Result<FlashCacheComponent> FlashCacheComponent::create(
    std::string name, navy::BlockCache::Config&& config) noexcept {
  try {
    return FlashCacheComponent(std::move(name), std::move(config));
  } catch (const std::invalid_argument& ia) {
    return makeError(Error::Code::INVALID_CONFIG, ia.what());
  }
}

const std::string& FlashCacheComponent::getName() const noexcept {
  return name_;
}

folly::coro::Task<Result<AllocatedHandle>> FlashCacheComponent::allocate(
    Key /* key */,
    uint32_t /* size */,
    uint32_t /* creationTime */,
    uint32_t /* ttlSecs */) {
  co_return makeError(Error::Code::UNIMPLEMENTED, "not yet implemented");
}

folly::coro::Task<UnitResult> FlashCacheComponent::insert(
    AllocatedHandle&& /* handle */) {
  co_return makeError(Error::Code::UNIMPLEMENTED, "not yet implemented");
}

folly::coro::Task<Result<std::optional<AllocatedHandle>>>
FlashCacheComponent::insertOrReplace(AllocatedHandle&& /* handle */) {
  co_return makeError(Error::Code::UNIMPLEMENTED, "not yet implemented");
}

folly::coro::Task<Result<std::optional<ReadHandle>>> FlashCacheComponent::find(
    Key /* key */) {
  co_return makeError(Error::Code::UNIMPLEMENTED, "not yet implemented");
}

folly::coro::Task<Result<std::optional<WriteHandle>>>
FlashCacheComponent::findToWrite(Key /* key */) {
  co_return makeError(Error::Code::UNIMPLEMENTED, "not yet implemented");
}

folly::coro::Task<Result<bool>> FlashCacheComponent::remove(Key /* key */) {
  co_return makeError(Error::Code::UNIMPLEMENTED, "not yet implemented");
}

folly::coro::Task<UnitResult> FlashCacheComponent::remove(
    ReadHandle&& /* handle */) {
  co_return makeError(Error::Code::UNIMPLEMENTED, "not yet implemented");
}

FlashCacheComponent::FlashCacheComponent(std::string&& name,
                                         navy::BlockCache::Config&& config)
    : name_(std::move(name)),
      cache_(std::make_unique<navy::BlockCache>(std::move(config))) {}

folly::coro::Task<void> FlashCacheComponent::release(CacheItem& /* item */,
                                                     bool /* inserted */) {
  co_return;
}

} // namespace facebook::cachelib::interface
