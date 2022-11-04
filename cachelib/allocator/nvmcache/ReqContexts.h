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

#include <folly/Format.h>
#include <folly/Range.h>
#include <folly/container/F14Map.h>
#include <folly/io/IOBuf.h>

#include <list>
#include <memory>
#include <mutex>
#include <string>

#include "cachelib/allocator/nvmcache/TombStones.h"
#include "cachelib/common/Exceptions.h"
#include "cachelib/common/PercentileStats.h"
#include "cachelib/common/Utils.h"

namespace facebook {
namespace cachelib {

// Holds all necessary data to do an async nvm put that is queued to nvm
class PutCtx {
 public:
  PutCtx(folly::StringPiece _key,
         folly::IOBuf buf,
         util::LatencyTracker tracker)
      : buf_(std::move(buf)),
        key_(_key.toString()),
        tracker_{std::move(tracker)} {}

  // @return   key as StringPiece
  folly::StringPiece key() const { return {key_.data(), key_.length()}; }

  static folly::StringPiece type() { return "put ctx"; }

 private:
  folly::IOBuf buf_; //< iobuf holding the value to put
  std::string key_;  //< key to store
  //< tracking latency of the put operation
  util::LatencyTracker tracker_;
};

// Holds all necessary data to do an async nvm remove
class DelCtx {
 public:
  DelCtx(folly::StringPiece,
         util::LatencyTracker tracker,
         TombStones::Guard tombstone)
      : tracker_(std::move(tracker)), tombstone_(std::move(tombstone)) {}

  folly::StringPiece key() const { return tombstone_.key(); }

  static folly::StringPiece type() { return "del ctx"; }

 private:
  //< tracking latency of the put operation
  util::LatencyTracker tracker_;

  // a tombstone for the remove job, preventing concurrent get that could cause
  // inconsistent result
  TombStones::Guard tombstone_;
};

namespace detail {
// Keeps track of the contexts that are utilized by in-flight requests in
// nvm. The contexts need to be accessed through their reference. Hence
// they are keyed by the actual instance of the context.
template <typename T>
class ContextMap {
 public:
  // create, return a context for an in-flight request for given key,
  // and track this context in the map.
  // @param key   the item key
  // @param args  the arguments being forward to create context
  // @return      the created context
  // @throw std::invalid_argument if key already has context being tracked
  template <typename... Args>
  T& createContext(folly::StringPiece key, Args&&... args) {
    // construct the context first since it is the source of the key for its
    // lookup.
    auto ctx = std::make_unique<T>(key, std::forward<Args>(args)...);
    uintptr_t ctxKey = reinterpret_cast<uintptr_t>(ctx.get());
    std::lock_guard<std::mutex> l(mutex_);
    auto it = map_.find(ctxKey);
    if (it != map_.end()) {
      throw std::invalid_argument(
          folly::sformat("Duplicate {} for key {}", T::type(), key));
    }

    auto [kv, inserted] = map_.emplace(ctxKey, std::move(ctx));
    DCHECK(inserted);
    return *kv->second;
  }

  // returns true if this map has active contexts and false if the map is
  // empty.
  bool hasContexts() const {
    std::lock_guard<std::mutex> l(mutex_);
    return map_.size() != 0;
  }

  // Remove the put context by passing a reference to it. After this, the
  // caller is supposed to not refer to it anymore.
  void destroyContext(const T& ctx) {
    uintptr_t ctxKey = reinterpret_cast<uintptr_t>(&ctx);
    std::lock_guard<std::mutex> l(mutex_);
    size_t numRemoved = map_.erase(ctxKey);
    if (numRemoved != 1) {
      throw std::invalid_argument(
          folly::sformat("Invalid {} state for {}, found {}",
                         T::type(),
                         ctx.key(),
                         numRemoved));
    }
  }

 private:
  folly::F14FastMap<uintptr_t, std::unique_ptr<T>> map_;
  mutable std::mutex mutex_;
};
} // namespace detail

using PutContexts = detail::ContextMap<PutCtx>;
using DelContexts = detail::ContextMap<DelCtx>;

} // namespace cachelib
} // namespace facebook
