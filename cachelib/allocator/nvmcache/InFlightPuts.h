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

#include <folly/Hash.h>
#include <folly/Range.h>
#include <folly/container/F14Map.h>
#include <folly/fibers/TimedMutex.h>
#include <folly/lang/Align.h>
#include <folly/logging/xlog.h>

#include <utility>

namespace facebook {
namespace cachelib {

using folly::fibers::TimedMutex;

// Utility to track inflight puts in nvmcache through a token. Tokens can be
// invalidated and can be used to execute some function if not invalidated. The
// user guarantees that the lifetime of the token is within the lifetime of the
// string piece with which they obtain the token.
class alignas(folly::hardware_destructive_interference_size) InFlightPuts {
 public:
  class PutToken;

  enum class PutTokenError { TRY_LOCK_FAIL, TOKEN_EXISTS, CALLBACK_FAILED };

  // inserts an in-flight put into the map if none exists and acquires a
  // token.  If a token is acquired successfully, will call fn() while
  // holding the mutex_. If fn() returns false, deletes the token otherwise
  // return the valid token.
  template <typename F>
  folly::Expected<PutToken, PutTokenError> tryAcquireToken(
      folly::StringPiece key, F&& fn) {
    std::unique_lock l{rwMutex_, std::try_to_lock};
    if (!l.owns_lock()) {
      return folly::makeUnexpected(PutTokenError::TRY_LOCK_FAIL);
    }

    auto ret = keys_.emplace(key, std::make_unique<std::atomic<bool>>(true));
    // record for same key being inflight written to nvmcache should be rare.
    // In that case, fail the latter one.
    if (!ret.second) {
      return folly::makeUnexpected(PutTokenError::TOKEN_EXISTS);
    }

    bool fnRet = std::forward<F>(fn)();
    if (fnRet) {
      return PutToken{key, *this};
    }

    // if fn() failed, erase the token
    auto eraseRet = keys_.erase(key);
    XDCHECK_EQ(eraseRet, 1u);
    return folly::makeUnexpected(PutTokenError::CALLBACK_FAILED);
  }

  // marks the token as invalidated. This will ensure that we dont execute any
  // function on this token and simply remove the token when the token gets
  // destroyed.
  void invalidateToken(folly::StringPiece key) {
    std::shared_lock l{rwMutex_};
    auto it = keys_.find(key);
    if (it != keys_.end()) {
      it->second->store(false);
    }
  }

  // Represents an insertion into the inflight map. this token can be used to
  // execute some action if the token was not invalidated in the mean time.
  class PutToken {
   public:
    PutToken() noexcept {}
    ~PutToken() {
      if (puts_) {
        puts_->removeToken(key_);
      }
    }

    // disallow copying
    PutToken(const PutToken&) = delete;
    PutToken& operator=(const PutToken&) = delete;

    // moving is okay
    PutToken(PutToken&& other) noexcept : key_(other.key_), puts_(other.puts_) {
      other.reset();
    }

    PutToken& operator=(PutToken&& other) noexcept {
      if (this != &other) {
        this->~PutToken();
        new (this) PutToken(std::move(other));
      }
      return *this;
    }

    // returns true if the token was valid one upon construction. Does not
    // reflect if the token was invalidated after construction.
    bool isValid() const noexcept { return puts_ != nullptr; }

    // executes the fn if the token is valid and the there has been no
    // invalidation. destroys the token state accordingly.
    template <typename F>
    bool executeIfValid(F&& fn) {
      if (isValid() &&
          puts_->executeIfValid(key_, std::forward<decltype(fn)>(fn))) {
        // successfully executed, reset the token.
        reset();
        return true;
      }
      return false;
    }

   private:
    void reset() noexcept {
      puts_ = nullptr;
      key_.clear();
      XDCHECK(!isValid());
    }

    friend InFlightPuts;
    PutToken(folly::StringPiece key, InFlightPuts& puts)
        : key_(key), puts_(&puts) {}

    // key corresponding to the token
    folly::StringPiece key_{};

    // map holding the state
    InFlightPuts* puts_{nullptr};
  };

 private:
  // execute only if the token is present and was not invalidated.
  //  @param key the item key
  //  @param fn  function to execute
  //
  //  @return  true if the function was executed and token was destroyed
  //          appropriately
  //  @throw    if fn throws, token is preserved.
  template <typename F>
  bool executeIfValid(folly::StringPiece key, F&& fn) {
    std::unique_lock l{rwMutex_};
    auto it = keys_.find(key);
    const bool valid = it != keys_.end() && it->second->load();
    if (valid) {
      fn();
      keys_.erase(it);
      return true;
    }
    return false;
  }

  // erases the record from inflight map.
  void removeToken(folly::StringPiece key) {
    std::unique_lock l{rwMutex_};
    auto res = keys_.erase(key);
    XDCHECK_EQ(res, 1u);
  }

  // map storing the presence of a token  and its validity
  folly::F14FastMap<folly::StringPiece,
                    std::unique_ptr<std::atomic<bool>>,
                    folly::Hash>
      keys_;

  // mutex protecting the map.
  folly::fibers::TimedRWMutexWritePriority<folly::fibers::GenericBaton>
      rwMutex_;
};

} // namespace cachelib
} // namespace facebook
