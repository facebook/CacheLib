#pragma once

#include <folly/Format.h>
#include <folly/Range.h>
#include <folly/container/F14Map.h>
#include <folly/io/IOBuf.h>

#include <list>
#include <memory>
#include <mutex>
#include <string>

#include "cachelib/common/Exceptions.h"
#include "cachelib/common/PercentileStats.h"
#include "cachelib/common/Utils.h"

namespace facebook {
namespace cachelib {

// Holds all necessary data to do an async dipper put that is queued to dipper
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

// Holds all necessary data to do an async dipper remove
class DelCtx {
 public:
  DelCtx(folly::StringPiece key, util::LatencyTracker tracker)
      : key_(key.toString()), tracker_(std::move(tracker)) {}

  folly::StringPiece key() const { return key_; }

  static folly::StringPiece type() { return "del ctx"; }

 private:
  // key corresponding to the context
  std::string key_;

  //< tracking latency of the put operation
  util::LatencyTracker tracker_;
};

namespace detail {
// Keeps track of the contexts that are utilized by in-flight requests in
// dipper. The contexts need to be accessed through their reference. Hence
// they are keyed by the actual instance of the context.
template <typename T>
class ContextMap {
 public:
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
  std::mutex mutex_;
};
} // namespace detail

using PutContexts = detail::ContextMap<PutCtx>;
using DelContexts = detail::ContextMap<DelCtx>;

} // namespace cachelib
} // namespace facebook
