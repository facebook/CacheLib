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
#include <folly/Random.h>

#include <chrono>
#include <ctime>
#include <string>
#include <vector>

namespace facebook {
namespace cachelib {
namespace cachebench {

// Operations that the stressor supports
// They translate into the following cachelib operations
//  Set: allocate + insertOrReplace
//  get: find
//  del: remove
enum class OpType {
  kSet = 0,
  kGet,
  kDel,

  kAddChained, // allocate a parent and a certain number of chained items
  // key will be randomly generated, operation will be get
  kLoneGet,
  kLoneSet,

  kUpdate, // in-place mutation

  kCouldExist,

  kSize
};

enum class OpResultType {
  kNop = 0,
  kGetMiss,
  kGetHit,
  kSetSuccess,
  kSetFailure,
  kSetSkip,
  kCouldExistTrue,
  kCouldExistFalse
};

struct Request {
  Request(std::string& k,
          std::vector<size_t>::iterator b,
          std::vector<size_t>::iterator e)
      : key(k), sizeBegin(b), sizeEnd(e) {}

  Request(std::string& k,
          std::vector<size_t>::iterator b,
          std::vector<size_t>::iterator e,
          OpType o)
      : key(k), sizeBegin(b), sizeEnd(e), op(o) {}

  Request(std::string& k,
          std::vector<size_t>::iterator b,
          std::vector<size_t>::iterator e,
          OpType o,
          uint64_t reqId)
      : key(k), sizeBegin(b), sizeEnd(e), requestId(reqId), op(o) {}

  Request(std::string& k,
          std::vector<size_t>::iterator b,
          std::vector<size_t>::iterator e,
          OpType o,
          uint32_t ttl,
          uint64_t reqId,
          const std::unordered_map<std::string, std::string>& admFeatureM,
          const std::string& value)
      : key(k),
        sizeBegin(b),
        sizeEnd(e),
        ttlSecs(ttl),
        requestId(reqId),
        admFeatureMap(admFeatureM),
        itemValue(value),
        op(o) {}

  Request(std::string& k,
          std::vector<size_t>::iterator b,
          std::vector<size_t>::iterator e,
          uint64_t reqId,
          const Request& other)
      : key(k),
        sizeBegin(b),
        sizeEnd(e),
        ttlSecs(other.ttlSecs),
        requestId(reqId),
        admFeatureMap(other.admFeatureMap),
        timestamp(other.timestamp),
        itemValue(other.itemValue),
        op(other.getOp()) {}

  static std::string getUniqueKey() {
    return std::string(folly::to<std::string>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count() +
        folly::Random::rand32()));
  }

  Request(Request&& r) noexcept
      : key(r.key), sizeBegin(r.sizeBegin), sizeEnd(r.sizeEnd) {}
  Request& operator=(Request&& r) = delete;

  OpType getOp() const noexcept { return op.load(); }
  void setOp(OpType o) noexcept { op = o; }

  std::string& key;

  // size iterators in case this request is
  // deemed to be a chained item.
  // If not chained, the size is *sizeBegin
  std::vector<size_t>::iterator sizeBegin;
  std::vector<size_t>::iterator sizeEnd;

  // TTL in seconds.
  uint32_t ttlSecs{0};

  const std::optional<uint64_t> requestId;

  // Feature map for this request sample, which is used for for admission
  // policy: feature name --> feature value
  const std::unordered_map<std::string, std::string> admFeatureMap;

  // Custom timestamp in second associated with the request
  // May not have to be the same as wall clock
  uint64_t timestamp{0};

  // Use case specific data that will be included in the request. This can be
  // used to track metadata that is specific to a particular application.
  std::string itemValue;

 private:
  std::atomic<OpType> op{OpType::kGet};
};

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
