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

#include <folly/Random.h>
#include <gtest/gtest.h>

#include "cachelib/cachebench/workload/KVReplayGenerator.h"

namespace facebook {
namespace cachelib {
namespace cachebench {
namespace tests {

struct TraceEntry {
  TraceEntry(size_t keySize,
             const char* op,
             size_t size,
             size_t opCnt,
             std::optional<size_t> ttl,
             bool valid)
      : keySize_(keySize),
        op_(op),
        size_(size),
        opCnt_(opCnt),
        ttl_(ttl),
        valid_(valid) {
    key_ = folly::sformat("{}", folly::Random::rand32());
    // supported input formats are:
    //      <key>,<op>,<size>,<op_count>,<key_size>[,[<ttl>[,.*]]]
    line_ =
        folly::sformat("{},{},{},{},{}", key_, op_, size_, opCnt_, keySize_);
    if (ttl_) {
      line_ += folly::sformat(",{}", *ttl_);
    }
  }

  void validate(const ReqWrapper& req) {
    if (!valid_) {
      return;
    }
    const std::string& reqKey = req.req_.key;
    ASSERT_EQ(0, reqKey.compare(0, key_.size(), key_));
    size_t expKeySize = std::max<size_t>(keySize_, reqKey.size());
    expKeySize = std::min<size_t>(expKeySize, 256);
    ASSERT_EQ(reqKey.size(), expKeySize);
    ASSERT_EQ(req.req_.getOp(), getOpType());
  }

  OpType getOpType() {
    if (!op_.compare("GET") || !op_.compare("GET_LEASE")) {
      return OpType::kGet;
    } else if (!op_.compare("SET") || !op_.compare("SET_LEASE")) {
      return OpType::kSet;
    } else if (!op_.compare("DELETE")) {
      return OpType::kDel;
    }
    return OpType::kSize;
  }

  std::string key_;
  std::string line_;

  size_t keySize_;
  std::string op_;
  size_t size_;
  size_t opCnt_;
  std::optional<size_t> ttl_;
  bool valid_;
};

TEST(KVReplayGeneratorTest, BasicFormat) {
  StressorConfig config;
  KVReplayGenerator replayer{config};
  std::mt19937_64 gen;

  auto req = std::make_unique<ReqWrapper>();
  // blank line
  ASSERT_FALSE(replayer.parseRequest("", req));
  // header lines
  ASSERT_FALSE(replayer.parseRequest("key,op,size,op_count,key_size", req));
  ASSERT_FALSE(replayer.parseRequest("key,op,size,op_count,key_size,ttl", req));

  // test set
  std::vector<TraceEntry> traces = {
      // <key_size>,<op>,<size>,<op_count>,<ttl>,<valid>
      {7, "GET", 0, 2, std::nullopt, true},
      {7, "GET", 0, 2, 50, true},
      {7, "GET_LEASE", 0, 2, 50, true},
      {20, "SET", 100, 35, std::nullopt, true},
      {20, "SET", 100, 35, 3600, true},
      {20, "SAT", 100, 35, 3600, false}, // invalid op name
      {20, "SET_LEASE", 100, 35, 3600, true},
      {7, "GET", 0, 0, std::nullopt, false},      // invalid op count
      {7, "GET", 0, 0, 600, false},               // invalid op count
      {1024, "SET", 100, 35, 300, true},          // key truncated
      {1024, "SET", 100, 35, std::nullopt, true}, // key truncated
  };

  for (auto& trace : traces) {
    ASSERT_EQ(replayer.parseRequest(trace.line_, req), trace.valid_);
    trace.validate(*req);
  }

  // trailing comma
  for (auto& trace : traces) {
    trace.line_.append(",");

    ASSERT_EQ(replayer.parseRequest(trace.line_, req), trace.valid_);
    trace.validate(*req);
  }

  replayer.markShutdown();
}

} // namespace tests
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
