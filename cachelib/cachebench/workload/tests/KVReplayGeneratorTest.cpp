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

enum class HeaderFormat { v1, v2 };

struct TraceEntry {
  TraceEntry(uint64_t opTime,
             size_t keySize,
             const char* op,
             size_t size,
             size_t opCnt,
             size_t cacheHits,
             std::optional<size_t> ttl,
             bool valid)
      : opTime_(opTime),
        keySize_(keySize),
        op_(op),
        size_(size),
        opCnt_(opCnt),
        cacheHits_(cacheHits),
        ttl_(ttl),
        valid_(valid) {
    key_ = folly::sformat("{}", folly::Random::rand32());
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

  std::string getline(HeaderFormat format) {
    std::string line;
    if (format == HeaderFormat::v1) {
      // v1: <key>,<op>,<size>,<op_count>,<key_size>[,[<ttl>[,.*]]]
      line =
          folly::sformat("{},{},{},{},{}", key_, op_, size_, opCnt_, keySize_);
      if (ttl_) {
        line += folly::sformat(",{}", *ttl_);
      }
    } else if (format == HeaderFormat::v2) {
      // v2: op_time,key,key_size,op,op_count,size,cache_hits,ttl
      line = folly::sformat("{},{},{},{},{},{},{}",
                            opTime_,
                            key_,
                            keySize_,
                            op_,
                            opCnt_,
                            size_,
                            cacheHits_);
      if (ttl_) {
        line += folly::sformat(",{}", *ttl_);
      }
    }

    return line;
  }

  std::string key_;

  size_t opTime_;
  size_t keySize_;
  std::string op_;
  size_t size_;
  size_t opCnt_;
  size_t cacheHits_;
  std::optional<size_t> ttl_;
  bool valid_;
};

// test set
std::vector<TraceEntry> kTraces = {
    // <op_time>,<key_size>,<op>,<size>,<op_count>,<cache_hits>,<ttl>,<valid>
    {564726470, 7, "GET", 0, 2, 1, std::nullopt, true},
    {564726470, 7, "GET", 0, 2, 2, 50, true},
    {564726471, 7, "GET_LEASE", 0, 2, 2, 50, true},
    {564726471, 20, "SET", 100, 35, 0, std::nullopt, true},
    {564726490, 20, "SET", 100, 35, 0, 3600, true},
    {564726493, 20, "SAT", 100, 35, 0, 3600, false}, // invalid op name
    {564726495, 20, "SET_LEASE", 100, 35, 0, 3600, true},
    {564726496, 7, "GET", 0, 0, 1, std::nullopt, false}, // invalid op count
    {564726497, 7, "GET", 0, 0, 1, 600, false},          // invalid op count
    {564726498, 1024, "SET", 100, 35, 0, 300, true},     // key truncated
    {564726498, 1024, "SET", 100, 35, 0, std::nullopt, true}, // key truncated
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

  for (auto& trace : kTraces) {
    ASSERT_EQ(replayer.parseRequest(trace.getline(HeaderFormat::v1), req),
              trace.valid_);
    trace.validate(*req);
  }

  // trailing comma
  for (auto& trace : kTraces) {
    trace.getline(HeaderFormat::v1).append(",");

    ASSERT_EQ(replayer.parseRequest(trace.getline(HeaderFormat::v1), req),
              trace.valid_);
    trace.validate(*req);
  }

  replayer.markShutdown();
}

TEST(KVReplayGeneratorTest, DynamicHeader) {
  StressorConfig config;
  KVReplayGenerator replayer{config};
  std::mt19937_64 gen;

  auto req = std::make_unique<ReqWrapper>();

  // v1 header
  ASSERT_TRUE(replayer.setHeaderRow("key,op,size,op_count,key_size,ttl"));
  for (auto& trace : kTraces) {
    ASSERT_EQ(replayer.parseRequest(trace.getline(HeaderFormat::v1), req),
              trace.valid_);
    trace.validate(*req);
  }

  // missing required field
  ASSERT_FALSE(replayer.setHeaderRow("key,_op,size,op_count,key_size,ttl"));

  // v2 header
  ASSERT_TRUE(replayer.setHeaderRow(
      "op_time,key,key_size,op,op_count,size,cache_hits,ttl"));
  for (auto& trace : kTraces) {
    // v1 trace is not compatible to v2 header
    ASSERT_FALSE(replayer.parseRequest(trace.getline(HeaderFormat::v1), req));
    // get line compatible to the header
    ASSERT_EQ(replayer.parseRequest(trace.getline(HeaderFormat::v2), req),
              trace.valid_);
    trace.validate(*req);
  }

  replayer.markShutdown();
}

} // namespace tests
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
