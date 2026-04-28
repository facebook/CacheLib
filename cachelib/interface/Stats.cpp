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

#include "cachelib/interface/Stats.h"

#include <folly/Format.h>

#include <set>

using namespace facebook::cachelib::interface;
using Estimates = facebook::cachelib::util::PercentileStats::Estimates;

namespace facebook::cachelib::interface::detail {

bool shouldSample(size_t sampleRate) {
  thread_local size_t counter = 0;
  if (sampleRate > 0) {
    return counter++ % sampleRate == 0;
  } else {
    return false;
  }
}

} // namespace facebook::cachelib::interface::detail

namespace {

void printRate(std::ostream& os,
               const char* name,
               size_t numerator,
               size_t denominator) {
  XDCHECK_GT(denominator, 0UL);
  os << folly::sformat(" {}={:.2f}%",
                       name,
                       static_cast<double>(numerator) / denominator * 100.0);
}

std::ostream& operator<<(std::ostream& os, const Estimates& e) {
  os << std::endl
     << folly::sformat(
            "           avg={:>10d} ns    p50={:>10d} ns     p99={:>10d} ns",
            e.avg,
            e.p50,
            e.p99)
     << std::endl
     << folly::sformat(
            "         p99.9={:>10d} ns p99.99={:>10d} ns p99.999={:>10d} ns",
            e.p999,
            e.p9999,
            e.p99999)
     << std::endl
     << folly::sformat(
            "      p99.9999={:>10d} ns   p100={:>10d} ns", e.p999999, e.p100);
  return os;
}

template <typename OpThroughputCounterT>
void printThroughput(std::ostream& os, const OpThroughputCounterT& t) {
  os << std::endl
     << "    calls=" << t.calls_ << " successes=" << t.successes_
     << " errors=" << t.errors_;
  printRate(os, "successRate", t.successes_, t.calls_);
  if constexpr (std::is_same_v<OpThroughputCounterT,
                               detail::OpFindThroughputCounters<size_t>>) {
    os << std::endl << "    hits=" << t.hits_ << " misses=" << t.misses_;
    printRate(os, "hitRate", t.hits_, t.hits_ + t.misses_);
  }
}

template <typename OpCounterT>
void printOpCounters(std::ostream& os, const char* name, const OpCounterT& op) {
  os << "  " << name << ":";
  if (op.throughput_.calls_ == 0) {
    os << " (not called)" << std::endl;
  } else {
    printThroughput(os, op.throughput_);
    os << std::endl << "    latency:" << op.latency_ << std::endl;
  }
}

void printExtraOpCounters(
    std::ostream& os,
    const std::string& type,
    const std::unordered_map<std::string, double>& counters) {
  if (!counters.empty()) {
    // Sort counter names to keep relevant counters together
    std::set<std::string> names;
    for (const auto& [name, _] : counters) {
      names.insert(name);
    }
    os << "  " << type << ":" << std::endl;
    for (const auto& name : names) {
      os << "    " << name << "=" << counters.at(name) << std::endl;
    }
  };
}

} // namespace

namespace std {
std::ostream& operator<<(
    std::ostream& os,
    const facebook::cachelib::interface::CacheComponentStats& stats) {
  os << "numItems: " << stats.numItems << std::endl;
  printOpCounters(os, "allocate", stats.allocate_);
  printOpCounters(os, "insert", stats.insert_);
  printOpCounters(os, "insertOrReplace", stats.insertOrReplace_);
  printOpCounters(os, "find", stats.find_);
  printOpCounters(os, "findToWrite", stats.findToWrite_);
  printOpCounters(os, "removeByKey", stats.removeByKey_);
  printOpCounters(os, "removeByHandle", stats.removeByHandle_);
  printOpCounters(os, "writeBack", stats.writeBack_);
  printOpCounters(os, "release", stats.release_);
  printExtraOpCounters(os, "extra counters", stats.extraStats_.getCounts());
  printExtraOpCounters(os, "extra rate counters", stats.extraStats_.getRates());
  return os;
}
} // namespace std
