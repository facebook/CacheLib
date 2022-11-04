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

#include "cachelib/navy/block_cache/LruPolicy.h"

#include <folly/Format.h>
#include <folly/logging/xlog.h>

#include <cstdio>

#include "cachelib/navy/common/Utils.h"

namespace facebook {
namespace cachelib {
namespace navy {

constexpr std::chrono::seconds LruPolicy::kEstimatorWindow;

LruPolicy::LruPolicy(uint32_t expectedNumRegions)
    : secSinceInsertionEstimator_{kEstimatorWindow},
      secSinceAccessEstimator_{kEstimatorWindow},
      hitsEstimator_{kEstimatorWindow} {
  array_.reserve(expectedNumRegions);
  XLOGF(INFO, "LRU policy: expected {} regions", expectedNumRegions);
}

void LruPolicy::touch(RegionId rid) {
  XDCHECK(rid.valid());
  auto i = rid.index();
  std::lock_guard<std::mutex> lock{mutex_};
  if (i >= array_.size()) {
    array_.resize(i + 1);
  }
  if (array_[i].inList()) {
    array_[i].hits++;
    array_[i].lastUpdateTime = getSteadyClockSeconds();
    unlink(i);
    linkAtHead(i);
  }
}

void LruPolicy::track(const Region& region) {
  auto rid = region.id();
  XDCHECK(rid.valid());
  auto i = rid.index();
  std::lock_guard<std::mutex> lock{mutex_};
  if (i >= array_.size()) {
    array_.resize(i + 1);
  }
  array_[i].hits = 0;
  array_[i].creationTime = getSteadyClockSeconds();
  array_[i].lastUpdateTime = getSteadyClockSeconds();
  if (!array_[i].inList()) {
    linkAtHead(i);
  }
}

RegionId LruPolicy::evict() {
  uint32_t retRegion{kInvalidIndex};
  uint32_t secsSinceAccess{0};
  uint32_t secsSinceCreate{0};
  uint32_t hits{0};

  {
    std::lock_guard<std::mutex> lock{mutex_};
    if (tail_ == kInvalidIndex) {
      return RegionId{};
    }
    retRegion = tail_;
    secsSinceCreate = array_[tail_].secondsSinceCreation().count();
    secsSinceAccess = array_[tail_].secondsSinceAccess().count();
    hits = array_[tail_].hits;
    unlink(tail_);
  }

  secSinceInsertionEstimator_.trackValue(secsSinceCreate);
  secSinceAccessEstimator_.trackValue(secsSinceAccess);
  hitsEstimator_.trackValue(hits);
  return RegionId{retRegion};
}

void LruPolicy::reset() {
  std::lock_guard<std::mutex> lock{mutex_};
  array_.clear();
  head_ = kInvalidIndex;
  tail_ = kInvalidIndex;
}

void LruPolicy::unlink(uint32_t i) {
  auto& node = array_[i];
  XDCHECK_NE(tail_, kInvalidIndex);
  if (tail_ == i) {
    tail_ = node.prev;
  } else {
    XDCHECK_NE(node.next, kInvalidIndex);
    array_[node.next].prev = node.prev;
  }
  XDCHECK_NE(head_, kInvalidIndex);
  if (head_ == i) {
    head_ = node.next;
  } else {
    XDCHECK_NE(node.prev, kInvalidIndex);
    array_[node.prev].next = node.next;
  }
  node.next = kInvalidIndex;
  node.prev = kInvalidIndex;
}

void LruPolicy::linkAtHead(uint32_t i) {
  if (i != head_) {
    if (head_ != kInvalidIndex) {
      array_[head_].prev = i;
    }
    array_[i].next = head_;
    head_ = i;
    if (tail_ == kInvalidIndex) {
      tail_ = i;
    }
  }
}

void LruPolicy::linkAtTail(uint32_t i) {
  if (i != tail_) {
    if (tail_ != kInvalidIndex) {
      array_[tail_].next = i;
    }
    array_[i].prev = tail_;
    tail_ = i;
    if (head_ == kInvalidIndex) {
      head_ = i;
    }
  }
}

size_t LruPolicy::memorySize() const {
  return sizeof(*this) + sizeof(ListNode) * array_.capacity();
}

void LruPolicy::dump(uint32_t n) const {
  dumpList("head", n, head_, &ListNode::next);
  dumpList("tail", n, tail_, &ListNode::prev);
}

void LruPolicy::dumpList(const char* tag,
                         uint32_t n,
                         uint32_t first,
                         uint32_t ListNode::*link) const {
  if (first == kInvalidIndex) {
    XLOGF(ERR, "LRU {} is empty", tag);
    return;
  }

  char buf[600];
  char* const bufEnd = buf + sizeof(buf);
  char* p = buf;
  int len = 0;
  std::snprintf(p, bufEnd - p, "LRU from %s %u %n", tag, first, &len);
  p += len;
  uint32_t i = 0;
  while (first != kInvalidIndex && (n == 0 || i < n) && p < bufEnd) {
    std::snprintf(
        p, bufEnd - p, "%u(h=%u) %n", first, array_[first].hits, &len);
    p += len;
    first = array_[first].*link;
    i++;
  }
  if (first != kInvalidIndex && p < bufEnd) {
    std::snprintf(p, bufEnd - p, "...");
  }
  XLOG(ERR, buf);
}

void LruPolicy::getCounters(const CounterVisitor& v) const {
  secSinceInsertionEstimator_.visitQuantileEstimator(
      v, "navy_bc_lru_secs_since_insertion");
  secSinceAccessEstimator_.visitQuantileEstimator(
      v, "navy_bc_lru_secs_since_access");
  hitsEstimator_.visitQuantileEstimator(v, "navy_bc_lru_region_hits_estimate");
}

void LruPolicy::persist(RecordWriter& rw) const {
  std::ignore = rw;
  throw std::runtime_error("Not Implemented.");
}

void LruPolicy::recover(RecordReader& rr) {
  std::ignore = rr;
  throw std::runtime_error("Not Implemented.");
}

} // namespace navy
} // namespace cachelib
} // namespace facebook
