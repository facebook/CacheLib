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

#include "cachelib/common/hothash/HotHashDetector.h"

#include <folly/Likely.h>
#include <glog/logging.h>

#include <algorithm>
#include <deque>

namespace facebook {
namespace cachelib {

uint8_t HotHashDetector::bumpHash(uint64_t hash) {
  if (UNLIKELY(++bumpsSinceMaintenance_ >= maintenanceInterval_)) {
    doMaintenance();
  };
  auto idx = l1HashFunction(hash);
  auto l1count = ++l1Vector_[idx];
  // Checking against threshold/2, hot index should pass after one decay
  if (l1count < (l1Threshold_ / 2)) {
    return 0;
  }
  uint8_t result = 0;
  // different hash for L2:
  auto idx2 = l2HashFunction(hash);

  // If L2 has a count, then there is a chance that the given hash is hot,
  // scan a few elements for this hash.
  uint32_t l2count = l2Vector_[idx2].count;
  if (l2count > 0) {
    for (unsigned i = 0; i < kScanLen; ++i) {
      auto& cell = l2Vector_[(idx2 + i) & bucketsMask_];
      if (cell.hash == 0) {
        break;
      }
      if (cell.hash == hash) {
        // Get a number between 1 and 255 for how much hot the L2 entry is.
        result = static_cast<uint8_t>(std::min<uint32_t>(
            255, std::max<uint32_t>(1, l2count / hotnessMultiplier_)));
        ++cell.hashHits;
        break;
      }
    }
  }

  // Proceed to bump L2 if the l1count is divisible by current L1 threshold.
  if (l1count % l1Threshold_ != 0) {
    return result;
  }
  if (++l2Vector_[idx2].count < hotnessMultiplier_) {
    return result;
  }
  for (unsigned i = 0; i < kScanLen; ++i) {
    auto& cell = l2Vector_[(idx2 + i) & bucketsMask_];
    if (cell.hash == 0) {
      cell.hash = hash;
      break;
    }
    if (cell.hash == hash) {
      break;
    }
    // NOTE: in the unlikely case that we scanned kScanLen entries, we will drop
    // the hash without inserting it.
  }

  return result;
}

bool HotHashDetector::isHotHash(uint64_t hash) const {
  auto idx = l1HashFunction(hash);
  auto l1count = l1Vector_[idx];
  // Checking against threshold/2 since hot index should pass after one decay
  if (LIKELY(l1count < (l1Threshold_ / 2))) {
    return false;
  }
  auto idx2 = l2HashFunction(hash);
  uint32_t l2count = l2Vector_[idx2].count;
  if (l2count == 0) {
    return false;
  }
  for (unsigned i = 0; i < kScanLen; ++i) {
    auto& cell = l2Vector_[(idx2 + i) & bucketsMask_];
    if (cell.hash == hash) {
      return true;
    }
  }
  return false;
}

void HotHashDetector::doMaintenance() {
  bumpsSinceMaintenance_ = 0;

  // Decay L1
  for (uint32_t& val : l1Vector_) {
    val /= 2;
  }

  // Decay L2
  for (auto& cell : l2Vector_) {
    cell.count = std::min<uint32_t>(hotnessMultiplier_ - 1, cell.count / 2);
    cell.hashHits = cell.hashHits / 2;
  };

  // Hashes in L2 may need to move if their preceding cell has decayed. We
  // have to run at least size() elements, but also continue running in case
  // the last kScanLen elements had at least one move. We keep a running sum
  // of the number of moves in the last kScanLen iterations.
  size_t runningSum = 0;
  std::deque<size_t> lastMoves(kScanLen, 0);
  for (unsigned i = 0; i < l2Vector_.size() || runningSum > 0; ++i) {
    size_t moved = fixL2Holes(i & bucketsMask_) ? 1 : 0;
    runningSum += moved - lastMoves.front();
    lastMoves.push_back(moved);
    lastMoves.pop_front();
  }

  // Adjust L1 threshold so the number of L2 non-empty slots will be in some
  // range.
  auto nonZeroCount = numBuckets_ - std::count_if(l2Vector_.cbegin(),
                                                  l2Vector_.cend(),
                                                  [](L2Record const& cell) {
                                                    return cell.count == 0;
                                                  });
  if (nonZeroCount == 0) {
    l1Threshold_ = std::max<size_t>(2U, l1Threshold_ / 2);
  } else if (nonZeroCount > numWarmItems_) {
    l1Threshold_ = std::min<size_t>(1U << 20, l1Threshold_ * 2);
  }
  calcMaintenanceInterval();
}

bool HotHashDetector::fixL2Holes(unsigned idx) {
  assert(idx < l2Vector_.size());
  auto hash = l2Vector_[idx].hash;
  if (hash == 0) {
    return false;
  }
  auto correctIdx = l2HashFunction(hash);
  if (l2Vector_[correctIdx].count == 0 ||
      l2Vector_[idx].hashHits < (l1Threshold_ / 2)) {
    l2Vector_[idx].hash = 0;
    l2Vector_[idx].hashHits = 0;
    return true;
  }
  if (idx == correctIdx) {
    return false;
  }
  for (unsigned j = 1; j < kScanLen; ++j) {
    auto candidateIdx = (correctIdx + j) & bucketsMask_;
    if (candidateIdx == idx) {
      return false;
    }
    if (l2Vector_[candidateIdx].hash == 0) {
      l2Vector_[candidateIdx].hash = hash;
      l2Vector_[candidateIdx].hashHits = l2Vector_[idx].hashHits;
      l2Vector_[idx].hash = 0;
      l2Vector_[idx].hashHits = 0;
      return true;
    }
  }
  // We always expect the hash to reside at most kScanLen-1 cells away from
  // its correct index, we shouldn't get here.
  assert(false);

  return false;
}

} // namespace cachelib
} // namespace facebook
