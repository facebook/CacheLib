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

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace facebook {

namespace cachelib {

/*
 * Principles of the algorithm
 *
 * - Maintain a fixed-size Level-1 (L1) array of S counters. On bumpHash, the
 *   counter mapped to the provided hash is incremented.
 * - Maintain a fixed-size Level-2 (L2) array of S records, each of which has a
 *   counter (and two other fields). The L2 counters map with a different hash
 *   function, and are incremented only every T L1 bumps, where T is a dynamic
 *   threshold.
 * - The dynamic threshold T expands and shrinks to maintain an invariant that
 *   the number of non-zero L2 counters is between 1 and W, where W is the size
 *   of "warm" set of L2 records.
 * - On bumpHash, if the given hash matches an L1 entry with at least T/2 bumps
 *   and an L2 entry with a value of at least M, we record the hash itself next
 *   to the L2 counter in an open-addressing (Wikipedia)
 *   (https://en.wikipedia.org/wiki/Open_addressing) way. M is the "multiplier"
 *   for hotness - a hash has to hit at least T*M times in order to be recorded,
 *   so it has to be at least M times "hotter" than the warm set.
 * - On bumpHash, if the given hash matches an L1 entry with at least T/2 bumps
 *   and an L2 entry with a non-zero value, it is compared to the associated
 *   hash stored in L2, and if it matches - bumpHash returns non-zero ("very hot
 *   key") - that number is the L2 counter divided by M. Since L2 hashes are
 *   arranged in open-addressing, the lookup may involve a short scan. The
 *   number of exact hash hits is also tracked in the L2 record where the hash
 *   is stored.
 *
 * Periodical Maintenance
 *
 * - On every P bumps, data structure maintenance takes place. P = (S + W*M/2)*T
 * - L1 counters are decayed - divided by two
 * - L2 counters are decayed - divided by two, set not to exceed M-1
 * - Hashes with zero L2 count or with exact hits less than T/2 are removed from
 *   L2.
 * - L2 open-addressing is maintained by pushing hashes back to cover holes.
 * - The value of T is modified to maintain the [1, W] range for the number of
 *   non-zero L2 counts.
 * - The maintenance period P is recalculated with the new value of T.
 *
 * Algorithm parameters
 *
 * - Size of L1 and L2 arrays - S, suggested value: 1024
 * - Number of warm entries - W, suggested value: 8
 * - Hotness multiplier - M, suggested value: 30
 * - initial L1 threshold - This indicates the threshold for qualifying for L2.
 *   Important thing to remember is that this threshold is per each thread.
 *   If a key is accessed X times, each of the N threads in the service will
 *   get about X/N accesses. So, the item may not qualify for L2 easily if the
 *   initial L1 threshold is too high.
 *
 * Note on thread safety: The data structures should not be accessed from
 * multiple threads, each server thread should maintain its own thread-local
 * detector.
 *
 */
class HotHashDetector {
 public:
  HotHashDetector(size_t numBuckets,
                  size_t numWarmItems,
                  size_t hotnessMultiplier,
                  uint32_t initialL1Threshold = kInitialL1Threshold)
      : numBuckets_{numBuckets},
        numWarmItems_{numWarmItems},
        hotnessMultiplier_{hotnessMultiplier},
        bucketsMask_{numBuckets - 1},
        l1Threshold_{initialL1Threshold},
        l1Vector_(numBuckets),
        l2Vector_(numBuckets) {
    // Enforce that the number of buckets is a power of two.
    assert((numBuckets & (numBuckets - 1)) == 0);
    calcMaintenanceInterval();
  }

  // Bump a hash value for tracking and perform maintenance when needed.
  //
  // @param hash    hash value of an item. Zero is a valid value for hash, but
  //                may cause false positives.
  //
  // @return        non-zero if the hash is hot. Higher numbers mean even
  //                hotter, but any non-zero result here means it's very hot.
  uint8_t bumpHash(uint64_t hash);

  // Read-only method to check the hotness of a key-hash.
  //
  // @param hash    hash value of an item. Zero is a valid value for hash, but
  //                may cause false positives.
  //
  // @return        True if the hash is hot.
  bool isHotHash(uint64_t hash) const;

  // Manually trigger the data structure maintenance procedure.
  void doMaintenance();

 private:
  struct L2Record {
    uint64_t hash{0};
    uint32_t count{0};
    uint32_t hashHits{0};
  };

  size_t l1HashFunction(uint64_t hash) const { return hash & bucketsMask_; }

  size_t l2HashFunction(uint64_t hash) const {
    return (hash * 351551 /* prime */) & bucketsMask_;
  }

  void calcMaintenanceInterval() {
    maintenanceInterval_ =
        (numBuckets_ + numWarmItems_ * hotnessMultiplier_ / 2) * l1Threshold_;
  }

  // Returns true if a hash was moved to a hole
  bool fixL2Holes(unsigned idx);

  static constexpr unsigned kInitialL1Threshold = 128;
  static constexpr unsigned kScanLen = 5; // Max open-addressing scan length.
  const size_t numBuckets_;
  const size_t numWarmItems_;      // See algorithm description at the top.
  const size_t hotnessMultiplier_; // See algorithm description at the top.
  const size_t bucketsMask_;
  size_t l1Threshold_; // Threshold for qualifying for L2
  std::vector<uint32_t> l1Vector_;
  std::vector<L2Record> l2Vector_;
  size_t maintenanceInterval_; // In units of bumpHash() invocations.
  size_t bumpsSinceMaintenance_ = 0;
};

} // namespace cachelib
} // namespace facebook
