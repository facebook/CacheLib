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

#include <folly/logging/xlog.h>

#include <atomic>

namespace facebook {
namespace cachelib {

// This class stores two refcounts in one atomic, and allows switching between
// them and waiting for them to drain. Used for copy-on-write with memory
// management for non-blocking read wrapper around thread-unsafe
// datastructures. Expects that switching of cohorts is co-ordinated and can
// be done by only one at a time.
class Cohort {
 public:
  Cohort() {}

  Cohort(const Cohort&) = delete;            // no copy
  Cohort& operator=(const Cohort&) = delete; // no copy

  // This class represents the top/bottom state of the cohort as returned
  // by Cohort::incrActiveReqs().  The corresponding decrement will be
  // performed when the token goes out of scope or decrement() is called.
  // Make sure the owner Cohort is longer-lived than the token.
  class Token {
    friend class Cohort;

   public:
    ~Token() {
      if (owner_) {
        decrement();
      }
    }

    Token(Token&& other) noexcept
        : owner_(std::exchange(other.owner_, nullptr)),
          top_(std::exchange(other.top_, false)) {}

    Token& operator=(Token&& other) noexcept {
      if (this != &other) {
        this->~Token();
        new (this) Token(std::move(other));
      }
      return *this;
    }

    Token(const Token&) = delete;            // no copy
    Token& operator=(const Token&) = delete; // no copy

    void decrement() noexcept {
      XDCHECK_NE(owner_, nullptr);
      owner_->decrActiveReqs(top_);
      owner_ = nullptr;
    }

    bool isTop() const noexcept { return top_; }

   private:
    Token(Cohort* c, bool top) : owner_(c), top_(top) {}

    Cohort* owner_;
    bool top_;
  };

  // Switch to the currently unused cohort, and  wait for all readers to drain
  // from the old cohort
  void switchCohorts() noexcept {
    const uint64_t cohort = cohortVal_.load();
    // exactly one of the cohorts should be set.
    XDCHECK(static_cast<bool>(cohort & kBottomCohortBit) ^
            static_cast<bool>(cohort & kTopCohortBit));
    std::ignore = cohort;

    // swap cohorts
    cohortVal_.fetch_xor(kTopCohortBit + kBottomCohortBit);

    while (true) {
      const uint64_t current = cohortVal_.load();
      if ((current << 32) != 0 && (current >> 32) != 0) {
        // the old refcount is non-zero so it's still draining
        // reads are pretty quick; sleep for 1 ms for them to drain
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        continue;
      }
      break;
    }
  }

  // increments the refcount for the current cohort.
  // Optimisitically assumes the cohort is not switching. Just increment and
  // if it actually changed in the interim then we subtract and try again.
  // This should never run more than twice if the user of refcount takes
  // suffiently long time to drain the refcount and switch.
  //
  // @returns bool topCohort, if we ended up in top cohort (versus bottom
  //          one). This must be passed on to the corresponding decr
  Token incrActiveReqs() noexcept {
    while (true) {
      const uint64_t cohort = cohortVal_.load();
      XDCHECK_NE(cohort, 0ULL);
      /* only top or bottom can be set */
      XDCHECK(static_cast<bool>(cohort & kBottomCohortBit) ^
              static_cast<bool>(cohort & kTopCohortBit));

      /* no overflow */
      XDCHECK_EQ(cohort & (kBottomCohortBit >> 1), 0ULL);
      XDCHECK_EQ(cohort & (kTopCohortBit >> 1), 0ULL);

      const bool topCohort = (cohort & kTopCohortBit);
      const uint64_t newCohort =
          cohortVal_.fetch_add(topCohort ? kTopRef : kBottomRef);
      if (static_cast<bool>(newCohort & kTopCohortBit) == topCohort) {
        return Token(this, topCohort);
      }
      cohortVal_.fetch_sub(topCohort ? kTopRef : kBottomRef);
    }
  }

  bool isTopCohort() const noexcept {
    auto cohort = cohortVal_.load();
    XDCHECK(static_cast<bool>(cohort & kBottomCohortBit) ^
            static_cast<bool>(cohort & kTopCohortBit));
    return cohort & kTopCohortBit;
  }

  uint64_t getPending(bool isTop) const noexcept {
    uint64_t cohort = cohortVal_;
    return isTop ? (cohort & ~kTopCohortBit) >> 32
                 : ((cohort & ~kBottomCohortBit) << 32) >> 32;
  }

 private:
  // If a cohort is specified, decrements its refcount. Guards against refcounts
  // going negative and will assert in this case if enabled.
  // TODO (sathya) catch incorrect decRefs and throw
  //
  // @param topCohort bool if the current used cohort was the top one
  void decrActiveReqs(bool isTop) noexcept {
    uint64_t newVal = cohortVal_.fetch_sub(isTop ? kTopRef : kBottomRef);
    std::ignore = newVal;

    // ensure refs didn't go to zero
    // TODO(sathya) this actually does not ensure that we decremented below 0
    XDCHECK_NE(newVal, kTopRef);
    XDCHECK_NE(newVal, kBottomRef);
    XDCHECK_NE(newVal, 0ULL);
  }

  // Store the refcount for current and future cohorts in a single uint64_t.
  // Readers update the refcount only of the cohort that is currently marked
  // as active. Writers busywait until the refcount drains from the other
  // cohort before continuing.
  static const uint64_t kTopCohortBit = 1ULL << 63;
  static const uint64_t kBottomCohortBit = 1ULL << 31;
  static const uint64_t kBottomRef = 1ULL;
  static const uint64_t kTopRef = 1ULL << 32;

  std::atomic<uint64_t> cohortVal_{kBottomCohortBit};
};

} // namespace cachelib
} // namespace facebook
