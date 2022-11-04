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
#include <sys/mman.h>

#include <array>
#include <utility>

namespace facebook {
namespace cachelib {

/* Missing madvise(2) flag on Mac OS */
#ifndef MADV_DODUMP
#define MADV_DODUMP 0
#endif

// Thread local class for tracking recently accessed item.
class TlsActiveItemRing {
  static const size_t KB = 1024ULL;
  static const size_t MB = 1024ULL * KB;
  static const size_t GB = 1024ULL * MB;

  /**
   * Number of items to track in a per-thread ring
   * (circular buffer)
   */
  static constexpr size_t kItemsPerThread{8};
  static_assert((kItemsPerThread & (kItemsPerThread - 1)) == 0,
                "kItemsPerThread must be a power of two");

 public:
  TlsActiveItemRing()
      : head_{0},
        pageSize_{static_cast<size_t>(::sysconf(_SC_PAGESIZE))},
        pageMask_{~(pageSize_ - 1)} {
    items_.fill(std::make_pair(reinterpret_cast<uintptr_t>(nullptr), 0));
    XDCHECK_NE(0u, pageSize_);
    XDCHECK_EQ(0u, pageSize_ & (pageSize_ - 1));
  }

  void trackItem(const uintptr_t it, uint32_t allocSize) {
    items_[head_++] = std::make_pair(it, allocSize);
    head_ &= kItemsPerThread - 1; // head %= 8;
  }

  /**
   * madvise(DODUMP) all recorded items on this thread.
   *
   * @param maxDumpBytes  As a safety stopgap, dump at most extra
   *        maxDumpBytes(default 10GB) worth of pages.
   */
  void madviseItems(const size_t maxDumpBytes = 10ULL * GB) const {
    size_t bytesDumped = 0;
    for (const auto& it : items_) {
      if (it.first) {
        bytesDumped += madviseItem(it.first, it.second);
        // TBD: Check why pagesize() is called multiple times
        if (bytesDumped > maxDumpBytes) {
          return;
        }
      }
    }
  }

 private:
  /**
   * madvise(DODUMP) the item
   *
   * @param it  Pair of memory address and size
   * @return number of bytes included in the core
   */
  size_t madviseItem(uintptr_t it, size_t size) const {
    // The item can be in any mem addr, but we have to madvise on page
    // boundaries. This means we need to find out where a prior page starts, and
    // where the last page ends. Note that an item can be in a single page or
    // straddles across multiple pages.
    //
    // |----------- *************** ----------|
    // ^            ^             ^           ^
    // Page begin   Item begin    Item end    Page End
    auto pageBegin = it & pageMask_;
    auto pageEnd = (it + size + (pageSize_ - 1)) & pageMask_;
    size_t sizeTotal = pageEnd - pageBegin;

    ::madvise(reinterpret_cast<void*>(pageBegin), sizeTotal, MADV_DODUMP);
    return sizeTotal;
  }

  // The tracked item in the form of address and size.
  std::array<std::pair<uintptr_t, size_t>, kItemsPerThread> items_;
  size_t head_{0};
  // compute this one time to avoid static overhead and locks etc.
  const size_t pageSize_{0};
  const size_t pageMask_{0};
}; // class TlsActiveItemRing

} // namespace cachelib
} // namespace facebook
