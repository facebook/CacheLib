#pragma once
#include <folly/logging/xlog.h>
#include <sys/mman.h>

#include <array>
#include <utility>

namespace facebook {
namespace cachelib {

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
    auto pageBegin = it & pageMask_;
    size_t sizeBegin = size + (it - pageBegin);
    size_t sizeTotal = ((sizeBegin - 1) | pageMask_) + 1;

    ::madvise(reinterpret_cast<void*>(pageBegin), sizeTotal, MADV_DODUMP);
    return sizeTotal;
  }

  std::array<std::pair<uintptr_t, size_t>, kItemsPerThread> items_;
  size_t head_{0};
  // compute this one time to avoid static overhead and locks etc.
  const size_t pageSize_{0};
  const size_t pageMask_{0};
}; // class TlsActiveItemRing

} // namespace cachelib
} // namespace facebook
