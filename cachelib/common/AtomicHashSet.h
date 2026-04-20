#pragma once

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>

namespace facebook::cachelib::util {
namespace detail {

template <typename KeyT = uint32_t>
class AtomicFIFOHashSetBase {
 public:
  using Key = KeyT;
  using TS = uint32_t;

  static_assert(
      sizeof(KeyT) == sizeof(uint32_t),
      "AtomicFIFOHashSetBase currently supports 32-bit keys only.");

  AtomicFIFOHashSetBase() = default;

  explicit AtomicFIFOHashSetBase(size_t capacity) noexcept {
    resize(capacity);
  }

  AtomicFIFOHashSetBase(const AtomicFIFOHashSetBase&) = delete;
  AtomicFIFOHashSetBase& operator=(const AtomicFIFOHashSetBase&) = delete;
  AtomicFIFOHashSetBase(AtomicFIFOHashSetBase&&) = delete;
  AtomicFIFOHashSetBase& operator=(AtomicFIFOHashSetBase&&) = delete;

  bool initialized() const noexcept {
    return table_.load(std::memory_order_acquire) != nullptr;
  }

  size_t capacity() const noexcept {
    return fifoSize_.load(std::memory_order_relaxed);
  }

  void resize(size_t newSize) noexcept {
    if (newSize == 0) {
      fifoSize_.store(0, std::memory_order_relaxed);
      return;
    }

    if (!initialized()) {
      initStorage(newSize);
    }

    fifoSize_.store(static_cast<TS>(newSize), std::memory_order_relaxed);
  }

  bool contains(Key key) noexcept {
    auto* tbl = table_.load(std::memory_order_acquire);
    if (tbl == nullptr) {
      return false;
    }

    const TS now = seq_.load(std::memory_order_relaxed);
    const size_t bucketIdx = getBucketIdx(key, tbl->numElem);

    for (size_t i = 0; i < kItemsPerBucket; ++i) {
      auto* slot = &tbl->slots[bucketIdx + i];
      uint64_t val = __atomic_load_n(slot, __ATOMIC_RELAXED);

      if (val == 0) {
        continue;
      }

      if (isExpired(val, now)) {
        tryClear(slot, val);
        continue;
      }

      if (matchKey(val, key)) {
        tryClear(slot, val);
        return true;
      }
    }

    return false;
  }

  void insert(Key key) noexcept {
    auto* tbl = table_.load(std::memory_order_acquire);
    if (tbl == nullptr) {
      return;
    }

    const TS ts = nextTimestamp();
    const uint64_t newVal = pack(key, ts);
    const size_t bucketIdx = getBucketIdx(key, tbl->numElem);

    for (size_t i = 0; i < kItemsPerBucket; ++i) {
      auto* slot = &tbl->slots[bucketIdx + i];
      uint64_t val = __atomic_load_n(slot, __ATOMIC_RELAXED);

      if (val == 0) {
        uint64_t expected = 0;
        if (__atomic_compare_exchange_n(slot,
                                        &expected,
                                        newVal,
                                        false,
                                        __ATOMIC_RELAXED,
                                        __ATOMIC_RELAXED)) {
          return;
        }
        continue;
      }

      if (isExpired(val, ts)) {
        if (tryClear(slot, val)) {
          uint64_t expected = 0;
          if (__atomic_compare_exchange_n(slot,
                                          &expected,
                                          newVal,
                                          false,
                                          __ATOMIC_RELAXED,
                                          __ATOMIC_RELAXED)) {
            return;
          }
        }
        continue;
      }

      if (matchKey(val, key)) {
        return;
      }
    }

    const size_t idx =
        bucketIdx + (static_cast<uint32_t>(key) & (kItemsPerBucket - 1));
    __atomic_store_n(&tbl->slots[idx], newVal, __ATOMIC_RELAXED);
    numEvicts_.fetch_add(1, std::memory_order_relaxed);
  }

 private:
  static constexpr size_t kLoadFactorInv = 2;
  static constexpr size_t kItemsPerBucket = 8;
  static constexpr uint64_t kKeyMask = 0x00000000FFFFFFFFULL;

  struct Table {
    explicit Table(size_t n) : numElem(n), slots(new uint64_t[n]) {
      std::memset(slots.get(), 0, n * sizeof(uint64_t));
    }

    size_t numElem{0};
    std::unique_ptr<uint64_t[]> slots;
  };

  static size_t roundUpToBucket(size_t n) noexcept {
    return (n + (kItemsPerBucket - 1)) & ~(kItemsPerBucket - 1);
  }

  void initStorage(size_t logicalCap) noexcept {
    if (logicalCap == 0 || initialized()) {
      return;
    }

    const size_t numElem = roundUpToBucket(
        std::max<size_t>(logicalCap * kLoadFactorInv, kItemsPerBucket));
    auto newTable = std::make_unique<Table>(numElem);
    auto* raw = newTable.get();
    Table* expected = nullptr;

    if (table_.compare_exchange_strong(expected,
                                       raw,
                                       std::memory_order_release,
                                       std::memory_order_acquire)) {
      owner_ = std::move(newTable);
    }
  }

  static size_t getBucketIdx(Key key, size_t numElem) noexcept {
    const size_t h = static_cast<uint32_t>(key) % numElem;
    return h & ~(kItemsPerBucket - 1);
  }

  static bool matchKey(uint64_t val, Key key) noexcept {
    return static_cast<uint32_t>(val & kKeyMask) ==
           static_cast<uint32_t>(key);
  }

  static uint64_t pack(Key key, TS ts) noexcept {
    const uint64_t uKey = static_cast<uint32_t>(key);
    const uint64_t uTsStored = static_cast<uint64_t>(ts + 1);
    return uKey | (uTsStored << 32);
  }

  static TS unpackTs(uint64_t val) noexcept {
    return static_cast<TS>((val >> 32) - 1);
  }

  bool isExpired(uint64_t val, TS now) const noexcept {
    const TS cap = fifoSize_.load(std::memory_order_relaxed);
    if (cap == 0) {
      return true;
    }

    const TS age = static_cast<TS>(now - unpackTs(val));
    return age > cap;
  }

  static bool tryClear(uint64_t* slot, uint64_t expected) noexcept {
    return __atomic_compare_exchange_n(slot,
                                       &expected,
                                       uint64_t{0},
                                       false,
                                       __ATOMIC_RELAXED,
                                       __ATOMIC_RELAXED);
  }

  TS nextTimestamp() noexcept {
    TS cur = seq_.load(std::memory_order_relaxed);
    while (true) {
      const TS next =
          (cur >= std::numeric_limits<TS>::max() - 1) ? 0
                                                      : static_cast<TS>(cur + 1);

      if (seq_.compare_exchange_weak(cur,
                                     next,
                                     std::memory_order_relaxed,
                                     std::memory_order_relaxed)) {
        return cur;
      }
    }
  }

  std::unique_ptr<Table> owner_;
  std::atomic<Table*> table_{nullptr};
  std::atomic<TS> fifoSize_{0};
  std::atomic<TS> seq_{0};
  std::atomic<uint64_t> numEvicts_{0};
};

} // namespace detail

using AtomicFIFOHashSet32 = detail::AtomicFIFOHashSetBase<uint32_t>;

} // namespace facebook::cachelib::util

namespace facebook::cachelib {
using AtomicFIFOHashTable = util::AtomicFIFOHashSet32;
}
