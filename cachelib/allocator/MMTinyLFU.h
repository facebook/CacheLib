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

#include <atomic>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#include <folly/Format.h>
#include <folly/Math.h>
#pragma GCC diagnostic pop

#include "cachelib/allocator/Cache.h"
#include "cachelib/allocator/CacheStats.h"
#include "cachelib/allocator/Util.h"
#include "cachelib/allocator/datastruct/MultiDList.h"
#include "cachelib/allocator/memory/serialize/gen-cpp2/objects_types.h"
#include "cachelib/common/CompilerUtils.h"
#include "cachelib/common/CountMinSketch.h"
#include "cachelib/common/Mutex.h"

namespace facebook {
namespace cachelib {

// Implements the W-TinyLFU cache eviction policy as described in -
// https://arxiv.org/pdf/1512.00727.pdf
//
// The cache is split into 2 parts, the main cache and the tiny cache.
// The tiny cache is typically sized to be 1% of the total cache with
// the main cache being the rest 99%. Both caches are implemented using
// LRUs. New items land in tiny cache. During eviction, the tail item
// from the tiny cache is promoted to main cache if its frequency is
// higher than the tail item of of main cache, and the tail of main
// cache is evicted. This gives the frequency based admission into main
// cache. Hits in each cache simply move the item to the head of each
// LRU cache.
// The frequency counts are maintained in CountMinSketch approximate
// counters -

// Counter Overhead:
// The windowToCacheSizeRatio determines the size of counters. The default
// value is 32 which means the counting window size is 32 times the
// cache size. After every 32 X cache_capacity number of items, the
// counts are halved to weigh frequency by recency. The function
// counterSize() returns the size of the counters
// in bytes. See MMTinyLFU-inl.h::maybeGrowAccessCountersLocked()
// implementation for how the size is computed.
//
// Tiny cache size:
// This default to 1%. There's no need to tune this parameter.
class MMTinyLFU {
 public:
  // unique identifier per MMType
  static const int kId;

  // forward declaration;
  template <typename T>
  using Hook = DListHook<T>;
  using SerializationType = serialization::MMTinyLFUObject;
  using SerializationConfigType = serialization::MMTinyLFUConfig;
  using SerializationTypeContainer = serialization::MMTinyLFUCollection;

  enum LruType { Main, Tiny, NumTypes };

  // Config class for MMTinfyLFU
  struct Config {
    // create from serialized config
    explicit Config(SerializationConfigType configState)
        : Config(*configState.lruRefreshTime(),
                 *configState.lruRefreshRatio(),
                 *configState.updateOnWrite(),
                 *configState.updateOnRead(),
                 *configState.tryLockUpdate(),
                 *configState.windowToCacheSizeRatio(),
                 *configState.tinySizePercent()) {}

    // @param time        the LRU refresh time in seconds.
    //                    An item will be promoted only once in each lru refresh
    //                    time depite the number of accesses it gets.
    // @param udpateOnW   whether to promote the item on write
    // @param updateOnR   whether to promote the item on read
    Config(uint32_t time, bool updateOnW, bool updateOnR)
        : Config(time,
                 updateOnW,
                 updateOnR,
                 /* try lock update */ false,
                 16,
                 1) {}

    // @param time              the LRU refresh time in seconds.
    //                          An item will be promoted only once in each lru
    //                          refresh time depite the number of accesses it
    //                          gets.
    // @param udpateOnW         whether to promote the item on write
    // @param updateOnR         whether to promote the item on read
    // @param windowToCacheSize multiplier of window size to cache size
    // @param tinySizePct       percentage number of tiny size to overall size
    Config(uint32_t time,
           bool updateOnW,
           bool updateOnR,
           size_t windowToCacheSize,
           size_t tinySizePct)
        : Config(time,
                 updateOnW,
                 updateOnR,
                 /* try lock update */ false,
                 windowToCacheSize,
                 tinySizePct) {}

    // @param time              the LRU refresh time in seconds.
    //                          An item will be promoted only once in each lru
    //                          refresh time depite the number of accesses it
    //                          gets.
    // @param udpateOnW         whether to promote the item on write
    // @param updateOnR         whether to promote the item on read
    // @param tryLockU          whether to use a try lock when doing update.
    // @param windowToCacheSize multiplier of window size to cache size
    // @param tinySizePct       percentage number of tiny size to overall size
    Config(uint32_t time,
           bool updateOnW,
           bool updateOnR,
           bool tryLockU,
           size_t windowToCacheSize,
           size_t tinySizePct)
        : Config(time,
                 0.,
                 updateOnW,
                 updateOnR,
                 tryLockU,
                 windowToCacheSize,
                 tinySizePct) {}

    // @param time                    the LRU refresh time in seconds.
    //                                An item will be promoted only once in each
    //                                lru refresh time depite the number of
    //                                accesses it gets.
    // @param ratio                   the lru refresh ratio. The ratio times the
    //                                oldest element's lifetime in warm queue
    //                                would be the minimum value of LRU refresh
    //                                time.
    // @param udpateOnW               whether to promote the item on write
    // @param updateOnR               whether to promote the item on read
    // @param tryLockU                whether to use a try lock when doing
    //                                update.
    // @param windowToCacheSize       multiplier of window size to cache size
    // @param tinySizePct             percentage number of tiny size to overall
    //                                size
    Config(uint32_t time,
           double ratio,
           bool updateOnW,
           bool updateOnR,
           bool tryLockU,
           size_t windowToCacheSize,
           size_t tinySizePct)
        : Config(time,
                 ratio,
                 updateOnW,
                 updateOnR,
                 tryLockU,
                 windowToCacheSize,
                 tinySizePct,
                 0) {}
    // @param time                    the LRU refresh time in seconds.
    //                                An item will be promoted only once in each
    //                                lru refresh time depite the number of
    //                                accesses it gets.
    // @param ratio                   the lru refresh ratio. The ratio times the
    //                                oldest element's lifetime in warm queue
    //                                would be the minimum value of LRU refresh
    //                                time.
    // @param udpateOnW               whether to promote the item on write
    // @param updateOnR               whether to promote the item on read
    // @param tryLockU                whether to use a try lock when doing
    //                                update.
    // @param windowToCacheSize       multiplier of window size to cache size
    // @param tinySizePct             percentage number of tiny size to overall
    //                                size
    // @param mmReconfigureInterval   Time interval for recalculating lru
    //                                refresh time according to the ratio.
    Config(uint32_t time,
           double ratio,
           bool updateOnW,
           bool updateOnR,
           bool tryLockU,
           size_t windowToCacheSize,
           size_t tinySizePct,
           uint32_t mmReconfigureInterval)
        : defaultLruRefreshTime(time),
          lruRefreshRatio(ratio),
          updateOnWrite(updateOnW),
          updateOnRead(updateOnR),
          tryLockUpdate(tryLockU),
          windowToCacheSizeRatio(windowToCacheSize),
          tinySizePercent(tinySizePct),
          mmReconfigureIntervalSecs(
              std::chrono::seconds(mmReconfigureInterval)) {
      checkConfig();
    }

    Config() = default;
    Config(const Config& rhs) = default;
    Config(Config&& rhs) = default;

    Config& operator=(const Config& rhs) = default;
    Config& operator=(Config&& rhs) = default;

    void checkConfig() {
      if (tinySizePercent < 1 || tinySizePercent > 50) {
        throw std::invalid_argument(
            folly::sformat("Invalid tiny cache size {}. Tiny cache size "
                           "must be between 1% and 50% of total cache size ",
                           tinySizePercent));
      }
      if (windowToCacheSizeRatio < 2 || windowToCacheSizeRatio > 128) {
        throw std::invalid_argument(
            folly::sformat("Invalid window to cache size ratio {}. The ratio "
                           "must be between 2 and 128",
                           windowToCacheSizeRatio));
      }
    }

    template <typename... Args>
    void addExtraConfig(Args...) {}

    // threshold value in seconds to compare with a node's update time to
    // determine if we need to update the position of the node in the linked
    // list. By default this is 60s to reduce the contention on the lru lock.
    uint32_t defaultLruRefreshTime{60};
    uint32_t lruRefreshTime{defaultLruRefreshTime};

    // ratio of LRU refresh time to the tail age. If a refresh time computed
    // according to this ratio is larger than lruRefreshtime, we will adopt
    // this one instead of the lruRefreshTime set.
    double lruRefreshRatio{0.};

    // whether the lru needs to be updated on writes for recordAccess. If
    // false, accessing the cache for writes does not promote the cached item
    // to the head of the lru.
    bool updateOnWrite{false};

    // whether the lru needs to be updated on reads for recordAccess. If
    // false, accessing the cache for reads does not promote the cached item
    // to the head of the lru.
    bool updateOnRead{true};

    // whether to tryLock or lock the lru lock when attempting promotion on
    // access. If set, and tryLock fails, access will not result in promotion.
    bool tryLockUpdate{false};

    // The multiplier for window size given the cache size.
    size_t windowToCacheSizeRatio{32};

    // The size of tiny cache, as a percentage of the total size.
    size_t tinySizePercent{1};

    // Minimum interval between reconfigurations. If 0, reconfigure is never
    // called.
    std::chrono::seconds mmReconfigureIntervalSecs{};
  };

  // The container object which can be used to keep track of objects of type
  // T. T must have a public member of type Hook. This object is wrapper
  // around DList, is thread safe and can be accessed from multiple threads.
  // The current implementation models an LRU using the above DList
  // implementation.
  template <typename T, Hook<T> T::*HookPtr>
  struct Container {
   private:
    using LruList = MultiDList<T, HookPtr>;
    using Mutex = folly::SpinLock;
    using LockHolder = std::unique_lock<Mutex>;
    using PtrCompressor = typename T::PtrCompressor;
    using Time = typename Hook<T>::Time;
    using CompressedPtr = typename T::CompressedPtr;
    using RefFlags = typename T::Flags;

   public:
    Container() = default;
    Container(Config c, PtrCompressor compressor)
        : lru_(LruType::NumTypes, std::move(compressor)),
          config_(std::move(c)) {
      maybeGrowAccessCountersLocked();
      lruRefreshTime_ = config_.lruRefreshTime;
      nextReconfigureTime_ =
          config_.mmReconfigureIntervalSecs.count() == 0
              ? std::numeric_limits<Time>::max()
              : static_cast<Time>(util::getCurrentTimeSec()) +
                    config_.mmReconfigureIntervalSecs.count();
    }
    Container(serialization::MMTinyLFUObject object, PtrCompressor compressor);

    Container(const Container&) = delete;
    Container& operator=(const Container&) = delete;

    // records the information that the node was accessed. This could bump up
    // the node to the head of the lru depending on the time when the node was
    // last updated in lru and the kLruRefreshTime. If the node was moved to
    // the head in the lru, the node's updateTime will be updated
    // accordingly.
    //
    // @param node  node that we want to mark as relevant/accessed
    // @param mode  the mode for the access operation.
    //
    // @return      True if the information is recorded and bumped the node
    //              to the head of the lru, returns false otherwise
    bool recordAccess(T& node, AccessMode mode) noexcept;

    // adds the given node into the container and marks it as being present in
    // the container. The node is added to the head of the lru.
    //
    // @param node  The node to be added to the container.
    // @return  True if the node was successfully added to the container. False
    //          if the node was already in the contianer. On error state of node
    //          is unchanged.
    bool add(T& node) noexcept;

    // removes the node from the lru and sets it previous and next to nullptr.
    //
    // @param node  The node to be removed from the container.
    // @return  True if the node was successfully removed from the container.
    //          False if the node was not part of the container. On error, the
    //          state of node is unchanged.
    bool remove(T& node) noexcept;

    class LockedIterator;
    // same as the above but uses an iterator context. The iterator is updated
    // on removal of the corresponding node to point to the next node. The
    // iterator context holds the lock on the lru.
    //
    // iterator will be advanced to the next node after removing the node
    //
    // @param it    Iterator that will be removed
    void remove(LockedIterator& it) noexcept;

    // replaces one node with another, at the same position
    //
    // @param oldNode   node being replaced
    // @param newNode   node to replace oldNode with
    //
    // @return true  If the replace was successful. Returns false if the
    //               destination node did not exist in the container, or if the
    //               source node already existed.
    bool replace(T& oldNode, T& newNode) noexcept;

    // context for iterating the MM container. At any given point of time,
    // there can be only one iterator active since we need to lock the LRU for
    // iteration. we can support multiple iterators at same time, by using a
    // shared ptr in the context for the lock holder in the future.
    class LockedIterator {
     public:
      using ListIterator = typename LruList::DListIterator;
      // noncopyable but movable.
      LockedIterator(const LockedIterator&) = delete;
      LockedIterator& operator=(const LockedIterator&) = delete;
      LockedIterator(LockedIterator&&) noexcept = default;

      LockedIterator& operator++() noexcept {
        ++getIter();
        return *this;
      }

      LockedIterator& operator--() {
        throw std::invalid_argument(
            "Decrementing eviction iterator is not supported");
      }

      T* operator->() const noexcept { return getIter().operator->(); }
      T& operator*() const noexcept { return getIter().operator*(); }

      bool operator==(const LockedIterator& other) const noexcept {
        return &c_ == &other.c_ && tIter_ == other.tIter_ &&
               mIter_ == other.mIter_;
      }

      bool operator!=(const LockedIterator& other) const noexcept {
        return !(*this == other);
      }

      explicit operator bool() const noexcept { return tIter_ || mIter_; }

      T* get() const noexcept { return getIter().get(); }

      // Invalidates this iterator
      void reset() noexcept {
        // Point iterator to first list's rend
        tIter_.reset();
        mIter_.reset();
      }

      // 1. Invalidate this iterator
      // 2. Unlock
      void destroy() {
        reset();
        if (l_.owns_lock()) {
          l_.unlock();
        }
      }

      // Reset this iterator to the beginning
      void resetToBegin() {
        if (!l_.owns_lock()) {
          l_.lock();
        }
        tIter_.resetToBegin();
        mIter_.resetToBegin();
      }

     private:
      // private because it's easy to misuse and cause deadlock for MMTinyLFU
      LockedIterator& operator=(LockedIterator&&) noexcept = default;

      // create an lru iterator with the lock being held.
      explicit LockedIterator(LockHolder l,
                              const Container<T, HookPtr>& c) noexcept;

      const ListIterator& getIter() const noexcept {
        return evictTiny() ? tIter_ : mIter_;
      }

      ListIterator& getIter() noexcept {
        return const_cast<ListIterator&>(
            static_cast<const LockedIterator*>(this)->getIter());
      }

      bool evictTiny() const noexcept {
        if (!mIter_) {
          return true;
        }
        if (!tIter_) {
          return false;
        }
        // Since iterators don't change the state of the container, we evict
        // from tiny or main depending on whether the tiny node would be
        // admitted to main cache. If it would be, we evict from main cache,
        // otherwise tiny cache.
        return !c_.admitToMain(*tIter_, *mIter_);
      }

      // only the container can create iterators
      friend Container<T, HookPtr>;

      const Container<T, HookPtr>& c_;
      // Tiny and main cache iterators
      ListIterator tIter_;
      ListIterator mIter_;
      // lock protecting the validity of the iterator
      LockHolder l_;
    };

    Config getConfig() const;

    void setConfig(const Config& newConfig);

    bool isEmpty() const noexcept {
      LockHolder l(lruMutex_);
      return lru_.size() == 0;
    }

    size_t size() const noexcept {
      LockHolder l(lruMutex_);
      return lru_.size();
    }

    // reconfigure the MMContainer: update refresh time according to current
    // tail age
    void reconfigureLocked(const Time& currTime);

    size_t counterSize() const noexcept {
      LockHolder l(lruMutex_);
      return accessFreq_.getByteSize();
    }

    // Returns the eviction age stats. See CacheStats.h for details
    EvictionAgeStat getEvictionAgeStat(uint64_t projectedLength) const noexcept;

    // Obtain an iterator that start from the tail and can be used
    // to search for evictions. This iterator holds a lock to this
    // container and only one such iterator can exist at a time
    LockedIterator getEvictionIterator() const noexcept;

    // Execute provided function under container lock. Function gets
    // iterator passed as parameter.
    template <typename F>
    void withEvictionIterator(F&& f);

    // for saving the state of the lru
    //
    // precondition:  serialization must happen without any reader or writer
    // present. Any modification of this object afterwards will result in an
    // invalid, inconsistent state for the serialized data.
    //
    serialization::MMTinyLFUObject saveState() const noexcept;

    // return the stats for this container.
    MMContainerStat getStats() const noexcept;

    static LruType getLruType(const T& node) noexcept {
      return isTiny(node) ? LruType::Tiny : LruType::Main;
    }

   private:
    EvictionAgeStat getEvictionAgeStatLocked(
        uint64_t projectedLength) const noexcept;

    static Time getUpdateTime(const T& node) noexcept {
      return (node.*HookPtr).getUpdateTime();
    }

    static void setUpdateTime(T& node, Time time) noexcept {
      (node.*HookPtr).setUpdateTime(time);
    }

    // As the cache grows, the frequency counters may need to grow.
    void maybeGrowAccessCountersLocked() noexcept;

    // Update frequency count for the node. Halve all counts if
    // we've reached the end of the window.
    void updateFrequenciesLocked(const T& node) noexcept;

    // Promote the tail of tiny cache to main cache if it has higher
    // frequency count than the tail of the main cache.
    void maybePromoteTailLocked() noexcept;

    // Returns the hash of node's key
    static size_t hashNode(const T& node) noexcept {
      return folly::hasher<folly::StringPiece>()(node.getKey());
    }

    // Returns true if tiny node must be admitted to main cache since its
    // frequency is higher than that of the main node.
    bool admitToMain(const T& tinyNode, const T& mainNode) const noexcept {
      XDCHECK(isTiny(tinyNode));
      XDCHECK(!isTiny(mainNode));
      auto tinyFreq = accessFreq_.getCount(hashNode(tinyNode));
      auto mainFreq = accessFreq_.getCount(hashNode(mainNode));
      return tinyFreq >= mainFreq;
    }

    // remove node from lru and adjust insertion points
    //
    // @param node          node to remove
    void removeLocked(T& node) noexcept;

    static bool isTiny(const T& node) noexcept {
      return node.template isFlagSet<RefFlags::kMMFlag0>();
    }

    static bool isAccessed(const T& node) noexcept {
      return node.template isFlagSet<RefFlags::kMMFlag1>();
    }

    // Bit MM_BIT_0 is used to record if the item is in tiny cache.
    static void markTiny(T& node) noexcept {
      node.template setFlag<RefFlags::kMMFlag0>();
    }
    static void unmarkTiny(T& node) noexcept {
      node.template unSetFlag<RefFlags::kMMFlag0>();
    }

    // Bit MM_BIT_1 is used to record if the item has been accessed since being
    // written in cache. Unaccessed items are ignored when determining projected
    // update time.
    static void markAccessed(T& node) noexcept {
      node.template setFlag<RefFlags::kMMFlag1>();
    }
    static void unmarkAccessed(T& node) noexcept {
      node.template unSetFlag<RefFlags::kMMFlag1>();
    }

    // Initial cache capacity estimate for count-min-sketch
    static constexpr size_t kDefaultCapacity = 100;

    // Number of hashes
    static constexpr size_t kHashCount = 4;

    // The error threshold for frequency calculation
    static constexpr size_t kErrorThreshold = 5;

    // decay rate for frequency
    static constexpr double kDecayFactor = 0.5;

    // protects all operations on the lru. We never really just read the state
    // of the LRU. Hence we dont really require a RW mutex at this point of
    // time.
    mutable Mutex lruMutex_;

    // the lru
    LruList lru_;

    // the window size counter
    size_t windowSize_{0};

    // maximum value of window size which when hit the counters are halved
    size_t maxWindowSize_{0};

    // The capacity for which the counters are sized
    size_t capacity_{0};

    // The next time to reconfigure the container.
    std::atomic<Time> nextReconfigureTime_{};

    // How often to promote an item in the eviction queue.
    std::atomic<uint32_t> lruRefreshTime_{};

    // Max lruFreshTime.
    static constexpr uint32_t kLruRefreshTimeCap{900};

    // Config for this lru.
    // Write access to the MMTinyLFU Config is serialized.
    // Reads may be racy.
    Config config_{};

    // Approximate streaming frequency counters. The counts are halved every
    // time the maxWindowSize is hit.
    facebook::cachelib::util::CountMinSketch accessFreq_{};

    FRIEND_TEST(MMTinyLFUTest, SegmentStress);
    FRIEND_TEST(MMTinyLFUTest, TinyLFUBasic);
    FRIEND_TEST(MMTinyLFUTest, Reconfigure);
  };
};
} // namespace cachelib
} // namespace facebook

#include "cachelib/allocator/MMTinyLFU-inl.h"
