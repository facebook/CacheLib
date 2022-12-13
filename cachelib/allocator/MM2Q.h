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
#pragma GCC diagnostic pop

#include <folly/lang/Align.h>
#include <folly/synchronization/DistributedMutex.h>

#include "cachelib/allocator/Cache.h"
#include "cachelib/allocator/CacheStats.h"
#include "cachelib/allocator/Util.h"
#include "cachelib/allocator/datastruct/MultiDList.h"
#include "cachelib/allocator/memory/serialize/gen-cpp2/objects_types.h"
#include "cachelib/common/CompilerUtils.h"
#include "cachelib/common/Mutex.h"

namespace facebook {
namespace cachelib {

template <typename MMType>
class MMTypeTest;

// The basic version of this cache has 3 queues (Hot, Warm, and Cold). When an
// item is added, it goes to Hot queue; we have percentage sizes for Hot and
// Warm, and when they are too large, in rebalance() items can be moved to
// Cold. Promotion moves items to the head of its queue except for Cold it
// moves to the head of Warm queue. The eviction order is Cold, Hot, and Warm.
//
// When config.tailSize > 0, we enable two extra queues (ColdTail, WarmTail) of
// memory size 1 slab to provide access stats for the tail lru slab. In this
// case, in rebalance() we move items in WarmTail to Cold before moving from
// Warm to Cold if the total size of Warm and WarmTail is larger than expected.
// Also, tail queues are adjusted in rebalance() so that their sizes are
// config.tailSize. The eviction order is ColdTail, Cold, Hot, WarmTail, and
// Warm.
class MM2Q {
 public:
  // unique identifier per MMType
  static const int kId;

  // forward declaration;
  template <typename T>
  using Hook = DListHook<T>;
  using SerializationType = serialization::MM2QObject;
  using SerializationConfigType = serialization::MM2QConfig;
  using SerializationTypeContainer = serialization::MM2QCollection;

  enum LruType { Warm, WarmTail, Hot, Cold, ColdTail, NumTypes };

  // Config class for MM2Q
  struct Config {
    // Create from serialized config
    explicit Config(SerializationConfigType configState)
        : Config(*configState.lruRefreshTime(),
                 *configState.lruRefreshRatio(),
                 *configState.updateOnWrite(),
                 *configState.updateOnRead(),
                 *configState.tryLockUpdate(),
                 *configState.rebalanceOnRecordAccess(),
                 *configState.hotSizePercent(),
                 *configState.coldSizePercent()) {}

    // @param time      the refresh time in seconds to trigger an update in
    // position upon access. An item will be promoted only once in each lru
    // refresh time depite the number of accesses it gets.
    // @param udpateOnW whether to promote the item on write
    // @param updateOnR whether to promote the item on read
    Config(uint32_t time, bool updateOnW, bool updateOnR)
        : Config(time,
                 updateOnW,
                 updateOnR,
                 /* try lock update */ false,
                 /* rebalanceOnRecordAccess */ false,
                 30,
                 30) {}

    // @param time      the LRU refresh time.
    // An item will be promoted only once in each lru refresh time depite the
    // number of accesses it gets.
    // @param udpateOnW whether to promote the item on write
    // @param updateOnR whether to promote the item on read
    // @param hotPercent percentage number for the size of the hot queue in the
    // overall size.
    // @param coldPercent percentage number for the size of the cold queue in
    // the overall size.
    Config(uint32_t time,
           bool updateOnW,
           bool updateOnR,
           size_t hotPercent,
           size_t coldPercent)
        : Config(time,
                 updateOnW,
                 updateOnR,
                 /* try lock update */ false,
                 /* rebalanceOnRecordAccess */ false,
                 hotPercent,
                 coldPercent) {}

    // @param time                    the LRU refresh time.
    //                                An item will be promoted only once in each
    //                                lru refresh time depite the
    //                                number of accesses it gets.
    // @param udpateOnW               whether to promote the item on write
    // @param updateOnR               whether to promote the item on read
    // @param tryLockU                whether to use a try lock when doing
    // update.
    // @param rebalanceOnRecordAccs   whether to do rebalance on access. If set
    //                                to false, rebalance only happens when
    //                                items are added or removed to the queue.
    // @param hotPercent              percentage number for the size of the hot
    //                                queue in the overall size.
    // @param coldPercent             percentage number for the size of the cold
    //                                queue in the overall size.
    Config(uint32_t time,
           bool updateOnW,
           bool updateOnR,
           bool tryLockU,
           bool rebalanceOnRecordAccs,
           size_t hotPercent,
           size_t coldPercent)
        : Config(time,
                 0.,
                 updateOnW,
                 updateOnR,
                 tryLockU,
                 rebalanceOnRecordAccs,
                 hotPercent,
                 coldPercent) {}

    // @param time                    the LRU refresh time.
    //                                An item will be promoted only once in each
    //                                lru refresh time depite the
    //                                number of accesses it gets.
    // @param ratio                   the LRU refresh ratio. The ratio times the
    //                                oldest element's lifetime in warm queue
    //                                would be the minimum value of LRU refresh
    //                                time.
    // @param udpateOnW               whether to promote the item on write
    // @param updateOnR               whether to promote the item on read
    // @param tryLockU                whether to use a try lock when doing
    // update.
    // @param rebalanceOnRecordAccs   whether to do rebalance on access. If set
    //                                to false, rebalance only happens when
    //                                items are added or removed to the queue.
    // @param hotPercent              percentage number for the size of the hot
    //                                queue in the overall size.
    // @param coldPercent             percentage number for the size of the cold
    //                                queue in the overall size.
    Config(uint32_t time,
           double ratio,
           bool updateOnW,
           bool updateOnR,
           bool tryLockU,
           bool rebalanceOnRecordAccs,
           size_t hotPercent,
           size_t coldPercent)
        : Config(time,
                 ratio,
                 updateOnW,
                 updateOnR,
                 tryLockU,
                 rebalanceOnRecordAccs,
                 hotPercent,
                 coldPercent,
                 0) {}

    // @param time                    the LRU refresh time.
    //                                An item will be promoted only once in each
    //                                lru refresh time depite the
    //                                number of accesses it gets.
    // @param ratio                   the LRU refresh ratio. The ratio times the
    //                                oldest element's lifetime in warm queue
    //                                would be the minimum value of LRU refresh
    //                                time.
    // @param udpateOnW               whether to promote the item on write
    // @param updateOnR               whether to promote the item on read
    // @param tryLockU                whether to use a try lock when doing
    // update.
    // @param rebalanceOnRecordAccs   whether to do rebalance on access. If set
    //                                to false, rebalance only happens when
    //                                items are added or removed to the queue.
    // @param hotPercent              percentage number for the size of the hot
    //                                queue in the overall size.
    // @param coldPercent             percentage number for the size of the cold
    //                                queue in the overall size.
    // @param mmReconfigureInterval   Time interval for recalculating lru
    //                                refresh time according to the ratio.
    Config(uint32_t time,
           double ratio,
           bool updateOnW,
           bool updateOnR,
           bool tryLockU,
           bool rebalanceOnRecordAccs,
           size_t hotPercent,
           size_t coldPercent,
           uint32_t mmReconfigureInterval)
        : Config(time,
                 ratio,
                 updateOnW,
                 updateOnR,
                 tryLockU,
                 rebalanceOnRecordAccs,
                 hotPercent,
                 coldPercent,
                 mmReconfigureInterval,
                 false) {}

    // @param time                    the LRU refresh time.
    //                                An item will be promoted only once in each
    //                                lru refresh time depite the
    //                                number of accesses it gets.
    // @param ratio                   the LRU refresh ratio. The ratio times the
    //                                oldest element's lifetime in warm queue
    //                                would be the minimum value of LRU refresh
    //                                time.
    // @param udpateOnW               whether to promote the item on write
    // @param updateOnR               whether to promote the item on read
    // @param tryLockU                whether to use a try lock when doing
    // update.
    // @param rebalanceOnRecordAccs   whether to do rebalance on access. If set
    //                                to false, rebalance only happens when
    //                                items are added or removed to the queue.
    // @param hotPercent              percentage number for the size of the hot
    //                                queue in the overall size.
    // @param coldPercent             percentage number for the size of the cold
    //                                queue in the overall size.
    // @param mmReconfigureInterval   Time interval for recalculating lru
    //                                refresh time according to the ratio.
    // useCombinedLockForIterators    Whether to use combined locking for
    //                                withEvictionIterator
    Config(uint32_t time,
           double ratio,
           bool updateOnW,
           bool updateOnR,
           bool tryLockU,
           bool rebalanceOnRecordAccs,
           size_t hotPercent,
           size_t coldPercent,
           uint32_t mmReconfigureInterval,
           bool useCombinedLockForIterators)
        : defaultLruRefreshTime(time),
          lruRefreshRatio(ratio),
          updateOnWrite(updateOnW),
          updateOnRead(updateOnR),
          tryLockUpdate(tryLockU),
          rebalanceOnRecordAccess(rebalanceOnRecordAccs),
          hotSizePercent(hotPercent),
          coldSizePercent(coldPercent),
          mmReconfigureIntervalSecs(
              std::chrono::seconds(mmReconfigureInterval)),
          useCombinedLockForIterators(useCombinedLockForIterators) {
      checkLruSizes();
    }

    Config() = default;
    Config(const Config& rhs) = default;
    Config(Config&& rhs) = default;

    Config& operator=(const Config& rhs) = default;
    Config& operator=(Config&& rhs) = default;

    size_t getWarmSizePercent() const noexcept {
      return 100 - hotSizePercent - coldSizePercent;
    }

    // Make sure that the size percent numbers are sane.
    void checkLruSizes() {
      auto warmSizePercent = getWarmSizePercent();
      // 100% hot is allowed as a drop-in replacement for LRU

      if (hotSizePercent == 100) {
        if (coldSizePercent != 0 && warmSizePercent != 0) {
          throw std::invalid_argument(folly::sformat(
              "Invalid hot/cold/warm lru size {}/{}/{}. When Hot is 100%,"
              " Warm and Cold must be 0.",
              hotSizePercent, coldSizePercent, warmSizePercent));
        }
      } else if (hotSizePercent <= 0 || hotSizePercent > 100 ||
                 coldSizePercent <= 0 || coldSizePercent >= 100 ||
                 warmSizePercent <= 0 || warmSizePercent >= 100) {
        throw std::invalid_argument(
            folly::sformat("Invalid hot/cold/warm lru size {}/{}/{}. Hot, "
                           "Warm and Cold lru's "
                           "must all have non-zero sizes.",
                           hotSizePercent, coldSizePercent, warmSizePercent));
      }
    }

    // adding extra config after generating the config: tailSize
    void addExtraConfig(size_t tSize) { tailSize = tSize; }

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

    // if set to true, we attempt to rebalance the hot cold and warm sizes on
    // every record access. If you are sensitive to access latencies to your
    // cache on reads, disable this.
    bool rebalanceOnRecordAccess{true};

    // Size of hot and cold lru sizes.
    size_t hotSizePercent{30};
    size_t coldSizePercent{30};

    // number of items in the tail slab (set to 0 to disable)
    // This should be set by cache allocator through addExtraConfig(); user
    // should not set this manually.
    size_t tailSize{0};

    // Minimum interval between reconfigurations. If 0, reconfigure is never
    // called.
    std::chrono::seconds mmReconfigureIntervalSecs{};

    // Whether to use combined locking for withEvictionIterator.
    bool useCombinedLockForIterators{false};
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
    using Mutex = folly::DistributedMutex;
    using LockHolder = std::unique_lock<Mutex>;
    using PtrCompressor = typename T::PtrCompressor;
    using Time = typename Hook<T>::Time;
    using CompressedPtr = typename T::CompressedPtr;
    using RefFlags = typename T::Flags;

   public:
    Container() = default;
    Container(Config c, PtrCompressor compressor)
        : lru_(LruType::NumTypes, std::move(compressor)),
          tailTrackingEnabled_(c.tailSize > 0),
          config_(std::move(c)) {
      lruRefreshTime_ = config_.lruRefreshTime;
      nextReconfigureTime_ =
          config_.mmReconfigureIntervalSecs.count() == 0
              ? std::numeric_limits<Time>::max()
              : static_cast<Time>(util::getCurrentTimeSec()) +
                    config_.mmReconfigureIntervalSecs.count();
    }

    // If the serialized data has 3 lists and we want to expand to 5 lists
    // without enabling tail hits tracking, we need to adjust list positions
    // so that a cold roll can be avoided.
    Container(const serialization::MM2QObject& object,
              PtrCompressor compressor);

    Container(const Container&) = delete;
    Container& operator=(const Container&) = delete;

    using Iterator = typename LruList::Iterator;

    // context for iterating the MM container. At any given point of time,
    // there can be only one iterator active since we need to lock the LRU for
    // iteration. we can support multiple iterators at same time, by using a
    // shared ptr in the context for the lock holder in the future.
    class LockedIterator : public Iterator {
     public:
      // noncopyable but movable.
      LockedIterator(const LockedIterator&) = delete;
      LockedIterator& operator=(const LockedIterator&) = delete;

      LockedIterator(LockedIterator&&) noexcept = default;

      // 1. Invalidate this iterator
      // 2. Unlock
      void destroy() {
        Iterator::reset();
        if (l_.owns_lock()) {
          l_.unlock();
        }
      }

      // Reset this iterator to the beginning
      void resetToBegin() {
        if (!l_.owns_lock()) {
          l_.lock();
        }
        Iterator::resetToBegin();
      }

     private:
      // private because it's easy to misuse and cause deadlock for MM2Q
      LockedIterator& operator=(LockedIterator&&) noexcept = default;

      // create an lru iterator with the lock being held.
      LockedIterator(LockHolder l, const Iterator& iter) noexcept;

      // only the container can create iterators
      friend Container<T, HookPtr>;

      // lock protecting the validity of the iterator
      LockHolder l_;
    };

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

    // same as the above but uses an iterator context. The iterator is updated
    // on removal of the corresponding node to point to the next node. The
    // iterator context is responsible for locking.
    //
    // iterator will be advanced to the next node after removing the node
    //
    // We do not do rebalance in this remove because changing the queues while
    // having the iterator can cause probelms.
    //
    // @param it    Iterator that will be removed
    void remove(Iterator& it) noexcept;

    // replaces one node with another, at the same position
    //
    // @param oldNode   node being replaced
    // @param newNode   node to replace oldNode with
    //
    // @return true  If the replace was successful. Returns false if the
    //               destination node did not exist in the container, or if the
    //               source node already existed.
    bool replace(T& oldNode, T& newNode) noexcept;

    // Obtain an iterator that start from the tail and can be used
    // to search for evictions. This iterator holds a lock to this
    // container and only one such iterator can exist at a time
    LockedIterator getEvictionIterator() const noexcept;

    // Execute provided function under container lock. Function gets
    // Iterator passed as parameter.
    template <typename F>
    void withEvictionIterator(F&& f);

    // get the current config as a copy
    Config getConfig() const;

    // override the current config.
    void setConfig(const Config& newConfig);

    bool isEmpty() const noexcept { return size() == 0; }

    size_t size() const noexcept {
      return lruMutex_->lock_combine([this]() { return lru_.size(); });
    }

    // Returns the eviction age stats. See CacheStats.h for details
    EvictionAgeStat getEvictionAgeStat(uint64_t projectedLength) const noexcept;

    // for saving the state of the lru
    //
    // precondition:  serialization must happen without any reader or writer
    // present. Any modification of this object afterwards will result in an
    // invalid, inconsistent state for the serialized data.
    //
    serialization::MM2QObject saveState() const noexcept;

    // return the stats for this container.
    MMContainerStat getStats() const noexcept;

    // TODO: make this static and *Hot, *Cold, *Tail methods static
    //       This can only be done after T33401054 is finished.
    //       Since we need to always turn on the Tail feature.
    LruType getLruType(const T& node) const noexcept;

   private:
    // reconfigure the MMContainer: update LRU refresh time according to current
    // tail age
    void reconfigureLocked(const Time& currTime);

    EvictionAgeStat getEvictionAgeStatLocked(
        uint64_t projectedLength) const noexcept;

    uint32_t getOldestAgeLocked(LruType lruType,
                                Time currentTime) const noexcept;

    static Time getUpdateTime(const T& node) noexcept {
      return (node.*HookPtr).getUpdateTime();
    }

    static void setUpdateTime(T& node, Time time) noexcept {
      (node.*HookPtr).setUpdateTime(time);
    }

    // remove node from lru and adjust insertion points
    //
    // @param node          node to remove
    // @param doRebalance     whether to do rebalance in this remove
    void removeLocked(T& node, bool doRebalance = true) noexcept;

    // Bit MM_BIT_0 is used to record if the item is hot.
    void markHot(T& node) noexcept {
      node.template setFlag<RefFlags::kMMFlag0>();
    }

    void unmarkHot(T& node) noexcept {
      node.template unSetFlag<RefFlags::kMMFlag0>();
    }

    bool isHot(const T& node) const noexcept {
      return node.template isFlagSet<RefFlags::kMMFlag0>();
    }

    // Bit MM_BIT_2 is used to record if the item is cold.
    void markCold(T& node) noexcept {
      node.template setFlag<RefFlags::kMMFlag2>();
    }

    void unmarkCold(T& node) noexcept {
      node.template unSetFlag<RefFlags::kMMFlag2>();
    }

    bool isCold(const T& node) const noexcept {
      return node.template isFlagSet<RefFlags::kMMFlag2>();
    }

    bool isWarm(const T& node) const noexcept {
      return !isHot(node) && !isCold(node);
    }

    // Bit MM_Bit_1 is used to record whether the item is in tail
    // this flag should be set only if the 1-slab tail is enabled, i.e.
    // config_.tailSize > 0, and a node is in Tail only if the tail is enabled.
    void markTail(T& node) noexcept {
      XDCHECK(config_.tailSize > 0);
      node.template setFlag<RefFlags::kMMFlag1>();
    }

    void unmarkTail(T& node) noexcept {
      node.template unSetFlag<RefFlags::kMMFlag1>();
    }

    bool inTail(const T& node) const noexcept {
      return config_.tailSize && node.template isFlagSet<RefFlags::kMMFlag1>();
    }

    // get the tail lru for current lru, i.e. WarmTail for Warm, and ColdTail
    // for Cold
    //
    // @param list the Lru of which we want to know the tail (should be either
    // Warm or Cold)
    LruType getTailLru(LruType list) const;

    // adjust the size of tail list given a regular list. The tail size is
    // adjusted to reflect config.tailSize by moving nodes from the regular
    // list to the list's corresponding tail.
    //
    // @param list  the Lru of which the tail we want to adjust (should be
    // either Warm or Cold)
    void adjustTail(LruType list);

    // Rebalance hot/cold/warm lrus to hot/cold size percent.
    //
    // - First, move items from WarmTail (or Warm, if WarmTail is empty) to Cold
    // until Warm part is within expected size.
    // - Then move items from Hot to Cold until Hot is within expected size.
    // - Lastly, adjust Warm and Cold so that their tails are at expected sizes.
    void rebalance() noexcept;

    // protects all operations on the lru. We never really just read the state
    // of the LRU. Hence we dont really require a RW mutex at this point of
    // time.
    mutable folly::cacheline_aligned<Mutex> lruMutex_;

    // the lru
    LruList lru_{LruType::NumTypes, PtrCompressor{}};

    // size of tail after insertion point
    size_t tailSize_{0};

    // number of hits in each lru.
    uint64_t numHotAccesses_{0};
    uint64_t numColdAccesses_{0};
    uint64_t numWarmAccesses_{0};

    // number of hits in tail parts
    uint64_t numWarmTailAccesses_{0};
    uint64_t numColdTailAccesses_{0};

    // This boolean is to track whether tail tracking is enabled. When we are
    // turning on or off this feature by setting a new tailSize, cold roll is
    // required, which is indicated by throwing exception after checking this
    // flag. It is initialized to true when creating new MMContainers with
    // configs that specifies a non-zero tailSize.
    const bool tailTrackingEnabled_{false};

    // The next time to reconfigure the container.
    std::atomic<Time> nextReconfigureTime_{};

    // How often to promote an item in the eviction queue.
    std::atomic<uint32_t> lruRefreshTime_{};

    // Config for this lru.
    // Write access to the MM2Q Config is serialized.
    // Reads may be racy.
    Config config_{};

    // Max lruFreshTime.
    static constexpr uint32_t kLruRefreshTimeCap{900};

    FRIEND_TEST(MM2QTest, DetailedTest);
    FRIEND_TEST(MM2QTest, DeserializeToMoreLists);
    FRIEND_TEST(MM2QTest, TailHits);
    FRIEND_TEST(MM2QTest, TailTrackingEnabledCheck);
    // still useful for helper functions
    friend class MMTypeTest<MM2Q>;
  };
};
} // namespace cachelib
} // namespace facebook

#include "cachelib/allocator/MM2Q-inl.h"
