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

namespace facebook::cachelib {
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
    using CompressedPtrType = typename T::CompressedPtrType;
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

    // Execute provided function under container lock.
    template <typename F>
    void withContainerLock(F&& f);

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

/* Container Interface Implementation */
template <typename T, MM2Q::Hook<T> T::*HookPtr>
MM2Q::Container<T, HookPtr>::Container(const serialization::MM2QObject& object,
                                       PtrCompressor compressor)
    : lru_(*object.lrus(), compressor),
      tailTrackingEnabled_(*object.tailTrackingEnabled()),
      config_(*object.config()) {
  lruRefreshTime_ = config_.lruRefreshTime;
  nextReconfigureTime_ = config_.mmReconfigureIntervalSecs.count() == 0
                             ? std::numeric_limits<Time>::max()
                             : static_cast<Time>(util::getCurrentTimeSec()) +
                                   config_.mmReconfigureIntervalSecs.count();

  // We need to adjust list positions if the previous version does not have
  // tail lists (WarmTail & ColdTail), in order to potentially avoid cold roll
  if (object.lrus()->lists()->size() < LruType::NumTypes) {
    XDCHECK_EQ(false, tailTrackingEnabled_);
    XDCHECK_EQ(object.lrus()->lists()->size() + 2, LruType::NumTypes);
    lru_.insertEmptyListAt(LruType::WarmTail, compressor);
    lru_.insertEmptyListAt(LruType::ColdTail, compressor);
  }
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
bool MM2Q::Container<T, HookPtr>::recordAccess(T& node,
                                               AccessMode mode) noexcept {
  if ((mode == AccessMode::kWrite && !config_.updateOnWrite) ||
      (mode == AccessMode::kRead && !config_.updateOnRead)) {
    return false;
  }

  const auto curr = static_cast<Time>(util::getCurrentTimeSec());
  // check if the node is still being memory managed
  if (node.isInMMContainer() &&
      ((curr >= getUpdateTime(node) +
                    lruRefreshTime_.load(std::memory_order_relaxed)))) {
    auto func = [&]() {
      reconfigureLocked(curr);
      if (!node.isInMMContainer()) {
        return false;
      }
      if (isHot(node)) {
        lru_.getList(LruType::Hot).moveToHead(node);
        ++numHotAccesses_;
      } else if (isCold(node)) {
        if (inTail(node)) {
          unmarkTail(node);
          lru_.getList(LruType::ColdTail).remove(node);
          ++numColdTailAccesses_;
        } else {
          lru_.getList(LruType::Cold).remove(node);
        }
        lru_.getList(LruType::Warm).linkAtHead(node);
        unmarkCold(node);
        ++numColdAccesses_;
        // only rebalance if config says so. recordAccess is called mostly on
        // latency sensitive cache get operations.
        if (config_.rebalanceOnRecordAccess) {
          rebalance();
        }
      } else {
        if (inTail(node)) {
          unmarkTail(node);
          lru_.getList(LruType::WarmTail).remove(node);
          lru_.getList(LruType::Warm).linkAtHead(node);
          ++numWarmTailAccesses_;
        } else {
          lru_.getList(LruType::Warm).moveToHead(node);
        }
        ++numWarmAccesses_;
      }
      setUpdateTime(node, curr);
      return true;
    };

    // if the tryLockUpdate optimization is on, and we were able to grab the
    // lock, execute the critical section and return true, else return false
    //
    // if the tryLockUpdate optimization is off, we always execute the critical
    // section and return true
    if (config_.tryLockUpdate) {
      if (auto lck = LockHolder{*lruMutex_, std::try_to_lock}) {
        return func();
      }
      return false;
    }

    return lruMutex_->lock_combine(func);
  }
  return false;
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
cachelib::EvictionAgeStat MM2Q::Container<T, HookPtr>::getEvictionAgeStat(
    uint64_t projectedLength) const noexcept {
  return lruMutex_->lock_combine([this, projectedLength]() {
    return getEvictionAgeStatLocked(projectedLength);
  });
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
cachelib::EvictionAgeStat MM2Q::Container<T, HookPtr>::getEvictionAgeStatLocked(
    uint64_t projectedLength) const noexcept {
  const auto currTime = static_cast<Time>(util::getCurrentTimeSec());

  auto getProjectedAge = [this, currTime, projectedLength](
                             auto lruType, auto oldestElementAge) {
    auto it = lru_.rbegin(lruType);
    for (size_t numSeen = 0; numSeen < projectedLength && it != lru_.rend();
         ++numSeen, ++it) {
    }
    return (it != lru_.rend()) ? currTime - getUpdateTime(*it)
                               : oldestElementAge;
  };

  EvictionAgeStat stat;

  stat.hotQueueStat.oldestElementAge =
      getOldestAgeLocked(LruType::Hot, currTime);
  stat.hotQueueStat.size = lru_.getList(LruType::Hot).size();
  stat.hotQueueStat.projectedAge =
      getProjectedAge(LruType::Hot, stat.hotQueueStat.oldestElementAge);

  // sum the tail and the main list for the ones that have tail.
  stat.warmQueueStat.oldestElementAge =
      getOldestAgeLocked(LruType::WarmTail, currTime);
  stat.warmQueueStat.size = lru_.getList(LruType::Warm).size() +
                            lru_.getList(LruType::WarmTail).size();
  stat.warmQueueStat.projectedAge =
      getProjectedAge(LruType::WarmTail, stat.warmQueueStat.oldestElementAge);

  stat.coldQueueStat.oldestElementAge =
      getOldestAgeLocked(LruType::ColdTail, currTime);
  stat.coldQueueStat.size = lru_.getList(LruType::ColdTail).size() +
                            lru_.getList(LruType::Cold).size();
  stat.coldQueueStat.projectedAge =
      getProjectedAge(LruType::ColdTail, stat.coldQueueStat.oldestElementAge);

  return stat;
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
uint32_t MM2Q::Container<T, HookPtr>::getOldestAgeLocked(
    LruType lruType, Time currentTime) const noexcept {
  auto it = lru_.rbegin(lruType);
  return it != lru_.rend() ? currentTime - getUpdateTime(*it) : 0;
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
typename MM2Q::LruType MM2Q::Container<T, HookPtr>::getLruType(
    const T& node) const noexcept {
  if (isHot(node)) {
    return LruType::Hot;
  }
  if (isCold(node)) {
    return inTail(node) ? LruType::ColdTail : LruType::Cold;
  }
  return inTail(node) ? LruType::WarmTail : LruType::Warm;
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
void MM2Q::Container<T, HookPtr>::rebalance() noexcept {
  // shrink Warm (and WarmTail) if their total size is larger than expected
  size_t expectedSize = config_.getWarmSizePercent() * lru_.size() / 100;
  while (lru_.getList(LruType::Warm).size() +
             lru_.getList(LruType::WarmTail).size() >
         expectedSize) {
    auto popFrom = [&](LruType lruType) -> T* {
      auto& lru = lru_.getList(lruType);
      T* node = lru.getTail();
      XDCHECK(node);
      lru.remove(*node);
      if (lruType == WarmTail || lruType == ColdTail) {
        unmarkTail(*node);
      }
      return node;
    };
    // remove from warm tail if it is not empty. if empty, remove from warm.
    T* node = lru_.getList(LruType::WarmTail).size() > 0
                  ? popFrom(LruType::WarmTail)
                  : popFrom(LruType::Warm);
    XDCHECK(isWarm(*node));
    lru_.getList(LruType::Cold).linkAtHead(*node);
    markCold(*node);
  }

  // shrink Hot if its size is larger than expected
  expectedSize = config_.hotSizePercent * lru_.size() / 100;
  while (lru_.getList(LruType::Hot).size() > expectedSize) {
    auto node = lru_.getList(LruType::Hot).getTail();
    XDCHECK(node);
    XDCHECK(isHot(*node));
    lru_.getList(LruType::Hot).remove(*node);
    lru_.getList(LruType::Cold).linkAtHead(*node);
    unmarkHot(*node);
    markCold(*node);
  }

  // adjust tail sizes for Cold and Warm
  adjustTail(LruType::Cold);
  adjustTail(LruType::Warm);
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
bool MM2Q::Container<T, HookPtr>::add(T& node) noexcept {
  const auto currTime = static_cast<Time>(util::getCurrentTimeSec());
  return lruMutex_->lock_combine([this, &node, currTime]() {
    if (node.isInMMContainer()) {
      return false;
    }

    markHot(node);
    unmarkCold(node);
    unmarkTail(node);
    lru_.getList(LruType::Hot).linkAtHead(node);
    rebalance();

    node.markInMMContainer();
    setUpdateTime(node, currTime);
    return true;
  });
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
typename MM2Q::Container<T, HookPtr>::LockedIterator
MM2Q::Container<T, HookPtr>::getEvictionIterator() const noexcept {
  LockHolder l(*lruMutex_);
  return LockedIterator{std::move(l), lru_.rbegin()};
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
template <typename F>
void MM2Q::Container<T, HookPtr>::withEvictionIterator(F&& fun) {
  if (config_.useCombinedLockForIterators) {
    lruMutex_->lock_combine([this, &fun]() { fun(Iterator{lru_.rbegin()}); });
  } else {
    LockHolder lck{*lruMutex_};
    fun(Iterator{lru_.rbegin()});
  }
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
template <typename F>
void MM2Q::Container<T, HookPtr>::withContainerLock(F&& fun) {
  lruMutex_->lock_combine([&fun]() { fun(); });
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
void MM2Q::Container<T, HookPtr>::removeLocked(T& node,
                                               bool doRebalance) noexcept {
  LruType type = getLruType(node);
  lru_.getList(type).remove(node);
  if (doRebalance) {
    rebalance();
  }

  node.unmarkInMMContainer();
  return;
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
void MM2Q::Container<T, HookPtr>::setConfig(const Config& newConfig) {
  if (!tailTrackingEnabled_ && newConfig.tailSize > 0) {
    throw std::invalid_argument(
        "Cannot turn on tailHitsTracking (cache drop needed)");
  }
  if (tailTrackingEnabled_ && !newConfig.tailSize) {
    throw std::invalid_argument(
        "Cannot turn off tailHitsTracking (cache drop needed)");
  }

  lruMutex_->lock_combine([this, &newConfig]() {
    config_ = newConfig;
    lruRefreshTime_.store(config_.lruRefreshTime, std::memory_order_relaxed);
    nextReconfigureTime_ = config_.mmReconfigureIntervalSecs.count() == 0
                               ? std::numeric_limits<Time>::max()
                               : static_cast<Time>(util::getCurrentTimeSec()) +
                                     config_.mmReconfigureIntervalSecs.count();
  });
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
typename MM2Q::Config MM2Q::Container<T, HookPtr>::getConfig() const {
  return lruMutex_->lock_combine([this]() { return config_; });
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
bool MM2Q::Container<T, HookPtr>::remove(T& node) noexcept {
  return lruMutex_->lock_combine([this, &node]() {
    if (!node.isInMMContainer()) {
      return false;
    }
    removeLocked(node);
    return true;
  });
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
void MM2Q::Container<T, HookPtr>::remove(Iterator& it) noexcept {
  T& node = *it;
  XDCHECK(node.isInMMContainer());
  ++it;
  // rebalance should not be triggered inside this remove, because changing the
  // queues while having the EvictionIterator can cause inconsistency problem
  // for the iterator. Also, this remove is followed by an insertion into the
  // same container, which will trigger rebalance.
  removeLocked(node, /* doRebalance = */ false);
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
bool MM2Q::Container<T, HookPtr>::replace(T& oldNode, T& newNode) noexcept {
  return lruMutex_->lock_combine([this, &oldNode, &newNode]() {
    if (!oldNode.isInMMContainer() || newNode.isInMMContainer()) {
      return false;
    }
    const auto updateTime = getUpdateTime(oldNode);

    LruType type = getLruType(oldNode);
    lru_.getList(type).replace(oldNode, newNode);
    switch (type) {
    case LruType::Hot:
      markHot(newNode);
      break;
    case LruType::ColdTail:
      markTail(newNode);
      [[fallthrough]]; // pass through to also mark cold
    case LruType::Cold:
      markCold(newNode);
      break;
    case LruType::WarmTail:
      markTail(newNode);
      break; // warm is indicated by not marking hot or cold
    case LruType::Warm:
      break;
    case LruType::NumTypes:
      XDCHECK(false);
    }

    oldNode.unmarkInMMContainer();
    newNode.markInMMContainer();
    setUpdateTime(newNode, updateTime);
    return true;
  });
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
MM2Q::LruType MM2Q::Container<T, HookPtr>::getTailLru(LruType list) const {
  switch (list) {
  case LruType::Warm:
    return LruType::WarmTail;
  case LruType::Cold:
    return LruType::ColdTail;
  default:
    XDCHECK(false);
    throw std::invalid_argument("The LRU list does not have a tail list");
  }
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
void MM2Q::Container<T, HookPtr>::adjustTail(LruType list) {
  auto tailList = getTailLru(list);
  auto ptr = lru_.getList(list).getTail();
  while (ptr && lru_.getList(tailList).size() + 1 <= config_.tailSize) {
    markTail(*ptr);
    lru_.getList(list).remove(*ptr);
    lru_.getList(tailList).linkAtHead(*ptr);
    ptr = lru_.getList(list).getTail();
  }
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
serialization::MM2QObject MM2Q::Container<T, HookPtr>::saveState()
    const noexcept {
  serialization::MM2QConfig configObject;
  *configObject.lruRefreshTime() = lruRefreshTime_;
  *configObject.lruRefreshRatio() = config_.lruRefreshRatio;
  *configObject.updateOnWrite() = config_.updateOnWrite;
  *configObject.updateOnRead() = config_.updateOnRead;
  *configObject.hotSizePercent() = config_.hotSizePercent;
  *configObject.coldSizePercent() = config_.coldSizePercent;
  *configObject.rebalanceOnRecordAccess() = config_.rebalanceOnRecordAccess;

  serialization::MM2QObject object;
  *object.config() = configObject;
  *object.tailTrackingEnabled() = tailTrackingEnabled_;
  *object.lrus() = lru_.saveState();
  return object;
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
MMContainerStat MM2Q::Container<T, HookPtr>::getStats() const noexcept {
  return lruMutex_->lock_combine([this]() {
    auto* tail = lru_.size() == 0 ? nullptr : lru_.rbegin().get();
    auto computeWeightedAccesses = [&](size_t warm, size_t cold) {
      return (warm * config_.getWarmSizePercent() +
              cold * config_.coldSizePercent) /
             100;
    };

    // note that in the analagous code in MMLru, we return an instance of
    // std::array<std::uint64_t, 6> and construct the MMContainerStat instance
    // outside the lock with some other data that is not required to be read
    // under a lock.  This is done there to take advantage of
    // folly::DistributedMutex's return-value inlining feature which coalesces
    // the write from the critical section with the atomic operations required
    // as part of the internal synchronization mechanism.  That then transfers
    // the synchronization signal as well as the return value in one single
    // cacheline invalidation message.  At the time of writing this decision was
    // based on an implementation-detail aware to the author - 48 bytes can be
    // coalesced with the synchronization signal in folly::DistributedMutex.
    //
    // we cannot do that here because this critical section returns more data
    // than can be coalesced internally by folly::DistributedMutex (> 48 bytes).
    // So we construct and return the entire object under the lock.
    return MMContainerStat{
        lru_.size(),
        tail == nullptr ? 0 : getUpdateTime(*tail),
        lruRefreshTime_.load(std::memory_order_relaxed),
        numHotAccesses_,
        numColdAccesses_,
        numWarmAccesses_,
        computeWeightedAccesses(numWarmTailAccesses_, numColdTailAccesses_)};
  });
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
void MM2Q::Container<T, HookPtr>::reconfigureLocked(const Time& currTime) {
  if (currTime < nextReconfigureTime_) {
    return;
  }
  nextReconfigureTime_ = currTime + config_.mmReconfigureIntervalSecs.count();

  // update LRU refresh time
  auto stat = getEvictionAgeStatLocked(0);
  auto lruRefreshTime = std::min(
      std::max(config_.defaultLruRefreshTime,
               static_cast<uint32_t>(stat.warmQueueStat.oldestElementAge *
                                     config_.lruRefreshRatio)),
      kLruRefreshTimeCap);
  lruRefreshTime_.store(lruRefreshTime, std::memory_order_relaxed);
}

// Iterator Context Implementation
template <typename T, MM2Q::Hook<T> T::*HookPtr>
MM2Q::Container<T, HookPtr>::LockedIterator::LockedIterator(
    LockHolder l, const Iterator& iter) noexcept
    : Iterator(iter), l_(std::move(l)) {}
} // namespace facebook::cachelib
