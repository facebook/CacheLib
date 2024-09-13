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
#include <cstring>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#include <folly/Format.h>
#pragma GCC diagnostic pop
#include <folly/container/Array.h>
#include <folly/lang/Aligned.h>
#include <folly/synchronization/DistributedMutex.h>

#include "cachelib/allocator/Cache.h"
#include "cachelib/allocator/CacheStats.h"
#include "cachelib/allocator/Util.h"
#include "cachelib/allocator/datastruct/DList.h"
#include "cachelib/allocator/memory/serialize/gen-cpp2/objects_types.h"
#include "cachelib/common/CompilerUtils.h"
#include "cachelib/common/Mutex.h"

namespace facebook::cachelib {
// CacheLib's modified LRU policy.
// In classic LRU, the items form a queue according to the last access time.
// Items are inserted to the head of the queue and removed from the tail of the
// queue. Items accessed (used) are moved (promoted) to the head of the queue.
// CacheLib made two variations on top of the classic LRU:
// 1. Insertion point. The items are inserted into a configured insertion point
// instead of always to the head.
// 2. Delayed promotion. Items get promoted at most once in any lru refresh time
// window. lru refresh time and lru refresh ratio controls this internval
// length.
class MMLru {
 public:
  // unique identifier per MMType
  static const int kId;

  // forward declaration;
  template <typename T>
  using Hook = DListHook<T>;
  using SerializationType = serialization::MMLruObject;
  using SerializationConfigType = serialization::MMLruConfig;
  using SerializationTypeContainer = serialization::MMLruCollection;

  // This is not applicable for MMLru, just for compile of cache allocator
  enum LruType { NumTypes };

  // Config class for MMLru
  struct Config {
    // create from serialized config
    explicit Config(SerializationConfigType configState)
        : Config(*configState.lruRefreshTime(),
                 *configState.lruRefreshRatio(),
                 *configState.updateOnWrite(),
                 *configState.updateOnRead(),
                 *configState.tryLockUpdate(),
                 static_cast<uint8_t>(*configState.lruInsertionPointSpec())) {}

    // @param time        the LRU refresh time in seconds.
    //                    An item will be promoted only once in each lru refresh
    //                    time depite the number of accesses it gets.
    // @param udpateOnW   whether to promote the item on write
    // @param updateOnR   whether to promote the item on read
    Config(uint32_t time, bool updateOnW, bool updateOnR)
        : Config(time, updateOnW, updateOnR, false, 0) {}

    // @param time        the LRU refresh time in seconds.
    //                    An item will be promoted only once in each lru refresh
    //                    time depite the number of accesses it gets.
    // @param udpateOnW   whether to promote the item on write
    // @param updateOnR   whether to promote the item on read
    // @param tryLockU    whether to use a try lock when doing update.
    // @param ipSpec      insertion point spec, which is the inverse power of
    //                    length from the end of the queue. For example, value 1
    //                    means the insertion point is 1/2 from the end of LRU;
    //                    value 2 means 1/4 from the end of LRU.
    Config(uint32_t time, bool updateOnW, bool updateOnR, uint8_t ipSpec)
        : Config(time, updateOnW, updateOnR, false, ipSpec) {}

    Config(uint32_t time,
           bool updateOnW,
           bool updateOnR,
           bool tryLockU,
           uint8_t ipSpec)
        : Config(time, 0., updateOnW, updateOnR, tryLockU, ipSpec) {}

    // @param time        the LRU refresh time in seconds.
    //                    An item will be promoted only once in each lru refresh
    //                    time depite the number of accesses it gets.
    // @param ratio       the lru refresh ratio. The ratio times the
    //                    oldest element's lifetime in warm queue
    //                    would be the minimum value of LRU refresh time.
    // @param udpateOnW   whether to promote the item on write
    // @param updateOnR   whether to promote the item on read
    // @param tryLockU    whether to use a try lock when doing update.
    // @param ipSpec      insertion point spec, which is the inverse power of
    //                    length from the end of the queue. For example, value 1
    //                    means the insertion point is 1/2 from the end of LRU;
    //                    value 2 means 1/4 from the end of LRU.
    Config(uint32_t time,
           double ratio,
           bool updateOnW,
           bool updateOnR,
           bool tryLockU,
           uint8_t ipSpec)
        : Config(time, ratio, updateOnW, updateOnR, tryLockU, ipSpec, 0) {}

    // @param time        the LRU refresh time in seconds.
    //                    An item will be promoted only once in each lru refresh
    //                    time depite the number of accesses it gets.
    // @param ratio       the lru refresh ratio. The ratio times the
    //                    oldest element's lifetime in warm queue
    //                    would be the minimum value of LRU refresh time.
    // @param udpateOnW   whether to promote the item on write
    // @param updateOnR   whether to promote the item on read
    // @param tryLockU    whether to use a try lock when doing update.
    // @param ipSpec      insertion point spec, which is the inverse power of
    //                    length from the end of the queue. For example, value 1
    //                    means the insertion point is 1/2 from the end of LRU;
    //                    value 2 means 1/4 from the end of LRU.
    // @param mmReconfigureInterval   Time interval for recalculating lru
    //                                refresh time according to the ratio.
    Config(uint32_t time,
           double ratio,
           bool updateOnW,
           bool updateOnR,
           bool tryLockU,
           uint8_t ipSpec,
           uint32_t mmReconfigureInterval)
        : Config(time,
                 ratio,
                 updateOnW,
                 updateOnR,
                 tryLockU,
                 ipSpec,
                 mmReconfigureInterval,
                 false) {}

    // @param time        the LRU refresh time in seconds.
    //                    An item will be promoted only once in each lru refresh
    //                    time depite the number of accesses it gets.
    // @param ratio       the lru refresh ratio. The ratio times the
    //                    oldest element's lifetime in warm queue
    //                    would be the minimum value of LRU refresh time.
    // @param udpateOnW   whether to promote the item on write
    // @param updateOnR   whether to promote the item on read
    // @param tryLockU    whether to use a try lock when doing update.
    // @param ipSpec      insertion point spec, which is the inverse power of
    //                    length from the end of the queue. For example, value 1
    //                    means the insertion point is 1/2 from the end of LRU;
    //                    value 2 means 1/4 from the end of LRU.
    // @param mmReconfigureInterval   Time interval for recalculating lru
    //                                refresh time according to the ratio.
    // useCombinedLockForIterators    Whether to use combined locking for
    //                                withEvictionIterator
    Config(uint32_t time,
           double ratio,
           bool updateOnW,
           bool updateOnR,
           bool tryLockU,
           uint8_t ipSpec,
           uint32_t mmReconfigureInterval,
           bool useCombinedLockForIterators)
        : defaultLruRefreshTime(time),
          lruRefreshRatio(ratio),
          updateOnWrite(updateOnW),
          updateOnRead(updateOnR),
          tryLockUpdate(tryLockU),
          lruInsertionPointSpec(ipSpec),
          mmReconfigureIntervalSecs(
              std::chrono::seconds(mmReconfigureInterval)),
          useCombinedLockForIterators(useCombinedLockForIterators) {}

    Config() = default;
    Config(const Config& rhs) = default;
    Config(Config&& rhs) = default;

    Config& operator=(const Config& rhs) = default;
    Config& operator=(Config&& rhs) = default;

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

    // By default insertions happen at the head of the LRU. If we need
    // insertions at the middle of lru we can adjust this to be a non-zero.
    // Ex: lruInsertionPointSpec = 1, we insert at the middle (1/2 from end)
    //     lruInsertionPointSpec = 2, we insert at a point 1/4th from tail
    uint8_t lruInsertionPointSpec{0};

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
    using LruList = DList<T, HookPtr>;
    using Mutex = folly::DistributedMutex;
    using LockHolder = std::unique_lock<Mutex>;
    using PtrCompressor = typename T::PtrCompressor;
    using Time = typename Hook<T>::Time;
    using CompressedPtrType = typename T::CompressedPtrType;
    using RefFlags = typename T::Flags;

   public:
    Container() = default;
    Container(Config c, PtrCompressor compressor)
        : compressor_(std::move(compressor)),
          lru_(compressor_),
          config_(std::move(c)) {
      lruRefreshTime_ = config_.lruRefreshTime;
      nextReconfigureTime_ =
          config_.mmReconfigureIntervalSecs.count() == 0
              ? std::numeric_limits<Time>::max()
              : static_cast<Time>(util::getCurrentTimeSec()) +
                    config_.mmReconfigureIntervalSecs.count();
    }
    Container(serialization::MMLruObject object, PtrCompressor compressor);

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
      // private because it's easy to misuse and cause deadlock for MMLru
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
    // iterator passed as parameter.
    template <typename F>
    void withEvictionIterator(F&& f);

    // Execute provided function under container lock.
    template <typename F>
    void withContainerLock(F&& f);

    // get copy of current config
    Config getConfig() const;

    // override the existing config with the new one.
    void setConfig(const Config& newConfig);

    bool isEmpty() const noexcept { return size() == 0; }

    // reconfigure the MMContainer: update refresh time according to current
    // tail age
    void reconfigureLocked(const Time& currTime);

    // returns the number of elements in the container
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
    serialization::MMLruObject saveState() const noexcept;

    // return the stats for this container.
    MMContainerStat getStats() const noexcept;

    static LruType getLruType(const T& /* node */) noexcept {
      return LruType{};
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

    // This function is invoked by remove or record access prior to
    // removing the node (or moving the node to head) to ensure that
    // the node being moved is not the insertion point and if it is
    // adjust it accordingly.
    void ensureNotInsertionPoint(T& node) noexcept;

    // update the lru insertion point after doing an insert or removal.
    // We need to ensure the insertionPoint_ is set to the correct node
    // to maintain the tailSize_, for the next insertion.
    void updateLruInsertionPoint() noexcept;

    // remove node from lru and adjust insertion points
    // @param node          node to remove
    void removeLocked(T& node);

    // Bit MM_BIT_0 is used to record if the item is in tail. This
    // is used to implement LRU insertion points
    void markTail(T& node) noexcept {
      node.template setFlag<RefFlags::kMMFlag0>();
    }

    void unmarkTail(T& node) noexcept {
      node.template unSetFlag<RefFlags::kMMFlag0>();
    }

    bool isTail(T& node) const noexcept {
      return node.template isFlagSet<RefFlags::kMMFlag0>();
    }

    // Bit MM_BIT_1 is used to record if the item has been accessed since
    // being written in cache. Unaccessed items are ignored when determining
    // projected update time.
    void markAccessed(T& node) noexcept {
      node.template setFlag<RefFlags::kMMFlag1>();
    }

    void unmarkAccessed(T& node) noexcept {
      node.template unSetFlag<RefFlags::kMMFlag1>();
    }

    bool isAccessed(const T& node) const noexcept {
      return node.template isFlagSet<RefFlags::kMMFlag1>();
    }

    // protects all operations on the lru. We never really just read the state
    // of the LRU. Hence we dont really require a RW mutex at this point of
    // time.
    mutable folly::cacheline_aligned<Mutex> lruMutex_;

    const PtrCompressor compressor_{};

    // the lru
    LruList lru_{};

    // insertion point
    T* insertionPoint_{nullptr};

    // size of tail after insertion point
    size_t tailSize_{0};

    // The next time to reconfigure the container.
    std::atomic<Time> nextReconfigureTime_{};

    // How often to promote an item in the eviction queue.
    std::atomic<uint32_t> lruRefreshTime_{};

    // Config for this lru.
    // Write access to the MMLru Config is serialized.
    // Reads may be racy.
    Config config_{};

    // Max lruFreshTime.
    static constexpr uint32_t kLruRefreshTimeCap{900};

    FRIEND_TEST(MMLruTest, Reconfigure);
  };
};

namespace detail {
template <typename T>
bool areBytesSame(const T& one, const T& two) {
  return std::memcmp(&one, &two, sizeof(T)) == 0;
}
} // namespace detail

/* Container Interface Implementation */
template <typename T, MMLru::Hook<T> T::*HookPtr>
MMLru::Container<T, HookPtr>::Container(serialization::MMLruObject object,
                                        PtrCompressor compressor)
    : compressor_(std::move(compressor)),
      lru_(*object.lru(), compressor_),
      insertionPoint_(compressor_.unCompress(
          CompressedPtrType{*object.compressedInsertionPoint()})),
      tailSize_(*object.tailSize()),
      config_(*object.config()) {
  lruRefreshTime_ = config_.lruRefreshTime;
  nextReconfigureTime_ = config_.mmReconfigureIntervalSecs.count() == 0
                             ? std::numeric_limits<Time>::max()
                             : static_cast<Time>(util::getCurrentTimeSec()) +
                                   config_.mmReconfigureIntervalSecs.count();
}

template <typename T, MMLru::Hook<T> T::*HookPtr>
bool MMLru::Container<T, HookPtr>::recordAccess(T& node,
                                                AccessMode mode) noexcept {
  if ((mode == AccessMode::kWrite && !config_.updateOnWrite) ||
      (mode == AccessMode::kRead && !config_.updateOnRead)) {
    return false;
  }

  const auto curr = static_cast<Time>(util::getCurrentTimeSec());
  // check if the node is still being memory managed
  if (node.isInMMContainer() &&
      ((curr >= getUpdateTime(node) +
                    lruRefreshTime_.load(std::memory_order_relaxed)) ||
       !isAccessed(node))) {
    if (!isAccessed(node)) {
      markAccessed(node);
    }

    auto func = [this, &node, curr]() {
      reconfigureLocked(curr);
      ensureNotInsertionPoint(node);
      if (node.isInMMContainer()) {
        lru_.moveToHead(node);
        setUpdateTime(node, curr);
      }
      if (isTail(node)) {
        unmarkTail(node);
        tailSize_--;
        XDCHECK_LE(0u, tailSize_);
        updateLruInsertionPoint();
      }
    };

    // if the tryLockUpdate optimization is on, and we were able to grab the
    // lock, execute the critical section and return true, else return false
    //
    // if the tryLockUpdate optimization is off, we always execute the
    // critical section and return true
    if (config_.tryLockUpdate) {
      if (auto lck = LockHolder{*lruMutex_, std::try_to_lock}) {
        func();
        return true;
      }

      return false;
    }

    lruMutex_->lock_combine(func);
    return true;
  }
  return false;
}

template <typename T, MMLru::Hook<T> T::*HookPtr>
cachelib::EvictionAgeStat MMLru::Container<T, HookPtr>::getEvictionAgeStat(
    uint64_t projectedLength) const noexcept {
  return lruMutex_->lock_combine([this, projectedLength]() {
    return getEvictionAgeStatLocked(projectedLength);
  });
}

template <typename T, MMLru::Hook<T> T::*HookPtr>
cachelib::EvictionAgeStat
MMLru::Container<T, HookPtr>::getEvictionAgeStatLocked(
    uint64_t projectedLength) const noexcept {
  EvictionAgeStat stat{};
  const auto currTime = static_cast<Time>(util::getCurrentTimeSec());

  const T* node = lru_.getTail();
  stat.warmQueueStat.oldestElementAge =
      node ? currTime - getUpdateTime(*node) : 0;
  for (size_t numSeen = 0; numSeen < projectedLength && node != nullptr;
       numSeen++, node = lru_.getPrev(*node)) {
  }
  stat.warmQueueStat.projectedAge = node ? currTime - getUpdateTime(*node)
                                         : stat.warmQueueStat.oldestElementAge;
  XDCHECK(detail::areBytesSame(stat.hotQueueStat, EvictionStatPerType{}));
  XDCHECK(detail::areBytesSame(stat.coldQueueStat, EvictionStatPerType{}));
  return stat;
}

template <typename T, MMLru::Hook<T> T::*HookPtr>
void MMLru::Container<T, HookPtr>::setConfig(const Config& newConfig) {
  lruMutex_->lock_combine([this, newConfig]() {
    config_ = newConfig;
    if (config_.lruInsertionPointSpec == 0 && insertionPoint_ != nullptr) {
      auto curr = insertionPoint_;
      while (tailSize_ != 0) {
        XDCHECK(curr != nullptr);
        unmarkTail(*curr);
        tailSize_--;
        curr = lru_.getNext(*curr);
      }
      insertionPoint_ = nullptr;
    }
    lruRefreshTime_.store(config_.lruRefreshTime, std::memory_order_relaxed);
    nextReconfigureTime_ = config_.mmReconfigureIntervalSecs.count() == 0
                               ? std::numeric_limits<Time>::max()
                               : static_cast<Time>(util::getCurrentTimeSec()) +
                                     config_.mmReconfigureIntervalSecs.count();
  });
}

template <typename T, MMLru::Hook<T> T::*HookPtr>
typename MMLru::Config MMLru::Container<T, HookPtr>::getConfig() const {
  return lruMutex_->lock_combine([this]() { return config_; });
}

template <typename T, MMLru::Hook<T> T::*HookPtr>
void MMLru::Container<T, HookPtr>::updateLruInsertionPoint() noexcept {
  if (config_.lruInsertionPointSpec == 0) {
    return;
  }

  // If insertionPoint_ is nullptr initialize it to tail first
  if (insertionPoint_ == nullptr) {
    insertionPoint_ = lru_.getTail();
    tailSize_ = 0;
    if (insertionPoint_ != nullptr) {
      markTail(*insertionPoint_);
      tailSize_++;
    }
  }

  if (lru_.size() <= 1) {
    // we are done;
    return;
  }

  XDCHECK_NE(reinterpret_cast<uintptr_t>(nullptr),
             reinterpret_cast<uintptr_t>(insertionPoint_));

  const auto expectedSize = lru_.size() >> config_.lruInsertionPointSpec;
  auto curr = insertionPoint_;

  while (tailSize_ < expectedSize && curr != lru_.getHead()) {
    curr = lru_.getPrev(*curr);
    markTail(*curr);
    tailSize_++;
  }

  while (tailSize_ > expectedSize && curr != lru_.getTail()) {
    unmarkTail(*curr);
    tailSize_--;
    curr = lru_.getNext(*curr);
  }

  insertionPoint_ = curr;
}

template <typename T, MMLru::Hook<T> T::*HookPtr>
bool MMLru::Container<T, HookPtr>::add(T& node) noexcept {
  const auto currTime = static_cast<Time>(util::getCurrentTimeSec());

  return lruMutex_->lock_combine([this, &node, currTime]() {
    if (node.isInMMContainer()) {
      return false;
    }
    if (config_.lruInsertionPointSpec == 0 || insertionPoint_ == nullptr) {
      lru_.linkAtHead(node);
    } else {
      lru_.insertBefore(*insertionPoint_, node);
    }
    node.markInMMContainer();
    setUpdateTime(node, currTime);
    unmarkAccessed(node);
    updateLruInsertionPoint();
    return true;
  });
}

template <typename T, MMLru::Hook<T> T::*HookPtr>
typename MMLru::Container<T, HookPtr>::LockedIterator
MMLru::Container<T, HookPtr>::getEvictionIterator() const noexcept {
  LockHolder l(*lruMutex_);
  return LockedIterator{std::move(l), lru_.rbegin()};
}

template <typename T, MMLru::Hook<T> T::*HookPtr>
template <typename F>
void MMLru::Container<T, HookPtr>::withEvictionIterator(F&& fun) {
  if (config_.useCombinedLockForIterators) {
    lruMutex_->lock_combine([this, &fun]() { fun(Iterator{lru_.rbegin()}); });
  } else {
    LockHolder lck{*lruMutex_};
    fun(Iterator{lru_.rbegin()});
  }
}

template <typename T, MMLru::Hook<T> T::*HookPtr>
template <typename F>
void MMLru::Container<T, HookPtr>::withContainerLock(F&& fun) {
  lruMutex_->lock_combine([&fun]() { fun(); });
}

template <typename T, MMLru::Hook<T> T::*HookPtr>
void MMLru::Container<T, HookPtr>::ensureNotInsertionPoint(T& node) noexcept {
  // If we are removing the insertion point node, grow tail before we remove
  // so that insertionPoint_ is valid (or nullptr) after removal
  if (&node == insertionPoint_) {
    insertionPoint_ = lru_.getPrev(*insertionPoint_);
    if (insertionPoint_ != nullptr) {
      tailSize_++;
      markTail(*insertionPoint_);
    } else {
      XDCHECK_EQ(lru_.size(), 1u);
    }
  }
}

template <typename T, MMLru::Hook<T> T::*HookPtr>
void MMLru::Container<T, HookPtr>::removeLocked(T& node) {
  ensureNotInsertionPoint(node);
  lru_.remove(node);
  unmarkAccessed(node);
  if (isTail(node)) {
    unmarkTail(node);
    tailSize_--;
  }
  node.unmarkInMMContainer();
  updateLruInsertionPoint();
  return;
}

template <typename T, MMLru::Hook<T> T::*HookPtr>
bool MMLru::Container<T, HookPtr>::remove(T& node) noexcept {
  return lruMutex_->lock_combine([this, &node]() {
    if (!node.isInMMContainer()) {
      return false;
    }
    removeLocked(node);
    return true;
  });
}

template <typename T, MMLru::Hook<T> T::*HookPtr>
void MMLru::Container<T, HookPtr>::remove(Iterator& it) noexcept {
  T& node = *it;
  XDCHECK(node.isInMMContainer());
  ++it;
  removeLocked(node);
}

template <typename T, MMLru::Hook<T> T::*HookPtr>
bool MMLru::Container<T, HookPtr>::replace(T& oldNode, T& newNode) noexcept {
  return lruMutex_->lock_combine([this, &oldNode, &newNode]() {
    if (!oldNode.isInMMContainer() || newNode.isInMMContainer()) {
      return false;
    }
    const auto updateTime = getUpdateTime(oldNode);
    lru_.replace(oldNode, newNode);
    oldNode.unmarkInMMContainer();
    newNode.markInMMContainer();
    setUpdateTime(newNode, updateTime);
    if (isAccessed(oldNode)) {
      markAccessed(newNode);
    } else {
      unmarkAccessed(newNode);
    }
    XDCHECK(!isTail(newNode));
    if (isTail(oldNode)) {
      markTail(newNode);
      unmarkTail(oldNode);
    } else {
      unmarkTail(newNode);
    }
    if (insertionPoint_ == &oldNode) {
      insertionPoint_ = &newNode;
    }
    return true;
  });
}

template <typename T, MMLru::Hook<T> T::*HookPtr>
serialization::MMLruObject MMLru::Container<T, HookPtr>::saveState()
    const noexcept {
  serialization::MMLruConfig configObject;
  *configObject.lruRefreshTime() =
      lruRefreshTime_.load(std::memory_order_relaxed);
  *configObject.lruRefreshRatio() = config_.lruRefreshRatio;
  *configObject.updateOnWrite() = config_.updateOnWrite;
  *configObject.updateOnRead() = config_.updateOnRead;
  *configObject.tryLockUpdate() = config_.tryLockUpdate;
  *configObject.lruInsertionPointSpec() = config_.lruInsertionPointSpec;

  serialization::MMLruObject object;
  *object.config() = configObject;
  *object.compressedInsertionPoint() =
      compressor_.compress(insertionPoint_).saveState();
  *object.tailSize() = tailSize_;
  *object.lru() = lru_.saveState();
  return object;
}

template <typename T, MMLru::Hook<T> T::*HookPtr>
MMContainerStat MMLru::Container<T, HookPtr>::getStats() const noexcept {
  auto stat = lruMutex_->lock_combine([this]() {
    auto* tail = lru_.getTail();

    // we return by array here because DistributedMutex is fastest when the
    // output data fits within 48 bytes.  And the array is exactly 48 bytes, so
    // it can get optimized by the implementation.
    //
    // the rest of the parameters are 0, so we don't need the critical section
    // to return them
    return folly::make_array(lru_.size(),
                             tail == nullptr ? 0 : getUpdateTime(*tail),
                             lruRefreshTime_.load(std::memory_order_relaxed));
  });
  return {stat[0] /* lru size */,
          stat[1] /* tail time */,
          stat[2] /* refresh time */,
          0,
          0,
          0,
          0};
}

template <typename T, MMLru::Hook<T> T::*HookPtr>
void MMLru::Container<T, HookPtr>::reconfigureLocked(const Time& currTime) {
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
template <typename T, MMLru::Hook<T> T::*HookPtr>
MMLru::Container<T, HookPtr>::LockedIterator::LockedIterator(
    LockHolder l, const Iterator& iter) noexcept
    : Iterator(iter), l_(std::move(l)) {}
} // namespace facebook::cachelib
