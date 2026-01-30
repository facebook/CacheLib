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
#include "cachelib/common/Mutex.h"

namespace facebook::cachelib {
// MM S3FIFO V1
// This version modifies v0 and introduces a promotion policy.
// Only items that are unaccessed will be evicted
// Iterator will skip accessed items during eviction search
class MMS3FIFO {
 public:
  // unique identifier per MMType
  static const int kId;

  // forward declaration;
  template <typename T>
  using Hook = DListHook<T>;
  using SerializationType = serialization::MMS3FIFOObject;
  using SerializationConfigType = serialization::MMS3FIFOConfig;
  using SerializationTypeContainer = serialization::MMS3FIFOCollection;

  enum LruType { Main, Tiny, NumTypes };

  // Config class for MMS3FIFO
  struct Config {
    // create from serialized config
    explicit Config(SerializationConfigType configState)
        : Config(*configState.updateOnWrite(),
                 *configState.updateOnRead(),
                 *configState.tinySizePercent(),
                 *configState.ghostSizePercent()) {}

    // @param udpateOnW   whether to promote the item on write
    // @param updateOnR   whether to promote the item on read
    Config(bool updateOnW, bool updateOnR)
        : Config(updateOnW, updateOnR, 10, 90) {}

    // @param udpateOnW         whether to promote the item on write
    // @param updateOnR         whether to promote the item on read
    // @param tinySizePercent   percentage number of tiny size to overall size
    // @param ghostSizePercent  percentage number of ghost size to main size
    Config(bool updateOnW,
           bool updateOnR,
           size_t tinySizePercent,
           size_t ghostSizePercent)
        : updateOnWrite(updateOnW),
          updateOnRead(updateOnR),
          tinySizePercent(tinySizePercent),
          ghostSizePercent(ghostSizePercent) {}

    Config() = default;
    Config(const Config& rhs) = default;
    Config(Config&& rhs) = default;

    Config& operator=(const Config& rhs) = default;
    Config& operator=(Config&& rhs) = default;

    template <typename... Args>
    void addExtraConfig(Args...) {}

    // whether the lru needs to be updated on writes for recordAccess. If
    // false, accessing the cache for writes does not promote the cached item
    // to the head of the lru.
    bool updateOnWrite{false};

    // whether the lru needs to be updated on reads for recordAccess. If
    // false, accessing the cache for reads does not promote the cached item
    // to the head of the lru.
    bool updateOnRead{true};

    // The size of tiny cache, as a percentage of the total size.
    size_t tinySizePercent{10};

    size_t ghostSizePercent{90};
  };

  // The container object which can be used to keep track of objects of type
  // T. T must have a public member of type Hook. This object is wrapper
  // around DList, is thread safe and can be accessed from multiple threads.
  // The current implementation models an LRU using the above DList
  // implementation.
  template <typename T, Hook<T> T::* HookPtr>
  struct Container {
   private:
    using LruList = MultiDList<T, HookPtr>;
    using Mutex = folly::SpinLock;
    using LockHolder = std::unique_lock<Mutex>;
    using PtrCompressor = typename T::PtrCompressor;
    using Time = typename Hook<T>::Time;
    using CompressedPtrType = typename T::CompressedPtrType;
    using RefFlags = typename T::Flags;

   public:
    Container() = default;
    Container(Config c, PtrCompressor compressor)
        : lru_(LruType::NumTypes, std::move(compressor)),
          config_(std::move(c)) {}
    Container(serialization::MMS3FIFOObject object, PtrCompressor compressor);

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
        auto it = ++getIter();
        skipAccessed(it);
        return *this;
      }

      void skipAccessed(ListIterator& it) noexcept {
        while (it) {
          T& node = *it;
          if (!Container<T, HookPtr>::isAccessed(node)) {
            break; // found a valid eviction victim
          }
          ++it; // skip this accessed item
        }
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
      // private because it's easy to misuse and cause deadlock for MMS3FIFO
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

        const auto expectedSize =
            c_.config_.tinySizePercent * c_.lru_.size() / 100;
        auto isTinyBig = c_.lru_.getList(LruType::Tiny).size() > expectedSize;
        return isTinyBig;
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

    // Returns the eviction age stats. See CacheStats.h for details
    EvictionAgeStat getEvictionAgeStat(uint64_t projectedLength) const noexcept;

    // Obtain an iterator that start from the tail and can be used
    // to search for evictions. This iterator holds a lock to this
    // container and only one such iterator can exist at a time
    LockedIterator getEvictionIterator() noexcept;

    // Execute provided function under container lock. Function gets
    // iterator passed as parameter.
    template <typename F>
    void withEvictionIterator(F&& f);

    // Execute provided function under container lock.
    template <typename F>
    void withContainerLock(F&& f);

    // for saving the state of the lru
    //
    // precondition:  serialization must happen without any reader or writer
    // present. Any modification of this object afterwards will result in an
    // invalid, inconsistent state for the serialized data.
    //
    serialization::MMS3FIFOObject saveState() const noexcept;

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

    // Returns the hash of node's key
    static size_t hashNode(const T& node) noexcept {
      return folly::hasher<folly::StringPiece>()(node.getKey());
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

    void rebalanceForEviction();

    mutable Mutex lruMutex_;

    // the lru
    LruList lru_;

    // Current capacity to track ghost size
    size_t capacity_{0};

    // Config for this lru.
    // Write access to the MMS3FIFO Config is serialized.
    // Reads may be racy.
    Config config_{};

    // FRIEND_TEST(MMTinyLFUTest, SegmentStress);
    // FRIEND_TEST(MMTinyLFUTest, TinyLFUBasic);
    // FRIEND_TEST(MMTinyLFUTest, Reconfigure);
  };
};

/* Container Interface Implementation */
template <typename T, MMS3FIFO::Hook<T> T::* HookPtr>
MMS3FIFO::Container<T, HookPtr>::Container(serialization::MMS3FIFOObject object,
                                           PtrCompressor compressor)
    : lru_(*object.lrus(), std::move(compressor)), config_(*object.config()) {}

template <typename T, MMS3FIFO::Hook<T> T::* HookPtr>
bool MMS3FIFO::Container<T, HookPtr>::recordAccess(T& node,
                                                   AccessMode mode) noexcept {
  if ((mode == AccessMode::kWrite && !config_.updateOnWrite) ||
      (mode == AccessMode::kRead && !config_.updateOnRead)) {
    return false;
  }

  const auto curr = static_cast<Time>(util::getCurrentTimeSec());
  // check if the node is still being memory managed
  if (node.isInMMContainer() && !isAccessed(node)) {
    markAccessed(node);
    setUpdateTime(node, curr);
    return true;
  }
  return false;
}

template <typename T, MMS3FIFO::Hook<T> T::* HookPtr>
cachelib::EvictionAgeStat MMS3FIFO::Container<T, HookPtr>::getEvictionAgeStat(
    uint64_t projectedLength) const noexcept {
  LockHolder l(lruMutex_);
  return getEvictionAgeStatLocked(projectedLength);
}

template <typename T, MMS3FIFO::Hook<T> T::* HookPtr>
cachelib::EvictionAgeStat
MMS3FIFO::Container<T, HookPtr>::getEvictionAgeStatLocked(
    uint64_t projectedLength) const noexcept {
  EvictionAgeStat stat;
  const auto curr = static_cast<Time>(util::getCurrentTimeSec());

  auto& list = lru_.getList(LruType::Main);
  auto it = list.rbegin();
  stat.warmQueueStat.oldestElementAge =
      it != list.rend() ? curr - getUpdateTime(*it) : 0;
  stat.warmQueueStat.size = list.size();
  for (size_t numSeen = 0; numSeen < projectedLength && it != list.rend();
       ++numSeen, ++it) {
  }
  stat.warmQueueStat.projectedAge = it != list.rend()
                                        ? curr - getUpdateTime(*it)
                                        : stat.warmQueueStat.oldestElementAge;
  return stat;
}

template <typename T, MMS3FIFO::Hook<T> T::* HookPtr>
bool MMS3FIFO::Container<T, HookPtr>::add(T& node) noexcept {
  const auto currTime = static_cast<Time>(util::getCurrentTimeSec());
  LockHolder l(lruMutex_);
  if (node.isInMMContainer()) {
    return false;
  }

  auto& tinyLru = lru_.getList(LruType::Tiny);
  tinyLru.linkAtHead(node);

  markTiny(node);
  unmarkAccessed(node);
  setUpdateTime(node, currTime);

  node.markInMMContainer();
  return true;
}

template <typename T, MMS3FIFO::Hook<T> T::* HookPtr>
void MMS3FIFO::Container<T, HookPtr>::rebalanceForEviction() {
    // Bulk move accessed items from tiny to main cache
    // Until tinycache's tail is unaccessed or tiny cache is within size limit
    // Room for optimization:
    // We can bulk move segments to main instead of one by one
    auto& tinyLru = lru_.getList(LruType::Tiny);
    auto& mainLru = lru_.getList(LruType::Main);

    bool isTinyTailUnaccessed = false;
    const auto expectedSize = config_.tinySizePercent * lru_.size() / 100;
    while (!isTinyTailUnaccessed) {
        if (tinyLru.size() <= expectedSize) {
            break;
        }
        auto tailNode = tinyLru.getTail();
        if (tailNode == nullptr || !isAccessed(*tailNode)) {
            isTinyTailUnaccessed = true;
            break;
        }
        tinyLru.remove(*tailNode);

        mainLru.linkAtHead(*tailNode);
        unmarkTiny(*tailNode);
        unmarkAccessed(*tailNode);
    }

    // Reinsert items in main cache to head, until main cache's tail is unaccessed
    // TODO: Room for optimization: 
    // We can get the position of first unaccessed item from the tail
    // And move the whole segment to head in one operation
    bool isMainTailUnaccessed = false;
    while (!isMainTailUnaccessed) {
        auto tailNode = mainLru.getTail();
        if (tailNode == nullptr || !isAccessed(*tailNode)) {
            isMainTailUnaccessed = true;
            break;
        }
        mainLru.remove(*tailNode);
        mainLru.linkAtHead(*tailNode);
        unmarkAccessed(*tailNode);
    }
}

template <typename T, MMS3FIFO::Hook<T> T::* HookPtr>
typename MMS3FIFO::Container<T, HookPtr>::LockedIterator
MMS3FIFO::Container<T, HookPtr>::getEvictionIterator() noexcept {
  LockHolder l(lruMutex_);
  rebalanceForEviction();
  return LockedIterator{std::move(l), *this};
}

template <typename T, MMS3FIFO::Hook<T> T::* HookPtr>
template <typename F>
void MMS3FIFO::Container<T, HookPtr>::withEvictionIterator(F&& fun) {
  // uses spin lock which does not support combined locking
  fun(getEvictionIterator());
}

template <typename T, MMS3FIFO::Hook<T> T::* HookPtr>
template <typename F>
void MMS3FIFO::Container<T, HookPtr>::withContainerLock(F&& fun) {
  LockHolder l(lruMutex_);
  fun();
}

template <typename T, MMS3FIFO::Hook<T> T::* HookPtr>
void MMS3FIFO::Container<T, HookPtr>::removeLocked(T& node) noexcept {
  if (isTiny(node)) {
    lru_.getList(LruType::Tiny).remove(node);
    unmarkTiny(node);
  } else {
    lru_.getList(LruType::Main).remove(node);
  }

  unmarkAccessed(node);
  node.unmarkInMMContainer();
  return;
}

template <typename T, MMS3FIFO::Hook<T> T::* HookPtr>
bool MMS3FIFO::Container<T, HookPtr>::remove(T& node) noexcept {
  LockHolder l(lruMutex_);
  if (!node.isInMMContainer()) {
    return false;
  }
  removeLocked(node);
  return true;
}

template <typename T, MMS3FIFO::Hook<T> T::* HookPtr>
void MMS3FIFO::Container<T, HookPtr>::remove(LockedIterator& it) noexcept {
  T& node = *it;
  XDCHECK(node.isInMMContainer());
  ++it;
  removeLocked(node);
}

template <typename T, MMS3FIFO::Hook<T> T::* HookPtr>
bool MMS3FIFO::Container<T, HookPtr>::replace(T& oldNode, T& newNode) noexcept {
  LockHolder l(lruMutex_);
  if (!oldNode.isInMMContainer() || newNode.isInMMContainer()) {
    return false;
  }
  const auto updateTime = getUpdateTime(oldNode);

  if (isTiny(oldNode)) {
    lru_.getList(LruType::Tiny).replace(oldNode, newNode);
    unmarkTiny(oldNode);
    markTiny(newNode);
  } else {
    lru_.getList(LruType::Main).replace(oldNode, newNode);
  }

  oldNode.unmarkInMMContainer();
  newNode.markInMMContainer();
  setUpdateTime(newNode, updateTime);
  if (isAccessed(oldNode)) {
    markAccessed(newNode);
  } else {
    unmarkAccessed(newNode);
  }
  return true;
}

template <typename T, MMS3FIFO::Hook<T> T::* HookPtr>
typename MMS3FIFO::Config MMS3FIFO::Container<T, HookPtr>::getConfig() const {
  LockHolder l(lruMutex_);
  return config_;
}

template <typename T, MMS3FIFO::Hook<T> T::* HookPtr>
void MMS3FIFO::Container<T, HookPtr>::setConfig(const Config& c) {
  LockHolder l(lruMutex_);
  config_ = c;
}

template <typename T, MMS3FIFO::Hook<T> T::* HookPtr>
serialization::MMS3FIFOObject MMS3FIFO::Container<T, HookPtr>::saveState()
    const noexcept {
  serialization::MMS3FIFOConfig configObject;
  *configObject.updateOnWrite() = config_.updateOnWrite;
  *configObject.updateOnRead() = config_.updateOnRead;
  *configObject.ghostSizePercent() = config_.ghostSizePercent;
  *configObject.tinySizePercent() = config_.tinySizePercent;
  // TODO: Serialize the ghost queue too.
  serialization::MMS3FIFOObject object;
  *object.config() = configObject;
  *object.lrus() = lru_.saveState();
  return object;
}

template <typename T, MMS3FIFO::Hook<T> T::* HookPtr>
MMContainerStat MMS3FIFO::Container<T, HookPtr>::getStats() const noexcept {
  LockHolder l(lruMutex_);
  auto* tail = lru_.size() == 0 ? nullptr : lru_.rbegin().get();
  return {
      lru_.size(), tail == nullptr ? 0 : getUpdateTime(*tail), 0, 0, 0, 0, 0};
}

// Locked Iterator Context Implementation
template <typename T, MMS3FIFO::Hook<T> T::* HookPtr>
MMS3FIFO::Container<T, HookPtr>::LockedIterator::LockedIterator(
    LockHolder l, const Container<T, HookPtr>& c) noexcept
    : c_(c),
      tIter_(c.lru_.getList(LruType::Tiny).rbegin()),
      mIter_(c.lru_.getList(LruType::Main).rbegin()),
      l_(std::move(l)) {}
} // namespace facebook::cachelib
