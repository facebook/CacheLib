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
#include <folly/synchronization/DistributedMutex.h>

#pragma GCC diagnostic pop

#include "cachelib/allocator/Cache.h"
#include "cachelib/allocator/CacheStats.h"
#include "cachelib/allocator/Util.h"
#include "cachelib/allocator/datastruct/MultiDList.h"
#include "cachelib/allocator/memory/serialize/gen-cpp2/objects_types.h"
#include "cachelib/common/CompilerUtils.h"
#include "cachelib/common/FIFOHashSet.h"

namespace facebook::cachelib {

// Implements the S3-FIFO cache eviction policy as described in
// https://dl.acm.org/doi/pdf/10.1145/3600006.3613147
//
// S3-FIFO is combines a "Tiny" queue, a larger "Main" queue, and a ghost
// history. New items enter the Tiny queue; recently accessed items are promoted
// to the Main queue. Eviction always searches from the tails of these queues,
// skipping items that have been accessed since insertion (using a single access
// bit).
//
// A low overhead ghost queue (stores only 4 byte key hash/ item)tracks recently
// evicted keys from the small queue. Items found in ghost are directly into the
// Main queue.

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
          ghostSizePercent(ghostSizePercent) {
    }

    Config() = default;
    Config(const Config& rhs) = default;
    Config(Config&& rhs) = default;

    Config& operator=(const Config& rhs) = default;
    Config& operator=(Config&& rhs) = default;

    template <typename... Args>
    void addExtraConfig(Args...) {}

    // whether the cache needs to be updated on writes for recordAccess.
    bool updateOnWrite{false};

    // whether the cache needs to be updated on reads for recordAccess.
    bool updateOnRead{true};

    // The size of tiny cache, as a percentage of the total size.
    size_t tinySizePercent{10};

    // The size of ghost queue, as a percentage of total size
    size_t ghostSizePercent{90};

    size_t reserveCapacity{0};
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
    using Mutex = folly::DistributedMutex;
    using LockHolder = std::unique_lock<Mutex>;
    using PtrCompressor = typename T::PtrCompressor;
    using Time = typename Hook<T>::Time;
    using CompressedPtrType = typename T::CompressedPtrType;
    using RefFlags = typename T::Flags;

   public:
    Container() {};
    // Reserve ghost queue here if needed
    Container(Config c, PtrCompressor compressor)
        : lru_(LruType::NumTypes, std::move(compressor)),
          config_(std::move(c)) {
            if (config_.reserveCapacity > 0) {
              ghostQueue_.reserve(config_.reserveCapacity);
            }
          }
    Container(serialization::MMS3FIFOObject object, PtrCompressor compressor);

    Container(const Container&) = delete;
    Container& operator=(const Container&) = delete;

    // records the information that the node was accessed.
    // This doesn't move nodes and only sets the access bit.
    // It also updates the timestamp of last access.
    //
    // @param node  node that we want to mark as relevant/accessed
    // @param mode the mode for the access operation.
    // @return      True if the information is recorded, returns false otherwise
    bool recordAccess(T& node, AccessMode mode) noexcept;

    // adds the given node into the container and marks it as being present in
    // the container. The node is added to tiny queue, unless it is present
    // in ghost queue, in which case it is added to main queue.
    //
    // @param node  The node to be added to the container.
    // @return  True if the node was successfully added to the container. False
    //          if the node was already in the contianer. On error state of node
    //          is unchanged.
    bool add(T& node) noexcept;

    // removes the node from the container, adds it to ghost queue
    // if it was from tiny queue.
    //
    // @param node  The node to be removed from the container.
    // @return  True if the node was successfully removed from the container.
    //          False if the node was not part of the container. On error, the
    //          state of node is unchanged.
    bool remove(T& node) noexcept;

    class LockedIterator;

    // same as the above but uses an iterator context. The iterator is updated
    // on removal of the corresponding node to point to the next node. The
    // iterator context holds the lock.
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

    class LockedIterator {
     public:
      using ListIterator = typename LruList::DListIterator;
      // noncopyable but movable.
      LockedIterator(const LockedIterator&) = delete;
      LockedIterator& operator=(const LockedIterator&) = delete;
      LockedIterator(LockedIterator&&) noexcept = default;

      // Iterator in S3FIFO only returns unaccessed items, so ++ skips accessed
      // items.
      LockedIterator& operator++() noexcept {
        // Advance the underlying iterator
        ListIterator& it = getIter();

        if (!it) {
          return *this;
        }
        ++it;
        // Skip accessed items
        skipAccessed(it);
        return *this;
      }

      ListIterator& skipAccessed(ListIterator& it) noexcept {
        while (it) {
          T& node = *it;
          if (!Container<T, HookPtr>::isAccessed(node)) {
            break; // found a valid eviction victim
          }
          ++it; // skip this accessed item
        }
        return it;
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
        auto shouldEvictTiny = evictTiny();

        return shouldEvictTiny ? tIter_ : mIter_;
      }

      ListIterator& getIter() noexcept {
        return const_cast<ListIterator&>(
            static_cast<const LockedIterator*>(this)->getIter());
      }

      // Decides to return iterator from tiny or main cache based on capacity.
      // Will switch iterators when one of them is exhausted.
      bool evictTiny() const noexcept {
        if (!mIter_) {
          return true;
        }
        if (!tIter_) {
          return false;
        }
        // List size will not change during iteration, return cached decision
        if (evictTinyCache_ != -1) {
          return evictTinyCache_ == 1;
        }

        int smallSize = static_cast<int>(c_.lru_.getList(LruType::Tiny).size());

        const size_t targetTiny = static_cast<size_t>(
            c_.config_.tinySizePercent * c_.lru_.size() / 100);

        this->evictTinyCache_ = smallSize >= targetTiny;

        return evictTinyCache_;
      }

      // Cache the value
      mutable int evictTinyCache_{-1}; // -1 means not set, 0 false and 1 true

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

    // Returns the eviction age stats. See CacheStats.h for details
    // Is not really relevant for S3FIFO since we don't use age.
    // Todo: deprecate or remove?
    EvictionAgeStat getEvictionAgeStat(uint64_t projectedLength) const noexcept;

    // Obtain an iterator that start from the tail and can be used
    // to search for evictions. This iterator holds a lock to this
    // container and only one such iterator can exist at a time
    LockedIterator getEvictionIterator() noexcept;

    void rebalanceForEviction();

    // Execute provided function under container lock. Function gets
    // iterator passed as parameter.
    template <typename F>
    void withEvictionIterator(F&& f);

    // Execute provided function under container lock.
    template <typename F>
    void withContainerLock(F&& f);

    // for saving the state of the s3fifo
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

    // As the cache grows, the ghost queue may need to be resized
    void maybeGrowGhostLocked() noexcept;

    // Returns the hash of node's key
    static size_t hashNode(const T& node) noexcept {
      return folly::hasher<folly::StringPiece>()(node.getKey());
    }

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

    // protects all operations on the lrus. We never really just read the state
    // of the LRU. Hence we dont really require a RW mutex at this point of
    // time.
    mutable folly::cacheline_aligned<Mutex> lruMutex_;

    // Current capacity to track ghost size
    size_t capacity_{0};

    // the lru
    LruList lru_;

    facebook::cachelib::util::FIFOHashSet ghostQueue_;

    // Config for this lru.
    // Write access to the MMS3FIFO Config is serialized.
    // Reads may be racy.
    Config config_{};

    // // Todo: Test
    // FRIEND_TEST(MMS3FIFOTest, SegmentStress);
    // FRIEND_TEST(MMS3FIFOTest, TinyLFUBasic);
    // FRIEND_TEST(MMS3FIFOTest, Reconfigure);
  };
};

/* Container Interface Implementation */
template <typename T, MMS3FIFO::Hook<T> T::* HookPtr>
MMS3FIFO::Container<T, HookPtr>::Container(serialization::MMS3FIFOObject object,
                                           PtrCompressor compressor)
    : lru_(*object.lrus(), std::move(compressor)), config_(*object.config()) {
}

template <typename T, MMS3FIFO::Hook<T> T::* HookPtr>
void MMS3FIFO::Container<T, HookPtr>::maybeGrowGhostLocked() noexcept {
  size_t capacity = lru_.size();

  // Only consider growing ghost when lru size doubles.
  // Right now we don't shrink ghost queue.
  if (2 * capacity_ > capacity || capacity == 0) {
    return;
  }

  size_t expectedGhostSize =
      static_cast<size_t>(capacity * config_.ghostSizePercent / 100);
  ghostQueue_.resize(expectedGhostSize);
  capacity_ = capacity;
}

// We have no notion of "reconfiguring lock"
// or refresh interval since we are not manipulating the list lru on access.
// No need for locks, as we don't modify the list
template <typename T, MMS3FIFO::Hook<T> T::* HookPtr>
bool MMS3FIFO::Container<T, HookPtr>::recordAccess(T& node,
                                                   AccessMode mode) noexcept {
  if ((mode == AccessMode::kWrite && !config_.updateOnWrite) ||
      (mode == AccessMode::kRead && !config_.updateOnRead)) {
    return false;
  }
  const auto currTime = static_cast<Time>(util::getCurrentTimeSec());

  // Remove lock from record access wince we only set bits
  if (node.isInMMContainer()) {
    if (!isAccessed(node)) {
      markAccessed(node);
    }
    setUpdateTime(node, currTime);
    return true;
  }
  return false;
}

template <typename T, MMS3FIFO::Hook<T> T::* HookPtr>
cachelib::EvictionAgeStat MMS3FIFO::Container<T, HookPtr>::getEvictionAgeStat(
    uint64_t projectedLength) const noexcept {
  return lruMutex_->lock_combine([this, projectedLength]() {
    return getEvictionAgeStatLocked(projectedLength);
  });
}

// LRU Eviction age is not defined in a FIFO reinsertion strategy, since items
// in tail can be waiting for promotion. Unaccessed items may have been in the
// cache for a long time too. Right now it finds first unaccessed item from the
// tail and reports its age.
template <typename T, MMS3FIFO::Hook<T> T::* HookPtr>
cachelib::EvictionAgeStat
MMS3FIFO::Container<T, HookPtr>::getEvictionAgeStatLocked(
    uint64_t projectedLength) const noexcept {
  if (projectedLength != 0) {
    // Projected eviction age estimation is not supported in MMS3FIFO
    return EvictionAgeStat{};
  }

  EvictionAgeStat stat;
  const auto curr = static_cast<Time>(util::getCurrentTimeSec());

  auto& list = lru_.getList(LruType::Main);
  auto it = list.rbegin();

  // Check if accessed. If so, advance the iterator.
  while (it != list.rend() && isAccessed(*it)) {
    ++it;
  }
  stat.warmQueueStat.oldestElementAge =
      it != list.rend() ? curr - getUpdateTime(*it) : 0;

  stat.warmQueueStat.size = list.size();

  return stat;
}

template <typename T, MMS3FIFO::Hook<T> T::* HookPtr>
bool MMS3FIFO::Container<T, HookPtr>::add(T& node) noexcept {
  const auto currTime = static_cast<Time>(util::getCurrentTimeSec());

  const auto nodeHash = hashNode(node);
  return lruMutex_->lock_combine([this, &node, currTime, nodeHash]() {
    if (node.isInMMContainer()) {
      return false;
    }

    const auto ghostContains = ghostQueue_.contains(nodeHash);
    if (ghostContains) {
      // Insert to main queue
      auto& mainLru = lru_.getList(LruType::Main);
      mainLru.linkAtHead(node);
    } else {
      // Insert to tiny queue
      auto& tinyLru = lru_.getList(LruType::Tiny);
      tinyLru.linkAtHead(node);
      markTiny(node);
    }

    node.markInMMContainer();
    setUpdateTime(node, currTime);
    unmarkAccessed(node);

    return true;
  });
}

// This method is called before eviction iterator is called, so
// Iterator doesn't have to mutate the lists while searching for victims.
//
// It performs the necessary promotions and rebalancing to ensure
// that items in the tail are evictable.
template <typename T, MMS3FIFO::Hook<T> T::* HookPtr>
void MMS3FIFO::Container<T, HookPtr>::rebalanceForEviction() {
  auto& tinyLru = lru_.getList(LruType::Tiny);
  auto& mainLru = lru_.getList(LruType::Main);

  auto totalSize = tinyLru.size() + mainLru.size();
  auto expectedTinySize =
      static_cast<size_t>(config_.tinySizePercent * totalSize / 100);

  // Promote until we find a victim, so iterator won't have to mutate the lists
  while (true) {
    bool tryTiny = tinyLru.size() >= expectedTinySize;

    if (tryTiny) {
      // Tail of T
      T* nodePtr = tinyLru.getTail();
      if (!nodePtr) {
        break;
      }

      T& node = *nodePtr;
      if (Container<T, HookPtr>::isAccessed(node)) {
        // accessed tail in T → promote to M head
        tinyLru.remove(node);
        mainLru.linkAtHead(node);
        Container<T, HookPtr>::unmarkTiny(node);
        Container<T, HookPtr>::unmarkAccessed(node);
        continue;
      } else {
        // unaccessed tail in T → valid victim, we can stop here.
        break;
      }
    } else {
      // Tail of M
      T* nodePtr = mainLru.getTail();
      if (!nodePtr) {
        break;
      }

      T& node = *nodePtr;
      if (Container<T, HookPtr>::isAccessed(node)) {
        // accessed tail in M → move to head(M), clear bit
        mainLru.moveToHead(node);
        Container<T, HookPtr>::unmarkAccessed(node);
        continue;
      } else {
        // unaccessed tail in M → victim, we can stop here.
        break;
      }
    }
  }
}

template <typename T, MMS3FIFO::Hook<T> T::* HookPtr>
typename MMS3FIFO::Container<T, HookPtr>::LockedIterator
MMS3FIFO::Container<T, HookPtr>::getEvictionIterator() noexcept {
  LockHolder l(*lruMutex_);
  maybeGrowGhostLocked();
  rebalanceForEviction();
  // Cache is full now so we know it's max size
  return LockedIterator{std::move(l), *this};
}

template <typename T, MMS3FIFO::Hook<T> T::* HookPtr>
template <typename F>
void MMS3FIFO::Container<T, HookPtr>::withEvictionIterator(F&& fun) {
  fun(getEvictionIterator());
}

template <typename T, MMS3FIFO::Hook<T> T::* HookPtr>
template <typename F>
void MMS3FIFO::Container<T, HookPtr>::withContainerLock(F&& fun) {
  lruMutex_->lock_combine([&fun]() { fun(); });
}

template <typename T, MMS3FIFO::Hook<T> T::* HookPtr>
void MMS3FIFO::Container<T, HookPtr>::removeLocked(T& node) noexcept {
  if (isTiny(node)) {
    lru_.getList(LruType::Tiny).remove(node); 
    unmarkTiny(node);
    // Insert into ghost queue upon eviction from tiny queue
    ghostQueue_.insert(hashNode(node));
  } else {
    lru_.getList(LruType::Main).remove(node);
  }

  unmarkAccessed(node);
  node.unmarkInMMContainer();
  return;
}

template <typename T, MMS3FIFO::Hook<T> T::* HookPtr>
bool MMS3FIFO::Container<T, HookPtr>::remove(T& node) noexcept {
  return lruMutex_->lock_combine([this, &node]() {
    if (!node.isInMMContainer()) {
      return false;
    }
    removeLocked(node);
    return true;
  });
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
  return lruMutex_->lock_combine([this, &oldNode, &newNode]() {
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
  });
}

template <typename T, MMS3FIFO::Hook<T> T::* HookPtr>
typename MMS3FIFO::Config MMS3FIFO::Container<T, HookPtr>::getConfig() const {
  return lruMutex_->lock_combine([this]() { return config_; });
}

template <typename T, MMS3FIFO::Hook<T> T::* HookPtr>
void MMS3FIFO::Container<T, HookPtr>::setConfig(const Config& c) {
  lruMutex_->lock_combine([this, c]() { config_ = c; });
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

// Unless we use age for slab rebalancing, these stats are not used.
// E.g. Filter based on minTailAge, or using LRUTailAge strategy.
template <typename T, MMS3FIFO::Hook<T> T::* HookPtr>
MMContainerStat MMS3FIFO::Container<T, HookPtr>::getStats() const noexcept {
  // LockHolder l(*lruMutex_);
  return {lru_.size(),
          0, // Approximation of tail age from first unaccesessed items
          0, // We have no notion of refresh time, accesses only toggle access
             // bit
          0, 0, 0, 0};
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
