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

#include <folly/Optional.h>

#include <cstdint>
#include <map>
#include <type_traits>

#include "cachelib/allocator/Cache.h"
#include "cachelib/allocator/memory/serialize/gen-cpp2/objects_types.h"
#include "cachelib/common/CompilerUtils.h"
#include "cachelib/common/Mutex.h"
#include "cachelib/common/Throttler.h"
#include "cachelib/shm/Shm.h"

namespace facebook {
namespace cachelib {

/**
 * Implementation of a hash table with chaining. The elements of the hash
 * table need to have a public member of type Hook . Expects T to provide a
 * getKey(), getHash<Hasher>() and appropriate key comparison operators for
 * doing the key comparisons. The hashtable container guarantees thread
 * safety. The container acts as an intrusive member-hook hashtable.
 */
class ChainedHashTable {
 public:
  // unique identifier per AccessType
  static const int kId;

  template <typename T>
  struct Hook;

 private:
  // Implements a hash table with chaining.
  template <typename T, Hook<T> T::*HookPtr>
  class Impl {
   public:
    using Key = typename T::Key;
    using BucketId = size_t;
    using CompressedPtr = typename T::CompressedPtr;
    using PtrCompressor = typename T::PtrCompressor;

    // allocate memory for hash table; the memory is managed by Impl.
    //
    // @param numBuckets    the number of buckets to be allocated, power of two
    // @param compressor    object used to compress/decompress node pointers
    // @param hasher        object used to hash the key for its bucket id
    Impl(size_t numBuckets,
         const PtrCompressor& compressor,
         const Hasher& hasher);

    // allocate memory for hash table; the memory is managed by the user.
    //
    // @param numBuckets    the number of buckets to be allocated, power of two
    // @param memStart      user managed memory. The size must be enough to
    //                      accommodate the number of the buckets
    // @param compressor    object used to compress/decompress node pointers
    // @param hasher        object used to hash the key for its bucket id
    // @param resetMem      fill memory with CompressedPtr{}
    Impl(size_t numBuckets,
         void* memStart,
         const PtrCompressor& compressor,
         const Hasher& hasher,
         bool resetMem = false);

    // hash table memory is not released if managed by user.
    // i.e. Impl::isRestorable() == true
    ~Impl();

    // prohibit copying
    Impl(const Impl&) = delete;
    Impl& operator=(const Impl&) = delete;

    T* getHashNext(const T& node) const noexcept {
      return (node.*HookPtr).getHashNext(compressor_);
    }

    CompressedPtr getHashNextCompressed(const T& node) const noexcept {
      return (node.*HookPtr).getHashNext();
    }

    void setHashNext(T& node, T* next) const noexcept {
      (node.*HookPtr).setHashNext(next, compressor_);
    }

    void setHashNext(T& node, CompressedPtr next) {
      (node.*HookPtr).setHashNext(next);
    }

    // inserts the element into the bucket.
    //
    // @param node    node to be inserted into the hashtable
    // @param bucket  the hashtable bucket that the node belongs to
    // @return  True if the insertion was success. False if not. Insertion
    //          fails if there is already a node with similar key in the
    //          hashtable.
    bool insertInBucket(T& node, BucketId bucket) noexcept;

    // inserts or replaces the element into the bucket.
    //
    // @param node    node to be inserted into the hashtable
    // @param bucket  the hashtable bucket that the node belongs to
    // @return  old node if it exists, nullptr otherwise
    T* insertOrReplaceInBucket(T& node, BucketId bucket) noexcept;

    // removes the node from the bucket.
    //
    // precondition:  node must be in the bucket.
    // @param node    the node to be removed.
    // @param bucket  the hashtable bucket that the node belongs to
    void removeFromBucket(T& node, BucketId bucket) noexcept;

    // finds the node corresponding to the key from the bucket and returns it
    // if found.
    //
    // @param key     the key for the node we are looking for.
    // @param bucket  the hashtable bucket that the key belongs to
    // @return  a T* corresponding to the node or nullptr if there is no such
    //          node with the key in the bucket.
    T* findInBucket(Key key, BucketId bucket) const noexcept;

    // gets the bucket for the key by using the corresponding hash function.
    BucketId getBucket(Key k) const noexcept;

    // Call 'func' on each element in the given bucket.
    //
    // @param bucket  the bucket id to fetch.
    template <typename F>
    void forEachBucketElem(BucketId bucket, F&& func) const;

    // fetch the number of elements of a given bucket
    //
    // @param bucket  the bucket id to fetch.
    unsigned int getBucketNumElems(BucketId bucket) const;

    // true if the hash table can be restored
    bool isRestorable() const noexcept { return restorable_; }

    // return the hashtable size in bytes
    size_t size() const noexcept { return numBuckets_ * sizeof(CompressedPtr); }

    // return the number of buckets in hash table
    size_t getNumBuckets() const noexcept { return numBuckets_; }

   private:
    // finds the previous node in the hash chain for this node if one exists
    // such that prev->next is node.
    //
    // @param node    the node for which we are looking for the previous
    // @param bucket  the hashtable bucket that the node belongs to
    // @return  previous node for this node in the hash chain or nullptr if
    //          this node is in the head of the hash chain.
    T* findPrevInBucket(const T& node, BucketId bucket) const noexcept;

    // number of buckets we have in the hashtable, must be power of two
    const size_t numBuckets_{0};

    // materialized value of numBuckets_ - 1
    const size_t numBucketsMask_{0};

    // actual buckets.
    std::unique_ptr<CompressedPtr[]> hashTable_;

    // indicate whether or not the hash table uses user-managed memory and
    // is thus restorable from serialized state
    const bool restorable_{false};

    // object used to compress/decompress node pointers to reduce memory
    // footprint of Hook
    const PtrCompressor compressor_;

    // Hash the key
    const Hasher hasher_;
  };

 public:
  using SerializationType = serialization::ChainedHashTableObject;

  // node used for chaining the hash table for collision.
  template <typename T>
  struct CACHELIB_PACKED_ATTR Hook {
    using CompressedPtr = typename T::CompressedPtr;
    using PtrCompressor = typename T::PtrCompressor;
    // sets the next in the hash chain to the passed in value.
    void setHashNext(T* n, const PtrCompressor& compressor) noexcept {
      next_ = compressor.compress(n);
    }

    void setHashNext(CompressedPtr n) noexcept { next_ = n; }

    // gets the next in hash chain for this node.
    T* getHashNext(const PtrCompressor& compressor) const noexcept {
      return compressor.unCompress(next_);
    }

    CompressedPtr getHashNext() const noexcept { return next_; }

   private:
    CompressedPtr next_{};
  };

  // Config class for the chained hash table.
  class Config {
   public:
    // Do not add 'noexcept' here - causes GCC to delete this method:
    //    "config() is implicitly deleted because its exception-specification
    //     does not match the implicit exception-specification
    //     <noexcept (false)>"
    // followed by:
    //    "CacheAllocatorConfig.h:522:29: error: use of deleted function
    //     constexpr facebook::cachelib::ChainedHashTable::Config::Config()
    Config() = default;

    // @param bucketsPower number of buckets in base 2 logarithm
    // @param locksPower number of locks in base 2 logarithm
    // @param pageSize page size
    Config(unsigned int bucketsPower,
           unsigned int locksPower,
           PageSizeT pageSize = PageSizeT::NORMAL)
        : Config(bucketsPower,
                 locksPower,
                 std::make_shared<MurmurHash2>(),
                 pageSize) {}

    // @param bucketsPower number of buckets in base 2 logarithm
    // @param locksPower number of locks in base 2 logarithm
    // @param hasher the key hash function
    // @param pageSize page size
    Config(unsigned int bucketsPower,
           unsigned int locksPower,
           Hasher hasher,
           PageSizeT pageSize = PageSizeT::NORMAL)
        : bucketsPower_(bucketsPower),
          locksPower_(locksPower),
          pageSize_(pageSize),
          hasher_(std::move(hasher)) {
      if (bucketsPower_ > kMaxBucketPower || locksPower_ > kMaxLockPower ||
          locksPower_ > bucketsPower_) {
        throw std::invalid_argument(folly::sformat(
            "Invalid arguments to the config constructor bucketPower =  {}, "
            "lockPower = {}",
            bucketsPower_, locksPower_));
      }
    }

    Config(const Config&) = default;
    Config& operator=(const Config&) = default;

    size_t getNumBuckets() const noexcept {
      return static_cast<size_t>(1) << bucketsPower_;
    }

    size_t getNumLocks() const noexcept {
      return static_cast<size_t>(1) << locksPower_;
    }

    // Estimate bucketsPower and LocksPower based on cache entries.
    void sizeBucketsPowerAndLocksPower(size_t cacheEntries) {
      // The percentage of used buckets vs unused buckets is measured by a load
      // factor. For optimal performance, the load factor should not be more
      // than 60%.
      bucketsPower_ =
          static_cast<size_t>(ceil(log2(cacheEntries * 1.6 /* load factor */)));

      if (bucketsPower_ > kMaxBucketPower) {
        throw std::invalid_argument(folly::sformat(
            "Invalid arguments to the config constructor cacheEntries =  {}",
            cacheEntries));
      }

      // 1 lock per 1000 buckets.
      locksPower_ = std::max<unsigned int>(1, bucketsPower_ - 10);
    }

    unsigned int getBucketsPower() const noexcept { return bucketsPower_; }

    unsigned int getLocksPower() const noexcept { return locksPower_; }

    const Hasher& getHasher() const noexcept { return hasher_; }

    std::map<std::string, std::string> serialize() const {
      std::map<std::string, std::string> configMap;
      configMap["BucketsPower"] = std::to_string(bucketsPower_);
      configMap["LocksPower"] = std::to_string(locksPower_);
      configMap["Hasher"] =
          hasher_->getMagicId() == 1 ? "FNVHash" : "MurmurHash2";
      return configMap;
    }

    PageSizeT getPageSize() const { return pageSize_; }

   private:
    // 4 billion buckets should be good enough for everyone.
    static constexpr unsigned int kMaxBucketPower = 32;
    static constexpr unsigned int kMaxLockPower = 32;

    // The following are expressed as powers of two to make the modulo
    // arithmetic simpler.

    // total number of buckets in the hashtable expressed as power of two.
    unsigned int bucketsPower_{10};

    // total number of locks for the hashtable expressed as a power of two.
    unsigned int locksPower_{5};

    PageSizeT pageSize_{PageSizeT::NORMAL};

    Hasher hasher_ = std::make_shared<MurmurHash2>();
  };

  // Interface for the Container that implements a hash table. Maintains
  // the node's isInAccessContainer state. T must implement an interface to
  // markAccessible(), unmarkAccessible() and isAccessible().
  template <typename T,
            Hook<T> T::*HookPtr,
            typename LockT = facebook::cachelib::SharedMutexBuckets>
  struct Container {
   private:
    using BucketId = typename Impl<T, HookPtr>::BucketId;

   public:
    using Key = typename T::Key;
    using Handle = typename T::Handle;
    using HandleMaker = typename T::HandleMaker;
    using CompressedPtr = typename T::CompressedPtr;
    using PtrCompressor = typename T::PtrCompressor;

    // default handle maker that calls incRef
    static const HandleMaker kDefaultHandleMaker;

    // container with default config.
    Container() noexcept
        : Container(Config{}, PtrCompressor(), kDefaultHandleMaker) {}

    // create hash table container with local-managed memory
    // @param config      the config for the hashtable
    // @param compressor  object used to compress/decompress node pointers
    // @param hm          the functor that creates a Handle from T*
    Container(Config c,
              const PtrCompressor& compressor,
              HandleMaker hm = kDefaultHandleMaker)
        : config_(std::move(c)),
          handleMaker_(std::move(hm)),
          ht_{config_.getNumBuckets(), compressor, config_.getHasher()},
          locks_{config_.getLocksPower(), config_.getHasher()} {}

    // create hash table container with user-managed memory
    //
    // @param c           config for hash table
    // @param memStart    hash table memory managed by the user
    // @param compressor  object used to compress/decompress node pointers
    // @param hm          the functor that creates a Handle from T*
    Container(Config c,
              void* memStart,
              const PtrCompressor& compressor,
              HandleMaker hm = kDefaultHandleMaker)
        : config_(std::move(c)),
          handleMaker_(std::move(hm)),
          ht_{config_.getNumBuckets(), memStart, compressor,
              config_.getHasher(), true /* resetMem */},
          locks_{config_.getLocksPower(), config_.getHasher()} {}

    // restore hash table from serialized data.
    //
    // @param object      serialized object
    // @param newConfig   the new set of configurations
    // @param memSegment  shared memory segment for the hash table
    // @param compressor  object used to compress/decompress node pointers
    // @param hm          the functor that creates a Handle from T*
    //
    // @throw std::invalid argument if the bucket power in new config does not
    //        match the previous state or the size of the memSegment does not
    //        match the old state.
    Container(const serialization::ChainedHashTableObject& object,
              const Config& newConfig,
              ShmAddr memSegment,
              const PtrCompressor& compressor,
              HandleMaker hm = kDefaultHandleMaker);

    // restore hash table from previous state. This only works when the
    // hash table memory is managed by the user.
    //
    // @param object      serialized object
    // @param newConfig   the new set of configurations
    // @param memStart    hash table memory managed by the user
    // @param nBytes      size of memory allocation pointed to by memStart
    // @param compressor  object used to compress/decompress node pointers
    // @param hm          the functor that creates a Handle from T*
    //
    // @throw std::invalid argument if the bucket power in new config does not
    //        match the previous state or the size of the memSegment does not
    //        match the old state.
    Container(const serialization::ChainedHashTableObject& object,
              const Config& newConfig,
              void* memStart,
              size_t nBytes,
              const PtrCompressor& compressor,
              HandleMaker hm = kDefaultHandleMaker);

    Container(const Container&) = delete;
    Container& operator=(const Container&) = delete;

    // inserts the node into the hash table and marks it as being in the
    // hashtable upon success. If another node exists with the same key, the
    // insert fails. On failure the state of the node is unchanged.
    //
    // @param node  the node to be inserted into the hashtable
    // @return  True if the node was successfully inserted into the hashtable.
    //          False if not.
    bool insert(T& node) noexcept;

    // inserts or replaces the node into the hash table and marks it being in
    // the hashtable upon success. If another node exists with the same key, the
    // that node is removed. On failure the state of the node is unchanged.
    //
    // @param node  the node to be inserted into the hashtable
    // @return  if the node was successfully inserted into the hashtable,
    //          returns a null handle. If the node replaced an existing node,
    //          a handle to the old node is returned.
    //
    // @throw std::overflow_error is the maximum item refcount is execeeded by
    //        creating this item handle.
    Handle insertOrReplace(T& node);

    // replaces a node into the hash table, only if another node exists with
    // the same key and is marked accessible.
    //
    // @param oldNode   expected current node in the hash table
    // @param newNode   the new node for the key
    //
    // @return true  if oldNode exists, is accessible, and was replaced
    //               successfully.
    bool replaceIfAccessible(T& oldNode, T& newNode) noexcept;

    // replaces a node if predicate returns true on the existing node
    //
    // @param oldNode   expected current node in the hash table
    // @param newNode   the new node for the key
    // @param predicate   asseses if condition is met for the oldNode to merit
    //                    a replace
    //
    // @return true  if oldNode exists, is accessible, predicate is true, and
    //               was replaced successfully.
    template <typename F>
    bool replaceIf(T& oldNode, T& newNode, F&& predicate);

    // removes the node from the hashtable and unmarks it as accessible. If
    // the node does not exists, returns False.
    //
    // @param   node  node to be removed from the hashtable.
    // @return  True if the node was in the hashtable and if it was
    //          successfully removed. False if the node was not in the
    //          hashtable.
    bool remove(T& node) noexcept;

    // remove a node from the container if it exists for the key and the
    // predicate returns true for the node. This is intended to simplify the
    // eviction purposes to guarantee a good selection of candidate.
    //
    // @param  node       the node to be removed
    // @param  predicate  the predicate check for the node
    //
    // @return handle to the node if we successfully removed it. returns a
    // null handle if the node was either not in the container or the
    // predicate failed.
    Handle removeIf(T& node,
                    const std::function<bool(const T& node)>& predicate);

    // finds the node corresponding to the key in the hashtable and returns a
    // handle to that node.
    //
    // @param key   the lookup key
    // @param args  arguments to construct a handle for T.
    //
    // @return  Handle with valid T* if there is a node corresponding to the
    //          key or a Handle with nullptr if not.
    //
    // @throw std::overflow_error is the maximum item refcount is execeeded by
    //        creating this item handle.
    Handle find(Key key) const;

    // for saving the state of the hash table
    //
    // precondition:  serialization must happen without any reader or writer
    // present. Any modification of this object afterwards will result in an
    // invalid, inconsistent state for the serialized data.
    //
    // @throw std::logic_error if the container has any pending iterators that
    // need to be destroyed or if the container can not be restored.
    serialization::ChainedHashTableObject saveState() const;

    // get the required size for the buckets.
    static size_t getRequiredSize(size_t numBuckets) noexcept {
      return sizeof(CompressedPtr) * numBuckets;
    }

    const Config& getConfig() const noexcept { return config_; }

    unsigned int getHashpower() const noexcept {
      return config_.getBucketsPower();
    }

    // Iterator interface for the hashtable. Iterates over the hashtable
    // bucket by bucket and takes a snapshot of the bucket to iterate over. It
    // guarantees that all keys that were present when the iteration started
    // will be accessible unless they are removed. Keys that are
    // removed/inserted during the lifetime of an iterator are not guaranteed
    // to be either visited or not-visited. Adding/Removing from the hash
    // table while the iterator is alive will not invalidate any iterator or
    // the element that the iterator points at currently. The iterator
    // internally holds a Handle to the item.
    class Iterator {
     public:
      ~Iterator() {
        XDCHECK_GT(container_->numIterators_.load(), 0u);
        --container_->numIterators_;
      }
      Iterator(const Iterator&) = delete;
      Iterator& operator=(const Iterator&) = delete;

      Iterator(Iterator&&) noexcept;
      Iterator& operator=(Iterator&&) noexcept;
      enum EndIterT { EndIter };

      // increment the iterator to the next element.
      // with/without throttler
      Iterator& operator++();

      // dereference the current element that the iterator is pointing to.
      T& operator*();
      T* operator->() { return &(*(*this)); }
      const T& operator*() const;
      const T* operator->() const { return &(*(*this)); }

      bool operator==(const Iterator& other) const noexcept {
        return container_ == other.container_ &&
               currBucket_ == other.currBucket_ && curSor_ == other.curSor_;
      }

      bool operator!=(const Iterator& other) const noexcept {
        return !(*this == other);
      }

      // TODO(jiayueb): change to return ReadHandle after fixing all the breaks
      const Handle& asHandle() { return curr(); }

      // reset the Iterator to begin of container
      void reset();

     private:
      // container for the iterator
      using C = Container<T, HookPtr, LockT>;

      // construct an iterator with the given
      friend C;
      explicit Iterator(C& ht,
                        folly::Optional<util::Throttler::Config>
                            throttlerConfig = folly::none);

      Iterator(C& ht, EndIterT);

      // the container over which we are iterating
      mutable C* container_;

      // current bucket that the iterator is pointing to.
      mutable BucketId currBucket_{0};

      // cursor into the current bucket.
      mutable unsigned int curSor_{0};

      // current bucket.
      mutable std::vector<Handle> bucketElems_;

      // optional throttler
      folly::Optional<util::Throttler> throttler_ = folly::none;

      // returns the handle for current item in the iterator.
      Handle& curr() {
        if (curSor_ < bucketElems_.size()) {
          return bucketElems_[curSor_];
        }
        throw std::logic_error(
            "Iterator in invalid state with curSor_: " +
            folly::to<std::string>(curSor_) + ", currBucket_: " +
            folly::to<std::string>(currBucket_) + ", total buckets: " +
            folly::to<std::string>(container_->config_.getNumBuckets()));
      }
    };

    // Iterator interface to the container.
    // whether it constructs iterator of begin with a throttler config
    Iterator begin(folly::Optional<util::Throttler::Config> throttlerConfig);

    Iterator begin() { return Iterator(*this); }
    Iterator end() { return Iterator(*this, Iterator::EndIter); }

    // Stats describing the distribution of items (keys) in the hash table
    struct DistributionStats {
      uint64_t numKeys{0};
      uint64_t numBuckets{0};
      // map from bucket id to number of items in the bucket.
      std::map<unsigned int, uint64_t> itemDistribution{};
    };

    struct Stats {
      uint64_t numKeys;
      uint64_t numBuckets;
    };

    // Get the distribution stats. This function will use cached results
    // if the difference since last updated is not significant. This is
    // expensive. Call at your discretion.
    //
    // Critiera for refreshing the stats:
    //  - 10 minutes since last update, OR
    //  - 5% more or less number of keys in the hash table
    DistributionStats getDistributionStats() const;

    // lightweight stats that give the number of keys and buckets inside the
    // container. This is guaranteed to be fast.
    Stats getStats() const noexcept { return {numKeys_, ht_.getNumBuckets()}; }

    // Get the total number of keys inserted into the hash table
    uint64_t getNumKeys() const noexcept {
      return numKeys_.load(std::memory_order_relaxed);
    }

   private:
    using Hashtable = Impl<T, HookPtr>;

    // Fetch a vector of handle to the items belonging to a given bucket. This
    // is for use by the iterator. 'handles' will be cleared and then populated
    // with handles for the items in the given bucket. Items will be skipped if
    // the handle cannot be acquired for any reason.
    void getBucketElems(BucketId bucket, std::vector<Handle>& handles) const;

    // config for the hash table.
    const Config config_{};

    // handle maker to convert the T* to T::Handle
    HandleMaker handleMaker_;

    // the hashtable buckets
    Hashtable ht_;

    // locks protecting the hashtable buckets
    mutable LockT locks_;

    std::atomic<unsigned int> numIterators_{0};

    // Cached stats for distribution
    // This is updated if the number of keys changes by more than 5%, or
    // it has been 10 minutes since the stats has last been updated.
    mutable std::mutex cachedStatsLock_;
    mutable DistributionStats cachedStats_{};

    // if we can recompute the cachedStats if it is too old. Set to false when
    // another thread is computing it.
    mutable bool canRecomputeDistributionStats_{true};

    // when the distribution was last computed.
    mutable time_t cachedStatsUpdateTime_{0};

    // number of the keys stored in this hash table
    std::atomic<uint64_t> numKeys_{0};
  };
};

template <typename T,
          typename ChainedHashTable::Hook<T> T::*HookPtr,
          typename LockT>
const typename T::HandleMaker
    ChainedHashTable::Container<T, HookPtr, LockT>::kDefaultHandleMaker =
        [](T* t) -> typename T::Handle {
  if (t) {
    t->incRef();
  }
  return typename T::Handle{t};
};
} // namespace cachelib
} // namespace facebook

#include "cachelib/allocator/ChainedHashTable-inl.h"
