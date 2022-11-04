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

/**
 * The compact cache is a highly memory-efficient way of mapping a key to a
 * value. It is implemented as an N-way associative hash table. This means that
 * each bucket in the hash table has room for at most N items. If the bucket
 * fills up, the least recently accessed item inside that bucket will be
 * evicted. The compact cache interface functions are all thread safe (i.e, they
 * do their own locking).
 *
 * The compact cache is a class template with a template parameter called a
 * CompactCacheDescriptor which defines the type of the keys and values and what
 * the compact cache can do with them. The CompactCacheDescriptor is a wrapper
 * around a Key and a ValueDescriptor. The Key defines the type of a key. The
 * ValueDescriptor defines the type of the value (if any), and how values can be
 * added together (needed in order to have the compact cache provide the add
 * operation).
 * Read ccache_descriptor.h for more information on value descriptor.
 *
 * The LRU mechanism withing a bucket is achieved by sliding the entries down
 * each time an entry is added on top of the bucket or each time an entry is
 * promoted because it was read. This compact cache is very efficient for small
 * values but can have bad performance when used with big values as each read
 * requires moving N big values.
 */

#pragma once

#include <type_traits>

#include "cachelib/allocator/Cache.h"
#include "cachelib/allocator/CacheStats.h"
#include "cachelib/allocator/ICompactCache.h"
#include "cachelib/common/Cohort.h"
#include "cachelib/common/FastStats.h"
#include "cachelib/compact_cache/CCacheBucketLock.h"
#include "cachelib/compact_cache/CCacheFixedLruBucket.h"
#include "cachelib/compact_cache/CCacheVariableLruBucket.h"

/****************************************************************************/
/* CONFIGURATION */

/**
 * Default amount of entries per bucket.
 * Only applicable when creating a compact cache for fixed size values.
 */
#define NB_ENTRIES_PER_BUCKET 8

/****************************************************************************/
/* CompactCache definition */

namespace facebook {
namespace cachelib {

/**
 * Trait class used to define the default bucket descriptor to be used for a
 * given compact cache descriptor.
 * Use either FixedLruBucket or VariableLruBucket depending on whether or not
 * the values are of a variable size.
 *
 * @param C compact cache descriptor.
 */
template <typename C>
struct DefaultBucketDescriptor {
  using type =
      typename std::conditional<C::kValuesFixedSize,
                                FixedLruBucket<C, NB_ENTRIES_PER_BUCKET>,
                                VariableLruBucket<C>>::type;
};

enum class CCacheReturn : int {
  TIMEOUT = -2,
  ERROR = -1,
  NOTFOUND = 0,
  FOUND = 1
};

enum class RehashOperation { COPY, DELETE };

/**
 * Interface for a compact cache. User should not need to directly reference
 * this type. Instead, use CCacheCreator<...>::type as the Compact Cache type.
 *
 * @param C Class that provides information about the data (keys and values)
 *          processed by the compact cache. See ccache_descriptor.h for more
 *          information.
 * @param A Class that provides interface to allocate memory. It needs to
 *          implement the CCacheAllocatorBase interface.
 * @param B Class that handles the management of the data in the buckets. This
 *          will use FixedLruBucket by default if the values are of a fixed
 *          size and VariableLruBucket if they are of a variable size. You can
 *          provide another bucket descriptor provided that it is compatible
 *          with the values being stored in this compact cache.
 */
template <typename C,
          typename A,
          typename B = typename DefaultBucketDescriptor<C>::type>
class CompactCache : public ICompactCache {
 public:
  using SelfType = CompactCache<C, A, B>;
  using BucketDescriptor = B;
  using ValueDescriptor = typename C::ValueDescriptor;
  using Value = typename ValueDescriptor::Value;
  using Key = typename C::Key;
  using EntryHandle = typename BucketDescriptor::EntryHandle;
  using Bucket = typename BucketDescriptor::Bucket;
  using Allocator = A;

  constexpr static bool kHasValues = C::kHasValues;
  constexpr static bool kValuesFixedSize = C::kValuesFixedSize;

  enum Operation { READ, WRITE };

  /** Type of the callbacks called when an entry is removed.
   * The type of the callback depends on the type of the values in this
   * compact cache. By using std::conditional, we are able to provide a
   * different type depending on the use case, i.e the callback does not take
   * a value if the compact cache does not store values. The context is set
   * appropriately if its an eviction or deletion or garbage collection.
   * Note: if the values stored in this compact cache are of a variable size,
   * it is assumed that the user will be able to compute the size if needed.
   * For example, user has already stored a size field as part of the value,
   * or can tell how big a value should be given the key.
   */
  using RemoveCb = typename std::conditional<
      kHasValues,
      /* For a compact cache with values */
      std::function<void(const Key&, const Value*, const RemoveContext)>,
      /* For a compact cache with no values */
      std::function<void(const Key&, const RemoveContext)>>::type;

  using ReplaceCb = typename std::conditional<
      kHasValues,
      /* For a compact cache with values */
      std::function<void(const Key&, const Value*, const Value*)>,
      /* For a compact cache with no values */
      std::function<void(const Key&)>>::type;

  /** callbacks for validating an entry
   * An entry may be invalid because of something happened outside of compact
   * cache. If an entry is invalid, we can safely remove it without paying the
   * cost of potential cache miss. */
  using ValidCb = std::function<bool(const Key&)>;

  /**
   * Construct a new compact cache instance.
   * Use this constructor only if you do not need to track when elements are
   * removed or replaced (this is used by some tests for example).
   *
   * @param allocator       Compact cache allocator to be used by this instance.
   *                        The caller must guarantee the validity of
   *                        the allocator for the lifetime of the compact cache.
   * @param allowPromotions Whether we should allow promotions on read
   *                        operations. True by default
   */
  explicit CompactCache(Allocator& allocator, bool allowPromotions = true);

  /**
   * Construct a new compact cache instance with callbacks to track when
   * items are removed / replaced.
   *
   * @param allocator       Compact cache allocator to be used by this instance.
   *                        The caller must guarantee the validity of the
   *                        allocator for the lifetime of the compact cache.
   * @param removeCb        callback to be called when an item is removed.
   * @param replaceCb       callback to be called when an item is replaced.
   * @param validCb         callback to be called to check whether an entry is
   *                        valid
   * @param allowPromotions Whether we should allow promotions on read
   *                        operations. True by default
   */
  CompactCache(Allocator& allocator,
               RemoveCb removeCb,
               ReplaceCb replaceCb,
               ValidCb validCb,
               bool allowPromotions = true);

  /**
   * Destructor will detach the allocator. Only after a CompactCache instance
   * is destroyed, user can attach another instance to the same allocator.
   */
  ~CompactCache();

  /**
   * Resize the arena of this compact cache.
   * Shrinking the cache:
   *  (1) Reduce num_chunks, so new requests only hash to the reduced size
   *  (2) If currently taking requests, wait for all requests that picked up
   *      the old num_chunk value to drain out. During this time some requests
   *      will use the old and others will use the new num_chunk value, which
   *      is ok because we're using a consistent hash.
   *  (3) Walk the table and free the entries in the chunks we're not using.
   *  (4) Free no longer used chunks.
   * Growing the cache:
   *  (1) Allocate new chunks
   *  (2) Update num_chunks, wait for refcount on old value to hit 0
   *  (3) Walk all the entries in the old table size and see which ones would
   *      now hash to new chunks and move them.
   */
  void resize() override;

  /**
   * return whether the cache is enabled
   * this is non-virtual for better performance since it is hotly accessed
   */
  bool enabled() const {
    return numChunks_ > 0 && allocator_.getChunkSize() > 0;
  }

  /**
   * return the total size of currently used chunks (virtual function). This
   * could be different from the configured size since the resizing happens
   * offline.
   */
  size_t getSize() const override {
    return numChunks_ * allocator_.getChunkSize();
  }

  /**
   * return config size of this compact cache
   */
  size_t getConfiguredSize() const override {
    return allocator_.getConfiguredSize();
  }

  std::string getName() const override { return allocator_.getName(); }

  PoolId getPoolId() const { return allocator_.getPoolId(); }

  /**
   * return the total number of entries
   */
  size_t getNumEntries() const {
    return numChunks_ * bucketsPerChunk_ * BucketDescriptor::kEntriesPerBucket;
  }

  /**
   * Set or update a value in the compact cache.
   *
   * @param key     Key for which to set / update the value.
   * @param timeout if greater than 0, take a timed lock
   * @param val     Val to set / update for the key. Must not be nullptr unless
   *                the value type is NoValue which is a special indicator for
   *                a compact cache of no values.
   * @param size    Size of the value. 0 means the value is fixed size.
   * @return        CCacheReturn with appropriate result type:
   *                FOUND (on hit - the value was updated), NOTFOUND (on miss -
   *                the value was set), TIMEOUT, ERROR (other error)
   */
  CCacheReturn set(const Key& key,
                   const std::chrono::microseconds& timeout,
                   const Value* val = nullptr,
                   size_t size = 0);
  CCacheReturn set(const Key& key,
                   const Value* val = nullptr,
                   size_t size = 0) {
    return set(key, std::chrono::microseconds::zero(), val, size);
  }

  /**
   * Delete the entry mapped by a key and retrieve the old value.
   *
   * @param key     Key of the entry to be deleted.
   * @param timeout if greater than 0, take a timed lock
   * @param val     Pointer to the memory location where the old value will be
   *                written to. Left untouched on a miss. Nullptr means we don't
   *                have a value, or we don't need to obtain the old value.
   * @param size    Poiner to the memory location where the deleted value's size
   *                will be written to if the value is variable. Fixed values
   *                will NOT have their sizes written into this field. Nullptr
   *                means caller does not need the size of the deleted value.
   * @return        CCacheReturn with appropriate result type:
   *                FOUND (on hit - the value was deleted), NOTFOUND (on miss),
   *                TIMEOUT, ERROR (other error)
   */
  CCacheReturn del(const Key& key,
                   const std::chrono::microseconds& timeout,
                   Value* val = nullptr,
                   size_t* size = nullptr);
  CCacheReturn del(const Key& key,
                   Value* val = nullptr,
                   size_t* size = nullptr) {
    return del(key, std::chrono::microseconds::zero(), val, size);
  }

  /**
   * Retrieve the value mapped by a key.
   *
   * @param  key            Key of the entry to be read.
   * @param  timeout        if greater than 0, take a timed lock
   * @param  val            Pointer to the memory location where the value will
   *                        be written to. Left untouched on a miss. Nullptr
   *                        means caller does not need the value.
   * @param  size           Poiner to the memory location where the value's
   *                        size will be written to if the value is variable.
   *                        Fixed values will NOT have their sizes written into
   *                        this field. Nullptr means size info is not needed.
   * @param  shouldPromote  Whether key should be promoted. Note that if the
   *                        CCache promotion is disabled by default (set at
   *                        construction), this  parameter is ignored.
   *
   * @return                CCacheReturn with appropriate result type:
   *                        FOUND (on hit - the value was read), NOTFOUND (on
   *                        miss), TIMEOUT or ERROR (other error)
   */
  CCacheReturn get(const Key& key,
                   const std::chrono::microseconds& timeout,
                   Value* val = nullptr,
                   size_t* size = nullptr,
                   bool shouldPromote = true);
  CCacheReturn get(const Key& key,
                   Value* val = nullptr,
                   size_t* size = nullptr,
                   bool shouldPromote = true) {
    return get(
        key, std::chrono::microseconds::zero(), val, size, shouldPromote);
  }

  /**
   * Check if the key exists in the compact cache. Useful if the caller wants
   * to check for only existence and not copy the value out.
   *
   * @param key             key of the entry
   * @param timeout         if greater than 0, take a timed lock
   *
   * @return                CCacheReturn with appropriate result type:
   *                        FOUND (on hit - the value was found), NOTFOUND (on
   *                        miss), TIMEOUT or ERROR (other error)
   */
  CCacheReturn exists(const Key& key, const std::chrono::microseconds& timeout);
  CCacheReturn exists(const Key& key) {
    return exists(key, std::chrono::microseconds::zero());
  }

  /**
   * Accepts a prefix and value, returning whether or not to purge the entry.
   * @param key     key of the entry
   * @param value   value of the entry. Nullptr if compact cache has no value
   */
  enum class PurgeFilterResult { SKIP, PURGE, ABORT };
  using PurgeFilter =
      std::function<PurgeFilterResult(const Key&, const Value* const)>;

  /**
   * Go through all entries and dump the ones that shouldPurge returns true
   *
   * @param shouldPurge Function that returns whether or not to purge the
   *                    current value. In the case of a cache without values,
   *                    nullptr will be passed. This function is executed
   *                    under the bucket lock.
   * @return true on success, false on failure
   */
  bool purge(const PurgeFilter& shouldPurge);

  /**
   * Iterate on all the bucket in the ccache and call the passed-in callback to
   * collect stats, purge entries, etc.
   * @return true on success, false on failure
   */
  using BucketCallBack = std::function<bool(Bucket*)>;
  bool forEachBucket(const BucketCallBack& cb);

  /** Move or purge entries that would change which chunk they hash to
   * based on the specified old and new numbers of chunks. */
  void tableRehash(size_t oldNumChunks,
                   size_t newNumChunks,
                   RehashOperation op);

  /** return the current snapshot of all stats */
  CCacheStats getStats() const override { return stats_.getSnapshot(); }

 private:
  /**
   * Execute a request handler f on a given key.
   * The following operations are performed:
   *   1) Check if the cache should miss due to an ongoing purge.
   *   2) Increase the refcount on the current cohort;
   *   3) Find the hash table bucket for the key;
   *   4) Lock the bucket;
   *   5) Call the request handler;
   *   6) Unlock the bucket;
   *   7) Decrease the refcount on the cohort.
   *
   * @param key     Key on which to perform the request.
   * @param timeout if greater than 0, take a timed lock
   * @param f Handler to execute. The handler must have the following
   *          signature:
   *          int (*f)(Bucket*);
   *          The handler must return 0 on a miss, 1 on a hit, -1 in case of
   *          an error.
   * @param Args Any extra params neeeded for Fn
   *
   * @return 0 on a miss, 1 on a hit, -2 on timeout, -1 on other error
   */
  template <typename Fn, typename... Args>
  int callBucketFn(const Key& key,
                   Operation op,
                   const std::chrono::microseconds& timeout,
                   Fn f,
                   Args... args);

  /** Free chunks whose index is between chunk_index_low, inclusive, and
   * chunk_index_high, exclusive. Used which shrinking the cache. */
  int tableChunksFree(size_t chunkIndexLow, size_t chunkIndexHigh);

  /**
   * Find the chunk on which the specified key would be located given the
   * specified number of chunks. Used by tableFindBucket for looking update
   * buckets in the table (in which case numChunks = arena_->num_chunks) and
   * by tableRehashLarger (in which case numChunks is the new number of
   * chunks).
   *
   * @param numChunks Total number of chunks.
   * @praam key        Key for which to compute the corresponding chunk.
   *
   * @return pointer to the first bucket of the desired chunk.
   */
  Bucket* tableFindChunk(size_t numChunks, const Key& key);

  /**
   * Find out which bucket an entry might be in. Do this in a 2 step process:
   * 1. Use consistent hashing (furc_hash) to determine the table chunk.
   * 2. Treat each chunk as a small, single chunk compact cache--hash the key
   *    again and us that to find the right bucket.
   *
   * This means if chunk size remains fixed (ex 1MB) while the number of
   * chunks possibly varies (shrinking or growing the cache), a given key
   * will always map to the same bucket offset within the chunk no matter
   * which chunk it's in. So eg if a key maps to the 15th bucket of chunk 3,
   * and we grow the total number of chunks from 9 to 10, the key will map
   * either still to the 15th bucket of chunk 3, or to the 15th bucket of
   * chunk 10.
   *
   * @param const Key& key for which to determine the corresponding bucket.
   *
   * @return bucket that maps to the key.
   */
  Bucket* tableFindBucket(const Key& key);
  Bucket* tableFindDblWriteBucket(const Key& key);

  /**
   * Callback called by the bucket descriptor when an entry is evicted.
   *
   * @param handle Handle to the evicted entry.
   */
  void onEntryEvicted(const EntryHandle& handle);

  enum class BucketReturn { ERROR = -1, NOTFOUND = 0, FOUND = 1, PROMOTE = 2 };

  int toInt(const BucketReturn br) const {
    return static_cast<typename std::underlying_type<BucketReturn>::type>(br);
  }

  /**
   * Set a new entry in a bucket. If there already is an entry for the
   * specified key, update the entry's value (if any).
   *
   * @param bucket Bucket from which to look for an entry.
   * @param key    Key to search for in the bucket.
   * @param val    Value to insert into the bucket. Nullptr means no value.
   * @param size   Size of the value. Unused if this compact cache stores
   *               values of a fixed size.
   *
   * @return       FOUND if entry is found (an existing entry was updated),
   *               or FOUND if not found (a new entry was inserted).
   */
  BucketReturn bucketSet(Bucket* bucket,
                         const Key& key,
                         const Value* val = nullptr,
                         size_t size = 0);

  /**
   * Delete an entry from a bucket.
   *
   * @param bucket Bucket from which to delete an entry.
   * @param key    Key of the entry to be deleted.
   * @param val    The value of the entry that gets removed is written at the
   *               location pointed to by this pointer.
   *               If no entry is deleted, 0 is written.
   * @param size   Pointer to the memory location where to write the size of
   *               the deleted value. Ignored if this compact cache stores
   *               values of a fixed size.
   *
   * @return       FOUND if entry is found (an entry was deleted),
   *               or NOTFOUND if not found (no entry was deleted).
   */
  BucketReturn bucketDel(Bucket* bucket,
                         const Key& key,
                         Value* val,
                         size_t* size);

  /**
   * Check if entry is present. Doesn't promote.
   *
   * @param  bucket         Bucket from which to look for an entry.
   * @param  key            Key to search for in the bucket.
   * @param  val            The value of the found entry is written at the
   *                        location pointed to by this pointer. This is left
   *                        untouched if the entry is not present.
   * @param  size           Pointer to the memory location where to write the
   *                        size of the retrieved value. Ignored if this compact
   *                        cache stores values of a fixed size.
   * @param  shouldPromote  Whether key should be promoted.
   *
   * @return                FOUND if found, NOTFOUND if not found, or PROMOTE
   *                        if found and needs promotion
   */
  BucketReturn bucketGet(Bucket* bucket,
                         const Key& key,
                         Value* val,
                         size_t* size,
                         bool shouldPromote);

  /**
   * Promotes an entry if necessary. Must be called under write lock.
   *
   * @return FOUND if found, promoted if necessary
   *         NOTFOUND if not found
   */
  BucketReturn bucketPromote(Bucket* bucket, const Key& key);

  /**
   * Find the position of an entry in a bucket.
   *
   * @param bucket Bucket from which to look for an entry.
   * @param key    Key to search for in the bucket.
   *
   * @return       Handle to the found entry or invalid handle if not found.
   */
  EntryHandle bucketFind(Bucket* bucket, const Key& key);

  /**
   * Data for a compact cache instance.
   * The arena must remain alive (i.e. not free'd) during the lifetime of the
   * compact cache.
   */
  Allocator& allocator_;
  CCRWBucketLocks locks_;
  RemoveCb removeCb_;
  ReplaceCb replaceCb_;
  ValidCb validCb_;
  facebook::cachelib::Cohort cohort_; /**< resize cohort synchronization */
  folly::SharedMutex resizeLock_;     /**< Lock to prevent resize conflicts. */
  const size_t bucketsPerChunk_;
  util::FastStats<CCacheStats> stats_;
  const bool allowPromotions_; /**< Whether promotions are allowed on read
                                    operations */

 protected:
  // expose these two fields for test hack
  std::atomic<size_t> numChunks_;
  std::atomic<size_t> pendingNumChunks_;
};
} // namespace cachelib
} // namespace facebook

/* This file implements the ccache methods. */
#include "cachelib/compact_cache/CCache-inl.h"
