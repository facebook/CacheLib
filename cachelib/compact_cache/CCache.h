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

#include <folly/SharedMutex.h>
#include <folly/logging/xlog.h>

#include <type_traits>
#include <typeinfo>

#include "cachelib/allocator/Cache.h"
#include "cachelib/allocator/CacheStats.h"
#include "cachelib/allocator/ICompactCache.h"
#include "cachelib/common/Cohort.h"
#include "cachelib/common/FastStats.h"
#include "cachelib/common/Hash.h"
#include "cachelib/common/Mutex.h"
#include "cachelib/compact_cache/CCacheFixedLruBucket.h"
#include "cachelib/compact_cache/CCacheVariableLruBucket.h"

/**
 * Default amount of entries per bucket.
 * Only applicable when creating a compact cache for fixed size values.
 */
#define NB_ENTRIES_PER_BUCKET 8

namespace facebook::cachelib {
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
  RWBucketLocks<folly::SharedMutex> locks_;
  RemoveCb removeCb_;
  ReplaceCb replaceCb_;
  ValidCb validCb_;
  facebook::cachelib::Cohort cohort_;     /**< resize cohort synchronization */
  mutable folly::SharedMutex resizeLock_; /**< Lock to synchronize resize. */
  const size_t bucketsPerChunk_;
  util::FastStats<CCacheStats> stats_;
  const bool allowPromotions_; /**< Whether promotions are allowed on read
                                    operations */

 protected:
  // expose these two fields for test hack
  std::atomic<size_t> numChunks_;
  std::atomic<size_t> pendingNumChunks_;
};

namespace detail {
/**
 * Convenient macro for updating the stats about a particular operation and
 * returing an error value.
 * @param op_name Operation for which to update the stats.
 * @param rv return value:
 *    -1 -> error
 *     0 -> miss
 *     1 -> hit
 */
#define UPDATE_STATS_AND_RETURN(op_name, rv) \
  do {                                       \
    ++stats_.tlStats().op_name;              \
    switch (rv) {                            \
    case -2:                                 \
      /* already incremented stat */         \
      return CCacheReturn::TIMEOUT;          \
    case -1:                                 \
      ++stats_.tlStats().op_name##Err;       \
      return CCacheReturn::ERROR;            \
    case 0:                                  \
      ++stats_.tlStats().op_name##Miss;      \
      return CCacheReturn::NOTFOUND;         \
    case 1:                                  \
      ++stats_.tlStats().op_name##Hit;       \
      return CCacheReturn::FOUND;            \
    default:                                 \
      XDCHECK(false);                        \
    }                                        \
  } while (0);                               \
  return CCacheReturn::ERROR;

/** Function template used to call a remove callback.
 * The caller must take care of verifying that the callback is valid,
 * i.e the callback was not default constructed.
 *
 * This wrapper is needed so that the compact cache implementation can call the
 * remove callback in a consistent way (i.e, it does not check for the existence
 * of a value). The specialization takes care of stripping out the values when
 * the compact cache does not store values.
 */
template <typename CC>
void callRemoveCb(const typename CC::RemoveCb& cb,
                  typename CC::Key const& key,
                  typename CC::Value const* val,
                  RemoveContext const context,
                  typename std::enable_if<CC::kHasValues>::type* = 0) {
  cb(key, val, context);
}
template <typename CC>
void callRemoveCb(const typename CC::RemoveCb& cb,
                  typename CC::Key const& key,
                  typename CC::Value const* /*val*/ /* unused */,
                  RemoveContext const context,
                  typename std::enable_if<!CC::kHasValues>::type* = 0) {
  /* This compact cache does not store values, so the callback provided by the
   * user does not expect a value to be passed. */
  cb(key, context);
}

/** Function template used to call a replace callback.
 * The caller must take care of verifying that the callback is valid,
 * i.e the callback was not default constructed.
 *
 * This wrapper is needed so that the compact cache implementation can call the
 * replace callback in a consistent way (i.e, it does not check for the
 * existence of a value). The specialization takes care of stripping out the
 * values when the compact cache does not store values. */
template <typename CC>
void callReplaceCb(const typename CC::ReplaceCb& cb,
                   typename CC::Key const& key,
                   typename CC::Value const* old_val,
                   typename CC::Value const* new_val,
                   typename std::enable_if<CC::kHasValues>::type* = 0) {
  cb(key, old_val, new_val);
}
template <typename CC>
void callReplaceCb(const typename CC::ReplaceCb& cb,
                   typename CC::Key const& key,
                   typename CC::Value const* /*old_val*/ /* unused */,
                   typename CC::Value const* /*new_val*/ /* unused */,
                   typename std::enable_if<!CC::kHasValues>::type* = 0) {
  /* This compact cache does not store values, so the callback provided by the
   * user does not expect values to be passed. */
  cb(key);
}
} // namespace detail

template <typename C, typename A, typename B>
CompactCache<C, A, B>::CompactCache(Allocator& allocator, bool allowPromotions)
    : CompactCache(allocator, nullptr, nullptr, nullptr, allowPromotions) {}

template <typename C, typename A, typename B>
CompactCache<C, A, B>::CompactCache(Allocator& allocator,
                                    RemoveCb removeCb,
                                    ReplaceCb replaceCb,
                                    ValidCb validCb,
                                    bool allowPromotions)
    : allocator_(allocator),
      locks_(10 /* hashpower */, std::make_shared<MurmurHash2>()),
      removeCb_(removeCb),
      replaceCb_(replaceCb),
      validCb_(validCb),
      bucketsPerChunk_(allocator_.getChunkSize() / sizeof(Bucket)),
      stats_{},
      allowPromotions_(allowPromotions),
      numChunks_(allocator_.getNumChunks()),
      pendingNumChunks_(0) {
  allocator_.attach(this);
}

template <typename C, typename A, typename B>
CompactCache<C, A, B>::~CompactCache() {
  allocator_.detach();
}

/**
 * Move or purge entries that would change which chunk they hash to based on
 * the specified old and new numbers of chunks.
 *
 * @param oldNumChunks  old size of compact cache
 * @param newNumChunks  new size of compact cache
 * @param op            whether to create new entries COPY or delete
 *                      old ones DELETE in this call
 */
template <typename C, typename A, typename B>
void CompactCache<C, A, B>::tableRehash(size_t oldNumChunks,
                                        size_t newNumChunks,
                                        RehashOperation op) {
  XDCHECK_LE(newNumChunks, allocator_.getNumChunks());
  XDCHECK_GT(newNumChunks, 0u);
  XDCHECK_GT(oldNumChunks, 0u);

  /* Loop through all entries in all buckets of the hash table. */
  for (size_t n = 0; n < oldNumChunks; n++) {
    Bucket* table_chunk = reinterpret_cast<Bucket*>(allocator_.getChunk(n));
    for (size_t i = 0; i < bucketsPerChunk_; i++) {
      Bucket* bucket = &table_chunk[i];
      auto lock = locks_.lockExclusive(bucket);

      const size_t capacity = BucketDescriptor::nEntriesCapacity(*bucket);
      /* When expanding the cache (newNumChunks > oldNumChunks) move
       * the entire capacity elements over.
       * When shrinking cache to some fraction of its former size,
       * move that fraction of the to-be-deleted chunks in order to
       * maintain a non-zero cache lifetime throughout */
      const size_t max_move = std::min(
          std::max((newNumChunks * capacity) / oldNumChunks, (size_t)1),
          capacity);
      size_t moved = 0;
      EntryHandle entry = BucketDescriptor::first(&table_chunk[i]);
      while (entry) {
        const bool valid_entry = !validCb_ || validCb_(entry.key());
        auto remove_context =
            valid_entry ? RemoveContext::kEviction : RemoveContext::kNormal;
        Bucket* new_chunk = tableFindChunk(newNumChunks, entry.key());
        if (new_chunk == table_chunk) {
          entry.next();
          continue;
        }

        /* Moving takes two forms. If newNumChunks is bigger, we move
         * everything to its new place in the newly allocated chunks.
         * If smaller, we need to preserve read-after-write so we move
         * a small initial fraction of the newly lost buckets to their
         * new home, putting them at the head of the LRU.
         *
         * This occurs in two stages; first we insert the to-be-moved
         * entries into their new home and call the removal callbacks
         * for invalid or excess ones.
         *
         * Second, after reads have moved, we delete the old entries
         * This is done in a new cohort to ensure reads never see
         * a temporarily disappeared entry in cache.
         */
        if (op == RehashOperation::COPY) {
          if (valid_entry && moved < max_move) {
            /* Add new entry. Offset is the same, so no need to
             * re-compute that hash
             */
            Bucket* newBucket = &new_chunk[i];
            // lock ordering is established by the hash function;
            // old hash -> new hash
            // However there can be arbitrary hash collisions, so
            // don't relock the same lock (we don't use recursive
            // locks in general)
            bool sameLock = locks_.isSameLock(newBucket, bucket);
            auto higher_lock //
                = sameLock   //
                      ? std::unique_lock<folly::SharedMutex>()
                      : locks_.lockExclusive(newBucket);
            if (kHasValues) {
              if (kValuesFixedSize) {
                bucketSet(newBucket, entry.key(), entry.val());
              } else {
                bucketSet(newBucket, entry.key(), entry.val(), entry.size());
              }
            } else {
              bucketSet(newBucket, entry.key());
            }
            moved++;
          } else {
            // not moving this one, either invalid or we're full
            // call evict (or delete if invalid) callback
            if (removeCb_) {
              detail::callRemoveCb<SelfType>(
                  removeCb_, entry.key(), entry.val(), remove_context);
            }
          }
          entry.next();
        } else {
          // on the second pass we run the deletes, to avoid
          // read requests seeing no data in the interim
          // actually delete here
          BucketDescriptor::del(entry);
          // no entry.next() call as del advances ptr
        }
      }
      XDCHECK(newNumChunks <= oldNumChunks || !entry);
    }
  }
}

template <typename C, typename A, typename B>
void CompactCache<C, A, B>::resize() {
  const size_t oldNumChunks = numChunks_;
  const size_t configuredSize = allocator_.getConfiguredSize();
  const size_t numChunksWanted = configuredSize / allocator_.getChunkSize();

  /* No change in size */
  if (oldNumChunks == numChunksWanted) {
    return;
  }

  /* Lock resize operations to prevent more than one from occurring
   * at a time */
  auto lock = std::unique_lock(resizeLock_);

  size_t newNumChunks = numChunksWanted;

  if (numChunksWanted > oldNumChunks) {
    /* Grow the cache. */
    newNumChunks = allocator_.resize();
    /* Check to see if we were able to get the requested number of chunks */
    if (newNumChunks != numChunksWanted) {
      if (newNumChunks == oldNumChunks) {
        XLOG(CRITICAL) << "Failed to grow arena. Continuing with old size.";
        return;
      } else {
        XLOG(CRITICAL) << "Failed to grow arena by as much as we wanted.";
      }
    }
  }

  /* Update the number of chunks so new write requests start hitting both
   * old and new chunks */
  XDCHECK_NE(newNumChunks, oldNumChunks);
  XDCHECK_EQ(pendingNumChunks_.load(), 0u);
  /* only bother resharding if not going from/to 0 size */
  if (newNumChunks > 0 && oldNumChunks > 0) {
    pendingNumChunks_ = newNumChunks;
    /* Ensure all writes are double writing now */
    cohort_.switchCohorts();

    /* Sweep through the table and find all buckets that should now rehash.
     * In offline mode, move entries; online we just delete
     * to avoid invalidate races
     */
    tableRehash(oldNumChunks, newNumChunks, RehashOperation::COPY);

    // now that rehash happened, we can start reading from the new location
    // don't yet stop double writes as that ordering (no double write, read
    // from old location) may not be guaranteed
    // It likely is fine as those are ordered by a lock which should order
    // the loads of the num_chunks value properly
    numChunks_ = newNumChunks;
    cohort_.switchCohorts();

    // now stop double writes and wait for all requests to complete
    // so that the old chunk locations are totally unused
    pendingNumChunks_ = 0;
    cohort_.switchCohorts();

    // now go through again and delete stale entries in the old location
    tableRehash(oldNumChunks, newNumChunks, RehashOperation::DELETE);
  } else {
    numChunks_ = newNumChunks;

    // If we are disabling the compact cache (making the size to 0), we need to
    // make sure all requests are completed before we can release the chunks
    cohort_.switchCohorts();
  }

  // free slabs if we have extra
  if (newNumChunks < oldNumChunks) {
    allocator_.resize();
  }
}

template <typename C, typename A, typename B>
CCacheReturn CompactCache<C, A, B>::set(
    const Key& key,
    const std::chrono::microseconds& timeout,
    const Value* val,
    size_t size) {
  int rv = callBucketFn(
      key, Operation::WRITE, timeout, &SelfType::bucketSet, val, size);
  UPDATE_STATS_AND_RETURN(set, rv);
}

template <typename C, typename A, typename B>
CCacheReturn CompactCache<C, A, B>::del(
    const Key& key,
    const std::chrono::microseconds& timeout,
    Value* val,
    size_t* size) {
  int rv = callBucketFn(
      key, Operation::WRITE, timeout, &SelfType::bucketDel, val, size);
  UPDATE_STATS_AND_RETURN(del, rv);
}

template <typename C, typename A, typename B>
CCacheReturn CompactCache<C, A, B>::get(
    const Key& key,
    const std::chrono::microseconds& timeout,
    Value* val,
    size_t* size,
    bool shouldPromote) {
  int rv = callBucketFn(key,
                        Operation::READ,
                        timeout,
                        &SelfType::bucketGet,
                        val,
                        size,
                        shouldPromote);
  UPDATE_STATS_AND_RETURN(get, rv);
}

template <typename C, typename A, typename B>
CCacheReturn CompactCache<C, A, B>::exists(
    const Key& key, const std::chrono::microseconds& timeout) {
  int rv = callBucketFn(key,
                        Operation::READ,
                        timeout,
                        &SelfType::bucketGet,
                        nullptr /* val */,
                        nullptr /* size */,
                        false /* shouldPromote */);
  UPDATE_STATS_AND_RETURN(get, rv);
}

template <typename C, typename A, typename B>
bool CompactCache<C, A, B>::purge(const PurgeFilter& shouldPurge) {
  auto bucketCallback = [&](Bucket* bucket) {
    EntryHandle entry = BucketDescriptor::first(bucket);
    while (entry) {
      auto rv = shouldPurge(entry.key(), kHasValues ? entry.val() : nullptr);
      if (rv == PurgeFilterResult::ABORT) {
        return false;
      } else if (rv == PurgeFilterResult::SKIP) {
        entry.next();
        continue;
      }

      if (removeCb_) {
        detail::callRemoveCb<SelfType>(
            removeCb_, entry.key(), entry.val(), RemoveContext::kNormal);
      }
      BucketDescriptor::del(entry);

      /* No need to call entry.next() here because
       * BucketDescriptor::del advances the entry. */
    }
    return true;
  };

  const bool rv = forEachBucket(bucketCallback);
  if (rv) {
    ++stats_.tlStats().purgeSuccess;
  } else {
    ++stats_.tlStats().purgeErr;
  }
  return rv;
}

template <typename C, typename A, typename B>
template <typename Fn, typename... Args>
int CompactCache<C, A, B>::callBucketFn(
    const Key& key,
    Operation op,
    const std::chrono::microseconds& timeout,
    Fn f,
    Args... args) {
  if (numChunks_ == 0) {
    return -1;
  }

  /* 1) Increase the refcount of the current cohort. */
  Cohort::Token tok = cohort_.incrActiveReqs();

  /* 2) Find the hash table bucket for the key. */
  Bucket* bucket = tableFindBucket(key);

  /* 3) Lock the bucket. Immutable bucket is a parameter
   * regarding whether we're allowed to modify the bucket in any way,
   * meaning we take an exclusive lock, or not, meaning we take a
   * shared lock for reads without promotion. We may need to promote
   * in a second pass in the latter case. */
  BucketReturn rv;
  bool immutable_bucket = (op == Operation::READ);

  /* 4) Call the request handler. */
  if (immutable_bucket) {
    auto lock = locks_.lockShared(timeout, bucket);
    if (!lock.owns_lock()) {
      XDCHECK(timeout > std::chrono::microseconds::zero());
      ++stats_.tlStats().lockTimeout;
      return -2;
    }

    rv = (this->*f)(bucket, key, args...);
  } else {
    auto lock = locks_.lockExclusive(timeout, bucket);
    if (!lock.owns_lock()) {
      XDCHECK(timeout > std::chrono::microseconds::zero());
      ++stats_.tlStats().lockTimeout;
      return -2;
    }

    rv = (this->*f)(bucket, key, args...);
  }

  /* 5.5) Promote if necessary from a read operation */
  if (UNLIKELY(rv == BucketReturn::PROMOTE)) {
    XDCHECK(immutable_bucket);
    XDCHECK_EQ(op, Operation::READ);

    rv = BucketReturn::FOUND;

    if (allowPromotions_) {
      auto lock = locks_.lockExclusive(timeout, bucket);
      if (!lock.owns_lock()) {
        XDCHECK(timeout > std::chrono::microseconds::zero());
        ++stats_.tlStats().promoteTimeout;
      } else {
        this->bucketPromote(bucket, key);
      }
    }
  }
  XDCHECK(rv != BucketReturn::PROMOTE);

  /* 6) Do the operation on the new location if necessary
   * Note that although we release the old lock, the order is very
   * important. The rehasher will be doing lock old -> read old ->
   * lock new -> write new -> unlock both
   * So, we need to make sure that our old update either happens
   * before the rehasher reads, or if it happens after then the new update
   * also happens after. IE new update has to be second.
   */
  Bucket* bucketDbl =
      (op == Operation::WRITE) ? tableFindDblWriteBucket(key) : nullptr;

  if (bucketDbl != nullptr) {
    auto lockDbl = locks_.lockExclusive(bucketDbl);
    if ((this->*f)(bucketDbl, key, args...) == BucketReturn::ERROR) {
      rv = BucketReturn::ERROR;
    }
  }
  XDCHECK_NE(toInt(rv), 2);
  return toInt(rv);
}

template <typename C, typename A, typename B>
typename CompactCache<C, A, B>::Bucket* CompactCache<C, A, B>::tableFindChunk(
    size_t numChunks, const Key& key) {
  XDCHECK_GT(numChunks, 0u);
  XDCHECK_LE(numChunks, allocator_.getNumChunks());

  /* furcHash is well behaved; numChunks <= 1 returns 0 for chunkIndex */
  auto chunkIndex = facebook::cachelib::furcHash(
      reinterpret_cast<const void*>(&key), sizeof(key), numChunks);
  return reinterpret_cast<Bucket*>(allocator_.getChunk(chunkIndex));
}

template <typename C, typename A, typename B>
typename CompactCache<C, A, B>::Bucket* CompactCache<C, A, B>::tableFindBucket(
    const Key& key) {
  Bucket* chunk = tableFindChunk(numChunks_, key);

  uint32_t hv = MurmurHash2()(reinterpret_cast<const void*>(&key), sizeof(key));
  return &chunk[hv % bucketsPerChunk_];
}

template <typename C, typename A, typename B>
typename CompactCache<C, A, B>::Bucket*
CompactCache<C, A, B>::tableFindDblWriteBucket(const Key& key) {
  // this var is volatile and can be 0, which will cause
  // asserts later; avoid this case
  size_t pending = pendingNumChunks_;
  if (pending == 0) {
    return nullptr;
  }
  Bucket* chunk = tableFindChunk(numChunks_, key);
  Bucket* chunkNew = tableFindChunk(pending, key);
  if (chunk == chunkNew) {
    return nullptr;
  }
  uint32_t hv = MurmurHash2()(reinterpret_cast<const void*>(&key), sizeof(key));
  return &chunkNew[hv % bucketsPerChunk_];
}

template <typename C, typename A, typename B>
void CompactCache<C, A, B>::onEntryEvicted(const EntryHandle& handle) {
  ++stats_.tlStats().evictions;
  XDCHECK(handle);
  if (removeCb_) {
    RemoveContext c = validCb_ && !validCb_(handle.key())
                          ? RemoveContext::kNormal
                          : RemoveContext::kEviction;
    detail::callRemoveCb<SelfType>(removeCb_, handle.key(), handle.val(), c);
  }
}

template <typename C, typename A, typename B>
typename CompactCache<C, A, B>::BucketReturn CompactCache<C, A, B>::bucketSet(
    Bucket* bucket, const Key& key, const Value* val, size_t size) {
  if ((kHasValues && (val == nullptr)) ||
      (kValuesFixedSize && size != 0 && size != sizeof(Value))) {
    // val should not be null if kHasValues is true
    // size should be 0 if kValuesFixedSize is true
    return BucketReturn::ERROR;
  }

  auto evictionCallback =
      std::bind(&SelfType::onEntryEvicted, this, std::placeholders::_1);

  /* Look for an existing entry to be updated. */
  EntryHandle entry = bucketFind(bucket, key);
  if (!entry) {
    int rv = BucketDescriptor::insert(bucket, key, val, size, evictionCallback);

    if (rv == -1) {
      /* An error occured when inserting. */
      XLOG(ERR) << "Unable to insert entry in compact cache.";
      return BucketReturn::ERROR;
    }

    /* This is a miss meaning we inserted something new */
    return BucketReturn::NOTFOUND;
  } else {
    if (replaceCb_) {
      detail::callReplaceCb<SelfType>(replaceCb_, key, entry.val(), val);
    }
    /* Update the value of the already existing entry. */
    BucketDescriptor::updateVal(entry, val, size, evictionCallback);

    /* This is a hit. */
    return BucketReturn::FOUND;
  }
}

template <typename C, typename A, typename B>
typename CompactCache<C, A, B>::BucketReturn CompactCache<C, A, B>::bucketDel(
    Bucket* bucket, const Key& key, Value* val, size_t* size) {
  EntryHandle entry = bucketFind(bucket, key);
  if (!entry) {
    return BucketReturn::NOTFOUND;
  }

  if (kHasValues && val != nullptr) {
    BucketDescriptor::copyVal(val, size, entry);
  }
  if (removeCb_) {
    detail::callRemoveCb<SelfType>(
        removeCb_, key, entry.val(), RemoveContext::kNormal);
  }
  BucketDescriptor::del(entry);

  return BucketReturn::FOUND;
}

template <typename C, typename A, typename B>
typename CompactCache<C, A, B>::BucketReturn CompactCache<C, A, B>::bucketGet(
    Bucket* bucket,
    const Key& key,
    Value* val,
    size_t* size,
    bool shouldPromote) {
  EntryHandle entry = bucketFind(bucket, key);
  if (!entry) {
    return BucketReturn::NOTFOUND;
  }

  if (kHasValues && val != nullptr) {
    BucketDescriptor::copyVal(val, size, entry);
  }

  if (entry.isBucketTail()) {
    ++stats_.tlStats().tailHits;
  }

  return shouldPromote && BucketDescriptor::needs_promote(entry)
             ? BucketReturn::PROMOTE
             : BucketReturn::FOUND;
}

template <typename C, typename A, typename B>
typename CompactCache<C, A, B>::BucketReturn
CompactCache<C, A, B>::bucketPromote(Bucket* bucket, const Key& key) {
  EntryHandle entry = bucketFind(bucket, key);
  if (UNLIKELY(!entry)) {
    return BucketReturn::NOTFOUND;
  }
  if (BucketDescriptor::needs_promote(entry)) {
    BucketDescriptor::promote(entry);
  }
  return BucketReturn::FOUND;
}

/** This iterates on all the entries in the bucket and compare their keys
 *  with the key until a match is found. */
template <typename C, typename A, typename B>
typename CompactCache<C, A, B>::EntryHandle CompactCache<C, A, B>::bucketFind(
    Bucket* bucket, const Key& key) {
  for (EntryHandle handle = BucketDescriptor::first(bucket); handle;
       handle.next()) {
    if (handle.key() == key) {
      /* Entry found. */
      return handle;
    }
  }

  /* Entry not found. */
  return EntryHandle();
}

template <typename C, typename A, typename B>
bool CompactCache<C, A, B>::forEachBucket(const BucketCallBack& cb) {
  auto lock = std::shared_lock(resizeLock_);

  // this obtains a resize lock so it cannot be occuring during an actual
  // resize; assert that
  XDCHECK_EQ(pendingNumChunks_.load(), 0u);

  /* Loop through all buckets in the table. */
  for (size_t n = 0; n < numChunks_; n++) {
    Bucket* tableChunk = reinterpret_cast<Bucket*>(allocator_.getChunk(n));
    for (size_t i = 0; i < bucketsPerChunk_; i++) {
      /* Lock the bucket. */
      Bucket* bucket = &tableChunk[i];
      auto bucketLock = locks_.lockExclusive(bucket);

      if (!cb(bucket)) {
        return false;
      }
    }
  }

  return true;
}
} // namespace facebook::cachelib
