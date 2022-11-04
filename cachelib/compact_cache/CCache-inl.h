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

/**
 * This file implements the methods defined in CCache.h
 */

#include <folly/logging/xlog.h>

#include <typeinfo>

#include "cachelib/common/Hash.h"

/****************************************************************************/
/* CONFIGURATION */

namespace facebook {
namespace cachelib {

/****************************************************************************/
/* FORWARD DECLARATIONS */

namespace detail {

/****************************************************************************/
/* IMPLEMENTATION */

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
            auto higher_lock =
                sameLock ? CCWriteHolder() : locks_.lockExclusive(newBucket);
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
  auto lock = folly::SharedMutex::WriteHolder(resizeLock_);

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

/****************************************************************************/
/* IMPLEMENTATION */

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
    if (!lock.locked()) {
      XDCHECK(timeout > std::chrono::microseconds::zero());
      ++stats_.tlStats().lockTimeout;
      return -2;
    }

    rv = (this->*f)(bucket, key, args...);
  } else {
    auto lock = locks_.lockExclusive(timeout, bucket);
    if (!lock.locked()) {
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
      if (!lock.locked()) {
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

  using namespace std::placeholders;
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
  auto lock = folly::SharedMutex::ReadHolder(resizeLock_);

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
} // namespace cachelib
} // namespace facebook
