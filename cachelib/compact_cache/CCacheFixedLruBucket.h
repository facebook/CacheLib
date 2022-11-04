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
 * This file implements a bucket management that uses entries of a fixed size
 * that are stored contiguously in the bucket (linear probing).
 *
 * When an entry is added, the entries are shifted down one position and the new
 * entries is written in the first position. If the last position contained an
 * entry, it is evicted.
 *
 * This implementation provides an lru mechanism by moving the promoted entry to
 * the top and shifting all the entries that were above it down one position.
 */

namespace facebook {
namespace cachelib {

template <typename CompactCacheDescriptor, unsigned EntriesPerBucket>
struct FixedLruBucket {
 public:
  using Descriptor = CompactCacheDescriptor;
  using ValueDescriptor = typename Descriptor::ValueDescriptor;
  using Key = typename Descriptor::Key;
  using Value = typename ValueDescriptor::Value;

  constexpr static int kEntriesPerBucket = EntriesPerBucket;
  constexpr static bool kHasValues = Descriptor::kHasValues;

  static_assert(Descriptor::kValuesFixedSize,
                "This bucket descriptor must be used with values of a fixed"
                "size");

  /** Type of the data stored in a bucket entry.
   *  This contains the key and the value (if any). */
  struct Entry {
    Key key;
    /* Expands to NoValue (size 0) if this cache does not store values */
    Value val;
  } __attribute__((__packed__));

  /** Type of a bucket.
   * Empty entry slots must be zeroed out to avoid spurious matches! */
  struct Bucket {
    Entry entries[kEntriesPerBucket];
  } __attribute__((__packed__));

  static_assert(sizeof(Entry) == sizeof(Key) + sizeof(Value),
                "Entry packing went awry");
  static_assert(sizeof(Bucket) == kEntriesPerBucket * sizeof(Entry),
                "Bucket packing went awry");

  /**
   * Handle to an entry. This class provides the compact cache implementation
   * with a way to keep a reference to an entry in a bucket as well as a way
   * to iterate over each valid entries without being aware of the underlying
   * bucket implementation.
   * This basically contains a pointer to the bucket and the entry offset.
   *
   * Example, iterate over the valid entries in a bucket:
   *
   *   FixedLruBucket<C>::EntryHandle h = FixedLruBucket<C>::first(myBucket);
   *   while (h) {
   *       // h refers to a valid entry in the bucket.
   *       // can use h.key(), h.val() to access content of the entry.
   *       h.next();
   *   }
   */
  class EntryHandle {
   public:
    /** Return true if the handle is valid, i.e it points to an non-empty
     * entry or we went passed the last entry. */
    explicit operator bool() const {
      return pos_ >= 0 && pos_ < kEntriesPerBucket &&
             !bucket_->entries[pos_].key.isEmpty();
    }
    /** Move to the next entry. The handle will become invalid after
     * reaching the last entry, or when reaching an empty entry.
     * Must be called on a valid handle. */
    void next() {
      XDCHECK(*this);
      ++pos_;
    }

    Key key() const { return bucket_->entries[pos_].key; }
    Value* val() const { return &bucket_->entries[pos_].val; }
    constexpr size_t size() const { return sizeof(Value); }

    EntryHandle() : bucket_(nullptr), pos_(-1) {}
    EntryHandle(Bucket* bucket, int pos) : bucket_(bucket), pos_(pos) {}

    bool isBucketTail() const { return *this && pos_ == kEntriesPerBucket - 1; }

   private:
    Bucket* bucket_;
    int pos_;
    friend struct FixedLruBucket<CompactCacheDescriptor, EntriesPerBucket>;
  };

  /** Type of the callback to be called when an entry is evicted. */
  using EvictionCb = std::function<void(const EntryHandle& handle)>;

  /**
   * Return a handle to the first entry in the bucket.
   *
   * @param bucket Bucket from which to retrieve a handle to the first entry.
   * @return       Handle to the first entry, or invalid handle if the bucket
   *               is empty.
   */
  static EntryHandle first(Bucket* bucket) {
    /* If the first entry is empty, this creates an invalid handle. */
    return EntryHandle(bucket, 0);
  }

  /**
   * Return capacity of this bucket in number of entries
   * @param bucket
   * @return number of entries this bucket can hold when filled
   */
  static uint32_t nEntriesCapacity(const Bucket& /*bucket*/) {
    return kEntriesPerBucket;
  }

  /*
   * Insert a new entry in a bucket.
   * The entry is written in the first position after we shift all the entries
   * down one position, evicting the last one if any.
   *
   * @param bucket         Bucket in which to insert the new entry.
   * @param key            key of the new entry.
   * @param val            Pointer to a value to be copied in the new
   *                       entry. Unused if Value Type is NoValue.
   * @param size           Unused because the size of values is already known
   *                       in a compact cache that stores values of a fixed
   *                       size.
   * @param evictionCb     Callback to be called for when an entry is evicted.
   *                       Cannot be empty.
   * @return               1 if an entry was evicted, 0 otherwise.
   */
  static bool insert(Bucket* bucket,
                     const Key& key,
                     const Value* val,
                     size_t,
                     EvictionCb evictionCb) {
    bool evicted = false;

    /* The last position is the LRU. If its key is not zero (i.e there is
     * an entry at the position), evict it. */
    if (!bucket->entries[kEntriesPerBucket - 1].key.isEmpty()) {
      /* We need to evict an entry. */
      XDCHECK(evictionCb);
      evictionCb(EntryHandle(bucket, kEntriesPerBucket - 1));
      evicted = true;
    }

    /* Slide all the entries down one position. */
    memmove(&bucket->entries[1],
            &bucket->entries[0],
            sizeof(Entry) * (kEntriesPerBucket - 1));

    /* Write our new entry in top of bucket. */
    memcpy(&bucket->entries[0].key, &key, sizeof(Key));
    if (kHasValues) {
      copyValue(&bucket->entries[0].val, val);
    }

    return evicted ? 1 : 0;
  }

  /**
   * Promote an entry.
   *
   * @param handle Handle of the entry to be promoted. Remains valid after
   *               this method returns, and points to the next location of the
   *               entry.
   */
  static void promote(EntryHandle& handle) {
    XDCHECK(handle);
    if (handle.pos_ != 0) {
      Entry winner = handle.bucket_->entries[handle.pos_];
      memmove(&handle.bucket_->entries[1],
              &handle.bucket_->entries[0],
              sizeof(Entry) * handle.pos_);
      memcpy(&handle.bucket_->entries[0], &winner, sizeof(Entry));
      handle.pos_ = 0;
    }
  }

  static inline bool needs_promote(EntryHandle& handle) {
    XDCHECK(handle);
    if (handle.pos_ > kEntriesPerBucket / 4) {
      return true;
    }
    return false;
  }

  /**
   * Delete an entry.
   * We simply shift all the entries after it one position up.
   *
   * @param handle Handle of the entry to be deleted. After this method
   *               returns, the handle will point to the next entry, if any,
   *               or becomes invalid.
   */
  static void del(EntryHandle& handle) {
    XDCHECK(handle);
    memmove(&handle.bucket_->entries[handle.pos_],
            &handle.bucket_->entries[handle.pos_ + 1],
            sizeof(Entry) * (kEntriesPerBucket - handle.pos_ - 1));
    bzero(&handle.bucket_->entries[kEntriesPerBucket - 1], sizeof(Entry));
  }

  /**
   * Update the value of an entry.
   *
   * @param handle     Handle of the entry to be updated. Remains valid after
   *                   this function returns.
   * @param val        New value of the entry.
   * @param size       Unused because the size of values is already known in a
   *                   compact cache that stores values of a fixed size.
   * @param evictionCb Eviction callback to be called if this operation evicts
   *                   an entry. This is unused because this implementation
   *                   does not cause entries to be evicted when updating.
   */
  static void updateVal(EntryHandle& handle,
                        const Value* val,
                        size_t,
                        EvictionCb /*evictionCb*/) {
    if (kHasValues) {
      XDCHECK(val);
      copyValue(handle.val(), val);
    }
  }

  /**
   * Copy a an entry's value to a buffer.
   * @param val    Buffer in which to copy the entry's value.
   * @param size   Unused because the size of values is already known in a
   *               compact cache that stores values of a fixed size.
   * @param handle Entry from which to copy the value.
   */
  static void copyVal(Value* val, size_t*, EntryHandle& handle) {
    XDCHECK(handle);
    XDCHECK(val);
    copyValue(val, handle.val());
  }

 private:
  /**
   * Copy a value from one buffer to another.
   * The caller should ensure that this is called with non NULL values.
   * The source and destination buffers must not overlap.
   *
   * otherwise this function will assert.
   * @param destPtr Pointer to the destination buffer.
   * @param srcPtr  Pointer to the source buffer.
   */
  template <typename T>
  static void copyValue(T* destPtr, const T* srcPtr) {
    XDCHECK(destPtr != nullptr);
    XDCHECK(srcPtr != nullptr);
    memcpy(destPtr, srcPtr, sizeof(T));
  }
};
} // namespace cachelib
} // namespace facebook
