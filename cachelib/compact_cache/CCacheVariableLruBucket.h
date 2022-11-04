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

#include <folly/logging/xlog.h>

#include <algorithm>
#include <utility>

/**
 * This file implements a bucket management for variable sized objects that is
 * highly optimized for read operations and has a 4 byte overhead per entry + 3
 * bytes per bucket.
 *
 * +---------------+---------------+-----------------------------------------+
 * |               |               |                                         |
 * |               |               |                                         |
 * | Entry headers |    Empty      |                  Data                   |
 * |               |               |                                         |
 * |               |               |                                         |
 * +---------------+---------------+-----------------------------------------+
 *
 * In this implementation, the bucket is split in three sections:
 *
 * 1) Entry headers.
 *    This section contains the entries' headers (EntryHdr). There is one
 *    entry header per entry in the bucket. Each entry header contains the
 *    following data:
 *    - key: the key of the entry;
 *    - dataSize: the size of the entry's value.
 *    - dataOffset: the position where to find the entry's value in the "Data"
 *      part of the bucket.
 *    The number of entries is given by the "numEntries" field of Bucket.
 *
 * 2) Data.
 *    This section contains the entries' values (EntryData). Each
 *    EntryData field contains:
 *    - headerIndex: the position of the corresponding entry header in the
 *      "Entry headers" section.
 *    - data: the value's payload. The size of this payload is determined by the
 *      'dataSize' field of the corresponding entry header.
 *    This section has no 'holes', i.e all the entries are contiguous. The size
 *    of the data section is given by the "totalDataSize" field of Bucket.
 *
 * 3) Empty.
 *    This section shrinks/expands as entries are added/removed.
 *
 * The position of the entry headers in the "Entry headers" section determines
 * the age of the entries. The right-most entry is the LRU, the left-most is the
 * MRU. When an entry is promoted, the entry headers are shifted appropriately
 * so that the promoted entry ends up to the left. For each entry affected by
 * the shifting, the corrresponding EntryData in the "Data" section has its
 * headerIndex field updated accordingly.
 *
 * When an entry is inserted, we check if the "Empty" section is big enough to
 * hold both the new EntryHdr and the EntryData. If it is not big enough,
 * we compute how many entries need to evicted in order to make enough space.
 * The eviction algorithm is more expensive. Even though selecting the entries
 * to be evicted is straightforward as you only need to browse them from right
 * to left in the "Entry headers" section, removing them from the "Data" section
 * is not trivial. Each 'block' of entries that are between two evicted entries
 * are shifted to the right in order to expand the size of the "Empty" section.
 * Once the necessary items have been evicted and the "Empty" section is big
 * enough, the new entry's header is added to the "Entry headers" section, and
 * its data is written at the beginning of the "Data" section.
 *
 * When an entry is deleted, the entry headers that follow the deleted entry's
 * header are shifted to the left, and we update the headerIndex of the related
 * EntryData. The EntryData objects that precede the deleted EntryData
 * are shifted to the right. Deleting an entry expands the size of the "Empty
 * section" by adding sizeof(EntryHdr) + sizeof(EntryData) + the amount of
 * bytes in the payload.
 *
 * When an entry is updated, if the size does not change the EntryData is
 * updated in place. If the new size is smaller, the end of the data is the
 * same, but the beginning is moved to the right. This generates a space into
 * which the preceding data is moved. If the new size is bigger, we fall back
 * to deleting the entry and adding it again.
 */

namespace facebook {
namespace cachelib {

template <typename CompactCacheDescriptor>
struct VariableLruBucket {
 public:
  using Descriptor = CompactCacheDescriptor;
  using ValueDescriptor = typename Descriptor::ValueDescriptor;
  using Key = typename Descriptor::Key;
  using Value = typename ValueDescriptor::Value;

  static_assert(Descriptor::kHasValues,
                "This bucket descriptor must be used for compact caches with"
                "values.");
  static_assert(!Descriptor::kValuesFixedSize,
                "This bucket descriptor must be used with values of a"
                "variable size.");

  /** Type of the integral that contains the the size of an entry's data.
   * (see dataSize in EntryHdr). */
  using EntryDataSize = uint8_t;
  /** Type of the integral that gives an offset (in bytes), starting from the
   * 'data' field of Bucket. This is used to get the start offset of an
   * EntryData object (see dataOffset in EntryHdr), and the total data
   * size (in bytes) of a bucket (i.e the size of the 'Data' section). */
  using EntryDataOffset = uint16_t;
  /** Type of the integral that gives the index of an EntryHdr header in
   * the 'Entry Headers' section (see headerIndex in EntryData), and the
   * total number of entries (see numEntries in Bucket). */
  using EntryNum = uint8_t;

  constexpr static size_t kMaxValueSize = ValueDescriptor::kMaxSize;

  /** Maximum number of entries per bucket. The number of entries must be
   * small enough to fit in a value of type EntryNum, so take the max
   * possible value here. */
  constexpr static size_t kMaxEntries = std::numeric_limits<EntryNum>::max();

  /** An entry header. */
  struct EntryHdr {
    Key key;
    /* Size in bytes of the corresponding EntryData's data field. */
    EntryDataSize dataSize;
    /* Offset (in bytes) where to find the corresponding EntryData,
     * measured from the beginning of the bucket's data field. */
    EntryDataOffset dataOffset;
  } __attribute__((__packed__));

  struct EntryData {
    /* Index of the corresponding EntryHdr header in the 'Entry
     * Headers' section. This is a value between 0 and the number of entries
     * in the bucket. */
    EntryNum headerIndex;
    /* Beginning of the entry's data. The size of this data is determined by
     * the 'dataSize' field of the corresponding EntryHdr. */
    char data[0];
  } __attribute__((__packed__));

  /** Compute the size of the bucket so that we can guarantee that it will be
   * big enough to host an entry of the maximum size provided by the user. */
  constexpr static size_t kBucketDataSize =
      kMaxValueSize + sizeof(EntryHdr) + sizeof(EntryData);

  struct Bucket {
    /* Number of entries in the bucket. */
    EntryNum numEntries;
    /* Size of the "Data" section.
     * The free space in the bucket is given by computing:
     * kBucketDataSize - totalDataSize - sizeof(EntryHdr) * numEntries.
     */
    EntryDataOffset totalDataSize;
    /* Content of the bucket. This contains the three sections described in
     * this file's documentation. */
    char data[kBucketDataSize];
  } __attribute__((__packed__));

  /** This is to make sure that the 'totalDataSize' field of Bucket will
   * never overflow. */
  static_assert(kBucketDataSize <= std::numeric_limits<EntryDataOffset>::max(),
                "Bucket is too big");

  /**
   * Handle to an entry. This class provides the compact cache implementation
   * with a way to keep a reference to an entry in a bucket as well as a way
   * to iterate over each valid entries without being aware of the underlying
   * bucket implementation.
   * This basically contains a pointer to the bucket and the index of the
   * entry in the array of entry headers.
   *
   * Example, iterate over the valid entries in a bucket:
   *
   *   LogStoreBucket<C>::EntryHandle h = LogStoreBucket<C>::first(myBucket);
   *   while (h) {
   *       // h refers to a valid entry in the bucket.
   *       // can use h.key(), h.val() to access content
   *       // of the entry.
   *       h.next();
   *   }
   */
  class EntryHandle {
   public:
    explicit operator bool() const {
      return bucket_ && 0 <= pos_ && pos_ < bucket_->numEntries;
    }
    void next() {
      XDCHECK(*this);
      ++pos_;
    }
    Key key() const { return getEntry()->key; }
    constexpr size_t size() const { return getEntry()->dataSize; }

    Value* val() const {
      return reinterpret_cast<Value*>(getEntryData(bucket_, pos_)->data);
    }

    EntryHandle() : bucket_(nullptr), pos_(-1) {}
    EntryHandle(Bucket* bucket, int pos) : bucket_(bucket), pos_(pos) {}

    bool isBucketTail() const {
      return *this && pos_ == bucket_->numEntries - 1;
    }

   private:
    EntryHdr* getEntry() const { return getEntryHeader(bucket_, pos_); }

    Bucket* bucket_;
    int pos_;

    friend class VariableLruBucket<CompactCacheDescriptor>;
  };

  /** Type of the callback to be called when an entry is evicted. */
  using EvictionCb = std::function<void(const EntryHandle& handle)>;

  /**
   * Get a handle to the first entry in the bucket.
   *
   * @param bucket Bucket from which to retrieve the handle.
   * @return Handle to the first entry.
   */
  static EntryHandle first(Bucket* bucket) {
    /* If bucket->numEntries is 0, the created handle is invalid. */
    return EntryHandle(bucket, 0);
  }

  /**
   * Return the total number of items this bucket could hold.
   * Due to variable size this is imprecise; extrapolate capacity
   * by dividing current # of items by current fractional memory
   * in use
   * @param bucket Bucket to find out the number of entries it could hold
   * @return number of entries this bucket can hold (approx)
   */
  static uint32_t nEntriesCapacity(const Bucket& bucket) {
    const size_t n = bucket.numEntries;
    const size_t sz = bucket.totalDataSize;
    XDCHECK_LE(sz + sizeof(EntryHdr) * n, kBucketDataSize);
    if (n == 0) {
      return 0;
    }
    return (n * kBucketDataSize) / (sz + sizeof(EntryHdr) * n);
  }

  /**
   * Insert a new entry in a bucket.
   *
   * 1) Compute the required space for our entry.
   * 2) Call evictEntries, which takes care of evicting as many entries as
   *    required in order to have the necessary space.
   * 3) Allocate a new spot in the "Empty" section by increasing
   *    bucket->totalDataSize.
   * 4) Write the entry's data (EntryData) in the new spot.
   * 5) Update the headerIndex of all the existing EntryData objects
   *    since their header is about to get shifted one position to the
   *    right. This step is the aging process of the entries.
   * 6) Shift all the EntryHdr headers to the right so that we can write
   *    our new entry header in the first position.
   * 7) Write the entry's header (EntryHdr).
   * 8) Set the offsets in both the EntryHdr header and the EntryData so
   *    that they have a reference to each other.
   * 9) Update the number of entries in the bucket.
   *
   * @param bucket        Bucket in which to insert
   * @param key           key of the new entry.
   * @param val           Value to be inserted.
   * @param size          Size of the value to be inserted. The size must be
   *                      smaller than or equal to the maximum value size
   *                      described by the value descriptor, or else this
   *                      function will assert because the bucket might not be
   *                      large enough for the value.
   * @param evictionCb    Callback to be called for when an entry is evicted.
   *                      Cannot be empty.
   * @return              0 if no item was evicted, 1 if at least one item
   *                      was evicted, -1 on error (the given size was too
   *                      big).
   */
  static int insert(Bucket* bucket,
                    const Key& key,
                    const Value* val,
                    size_t size,
                    EvictionCb evictionCb) {
    /* The caller should ensure that the value is small enough to fit in
     * the bucket. */
    XDCHECK_LE(size, kMaxValueSize);
    if (size > kMaxValueSize) {
      XLOG(ERR) << "Cannot insert an value of size " << size
                << ", the size must be smaller than " << kMaxValueSize;
      return -1;
    }

    /* Make sure that EntryDataSize is wide enough to hold the given
     * size. */
    checkOverflow<EntryDataOffset>(size);
    if (size > std::numeric_limits<EntryDataSize>::max()) {
      XLOG(ERR)
          << "Cannot insert an value of size " << size
          << ", the size must be smaller than the max value of EntryDataSize";
      return -1;
    }

#ifndef NDEBUG
    checkBucketConsistency(bucket);
#endif

    /* 1) Compute the required space for our entry. */
    size_t requiredSpace = size + sizeof(EntryHdr) + sizeof(EntryData);

    /* 2) Call evictEntries, which takes care of evicting as many entries as
     * required in order to have the necessary space. */
    bool evicted = evictEntries(bucket, requiredSpace, evictionCb);

#ifndef NDEBUG
    /* EvictEntries should leave the bucket in a consistent state. */
    checkBucketConsistency(bucket);
#endif

    /* 3) Allocate a new spot in the "Empty" section by increasing
     * bucket->totalDataSize. */
    checkOverflow<EntryDataOffset>(bucket->totalDataSize + size +
                                   sizeof(EntryData));
    bucket->totalDataSize += size + sizeof(EntryData);

    /* 4) Write the entry's data (EntryData) in the new spot. */
    EntryData* newEntryData = getFirstEntryData(bucket);
    memcpy(newEntryData->data, val, size);

    /* 5) Update the headerIndex of all the existing EntryData objects
     * since their header is about to get shifted one position to the
     * right. */
    for (unsigned int i = 0; i < bucket->numEntries; i++) {
      getEntryData(bucket, i)->headerIndex++;
    }

    /* 6) Shift all the EntryHdr headers to the right so that we can
     * write our new entry header in the first position. */
    memmove(getEntryHeader(bucket, 1),
            getEntryHeader(bucket, 0),
            bucket->numEntries * sizeof(EntryHdr));

    /* 7) Write the entry's header (EntryHdr) */
    EntryHdr* newEntry = getEntryHeader(bucket, 0);
    memcpy(&newEntry->key, &key, sizeof(Key));
    newEntry->dataSize = size;

    /* 8) Set the offsets in both the EntryHdr header and the
     * EntryData so that they have a reference to each other. */
    newEntry->dataOffset = kBucketDataSize - bucket->totalDataSize;
    newEntryData->headerIndex = 0;

    /* 9) Update the number of entries in the bucket. */
    checkOverflow<EntryNum>(bucket->numEntries + 1);
    bucket->numEntries++;

#ifndef NDEBUG
    checkBucketConsistency(bucket);
#endif

    return evicted ? 1 : 0;
  }

  /**
   * Promote an entry.
   *
   * 1) Shift all the EntryHdr headers that precede the promoted
   *    EntryHdr one position to the right.
   * 2) Write the promoted entry to the first position.
   * 3) Update the headerIndex field of all EntryData objects for which
   *    the corresponding EntryHdr header was moved.
   *
   * @param handle Handle of the entry to be promoted. Remains valid after
   *               this call completes.
   */
  static void promote(EntryHandle& handle) {
    XDCHECK(handle);

    EntryHdr toPromote = *handle.getEntry();
    EntryHdr* firstEntry = getEntryHeader(handle.bucket_, 0);

    /* 1) Shift all the EntryHdr headers that precede the promoted
     * EntryHdr one position to the right. This is the aging process. */
    size_t shiftDistance = handle.pos_ * sizeof(EntryHdr);
    memmove(getEntryHeader(handle.bucket_, 1), firstEntry, shiftDistance);

    /* 2) Write the promoted entry to the first position. */
    memcpy(firstEntry, &toPromote, sizeof(EntryHdr));

    /* 3) Update the headerIndex field of all EntryData objects for
     * which the corresponding EntryHdr header was moved. */
    for (unsigned int i = 0; i <= handle.pos_; i++) {
      getEntryData(handle.bucket_, i)->headerIndex = i;
    }

    /* Modify handle so that it still points to the same entry. */
    handle.pos_ = 0;

#ifndef NDEBUG
    checkBucketConsistency(handle.bucket_);
#endif
  }

  /**
   * Whether an entry needs promotion. Do this if it's beyond the first
   * two. The ccache usually holds at least 8 items so this should be
   * reasonably safe.
   */
  static inline bool needs_promote(EntryHandle& handle) {
    XDCHECK(handle);
    return handle.pos_ > 1;
  }

  /**
   * Delete an entry.
   *
   * 1) Shift all the EntryData objects that precede the EntryData being
   *    deleted. Update the corresponding dataOffset fields in the EntryHdr
   *    headers for those moved entries.
   * 2) Update the headerIndex of all the EntryData objects that
   *   correspond to an EntryHdr that will be shifted to the left in 3).
   * 3) Shift all the EntryHdr headers that are after the deleted
   *    EntryHdr one position to the left.
   * 4) Reduce the total size of the bucket and decrement the number of
   *    entries in the bucket.
   *
   * @param handle Handle of the entry to be deleted. After this function
   *               completes, the handle points to the next valid entry or
   *               becomes invalid if no such entry.
   */
  static void del(EntryHandle& handle) {
    XDCHECK(handle);

    EntryDataOffset shiftDistance =
        handle.getEntry()->dataSize + sizeof(EntryData);
    EntryDataOffset shiftChunkSize =
        handle.getEntry()->dataOffset - getFirstEntryDataOffset(handle.bucket_);

    /* 1) Shift all the EntryData objects that precede the EntryData
     * object of the entry we are deleting. This function also takes care of
     * updating the dataOffset field of all the corresponding EntryHdr
     * headers so that these remain valid. */
    shiftEntriesData(handle.bucket_,
                     getFirstEntryData(handle.bucket_),
                     shiftDistance,
                     shiftChunkSize);

    /* 2) Update the headerIndex of all the EntryData objects that
     * correspond to an EntryHdr that will be shifted to the left in 3).
     */
    for (unsigned int i = handle.pos_ + 1; i < handle.bucket_->numEntries;
         i++) {
      getEntryData(handle.bucket_, i)->headerIndex--;
    }

    /* 3) Shift all the EntryHdr headers that are after the deleted
     * EntryHdr one position to the left. */
    if (handle.pos_ < handle.bucket_->numEntries - 1) {
      size_t delta = handle.bucket_->numEntries - handle.pos_ - 1;
      memmove(
          handle.getEntry(), handle.getEntry() + 1, delta * sizeof(EntryHdr));
    }

    /* 4) Reduce the total size of the bucket and decrement the number of
     * entries in the bucket. */
    handle.bucket_->totalDataSize -= shiftDistance;
    handle.bucket_->numEntries--;

#ifndef NDEBUG
    checkBucketConsistency(handle.bucket_);
#endif
  }

  /**
   * Update the value of an entry.
   *
   * There are three cases:
   *
   * 1/ The new size is equal to the old size:
   *    1) Copy the new data in place.
   *    2) Promote the entry.
   * 2/ The new size is smaller than the old size:
   *    1) Compute the new offset of the EntryData by moving the existing
   *       one to the right by the amount of bytes that makes the difference
   *       between the old size and the new size.
   *    2) Copy the new value at this offset.
   *    3) Shift the EntryData object that precede our updated entry so
   *       that we can expand the "Empty" section by the amount of bytes
   *       that are not used anymore by the entry.
   *    4) Update the dataOffset and dataSize fields of the EntryHdr.
   *    5) Update the total size of the bucket.
   *    6) Promote the entry.
   * 3/ The new size is bigger.
   *    1) Delete the old entry.
   *    2) Insert the entry again with the new size. We don't need to promote
   *       the entry here because insert will insert the entry in the MRU
   *       spot.
   *    3) Update the handle to point to the first position.
   *
   * @param handle     Handle to the entry to be updated. Remains valid after
   *                   this function returns.
   * @param val        New value of the entry.
   * @param size       Size of the new value.
   * @param evictionCb Eviction callback to be called when an entry is
   *                   evicted due to relocating the updated entry.
   */
  static void updateVal(EntryHandle& handle,
                        const Value* val,
                        size_t size,
                        EvictionCb evictionCb) {
    XDCHECK(handle);
    EntryHdr* existingEntry = handle.getEntry();
    EntryData* entryData = getEntryData(handle.bucket_, handle.pos_);

    if (existingEntry->dataSize == size) {
      /* The new size is equal to the old size. */

      /* 1) Copy the new data in place. */
      memcpy(entryData->data, val, size);

      /* 2) Promote the entry. */
      promote(handle);
    } else if (size < existingEntry->dataSize) {
      /* New data is smaller. Update the data in place starting at the new
       * offset and shift the EntryData objects that precede it. */

      /* 1) Compute the new offset of the EntryData by moving the
       * existing one to the right by the amount of bytes that makes the
       * difference between the old size and the new size. */
      const EntryDataOffset shiftDistance = existingEntry->dataSize - size;
      entryData = reinterpret_cast<EntryData*>(
          reinterpret_cast<char*>(entryData) + shiftDistance);

      /* 2) Copy the new value at this offset. */
      memcpy(entryData->data, val, size);
      entryData->headerIndex = handle.pos_;

      /* 3) Shift the EntryData object that precede our updated entry
       * so that we can expand the "Empty" section by the amount of bytes
       * that are not used anymore by the entry. */
      const size_t shiftChunkSize =
          existingEntry->dataOffset - getFirstEntryDataOffset(handle.bucket_);
      shiftEntriesData(handle.bucket_,
                       getFirstEntryData(handle.bucket_),
                       shiftDistance,
                       shiftChunkSize);

      /* 4) Update the dataOffset and dataSize fields of the
       * EntryHdr. */
      existingEntry->dataOffset += shiftDistance;
      existingEntry->dataSize = size;

      /* 5) Update the total size of the bucket. */
      handle.bucket_->totalDataSize -= shiftDistance;

      /* 6) Promote the entry. */
      promote(handle);

#ifndef NDEBUG
      checkBucketConsistency(handle.bucket_);
#endif
    } else {
      /* The new size is bigger. */
      XDCHECK_GT(size, existingEntry->dataSize);

      /* 1) Delete the old entry. */
      const EntryHdr copy = *existingEntry;
      del(handle);

      /* 2) Insert the entry again with the new size. */
      insert(handle.bucket_, copy.key, val, size, evictionCb);

      /* 3) Update the handle to point to the new position. */
      handle = first(handle.bucket_);
    }
  }

  /**
   * Copy a an entry's value to a buffer.
   * The buffer must be large enough to store the value, i.e the caller should
   * allocate a buffer of a size greater or equal to the maximum possible size
   * of a value in this compact cache.
   *
   * @param val    Buffer in which to copy the entry's value.
   * @param handle Handle of the entry from which to copy the value. Remains
   *               valid after this function returns.
   */
  static void copyVal(Value* val, size_t* size, const EntryHandle& handle) {
    XDCHECK(handle);
    XDCHECK(val);
    XDCHECK(size);
    *size = handle.size();
    const EntryData* entryData = getEntryData(handle.bucket_, handle.pos_);
    memcpy(val, entryData->data, *size);
  }

  constexpr static size_t maxValueSize() { return kMaxValueSize; }

 private:
  /**
   * Check that a bucket is consistent. This goes through all the entries and
   * checks the offsets to verify that they match between the EntryHdr
   * headers and the EntryData objects.
   * This also verifies that the total sum of the sizes of all the entries is
   * equal to the size of the "Data" section, and that the EntryData
   * objects are contiguous.
   *
   * This should be called after each operation when in debug mode in order to
   * verify that the operation leaves the bucket in a consistent state.
   *
   * @param bucket Bucket to be checked.
   */
  static void checkBucketConsistency(Bucket* bucket) {
    EntryHandle handle = first(bucket);
    size_t totalSize = 0;
    size_t nEntries = 0;
    bool headerOffsetError = false;

    /* Check that the data part does not overlap the entry headers. */
    XDCHECK_GE(reinterpret_cast<uintptr_t>(getFirstEntryData(bucket)),
               reinterpret_cast<uintptr_t>(
                   getEntryHeader(bucket, bucket->numEntries)));

    /* Keep track of the EntryData's offsets and sizes as we see then
     * when iterating on the EntryHdr headers. We will later use this to
     * check that the EntryData objects are contiguous. */
    using EntryInfo = std::pair<EntryDataOffset, EntryDataSize>;
    std::vector<EntryInfo> entryDataSeen;

    /* Iterate on the EntryHdr headers. */
    while (handle) {
      EntryData* entryData = getEntryData(handle.bucket_, handle.pos_);
      totalSize += handle.size() + sizeof(EntryData);
      /* The offset headers of the entries we are seeing should match
       * their order. */
      if (entryData->headerIndex != nEntries) {
        headerOffsetError = true;
      }

      EntryHdr* entryHeader = getEntryHeader(bucket, handle.pos_);
      EntryDataOffset dataOffset = entryHeader->dataOffset;
      EntryDataSize dataSize = entryHeader->dataSize;
      entryDataSeen.push_back(std::make_pair(dataOffset, dataSize));

      handle.next();
      nEntries++;
    }

    /* Sort entryDataSeen by offset, in increasing order. */
    auto compareFn = [](const EntryInfo& a, const EntryInfo& b) -> bool {
      return a.first < b.first;
    };
    std::sort(entryDataSeen.begin(), entryDataSeen.end(), compareFn);

    /* Check that the EntryData objects are contiguous. */
    bool dataOffsetError = false;
    EntryDataOffset expectedNextOffset = getFirstEntryDataOffset(bucket);
    for (unsigned int i = 0; i < entryDataSeen.size(); i++) {
      if (entryDataSeen[i].first != expectedNextOffset) {
        /* The current entry does not start where expected. */
        dataOffsetError = true;
        break;
      }
      /* The next entry should start right after the current entry. */
      expectedNextOffset += entryDataSeen[i].second + sizeof(EntryData);
    }

    /* the last EntryData should have ended at the very end of the
     * bucket. */
    if (expectedNextOffset != kBucketDataSize) {
      dataOffsetError = true;
    }

    /* Throw an assert if something is wrong. */
    if (headerOffsetError || dataOffsetError ||
        totalSize != bucket->totalDataSize || nEntries != bucket->numEntries) {
      /* Copy the bucket locally for easier debugging in case the slab is
       * not in the core dump file. */
      Bucket bucketCopy;
      memcpy(&bucketCopy, bucket, sizeof(Bucket));
      XDCHECK(false);
    }
  }

  /**
   * Shift the EntryData objects that belong to a particular window of
   * EntryData objects. This takes care of modifying the EntryHdr
   * headers that correspond to each shifted EntryData object so that these
   * headers have an updated dataOffset.
   *
   * This is used by:
   *   - del: When an EntryData is deleted, we need to shift the
   *     EntryData objects that precede it.
   *   - updateVal: when an EntryData is updated with a new value of a size
   *     smaller than the old one, the EntryData objects that precede the
   *     entry are shifted in order to free the space that is not used
   *     anymore.
   *   - evictEntries: when an EntryData is evicted, we need to shift the
   *     EntryData objects that precede it (same as del).
   *
   * @param bucket           Bucket from which to shift data.
   * @param src              Pointer to the first EntryData object to be
   *                         shifted.
   * @param shiftDistance    Offset to use for shifting the EntryData
   *                         objects.
   * @param shiftedChunkSize Amount of bytes to be shifted. Starting from src,
   *                         all the EntryData objects that span in the
   *                         window defined by this value will be shifted.
   *                         I.e all the EntryData objects that have an
   *                         offset between src and src + shiftDistance will
   *                         be shifted.
   */
  static void shiftEntriesData(Bucket* bucket,
                               EntryData* src,
                               EntryDataOffset shiftDistance,
                               size_t shiftedChunkSize) {
    char* dst = (reinterpret_cast<char*>(src) + shiftDistance);

    /* Adjust the dataOffset of all the EntryHdr headers that correspond
     * to the EntryData objects we are going to shift. */
    EntryData* cur = src;
    while (reinterpret_cast<char*>(cur) <
           reinterpret_cast<char*>(src) + shiftedChunkSize) {
      EntryHdr* header = getEntryHeader(bucket, cur->headerIndex);
      header->dataOffset += shiftDistance;
      cur = getNextEntryData(bucket, cur);
    }

    /* Move the EntryData objects. */
    memmove(dst, src, shiftedChunkSize);
  }

  /**
   * Evict as many entries as needed, starting from the oldest, until the
   * amount of free space in the bucket is equal or greater than
   * requiredSpace. This is used by insert.
   *
   * 1) Walk through the EntryHdr headers starting from the LRU while
   *    updating a counter that contains the memory made available if evicted
   *    all entries seen so far. Stop when this counter is high enough (i.e
   *    bigger than requiredSpace).
   * 2) Sort the list of entries we decided to evict by their offset in the
   *    "Data" section, in reverse order. The cost of sorting the evicted
   *    entries should be negligible since the number of evicted entries
   *    should be small.
   * 3) Move the "blocks" of EntryData objects that are between two evicted
   *    entries, one by one.
   * 4) reduce the number of entries and the total size of the entries in the
   *    bucket.
   *
   * @param bucket Bucket from which to evict entries.
   * @param requiredSpace Amount of space required (in bytes).
   * @param evictionCb callback to be called for each evicted entry.
   * @return true if at least one entry was evicted.
   */
  static bool evictEntries(Bucket* bucket,
                           size_t requiredSpace,
                           EvictionCb evictionCb) {
    /* Will contain all the entries we decide to evict. */
    std::vector<EntryHdr*> arr;

    size_t freeSpace = kBucketDataSize - bucket->totalDataSize -
                       sizeof(EntryHdr) * bucket->numEntries;

    if (freeSpace >= requiredSpace && bucket->numEntries != kMaxEntries) {
      /* We already have enough space. */
      return false;
    }

    /* 1) Walk through the EntryHdr headers starting from the LRU while
     * updating a counter that contains the memory made available if
     * evicted all entries seen so far. Stop when this counter is high
     * enough (i.e bigger than requiredSpace). */

    /* Start from the lru, which is the last entry in the entry header. */
    unsigned int evictedPos = bucket->numEntries - 1;

    /* Convenient lambda function for evicting an entry. */
    auto evictOneMoreEntryFn = [&]() {
      EntryHdr* entry = getEntryHeader(bucket, evictedPos);
      /* There should always be at least one entry for eviction until
       * we have enough space. */
      XDCHECK_GE(reinterpret_cast<uintptr_t>(entry),
                 reinterpret_cast<uintptr_t>(getEntryHeader(bucket, 0)));
      /* Evicting the entry releases the space of the entry data and the
       * space of the entry header. */
      freeSpace += entry->dataSize + sizeof(EntryHdr) + sizeof(EntryData);
      arr.push_back(entry);
      evictionCb(EntryHandle(bucket, evictedPos));
      evictedPos--;
    };

    /* Evict at least one entry if the current number of entries is about to
     * overflow. This should not happen very often.
     * If this happens often, this means that the bucket is too big for the
     * entries, and we are wasting a lot of memory. */
    if (bucket->numEntries == kMaxEntries) {
      XLOG(ERR) << "Overflow in number of entries in a bucket";
      evictOneMoreEntryFn();
    }

    /* Evict entries until we have enough space. */
    while (freeSpace < requiredSpace) {
      evictOneMoreEntryFn();
    }

    /* 2) Sort the list of entries we decided to evict by their offset in
     * the "Data" section, in increasing order, so that the left-most
     * evicted entry is the first element of the array, and the right-most
     * evected entry is the last element.
     * The cost of sorting the evicted entries should be negligible since
     * the number of evicted entries should be small. */
    auto compareFn = [](const EntryHdr* a, const EntryHdr* b) -> bool {
      return a->dataOffset < b->dataOffset;
    };
    XDCHECK_GT(arr.size(), 0);
    std::sort(arr.begin(), arr.end(), compareFn);

    /* 3) Move the "chunks" of EntryData objects that are between two
     * evicted entries, one by one. */
    size_t shiftDistance = 0;
    EntryDataOffset chunkStartOffset;
    int i = 0;
    for (i = arr.size() - 1; i >= 0; i--) {
      /* Amount of bytes by which to shift the chunk. We are removing the
       * current entry 'arr[i]', so we shift by the total amount of bytes
       * used by its corresponding EntryData. Here, we increment
       * shiftDistance instead of simply setting it to a new value because
       * we want to take into account the space made available by the
       * shifting performed by the previous iterations of this loop. */
      shiftDistance += arr[i]->dataSize + sizeof(EntryData);

      /* Compute the start offset of the chunk we are moving.
       * The chunk starts right at the end of the removed entry that is
       * at the left of the current removed entry. If the current removed
       * entry is the left-most, the start offset of the chunk is the
       * beginning of the data. */
      if (i == 0) {
        chunkStartOffset = getFirstEntryDataOffset(bucket);
      } else {
        chunkStartOffset =
            arr[i - 1]->dataOffset + arr[i - 1]->dataSize + sizeof(EntryData);
      }

      /* Size of the chunk to be moved. */
      size_t shiftedChunkSize = arr[i]->dataOffset - chunkStartOffset;
      if (shiftedChunkSize == 0) {
        /* The current removed entry is contiguous with the removed
         * entry on the left, so there is nothing between them to be
         * moved. */
        continue;
      }

      /* Pointer to the beginning of that chunk. */
      EntryData* src =
          reinterpret_cast<EntryData*>(bucket->data + chunkStartOffset);
      shiftEntriesData(bucket, src, shiftDistance, shiftedChunkSize);
    }

    /* 4) reduce the number of entries and the total size of the entries in
     * the bucket. */
    XDCHECK_GE(bucket->numEntries, arr.size());
    XDCHECK_GE(bucket->totalDataSize, shiftDistance);
    bucket->numEntries -= arr.size();
    bucket->totalDataSize -= shiftDistance;

    return true;
  }

  /* Utility functions */

  /**
   * Get the offset where the "Data" section of the given bucket starts.
   *
   * @param bucket Bucket for which we want to compute the offset where the
   * data section begins.
   * @return Offset where the "Data" section of the bucket starts.
   */
  constexpr static size_t getFirstEntryDataOffset(Bucket* bucket) {
    return kBucketDataSize - bucket->totalDataSize;
  }

  /**
   * Return a pointer to the first EntryData object in a bucket.
   *
   * @param bucket Bucket from which to retrieve the first EntryData.
   * @return Pointer to the first EntryData object in the bucket.
   */
  constexpr static EntryData* getFirstEntryData(Bucket* bucket) {
    return reinterpret_cast<EntryData*>(bucket->data +
                                        getFirstEntryDataOffset(bucket));
  }

  /**
   * Return a pointer to the EntryHdr header at position headerPos.
   *
   * @param bucket    Bucket from which to retrieve the EntryHdr header.
   * @param headerPos Position of the entry header.
   * @return Pointer to the EntryHdr header at position headerPos.
   */
  constexpr static EntryHdr* getEntryHeader(Bucket* bucket, uint8_t headerPos) {
    return reinterpret_cast<EntryHdr*>(bucket->data) + headerPos;
  }

  /**
   * Get a pointer to the entry data pointed to by the entry header at
   * position headerPos.
   * @param bucket    Bucket in which to look for the data.
   * @param headerPos Position of the header of the entry for which to
   *                  retrieve the data.
   * @return Pointer to the entry data for the given entry header.
   */
  constexpr static EntryData* getEntryData(Bucket* bucket, uint8_t headerPos) {
    return reinterpret_cast<EntryData*>(
        bucket->data + getEntryHeader(bucket, headerPos)->dataOffset);
  }

  /*
   * Get the EntryData object that follows a given EntryData object.
   * It's the caller's responsibility to check that the returned pointer
   * does not overflow the bucket.
   * This is called by shiftEntriesData.
   *
   * @param bucket Bucket from which to find the bucket EntryData object.
   * @param entryData Entry data for which we want to find the next one.
   * @return Entry data that follows entryData.
   */
  constexpr static EntryData* getNextEntryData(Bucket* bucket,
                                               EntryData* entryData) {
    return reinterpret_cast<EntryData*>(
        reinterpret_cast<char*>(entryData) + sizeof(EntryData) +
        getEntryHeader(bucket, entryData->headerIndex)->dataSize);
  }

  /**
   * Check that the given value will not overflow if written to an integral of
   * type T.
   */
  template <typename T>
  static void checkOverflow(size_t val) {
    XDCHECK_LE(val, std::numeric_limits<T>::max());
  }
};

template <typename CompactCacheDescriptor>
constexpr size_t VariableLruBucket<CompactCacheDescriptor>::kBucketDataSize;

template <typename CompactCacheDescriptor>
constexpr size_t VariableLruBucket<CompactCacheDescriptor>::kMaxValueSize;
} // namespace cachelib
} // namespace facebook
