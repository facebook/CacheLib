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

#include <limits>

#include "cachelib/allocator/TypedHandle.h"
#include "cachelib/common/Exceptions.h"
#include "cachelib/common/Hash.h"
#include "cachelib/common/Iterators.h"
#include "cachelib/datatype/Buffer.h"
#include "cachelib/datatype/DataTypes.h"

namespace facebook {
namespace cachelib {
namespace detail {
template <typename Key>
class FOLLY_PACK_ATTR BinaryIndex;

template <typename Key, typename Value, typename BufManager>
class BinaryIndexIterator;
} // namespace detail

// Range Map data structure for cachelib
// Key needs to be a fixed size POD and implements operator<.
// Value can be variable sized, but must be POD.
template <typename K, typename V, typename C>
class RangeMap {
 public:
  using EntryKey = K;
  using EntryValue = V;
  using Cache = C;
  using Item = typename Cache::Item;
  using WriteHandle = typename Item::WriteHandle;

  struct FOLLY_PACK_ATTR EntryKeyValue {
    EntryKey key;
    EntryValue value;
  };
  using Itr = detail::BinaryIndexIterator<EntryKey,
                                          EntryKeyValue,
                                          detail::BufferManager<Cache>>;
  using ConstItr = detail::BinaryIndexIterator<EntryKey,
                                               const EntryKeyValue,
                                               detail::BufferManager<Cache>>;

  // Create a new cachelib::RangeMap
  // @param cache   cache allocator to allocate from
  // @param pid     pool where we'll allocate the map from
  // @param key     key for the item in cache
  // @param numEntries   number of entries this map can contain initially
  // @param numBytes     number of bytes allocated for value storage initially
  // @return  valid cachelib::RangeMap on success
  // @throw   cachelib::exception::OutOfMemory if failing to allocate an index
  //          or a buffer associated with the map
  static RangeMap create(Cache& cache,
                         PoolId pid,
                         typename Cache::Key key,
                         uint32_t numEntries = kDefaultNumEntries,
                         uint32_t numBytes = kDefaultNumBytes);

  // Convert a write handle to a cachelib::RangeMap
  // @param cache   cache allocator to allocate from
  // @param handle  parent handle for this cachelib::RangeMap
  // @return cachelib::RangeMap
  static RangeMap fromWriteHandle(Cache& cache, WriteHandle handle);

  // Constructs null cachelib map
  RangeMap() = default;

  // Move constructor
  RangeMap(RangeMap&& other);
  RangeMap& operator=(RangeMap&& other);

  // Copy is disallowed
  RangeMap(const RangeMap& other) = delete;
  RangeMap& operator=(const RangeMap& other) = delete;

  // Insert value into RangeMap. False if key already exists.
  // @throw std::bad_alloc if we can't allocate for the value
  //                       map is still in a valid state. User can re-try.
  bool insert(const EntryKey& key, const EntryValue& value);

  // Insert or replace a {key, value} pair into the map.
  // @throw std::bad_alloc if we can't allocate for the value
  //                       map is still in a valid state. User can re-try.
  enum InsertOrReplaceResult {
    kInserted,
    kReplaced,
  };
  InsertOrReplaceResult insertOrReplace(const EntryKey& key,
                                        const EntryValue& value);

  // Remove key. False if not found. Calling remove invalidates any iterators
  // that point to (key) or later elements.
  bool remove(const EntryKey& key);

  // Removes keys less than the provided key, which does not
  // necessarily need to exist. Returns number of elements deleted.
  uint32_t removeBefore(const EntryKey& key);

  // Return an iterator for this key. Can iterate in sorted order.
  // itr == end() if not found.
  Itr lookup(const EntryKey& key);
  ConstItr lookup(const EntryKey& key) const;

  // Return an exact range or null if not found. [key1, key2] are inclusive.
  folly::Range<Itr> rangeLookup(const EntryKey& key1, const EntryKey& key2);
  folly::Range<ConstItr> rangeLookup(const EntryKey& key1,
                                     const EntryKey& key2) const;

  // Return cloest range to requested [key1, key2]. Null if nothing is in map.
  // I.e. for input [5, 10], we might return [6, 9].
  folly::Range<Itr> rangeLookupApproximate(const EntryKey& key1,
                                           const EntryKey& key2);
  folly::Range<ConstItr> rangeLookupApproximate(const EntryKey& key1,
                                                const EntryKey& key2) const;

  // Iterate through the map in a sorted order via mutable or const.
  Itr begin();
  Itr end();
  ConstItr begin() const;
  ConstItr end() const;

  // Compact storage to make more room for allocations
  // Cost: O(N*LOG(N)). Compacting storage is O(N) where N is number
  //       of the allocations. Updating the index is O(N*LOG(N)).
  //
  // @throw std::runtime_error if unrecoverable error is encountered.
  //                           this indicates a bug in our code.
  //                           Map is no longer in a usable state. User
  //                           should delete the whole map by its key from
  //                           cache.
  void compact();

  // Return number of bytes this map is using for hash table and the buffers
  // This doesn't include cachelib item overhead
  size_t sizeInBytes() const;

  // Return bytes left unused (can be used for future entries)
  size_t remainingBytes() const { return bufferManager_.remainingBytes(); }

  // Returns bytes left behind by removed entries
  size_t wastedBytes() const { return bufferManager_.wastedBytes(); }

  // Return number of elements in this map
  uint32_t size() const {
    return handle_->template getMemoryAs<BinaryIndex>()->numEntries();
  }

  // Return capacity of the index in this map
  uint32_t capacity() const {
    return handle_->template getMemoryAs<BinaryIndex>()->capacity();
  }

  // This does not modify the content of this structure.
  // It resets it to a write handle, which can be used with any API in
  // CacheAllocator that deals with ReadHandle/WriteHandle. After invoking this
  // function, this structure is left in a null state.
  WriteHandle resetToWriteHandle() && { return std::move(handle_); }

  // Borrow the write handle underneath this structure. This is useful to
  // implement insertion into CacheAllocator.
  const WriteHandle& viewWriteHandle() const { return handle_; }
  WriteHandle& viewWriteHandle() { return handle_; }

  bool isNullWriteHandle() const { return handle_ == nullptr; }

 private:
  using BinaryIndex = detail::BinaryIndex<EntryKey>;
  using BufferManager = detail::BufferManager<Cache>;

  static constexpr int kWastedBytesPctThreshold = 50;
  static constexpr uint32_t kDefaultNumEntries = 20;
  static constexpr uint32_t kDefaultNumBytes = kDefaultNumEntries * 8;

  // Create a new cachelib::RangeMap
  // @throw std::bad_alloc if fail to allocate hashtable or storage for a map
  RangeMap(Cache& cache,
           PoolId pid,
           typename Cache::Key key,
           uint32_t numEntries,
           uint32_t numBytes);

  // Attach to an existing cachelib::RangeMap
  RangeMap(Cache& cache, WriteHandle handle);

  // Expand index or storage if insufficient space
  InsertOrReplaceResult insertOrReplaceInternal(const EntryKey& key,
                                                const EntryValue& value);
  detail::BufferAddr cloneIndexAndAllocate(uint32_t allocSize,
                                           bool expandIndex);

  Cache* cache_{nullptr};
  WriteHandle handle_;
  BufferManager bufferManager_{nullptr};
};

namespace detail {
// An index that stores its entries in sorted order.
// Lookup: O(Log N). Insert/Remove: O(Log N) + O(N) memcpy.
// Key needs to be a POD and also support ordering. (i.e. implement operator<)
template <typename Key>
class FOLLY_PACK_ATTR BinaryIndex {
 public:
  struct FOLLY_PACK_ATTR Entry {
    Key key{};
    BufferAddr addr{};
  };

  typedef std::function<void(BufferAddr)> DeleteCB;

  static uint32_t computeStorageSize(uint32_t capacity);

  static BinaryIndex* createNewIndex(void* buffer, uint32_t capacity);
  static void cloneIndex(BinaryIndex& dst, const BinaryIndex& src);

  // Return end() if not found.
  Entry* lookup(Key key);
  const Entry* lookup(Key key) const;

  // Return the exact match or a lower bound. If empty index, return end().
  Entry* lookupLowerbound(Key key);
  const Entry* lookupLowerbound(Key key) const;

  // Return old addr if exists.
  BufferAddr insertOrReplace(Key key, BufferAddr addr);

  // Return false if key already exists.
  bool insert(Key key, BufferAddr addr);

  // Return addr removed if exists. Null addr otherwise.
  BufferAddr remove(Key key);

  // Removes index entries before key. returns the count of items removed
  // and addrRet returns a list of buffer addresses to free
  uint32_t removeBefore(Key key, DeleteCB deleteCB);

  Entry* begin() { return entries_; }
  Entry* end() { return entries_ + numEntries_; }
  const Entry* begin() const { return entries_; }
  const Entry* end() const { return entries_ + numEntries_; }

  uint32_t capacity() const { return capacity_; }
  uint32_t numEntries() const { return numEntries_; }

  // When number of entries get close to capacity, it's time to resize
  bool overLimit() const {
    return numEntries_ >= capacity_ - 1 ||
           numEntries_ > capacity_ * kCapacityOverlimitRatio;
  }

 private:
  static constexpr double kCapacityOverlimitRatio = 0.5;

  explicit BinaryIndex(uint32_t capacity);

  void insertInternal(Key key, BufferAddr addr);

  uint32_t capacity_{};
  uint32_t numEntries_{};
  Entry entries_[];
};

// An interator interface that can return a custom Value type. The iterators
// are sorted according to their order in BinaryIndex.
template <typename Key, typename Value, typename BufManager>
class BinaryIndexIterator
    : public IteratorFacade<BinaryIndexIterator<Key, Value, BufManager>,
                            Value,
                            std::forward_iterator_tag> {
 public:
  BinaryIndexIterator() = default;
  BinaryIndexIterator(const BufManager* manager,
                      BinaryIndex<Key>* index,
                      const typename BinaryIndex<Key>::Entry* entry)
      : entry_{entry}, index_{index}, manager_{manager} {
    if (index_ != nullptr && entry_ != index_->end()) {
      value_ = manager_->template get<Value>(entry_->addr);
    }
  }

  BinaryIndexIterator<Key, const Value, BufManager> toConstItr();

  Value& dereference() const;
  void increment();
  bool equal(const BinaryIndexIterator& other) const;

 private:
  const typename BinaryIndex<Key>::Entry* entry_{nullptr};
  Value* value_{nullptr};

  BinaryIndex<Key>* index_{nullptr};
  const BufManager* manager_{nullptr};
};
} // namespace detail
} // namespace cachelib
} // namespace facebook

#include "cachelib/datatype/RangeMap-inl.h"
