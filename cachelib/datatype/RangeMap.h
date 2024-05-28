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

namespace facebook::cachelib {
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
    EntryValue value __attribute__((__packed__));
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

template <typename Key>
uint32_t BinaryIndex<Key>::computeStorageSize(uint32_t capacity) {
  return sizeof(BinaryIndex) + capacity * sizeof(Entry);
}

template <typename Key>
BinaryIndex<Key>* BinaryIndex<Key>::createNewIndex(void* buffer,
                                                   uint32_t capacity) {
  return new (buffer) BinaryIndex(capacity);
}

template <typename Key>
void BinaryIndex<Key>::cloneIndex(BinaryIndex& dst, const BinaryIndex& src) {
  XDCHECK_EQ(0u, dst.numEntries());
  XDCHECK_GE(dst.capacity(), src.capacity());
  std::memcpy(dst.entries_, src.entries_, src.numEntries_ * sizeof(Entry));
  dst.numEntries_ = src.numEntries_;
}

template <typename Key>
BinaryIndex<Key>::BinaryIndex(uint32_t capacity) : capacity_{capacity} {}

template <typename Key>
typename BinaryIndex<Key>::Entry* BinaryIndex<Key>::lookup(Key key) {
  return const_cast<Entry*>(static_cast<const BinaryIndex*>(this)->lookup(key));
}

template <typename Key>
const typename BinaryIndex<Key>::Entry* BinaryIndex<Key>::lookup(
    Key key) const {
  auto res = lookupLowerbound(key);
  if (res == end() || res->key != key) {
    return end();
  }
  return res;
}

template <typename Key>
typename BinaryIndex<Key>::Entry* BinaryIndex<Key>::lookupLowerbound(Key key) {
  return const_cast<Entry*>(
      static_cast<const BinaryIndex*>(this)->lookupLowerbound(key));
}

template <typename Key>
const typename BinaryIndex<Key>::Entry* BinaryIndex<Key>::lookupLowerbound(
    Key key) const {
  return std::lower_bound(
      entries_, entries_ + numEntries_, key,
      [](const Entry& e, const Key& k) { return e.key < k; });
}

template <typename Key>
bool BinaryIndex<Key>::insert(Key key, BufferAddr addr) {
  auto* e = lookup(key);
  if (e != end()) {
    return false;
  }
  insertInternal(key, addr);
  return true;
}

template <typename Key>
BufferAddr BinaryIndex<Key>::insertOrReplace(Key key, BufferAddr addr) {
  auto* e = lookup(key);
  if (e != end()) {
    BufferAddr oldAddr = e->addr;
    e->addr = addr;
    return oldAddr;
  }
  insertInternal(key, addr);
  return {};
}

template <typename Key>
BufferAddr BinaryIndex<Key>::remove(Key key) {
  auto* e = lookup(key);
  if (e == end()) {
    return {};
  }

  // Two scenarios of deletion
  // | --------- | | ------------ | |
  //              ^                ^
  //              a                b
  // a). we need to memmove the second part to overwrite the removed item.
  // b). we don't need to do anything other than decrementing numEntries_.
  BufferAddr addr = e->addr;
  numEntries_--;
  if (e < end()) {
    std::memmove(e, (e + 1), (end() - e) * sizeof(Entry));
  }
  return addr;
}

template <typename Key>
uint32_t BinaryIndex<Key>::removeBefore(Key key, DeleteCB deleteCB) {
  auto* cur = begin();
  uint32_t count = 0;
  while (cur != end() && cur->key < key) {
    deleteCB(cur->addr);
    ++count;
    ++cur;
  }
  if (count == 0) {
    return 0;
  }

  if (cur != end()) {
    XDCHECK_LT(cur, end());
    std::memmove(begin(), begin() + count, (end() - cur) * sizeof(Entry));
  }
  numEntries_ -= count;
  return count;
}

template <typename Key>
void BinaryIndex<Key>::insertInternal(Key key, BufferAddr addr) {
  XDCHECK_LT(numEntries(), capacity());

  // Two scenarios of insertion
  // | --------- | | ------------ | |
  //              ^                ^
  //              a                b
  // a). we need to memmove the second part to make room for our new item.
  // b). we just need to add our item to the end and increment numEntries_.
  auto res = lookupLowerbound(key);
  if (res < end()) {
    std::memmove(res + 1, res, (end() - res) * sizeof(Entry));
  }
  numEntries_++;
  *res = Entry{key, addr};
}

template <typename Key, typename Value, typename BufManager>
BinaryIndexIterator<Key, const Value, BufManager>
BinaryIndexIterator<Key, Value, BufManager>::toConstItr() {
  return {manager_, index_, entry_};
}

template <typename Key, typename Value, typename BufManager>
Value& BinaryIndexIterator<Key, Value, BufManager>::dereference() const {
  return *value_;
}

template <typename Key, typename Value, typename BufManager>
void BinaryIndexIterator<Key, Value, BufManager>::increment() {
  XDCHECK_NE(reinterpret_cast<uintptr_t>(nullptr),
             reinterpret_cast<uintptr_t>(index_));
  entry_++;
  XDCHECK_LE(reinterpret_cast<uintptr_t>(entry_),
             reinterpret_cast<uintptr_t>(index_->end()));
  XDCHECK_NE(reinterpret_cast<uintptr_t>(nullptr),
             reinterpret_cast<uintptr_t>(manager_));
  if (entry_ == index_->end()) {
    value_ = nullptr;
  } else {
    value_ = manager_->template get<Value>(entry_->addr);
  }
}

template <typename Key, typename Value, typename BufManager>
bool BinaryIndexIterator<Key, Value, BufManager>::equal(
    const BinaryIndexIterator& other) const {
  return index_ == other.index_ && entry_ == other.entry_ &&
         value_ == other.value_ && manager_ == other.manager_;
}
} // namespace detail

template <typename K, typename V, typename C>
RangeMap<K, V, C> RangeMap<K, V, C>::create(Cache& cache,
                                            PoolId pid,
                                            typename Cache::Key key,
                                            uint32_t numEntries,
                                            uint32_t numBytes) {
  try {
    return RangeMap{cache, pid, key, numEntries, numBytes};
  } catch (const std::bad_alloc&) {
    return {};
  }
}

template <typename K, typename V, typename C>
RangeMap<K, V, C> RangeMap<K, V, C>::fromWriteHandle(Cache& cache,
                                                     WriteHandle handle) {
  if (!handle) {
    return {};
  }
  return RangeMap{cache, std::move(handle)};
}

template <typename K, typename V, typename C>
RangeMap<K, V, C>::RangeMap(Cache& cache,
                            PoolId pid,
                            typename Cache::Key key,
                            uint32_t numEntries,
                            uint32_t numBytes)
    : cache_{&cache},
      handle_{cache_->allocate(
          pid, key, BinaryIndex::computeStorageSize(numEntries))} {
  if (!handle_) {
    throw cachelib::exception::OutOfMemory(
        folly::sformat("Failed allocate index for range map. Key: {}, "
                       "numEntries: {}, numBytes: {}",
                       key, numEntries, numBytes));
  }

  bufferManager_ = BufferManager{*cache_, handle_, numBytes};
  BinaryIndex::createNewIndex(handle_->getMemory(), numEntries);
}

template <typename K, typename V, typename C>
RangeMap<K, V, C>::RangeMap(Cache& cache, WriteHandle handle)
    : cache_{&cache},
      handle_{std::move(handle)},
      bufferManager_{*cache_, handle_} {}

template <typename K, typename V, typename C>
RangeMap<K, V, C>::RangeMap(RangeMap&& other)
    : cache_(other.cache_),
      handle_(std::move(other.handle_)),
      bufferManager_(*cache_, handle_) {}

template <typename K, typename V, typename C>
RangeMap<K, V, C>& RangeMap<K, V, C>::operator=(RangeMap&& other) {
  if (this != &other) {
    this->~RangeMap();
    new (this) RangeMap(std::move(other));
  }
  return *this;
}

template <typename K, typename V, typename C>
bool RangeMap<K, V, C>::insert(const EntryKey& key, const EntryValue& value) {
  auto* index = handle_->template getMemoryAs<BinaryIndex>();
  if (index->lookup(key) != index->end()) {
    return false;
  }
  insertOrReplaceInternal(key, value);
  return true;
}

template <typename K, typename V, typename C>
typename RangeMap<K, V, C>::InsertOrReplaceResult
RangeMap<K, V, C>::insertOrReplace(const EntryKey& key,
                                   const EntryValue& value) {
  return insertOrReplaceInternal(key, value);
}

template <typename K, typename V, typename C>
bool RangeMap<K, V, C>::remove(const EntryKey& key) {
  auto* index = handle_->template getMemoryAs<BinaryIndex>();
  auto addr = index->remove(key);
  if (!addr) {
    return false;
  }

  bufferManager_.remove(addr);
  if (bufferManager_.wastedBytesPct() > kWastedBytesPctThreshold) {
    compact();
  }
  return true;
}

template <typename K, typename V, typename C>
uint32_t RangeMap<K, V, C>::removeBefore(const EntryKey& key) {
  auto* index = handle_->template getMemoryAs<BinaryIndex>();
  auto count = index->removeBefore(
      key, [this](auto addr) { bufferManager_.remove(addr); });
  if (bufferManager_.wastedBytesPct() > kWastedBytesPctThreshold) {
    compact();
  }
  return count;
}

template <typename K, typename V, typename C>
typename RangeMap<K, V, C>::Itr RangeMap<K, V, C>::lookup(const EntryKey& key) {
  auto* index = handle_->template getMemoryAs<BinaryIndex>();
  return Itr{&bufferManager_, index, index->lookup(key)};
}

template <typename K, typename V, typename C>
typename RangeMap<K, V, C>::ConstItr RangeMap<K, V, C>::lookup(
    const EntryKey& key) const {
  return const_cast<RangeMap<K, V, C>*>(this)->lookup(key).toConstItr();
}

template <typename K, typename V, typename C>
folly::Range<typename RangeMap<K, V, C>::Itr> RangeMap<K, V, C>::rangeLookup(
    const EntryKey& key1, const EntryKey& key2) {
  XDCHECK_LE(key1, key2);
  auto key1Itr = lookup(key1);
  if (key1Itr == end()) {
    return {end(), end()};
  }

  auto key2Itr = lookup(key2);
  if (key2Itr == end()) {
    return {end(), end()};
  }

  return {key1Itr, ++key2Itr};
}

template <typename K, typename V, typename C>
folly::Range<typename RangeMap<K, V, C>::ConstItr>
RangeMap<K, V, C>::rangeLookup(const EntryKey& key1,
                               const EntryKey& key2) const {
  auto mutableRange =
      const_cast<RangeMap<K, V, C>*>(this)->rangeLookup(key1, key2);
  return {mutableRange.begin().toConstItr(), mutableRange.end().toConstItr()};
}

template <typename K, typename V, typename C>
folly::Range<typename RangeMap<K, V, C>::Itr>
RangeMap<K, V, C>::rangeLookupApproximate(const EntryKey& key1,
                                          const EntryKey& key2) {
  auto* index = handle_->template getMemoryAs<BinaryIndex>();
  if (index->numEntries() == 0) {
    return {end(), end()};
  }

  auto* e1 = index->lookupLowerbound(key1);
  if (e1 == index->end()) {
    return {end(), end()};
  }

  auto* e2 = index->lookupLowerbound(key2);
  if (e2 != index->end() && e2->key == key2) {
    return {Itr{&bufferManager_, index, e1}, ++Itr{&bufferManager_, index, e2}};
  }
  return {Itr{&bufferManager_, index, e1}, Itr{&bufferManager_, index, e2}};
}

template <typename K, typename V, typename C>
folly::Range<typename RangeMap<K, V, C>::ConstItr>
RangeMap<K, V, C>::rangeLookupApproximate(const EntryKey& key1,
                                          const EntryKey& key2) const {
  auto mutableRange =
      const_cast<RangeMap<K, V, C>*>(this)->rangeLookupApproximate(key1, key2);
  return {mutableRange.begin().toConstItr(), mutableRange.end().toConstItr()};
}

template <typename K, typename V, typename C>
typename RangeMap<K, V, C>::Itr RangeMap<K, V, C>::begin() {
  auto* index = handle_->template getMemoryAs<BinaryIndex>();
  return Itr{&bufferManager_, index, index->begin()};
}

template <typename K, typename V, typename C>
typename RangeMap<K, V, C>::Itr RangeMap<K, V, C>::end() {
  auto* index = handle_->template getMemoryAs<BinaryIndex>();
  return Itr{&bufferManager_, index, index->end()};
}

template <typename K, typename V, typename C>
typename RangeMap<K, V, C>::ConstItr RangeMap<K, V, C>::begin() const {
  auto* index = handle_->template getMemoryAs<BinaryIndex>();
  return ConstItr{&bufferManager_, index, index->begin()};
}

template <typename K, typename V, typename C>
typename RangeMap<K, V, C>::ConstItr RangeMap<K, V, C>::end() const {
  auto* index = handle_->template getMemoryAs<BinaryIndex>();
  return ConstItr{&bufferManager_, index, index->end()};
}

template <typename K, typename V, typename C>
void RangeMap<K, V, C>::compact() {
  // The idea below is first we compact all allocations in buffer manager.
  // Afterwards, we iterate through each allocation in buffer manager,
  // and for each allocation, we replace it in the index with the new addr.
  bufferManager_.compact();

  auto* index = handle_->template getMemoryAs<BinaryIndex>();

  auto itr = detail::BufferManagerIterator<EntryKeyValue, BufferManager>{
      bufferManager_};
  auto endItr = detail::BufferManagerIterator<EntryKeyValue, BufferManager>{
      bufferManager_,
      detail::BufferManagerIterator<EntryKeyValue, BufferManager>::End};
  while (itr != endItr) {
    index->insertOrReplace(itr->key, itr.getAsBufferAddr());
    ++itr;
  }
}

template <typename K, typename V, typename C>
size_t RangeMap<K, V, C>::sizeInBytes() const {
  size_t numBytes = handle_->getSize();
  auto allocs = cache_->viewAsChainedAllocs(handle_);
  for (const auto& c : allocs.getChain()) {
    numBytes += c.getSize();
  }
  return numBytes;
}

template <typename K, typename V, typename C>
typename RangeMap<K, V, C>::InsertOrReplaceResult
RangeMap<K, V, C>::insertOrReplaceInternal(const EntryKey& key,
                                           const EntryValue& value) {
  // If wasted space is more than threshold, trigger compaction
  if (bufferManager_.wastedBytesPct() > kWastedBytesPctThreshold) {
    compact();
  }

  const auto valueSize = util::getValueSize(value);
  const auto allocSize = sizeof(EntryKey) + valueSize;

  detail::BufferAddr addr;
  if (handle_->template getMemoryAs<BinaryIndex>()->overLimit()) {
    addr = cloneIndexAndAllocate(allocSize, true /* expandIndex */);
  }
  if (!addr) {
    addr = bufferManager_.allocate(allocSize);
    if (!addr) {
      addr = cloneIndexAndAllocate(allocSize, false /* expandIndex */);
    }
  }

  auto* kv = bufferManager_.template get<EntryKeyValue>(addr);
  std::memcpy(&kv->key, &key, sizeof(key));
  std::memcpy(&kv->value, &value, valueSize);

  auto oldAddr =
      handle_->template getMemoryAs<BinaryIndex>()->insertOrReplace(key, addr);
  if (oldAddr) {
    bufferManager_.remove(oldAddr);
    return InsertOrReplaceResult::kReplaced;
  }
  return InsertOrReplaceResult::kInserted;
}

template <typename K, typename V, typename C>
detail::BufferAddr RangeMap<K, V, C>::cloneIndexAndAllocate(uint32_t allocSize,
                                                            bool expandIndex) {
  constexpr uint32_t kExpansionFactor = 2;
  auto* currIndex = handle_->template getMemoryAs<BinaryIndex>();
  const auto pid = cache_->getAllocInfo(handle_->getMemory()).poolId;
  const auto newIndexCapacity = expandIndex
                                    ? currIndex->capacity() * kExpansionFactor
                                    : currIndex->capacity();
  auto newHandle =
      cache_->allocate(pid, handle_->getKey(),
                       BinaryIndex::computeStorageSize(newIndexCapacity));
  if (!newHandle) {
    throw cachelib::exception::OutOfMemory(
        folly::sformat("Unable to clone the index during range map expansion. "
                       "Parent item: {}",
                       handle_->toString()));
  }
  auto* newIndex =
      BinaryIndex::createNewIndex(newHandle->getMemory(), newIndexCapacity);
  BinaryIndex::cloneIndex(*newIndex, *currIndex);

  // Clone the buffers (chaind items) when expanding the index,
  // so that if a user holds an old handle to the old range map,
  // it will still be valid and can still be accssed.
  auto newBufferManager = bufferManager_.clone(newHandle);
  if (newBufferManager.empty()) {
    throw cachelib::exception::OutOfMemory(
        folly::sformat("Unable to clone the buffers associated with range map. "
                       "Parent item: {}",
                       handle_->toString()));
  }

  auto addr = newBufferManager.allocate(allocSize);
  if (!addr) {
    if (newBufferManager.expand(allocSize)) {
      addr = newBufferManager.allocate(allocSize);
    }
    if (!addr) {
      throw cachelib::exception::OutOfMemory(
          folly::sformat("Unable to allocate for a new entry for range map. "
                         "Alloc size: {}, Parent item: {}",
                         allocSize, handle_->toString()));
    }
  }

  if (handle_->isAccessible()) {
    cache_->insertOrReplace(newHandle);
  }
  handle_ = std::move(newHandle);
  bufferManager_ = BufferManager{*cache_, handle_};
  return addr;
}
} // namespace facebook::cachelib
