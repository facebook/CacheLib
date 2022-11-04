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

#include "cachelib/datatype/RangeMap.h"

namespace facebook {
namespace cachelib {
namespace detail {
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
  } catch (const std::bad_alloc& ex) {
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
} // namespace cachelib
} // namespace facebook
