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
#include "cachelib/common/Hash.h"
#include "cachelib/common/Iterators.h"
#include "cachelib/datatype/Buffer.h"
#include "cachelib/datatype/DataTypes.h"

namespace facebook::cachelib {
// Exception when cachelib::Map's index has maxed out.
class MapIndexMaxedOut : public std::runtime_error {
  using std::runtime_error::runtime_error;
};

namespace detail {
// A hash table based on robin hood hashing
// https://en.wikipedia.org/wiki/Hash_table#Robin_Hood_hashing
// This is a very simple open addressing hash table. It has the benefit of
// low average probe distance. The main trick involved is that on insertion,
// whenever we see an item that has a probe distance less than ours, we swap.
template <typename Key, typename Hasher = MurmurHash2>
class FOLLY_PACK_ATTR HashTable {
  static_assert(std::is_trivially_copyable<Key>::value, "key requirements");

 public:
  struct FOLLY_PACK_ATTR Entry {
    Key key{};
    BufferAddr addr{nullptr};

    bool isNull() const { return addr == nullptr; }
    void setNull() { addr = nullptr; }
  };

  // @param capacity   number of maximum entries for the hash table
  // @return  bytes required for the hashtable to fit
  static uint32_t computeStorageSize(size_t capacity) {
    const auto totalSize = sizeof(HashTable) + capacity * sizeof(Entry);
    if (totalSize > std::numeric_limits<uint32_t>::max()) {
      throw std::invalid_argument(folly::sformat(
          "required storage size: {} is bigger than max(uint32_t)", totalSize));
    }
    return static_cast<uint32_t>(totalSize);
  }

  HashTable(size_t capacity);

  // @throw std::invalid_argument if capacity is smaller than "other"
  HashTable(size_t capacity, const HashTable& other);

  // Find an entry to this key. Nullptr if not found.
  const Entry* find(const Key& key) const;

  // Insert this key. Replace existing key if present.
  // @return addr of the replaced entry. Nullptr if no existing key.
  // @throw std::bad_alloc if hash table is full and cannot insert
  BufferAddr insertOrReplace(Key key, BufferAddr addr);

  // Remove entry for this key
  // @return valid BufferAddr on success, nullptr on a miss
  BufferAddr remove(Key key);

  uint32_t capacity() const { return capacity_; }
  uint32_t numEntries() const { return numEntries_; }

  // When number of entries get close to capacity, it's time to resize
  bool overLimit() const {
    return numEntries_ >= capacity_ - 1 ||
           numEntries_ > capacity_ * kCapacityOverlimitRatio;
  }

 private:
  uint32_t getDesiredIndex(const Key& key) const;

  static uint32_t hash(const Key& key);

  // Probe distance measures the distance between our current lookup or
  // insertion index against the *perfect* index this item should be located
  // if there were no collision
  static uint32_t probeDistance(uint32_t desiredIndex,
                                uint32_t index,
                                uint32_t capacity);

  // BEGIN private members
  const uint32_t capacity_;
  uint32_t numEntries_{0};
  Entry entries_[];
  // END private members

  static constexpr double kCapacityOverlimitRatio = 0.9;
};

template <typename Key, typename Hasher>
HashTable<Key, Hasher>::HashTable(size_t capacity) : capacity_(capacity) {
  std::fill(&entries_[0], &entries_[capacity_], Entry{});
}

template <typename Key, typename Hasher>
HashTable<Key, Hasher>::HashTable(size_t capacity, const HashTable& other)
    : HashTable(capacity) {
  if (capacity_ < other.capacity_) {
    throw std::invalid_argument(folly::sformat(
        "capacity too small. self: {}, other: {}", capacity_, other.capacity_));
  }
  for (size_t i = 0; i < other.capacity_; ++i) {
    auto& e = other.entries_[i];
    if (!e.isNull()) {
      insertOrReplace(e.key, e.addr);
    }
  }
}

template <typename Key, typename Hasher>
const typename HashTable<Key, Hasher>::Entry* HashTable<Key, Hasher>::find(
    const Key& key) const {
  uint32_t index = getDesiredIndex(key);
  uint32_t myDistance = 0;
  while (true) {
    auto& e = entries_[index];
    if (e.isNull()) {
      return nullptr;
    }

    if (e.key == key) {
      return &e;
    }

    // If the current entry's probe distance is than our probe distance,
    // then we know our key does not exist.
    const uint32_t curEntryDistance =
        probeDistance(getDesiredIndex(e.key), index, capacity_);
    if (curEntryDistance < myDistance) {
      return nullptr;
    }

    ++myDistance;
    ++index;
    if (index == capacity_) {
      index = 0;
    }
  }
  return nullptr;
}

template <typename Key, typename Hasher>
BufferAddr HashTable<Key, Hasher>::insertOrReplace(Key key, BufferAddr addr) {
  if (!addr) {
    throw std::invalid_argument("nullptr not allowed for address!");
  }

  // Always leave an empty slot
  if (numEntries_ == capacity_ - 1) {
    throw std::bad_alloc();
  }

  Entry newE{key, addr};
  uint32_t index = getDesiredIndex(newE.key);
  uint32_t myDistance = 0;
  while (true) {
    auto& e = entries_[index];
    if (e.isNull()) {
      e = newE;
      ++numEntries_;
      return nullptr;
    }

    // If same key already exists, we abort
    if (e.key == newE.key) {
      auto oldAddr = e.addr;
      e = newE;
      return oldAddr;
    }

    const uint32_t curEntryDistance =
        probeDistance(getDesiredIndex(e.key), index, capacity_);
    if (curEntryDistance < myDistance) {
      std::swap(e, newE);

      // Using curEntryDistance since we're now going to find a new slot for
      // the entry we have just displaced.
      myDistance = curEntryDistance;
    }

    ++myDistance;
    ++index;
    if (index == capacity_) {
      index = 0;
    }
  }
  return nullptr;
}

template <typename Key, typename Hasher>
BufferAddr HashTable<Key, Hasher>::remove(Key key) {
  auto* e = const_cast<Entry*>(find(key));
  if (e == nullptr) {
    return nullptr;
  }
  const auto addr = e->addr;

  // Now shift backwards if next entry is valid.
  // Repeat until we see a null entry or an entry in ideal position
  uint64_t index = e - &(entries_[0]);
  while (true) {
    uint64_t nextIndex = index == (capacity_ - 1) ? 0 : index + 1;

    auto* nextEntry = &(entries_[nextIndex]);
    if (nextEntry->isNull() || probeDistance(getDesiredIndex(nextEntry->key),
                                             nextIndex, capacity_) == 0) {
      break;
    }

    *e = *nextEntry;
    e = nextEntry;

    index = nextIndex;
  }

  e->setNull();
  --numEntries_;

  return addr;
}

template <typename Key, typename Hasher>
uint32_t HashTable<Key, Hasher>::getDesiredIndex(const Key& key) const {
  return static_cast<uint32_t>(
      (static_cast<uint64_t>(hash(key)) * static_cast<uint64_t>(capacity_)) >>
      32);
}

template <typename Key, typename Hasher>
inline uint32_t HashTable<Key, Hasher>::hash(const Key& key) {
  return Hasher{}(&key, sizeof(Key));
}

template <typename Key, typename Hasher>
uint32_t HashTable<Key, Hasher>::probeDistance(uint32_t desiredIndex,
                                               uint32_t index,
                                               uint32_t capacity) {
  // If desiredIndex is bigger than index, it means we've wrapped around
  if (desiredIndex <= index) {
    return index - desiredIndex;
  } else {
    return (index + capacity) - desiredIndex;
  }
}

// @throw std::bad_alloc if failing to allocate a new item
template <typename K, typename C>
auto createHashTable(C& cache,
                     PoolId pid,
                     typename C::Item::Key key,
                     uint32_t capacity) {
  using HT = HashTable<K>;
  using HTHandle = typename C::template TypedHandle<HT>;

  auto handle = cache.allocate(pid, key, HT::computeStorageSize(capacity));
  if (!handle) {
    throw std::bad_alloc();
  }
  new (handle->getMemory()) HT(capacity);
  return HTHandle{std::move(handle)};
}

template <typename K, typename C>
auto copyHashTable(
    C& cache,
    const typename C::template TypedHandle<HashTable<K>>& oldHashTable,
    const size_t newCapacity) {
  using HT = HashTable<K>;
  using HTHandle = typename C::template TypedHandle<HT>;

  // Maximum size for an item
  // TODO: This is just under 1MB to allow some room for the item header
  //       and HashTable header. Proper follow up tracked in T37573713
  const auto newSize = HT::computeStorageSize(newCapacity);
  const size_t kMaxHashTableSize =
      1024 * 1024 - HT::computeStorageSize(0) -
      C::Item::getRequiredSize(oldHashTable.viewWriteHandle()->getKey(), 0);
  if (newSize > kMaxHashTableSize) {
    // This shouldn't happen. Too many entries for a single object in memory.
    auto exStr = folly::sformat(
        "Index has maxed out for the provided key. Existing entries: {}, New "
        "requested capacity: {}. New requested size: {}",
        oldHashTable->numEntries(), newCapacity, newSize);
    throw cachelib::MapIndexMaxedOut(exStr.c_str());
  }

  const auto poolId =
      cache.getAllocInfo(oldHashTable.viewWriteHandle()->getMemory()).poolId;
  auto newHandle =
      cache.allocate(poolId, oldHashTable.viewWriteHandle()->getKey(), newSize);
  if (!newHandle) {
    return HTHandle{nullptr};
  }

  new (newHandle->getMemory())
      HT(static_cast<uint32_t>(newCapacity), *oldHashTable);
  return HTHandle{std::move(newHandle)};
}

template <typename K, typename C>
auto expandHashTable(
    C& cache,
    const typename C::template TypedHandle<HashTable<K>>& oldHashTable,
    double factor = 2.0) {
  XDCHECK_LT(1.0, factor) << "hash table can only grow not shrink";
  const size_t newCapacity =
      static_cast<size_t>(oldHashTable->capacity() * factor);
  return copyHashTable<K, C>(cache, oldHashTable, newCapacity);
}
} // namespace detail

template <typename K, typename V, typename C>
class MapView;

// Map data structure for cachelib
// Key needs to be a fixed size POD.
// Value can be variable sized, but must be POD.
template <typename K, typename V, typename C>
class Map {
 public:
  using EntryKey = K;
  using EntryValue = V;
  using CacheType = C;

  using Item = typename CacheType::Item;
  using WriteHandle = typename Item::WriteHandle;

  struct FOLLY_PACK_ATTR EntryKeyValue {
    EntryKey first;
    EntryValue second __attribute__((__packed__));
  };

  // Create a new cachelib::Map
  // @param cache   cache allocator to allocate from
  // @param pid     pool where we'll allocate the map from
  // @param key     key for the item in cache
  // @param numEntries   number of entries this map can contain initially
  // @param numBytes     number of bytes allocated for value storage initially
  // @return  valid cachelib::Map on success,
  //          cachelib::Map::isNullWriteHandle() == true on failure
  static Map create(CacheType& cache,
                    PoolId pid,
                    typename CacheType::Key key,
                    uint32_t numEntries = kDefaultNumEntries,
                    uint32_t numBytes = kDefaultNumBytes);

  // Convert a write handle to a cachelib::Map
  // @param cache   cache allocator to allocate from
  // @param handle  parent handle for this cachelib::Map
  // @return cachelib::Map
  static Map fromWriteHandle(CacheType& cache, WriteHandle handle);

  // Constructs null cachelib map
  Map() = default;
  Map(std::nullptr_t) : Map() {}

  // Move constructor
  Map(Map&& other);
  Map& operator=(Map&& other);

  // Copy is disallowed
  Map(const Map& other) = delete;
  Map& operator=(const Map& other) = delete;

  // Find a value given the key. Return nullptr if not found
  // @param key   key to an entry in this map
  EntryValue* find(const EntryKey& key);

  // Find a value given the key. Return nullptr if not found
  // @param key   key to an entry in this map
  const EntryValue* find(const EntryKey& key) const;

  // Inserts key and value into the map.
  //
  // Insert incurs two lookups on a successful insert. This is due to with
  // the current implementation. Hashtable lookup is much faster than an
  // allocation from BufferManager. So doing a lookup first is cheap and
  // let us avoid an unncessary allocation if the key already exists.
  //
  // @param key    key to the value
  // @param value  value itself
  //
  // @return true if inserted
  //         false if key already existed
  // @throw std::bad_alloc if we can't allocate for the value
  //                       map is still in a valid state. User can re-try.
  // @throw cachelib::IndexMaxedOut if cachelib::Map has reached its
  //                                maximum entry count.
  bool insert(const EntryKey& key, const EntryValue& value);

  // Inserts key and value into the map. Replaces an existing key/value
  // if already exists.
  // @param key    key to the value
  // @param value  value itself
  //
  // @throw std::bad_alloc if we can't allocate for the value
  //                       map is still in a valid state. User can re-try.
  // @throw cachelib::IndexMaxedOut if cachelib::Map has reached its
  //                                maximum entry count.
  enum InsertOrReplaceResult {
    kInserted,
    kReplaced,
  };
  InsertOrReplaceResult insertOrReplace(const EntryKey& key,
                                        const EntryValue& value);

  // Erase the value associated with this key
  // @return true on success, false if the key wasn't found
  bool erase(const EntryKey& key);

  // Return number of bytes this map is using for hash table and the buffers
  // This doesn't include cachelib item overhead
  size_t sizeInBytes() const;

  // Return number of elements in this map
  uint32_t size() const { return hashtable_->numEntries(); }

  // Compact storage to make more room for allocations
  // Cost: O(M + N) where M is the number of `EntryKeyValue` in total and
  //       and N is the number of chained items
  //
  // @throw std::runtime_error if unrecoverable error is encountered.
  //                           this indicates a bug in our code.
  //                           Map is no longer in a usable state. User
  //                           should delete the whole map by its key from
  //                           cache.
  void compact();

  // This does not modify the content of this structure.
  // It resets it to a write handle, which can be used with any API in
  // CacheAllocator that deals with WriteHandle. After invoking this function,
  // this structure is left in a null state.
  WriteHandle resetToWriteHandle() && {
    return std::move(hashtable_).resetToWriteHandle();
  }

  // Borrow the write handle underneath this structure. This is useful to
  // implement insertion into CacheAllocator.
  const WriteHandle& viewWriteHandle() const {
    return hashtable_.viewWriteHandle();
  }

  WriteHandle& viewWriteHandle() { return hashtable_.viewWriteHandle(); }

  bool isNullWriteHandle() const { return hashtable_ == nullptr; }

  // Convert a Map to a read-only MapView.
  // The view will become invalid as soon as any mutation happens to the
  // underlying map.
  MapView<EntryKey, EntryValue, CacheType> toView() const;

 private:
  using BufferManager = detail::BufferManager<CacheType>;

 public:
  using Iterator = detail::BufferManagerIterator<EntryKeyValue, BufferManager>;
  using ConstIterator =
      detail::BufferManagerIterator<const EntryKeyValue, BufferManager>;

  Iterator begin() { return Iterator{bufferManager_}; }
  Iterator end() { return Iterator{bufferManager_, Iterator::End}; }

  ConstIterator begin() const { return ConstIterator{bufferManager_}; }
  ConstIterator end() const {
    return ConstIterator{bufferManager_, ConstIterator::End};
  }

 private:
  using HashTable = detail::HashTable<EntryKey>;
  using HashTableHandle = typename CacheType::template TypedHandle<HashTable>;

  // Create a new cachelib::Map
  // @throw std::bad_alloc if fail to allocate hashtable or storage for a map
  Map(CacheType& cache,
      PoolId pid,
      typename CacheType::Key key,
      uint32_t numEntries,
      uint32_t numBytes);

  // Attach to an existing cachelib::Map
  Map(CacheType& cache, WriteHandle handle);

  // @return nullptr if not found
  const EntryValue* findImpl(const EntryKey& key) const;

  // @return kInserted if no old key exists,
  //         kReplaced otherwise.
  // @throw std::bad_alloc if failed to allocate
  InsertOrReplaceResult insertImpl(const EntryKey& key,
                                   const EntryValue& value);

  // @return false if failed to allocate a bigger item for hash table
  bool expandHashTable();

  // BEGIN private members
  CacheType* cache_{nullptr};
  HashTableHandle hashtable_{nullptr};
  BufferManager bufferManager_{nullptr};
  // END private members

  // Threshold after which we will trigger compaction automatically
  static constexpr int kWastedBytesPctThreshold = 50;
  static constexpr uint32_t kDefaultNumEntries = 20;
  static constexpr uint32_t kDefaultNumBytes = kDefaultNumEntries * 8;
};

template <typename K, typename V, typename C>
Map<K, V, C> Map<K, V, C>::create(CacheType& cache,
                                  PoolId pid,
                                  typename CacheType::Key key,
                                  uint32_t numEntries,
                                  uint32_t numBytes) {
  try {
    return Map{cache, pid, key, numEntries, numBytes};
  } catch (const std::bad_alloc&) {
    return nullptr;
  }
}

template <typename K, typename V, typename C>
Map<K, V, C> Map<K, V, C>::fromWriteHandle(CacheType& cache,
                                           WriteHandle handle) {
  if (!handle) {
    return nullptr;
  }
  return Map{cache, std::move(handle)};
}

template <typename K, typename V, typename C>
Map<K, V, C>::Map(CacheType& cache,
                  PoolId pid,
                  typename CacheType::Key key,
                  uint32_t numEntries,
                  uint32_t numBytes)
    : cache_(&cache),
      hashtable_(detail::createHashTable<K, C>(*cache_, pid, key, numEntries)),
      bufferManager_(
          BufferManager{*cache_, hashtable_.viewWriteHandle(), numBytes}) {}

template <typename K, typename V, typename C>
Map<K, V, C>::Map(CacheType& cache, WriteHandle handle)
    : cache_(&cache),
      hashtable_(std::move(handle)),
      bufferManager_(*cache_, hashtable_.viewWriteHandle()) {}

template <typename K, typename V, typename C>
Map<K, V, C>::Map(Map&& other)
    : cache_(other.cache_),
      hashtable_(std::move(other.hashtable_)),
      bufferManager_(*cache_, hashtable_.viewWriteHandle()) {}

template <typename K, typename V, typename C>
Map<K, V, C>& Map<K, V, C>::operator=(Map&& other) {
  if (this != &other) {
    this->~Map();
    new (this) Map(std::move(other));
  }
  return *this;
}

template <typename K, typename V, typename C>
const typename Map<K, V, C>::EntryValue* Map<K, V, C>::findImpl(
    const EntryKey& key) const {
  auto* entry = hashtable_->find(key);
  if (!entry) {
    return nullptr;
  }
  return &bufferManager_.template get<EntryKeyValue>(entry->addr)->second;
}

template <typename K, typename V, typename C>
typename Map<K, V, C>::EntryValue* Map<K, V, C>::find(const EntryKey& key) {
  return const_cast<EntryValue*>(findImpl(key));
}

template <typename K, typename V, typename C>
const typename Map<K, V, C>::EntryValue* Map<K, V, C>::find(
    const EntryKey& key) const {
  return findImpl(key);
}

template <typename K, typename V, typename C>
typename Map<K, V, C>::InsertOrReplaceResult Map<K, V, C>::insertImpl(
    const EntryKey& key, const EntryValue& value) {
  bool chainCloned = false;
  auto accessible = hashtable_.viewWriteHandle()->isAccessible();
  // We try to expand hash table if it's full, if we can't do it
  // we have to abort this insert because we may not be albe to insert
  if (hashtable_->overLimit()) {
    const auto res = expandHashTable();
    if (!res) {
      throw std::bad_alloc();
    }
    chainCloned = true;
  }

  // If wasted space is more than threshold, trigger compaction
  if (bufferManager_.wastedBytesPct() > kWastedBytesPctThreshold) {
    compact();
  }

  const uint32_t valueSize = util::getValueSize(value);
  const uint32_t keySize = sizeof(EntryKey);
  auto addr = bufferManager_.allocate(keySize + valueSize);
  if (!addr) {
    // Clone the buffers (chained items), if we have not already done that in
    // this insert, so that if a user holds an old handle to the Map, that
    // handle will still allow the user to access the old Map.
    if (!chainCloned) {
      auto newHashTable = detail::copyHashTable<K, C>(*cache_, hashtable_,
                                                      hashtable_->capacity());
      if (!newHashTable) {
        throw std::bad_alloc();
      }

      auto newBufferManager =
          bufferManager_.clone(newHashTable.viewWriteHandle());
      if (newBufferManager.empty()) {
        throw std::bad_alloc();
      }

      hashtable_ = std::move(newHashTable);
      bufferManager_ = BufferManager(*cache_, hashtable_.viewWriteHandle());
      chainCloned = true;
    }
    if (bufferManager_.expand(keySize + valueSize)) {
      addr = bufferManager_.allocate(keySize + valueSize);
    }
  }
  if (chainCloned && accessible) {
    cache_->insertOrReplace(hashtable_.viewWriteHandle());
  }
  if (!addr) {
    throw std::bad_alloc();
  }

  auto* kv = bufferManager_.template get<EntryKeyValue>(addr);
  std::memcpy(&kv->first, &key, keySize);
  std::memcpy(&kv->second, &value, valueSize);

  detail::BufferAddr oldAddr;
  try {
    oldAddr = hashtable_->insertOrReplace(key, addr);
  } catch (const std::bad_alloc&) {
    bufferManager_.remove(addr);
    throw;
  }
  if (oldAddr) {
    bufferManager_.remove(oldAddr);
    return kReplaced;
  }
  return kInserted;
}

template <typename K, typename V, typename C>
bool Map<K, V, C>::insert(const EntryKey& key, const EntryValue& value) {
  auto* entry = hashtable_->find(key);
  if (entry) {
    return false;
  }
  insertImpl(key, value);
  return true;
}

template <typename K, typename V, typename C>
typename Map<K, V, C>::InsertOrReplaceResult Map<K, V, C>::insertOrReplace(
    const EntryKey& key, const EntryValue& value) {
  return insertImpl(key, value);
}

template <typename K, typename V, typename C>
bool Map<K, V, C>::erase(const EntryKey& key) {
  auto addr = hashtable_->remove(key);
  if (addr) {
    bufferManager_.remove(addr);
    return true;
  }
  return false;
}

template <typename K, typename V, typename C>
size_t Map<K, V, C>::sizeInBytes() const {
  size_t numBytes = 0;

  const WriteHandle& parent = hashtable_.viewWriteHandle();
  numBytes += parent->getSize();

  auto allocs = cache_->viewAsChainedAllocs(parent);
  for (const auto& c : allocs.getChain()) {
    numBytes += c.getSize();
  }
  return numBytes;
}

template <typename K, typename V, typename C>
void Map<K, V, C>::compact() {
  // The idea below is first we compact all allocations in buffer manager.
  // Afterwards, we iterate through each allocation in buffer manager,
  // and for each allocation, we replace its key/value in the hashtable
  // with its new buffer address.
  bufferManager_.compact();
  for (auto itr = begin(), endItr = end(); itr != endItr; ++itr) {
    detail::BufferAddr oldAddr;
    try {
      oldAddr = hashtable_->insertOrReplace(itr->first, itr.getAsBufferAddr());
    } catch (const std::bad_alloc&) {
      throw std::runtime_error(
          "hashtable cannot have insufficient space during a compaction");
    }
    if (!oldAddr) {
      auto key = itr->first;
      throw std::runtime_error(folly::sformat(
          "old entry is missing, this should never happen. key: {}", key));
    }
  }
}

template <typename K, typename V, typename C>
bool Map<K, V, C>::expandHashTable() {
  auto newHashTable = detail::expandHashTable<K, C>(*cache_, hashtable_);
  if (!newHashTable) {
    return false;
  }

  // Clone the buffers (chaind items) when expanding hash table, so that if a
  // user holds an old handle to the old hashtable, it will still be valid and
  // can still access the old Map.
  auto newBufferManager = bufferManager_.clone(newHashTable.viewWriteHandle());
  if (newBufferManager.empty()) {
    return false;
  }

  hashtable_ = std::move(newHashTable);
  bufferManager_ = BufferManager{*cache_, hashtable_.viewWriteHandle()};
  return true;
}

template <typename K, typename V, typename C>
MapView<K, V, C> Map<K, V, C>::toView() const {
  auto& parent = hashtable_.viewWriteHandle();
  auto allocs = cache_->viewAsChainedAllocs(parent);
  return MapView<K, V, C>{*parent, allocs.getChain()};
}
} // namespace facebook::cachelib
