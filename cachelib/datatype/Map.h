#pragma once

#include <limits>

#include "cachelib/allocator/TypedHandle.h"
#include "cachelib/common/Hash.h"
#include "cachelib/common/Iterators.h"
#include "cachelib/datatype/Buffer.h"
#include "cachelib/datatype/DataTypes.h"

namespace facebook {
namespace cachelib {
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

    Entry(const Entry& rhs) = default;
    Entry& operator=(const Entry& rhs) = default;
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

  explicit HashTable(size_t capacity);

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
} // namespace detail

// Exception when cachelib::Map's index has maxed out.
class MapIndexMaxedOut : public std::runtime_error {
  using std::runtime_error::runtime_error;
};

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
  using ItemHandle = typename Item::Handle;

  struct FOLLY_PACK_ATTR EntryKeyValue {
    EntryKey key;
    EntryValue value;
  };

  // Create a new cachelib::Map
  // @param cache   cache allocator to allocate from
  // @param pid     pool where we'll allocate the map from
  // @param key     key for the item in cache
  // @param numEntries   number of entries this map can contain initially
  // @param numBytes     number of bytes allocated for value storage initially
  // @return  valid cachelib::Map on success,
  //          cachelib::Map::isNullItemHandle() == true on failure
  static Map create(CacheType& cache,
                    PoolId pid,
                    typename CacheType::Key key,
                    uint32_t numEntries = kDefaultNumEntries,
                    uint32_t numBytes = kDefaultNumBytes);

  // Convert a item handle to a cachelib::Map
  // @param cache   cache allocator to allocate from
  // @param handle  parent handle for this cachelib::Map
  // @return cachelib::Map
  static Map fromItemHandle(CacheType& cache, ItemHandle handle);

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
  // It resets it to an item handle, which can be used with any API in
  // CacheAllocator that deals with ItemHandle. After invoking this function,
  // this structure is left in a null state.
  ItemHandle resetToItemHandle() && {
    return std::move(hashtable_).resetToItemHandle();
  }

  // Borrow the item handle underneath this structure. This is useful to
  // implement insertion into CacheAllocator.
  const ItemHandle& viewItemHandle() const {
    return hashtable_.viewItemHandle();
  }

  bool isNullItemHandle() const { return hashtable_ == nullptr; }

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
  Map(CacheType& cache, ItemHandle handle);

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
  static constexpr int kWastedSpacePctThreshold = 50;
  static constexpr uint32_t kDefaultNumEntries = 20;
  static constexpr uint32_t kDefaultNumBytes = kDefaultNumEntries * 8;
};
} // namespace cachelib
} // namespace facebook

#include "cachelib/datatype/Map-inl.h"
