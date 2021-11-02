---
id: chained_items
title: Chained items
---

The `allocate()` method allocates memory for data whose size is less than the maximum allocation size (default: 4MB). To cache data whose size exceeds maximum allocation size, use chained allocations.
You can also use chained allocations to extend your data's size gradually.

## Chained allocations

When you call the the `allocate()` method to allocate memory for your data, your data's size must be less than the maximum slab size (4 MB):

```cpp
template <typename CacheTrait>;
class CacheAllocator : public CacheBase {
  public:
    // Allocate memory of a specific size from cache.
    ItemHandle allocate(
      PoolId id,
      Key key,
      uint32_t size,
      uint32_t ttlSecs = 0,
      uint32_t creationTime = 0,
    );
  // ...
};
```

For example:

```cpp
string data("Hello world");

// Allocate memory for the data.
auto item_handle = cache->allocate(pool_id, "key1", data.size());
```

The allocated memory can't be changed at runtime. To extend this memory, use chained allocations:

1. Call the `allocate()` method to allocate memory for an item (the parent item).
2. Add chained items to the parent item. Call the `allocateChainedItem()` method to allocate memory for these chained items. A chained item doesn't have a key; thus you must use its parent item to access it.

The following is the declaration of the `allocateChainedItem()` method:

```cpp
template <typename CacheTrait>;
class CacheAllocator : public CacheBase {
  public:
    ItemHandle allocateChainedItem(const ItemHandle& parent, uint32_t size);
  // ...
};
```

## Insertion order and read order
Chained items are inserted in LIFO order. When user reads through the chained item using the ChainedAllocs API. The iteration happens in LIFO order starting with the most recently inserted chained item until the chained item inserted first. When user uses `convertToIOBuf` API, it is in FIFO order starting with the parent, and end with the most recently inserted chained item.

For example:

```
auto parent = cache->allocate(0, "test key", 0);
for (int i = 0; i < 3; i++) {
  auto child = cache->allocateChainedItem(parent, sizeof(int));
  *reinterpret_cast<int*>(child->getMemory()) = i;
  cache->addChainedItem(std::move(child));
}

auto chainedAllocs = cache->viewAsChainedAllocs(parent);
for (const auto& c : chainedAllocs.getChain()) {}
// 3 -> 2 -> 1

auto iobuf = cache->convertToIOBuf(std::move(parent));
// parent -> 1 -> 2 -> 3
```

## Example: A custom data structure with a large data blob.


Let's assume we have a data structure that represents a large cache payload.

```cpp
struct LargeUserData {
  uint64_t version;
  uint64_t timestamp;
  size_t length;
  int[] data;
};
```

The following code breaks this large cache data and caches it using mulitple
items through `ChainedItems`.

<details> <summary> Caching `LargeUserData` with chained items </summary>

```cpp
std::unique_ptr<LargeUserData> userData = getLargeUserData();

size_t userDataSize = sizeof(LargeUserData) + sizeof(int) * userData->length;

// For simplicity, we'll split the user data into 1MB chunks
size_t numChunks = userDataSize / (1024 * 1024 * 1024);

struct CustomParentItem {
  size_t numChunks;
  void* dataPtr[];  // an array of pointers to the chunks
};

size_t parentItemSize = sizeof(CustomParentItem) + numChunks * sizeof(void*);

// for simplicity, assume this fits into 1MB
assert(parentItemSize <(1024 * 1024));

auto parentItemHandle =
    cache.allocate(defaultPool, "an item split into chunks", parentItemSize);

CustomParentItem* parentItem =
    reinterpret_cast<CustomParentItem*>(parentItemHandle->getMemory());

// Now split user data into chunks and cache them
for (size_t i = 0; i < numChunks; ++i) {
  size_t chunkSize = 1024 * 1024;
  auto chainedItemHandle =
      cache.allocateChainedItem(parentItemHandle, chunkSize);

  // For simplicity, assume we always have enough memory
  assert(chainedItemHandle != nullptr);

  // Compute user data offset and copy data over
  uint8_t* dataOffset =
      reinterpret_cast<uint8_t*>(userData->data) + chunkSize * i;
  std::memcpy(chainedItemHandle->getMemory(), dataOffset, chunkSize);

  // Add this chained item to the parent item
  cache.addChainedItem(parentItemHandle, std::move(chainedItemHandle));
}

// Now, make parent item visible to others
cache.insert(parentItemHandle);
```
</details>
