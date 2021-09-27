---
id: Write_data_to_cache
title: Write data to cache
---

After [setting up your cache](Set_up_a_simple_cache), you can start writing data to it.

To use cachelib to write data to your cache:

- Allocate memory for the data from the cache, which will return an item handle to the allocated memory. Item handle provides a reference counted wrapper to access a cache item.
- Write the data to the allocated memory and insert the item handle into the cache.

## Allocate memory for data from cache

The header file allocator/CacheAllocator.h declares the following methods to allocate memory from cache:


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

    // Allocate memory for a chained item of a specific size from cache.
    ItemHandle allocateChainedItem(const ItemHandle& parent, uint32_t size);
  // ...
};
```


For example:


```cpp
auto pool_id = cache->addPool(
  "default_pool",
  cache->getCacheMemoryStats().cacheSize
);
ItemHandle item_handle = cache->allocate(pool_id, "key1", 1024);
```


where:
- `cache` is a `unique_ptr` to `CacheAllocator<facebook::cachelib::LruAllocator>` (see [Set up a simple dram cache](Set_up_a_simple_cache)).
- `ItemHandle` is a `CacheItem<facebook::cachelib::LruAllocator>::Handle` (see allocator/CacheItem.h), which is `facebook::cachelib::detail::HandleImpl` defined in allocator/Handle.h. If allocation failed, an empty handle will be returned.

To get the writable memory from the allocated memory, call the `getMemory` method via the item handle:


```cpp
if (item_handle) {
  void* pwm = item_handle->getMemory();
}
```


If the data size is greater than the maximum slab size (4 MB), use [chained items](chained_items) to store the data with multiple items. To allocate memory for additional chained items from cache, call this method:


```cpp
ItemHandle allocateChainedItem(const ItemHandle& parent, uint32_t size);
```


For example:


```cpp
size_t size = 1024 * 1024;
auto parent_item_handle = cache->allocate(pool_id, "parent", size);

if (parent_item_handle) {
  // Call allocateChainedItem() to allocate memory for 3 chained items.
  auto chained_item_handle_1 = cache->allocateChainedItem(parent_item_handle, 2 * size);
  auto chained_item_handle_2 = cache->allocateChainedItem(parent_item_handle, 4 * size);
  auto chained_item_handle_3 = cache->allocateChainedItem(parent_item_handle, 6 * size);
}
```


## Write data to allocated memory and insert item handle into cache

To write data to the allocated memory in the cache, do the following:

1. Call the `memcpy()` function to write data to the allocated memory got from the item handle.
2. Insert the item handle into cache.

The `memcpy` function requires the destination address:


```cpp
void* memcpy(void* destination, const void* source, size_t num);
```


To get the destination address, call the `getMemory()` method via the `ItemHandle`:


```cpp
void* pwm = item_handle->getMemory();
```


To insert an item to the cache, call one of the following methods defined in
allocator/CacheAllocator.h.:


```cpp
template <typename CacheTrait>
class CacheAllocator : public CacheBase {
  public:
    // will fail insertion if key is already present
    bool insert(const ItemHandle& handle);

    // will insert or replace existing item for the key
    ItemHandle insertOrReplace(const ItemHandle& handle);

    // link the chained items to the parent
    void addChainedItem(const ItemHandle& parent, ItemHandle child);
  // ...
};
```


For example, the following code writes *new* data (i.e., data associated with a new key) into the cache:


```cpp
string data("Hello world");

// Allocate memory for the data.
auto item_handle = cache->allocate(pool_id, "key1", data.size());

if (item_handle) {
  // Write the data to the allocated memory.
  std::memcpy(item_handle->getMemory(), data.data(), data.size());

  // Insert the item handle into the cache.
  cache->insertOrReplace(item_handle);
} else {
  // handle allocation failure
}
```


And the following code writes *new* data (i.e., data associated with a new key) to the cache and *replace* the data associated with an existing key in the cache with replacement data:


```cpp
string data("new data");
// Allocate memory for the data.
auto item_handle = cache->allocate(pool_id, "key2", data.size());

// Write the data to the cache.
std::memcpy(handle->getMemory(), data.data(), data.size());

// Insert the item handle into the cache.
cache->insertOrReplace(item_handle);

data = "Repalce the data associated with key key1";
// Allocate memory for the replacement data.
item_handle = cache->allocate(pool_id, "key1", data.size());

// Write the replacement data to the cache.
std::memcpy(item_handle->getMemory(), data.data(), data.size());

// Insert the item handle into the cache.
cache->insertOrReplace(item_handle);
```


To write data to chainded items, do the following:

1. For each chained item, write data to its memory allocated by the `allocateChainedItem()` method.
2. Call the `addChainedItem()` method to add the chained items to the parent item.

For example:


```cpp
std::vector<std::string> chained_items = { "item 1", "item 2", "item 3" };

for (auto itr = chained_items.begin(); itr != chained_items.end(); ++itr) {
  // Allocate memory for the chained item.
  auto item_handle = cache->allocateChainedItem(parent_handle, size);

  if (!item_handle) {
    // failed to allocate for the chained item.
    throw "error";
  }

  // Write data to the chained item.
  std::memcpy(item_handle->getMemory(), itr->data(), itr->size());

  // Add the chained item to the parent item.
  cache->addChainedItem(parent_handle, std::move(item_handle));
}
```

A common thing is to try out the biggest allocate-able size for a regular item and chained item. User can find those out by calling the following methods.
```
// Regular item
auto largestSize = (Largest Alloc Granularity: 4MB by default, or user-specified) - (Item::getRequiredSize(key, 0))

// Chained item
auto largestSize = (Largest Alloc Granularity) - (ChainedItem::getRequiredSize(0))
```
