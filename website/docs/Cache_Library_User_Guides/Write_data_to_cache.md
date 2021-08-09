---
id: Write_data_to_cache
title: Write data to cache
---

After [setting up your cache](Set_up_a_simple_cache/ ), you can start writing data to it.

To use cachelib to write data to your cache:

- Allocate memory for the data from the cache, which will return an item handle to the allocated memory.
- Write the data to the allocated memory and insert the item handle into the cache.

## Allocate memory for data from cache

The header file {{#link: https://www.internalfb.com/intern/diffusion/FBS/browsefile/master/fbcode/cachelib/allocator/CacheAllocator.h | label=CacheAllocator.h, target=_blank, style=font-family:monospace; }} declares the following methods to allocate memory from cache:


```cpp
template &lt;typename CacheTrait&gt;
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
auto pool_id = cache-&gt;addPool(
  "default_pool",
  cache-&gt;getCacheMemoryStats().cacheSize
);
ItemHandle item_handle = cache-&gt;allocate(pool_id, "key1", 1024);
```


where:
- `cache` is a `unique_ptr` to `CacheAllocator<facebook::cachelib::LruAllocator>` (see [Set up a simple cache](Set_up_a_simple_cache/ )).
- `ItemHandle` is a `CacheItem<facebook::cachelib::LruAllocator>::Handle` (see {{#link: https://www.internalfb.com/intern/diffusion/FBS/browsefile/master/fbcode/cachelib/allocator//CacheItem.h | label=CacheItem.h, target=_blank, style=font-family:monospace; }}), which is `facebook::cachelib::detail::HandleImpl` defined in {{#link: https://www.internalfb.com/intern/diffusion/FBS/browsefile/master/fbcode/cachelib/allocator//Handle.h | label=Handle.h, target=_blank, style=font-family:monospace; }}. If allocation failed, an empty handle will be returned.

To get the writable memory from the allocated memory, call the `getWritableMemory` method via the item handle:


```cpp
if (item_handle) {
  void* pwm = item_handle-&gt;getWritableMemory();
}
```


If the data size is greater than the maximum slab size (4 MB), use [chained items](chained_items/ ) to store the data. To allocate memory for chained items from cache, call this method:


```cpp
ItemHandle allocateChainedItem(const ItemHandle& parent, uint32_t size);
```


For example:


```cpp
size_t size = 1024 * 1024;
auto parent_item_handle = cache-&gt;allocate(pool_id, "parent", size);

if (parent_item_handle) {
  // Call allocateChainedItem() to allocate memory for 3 chained items.
  auto chained_item_handle_1 = cache-&gt;allocateChainedItem(parent_item_handle, 2 * size);
  auto chained_item_handle_2 = cache-&gt;allocateChainedItem(parent_item_handle, 4 * size);
  auto chained_item_handle_3 = cache-&gt;allocateChainedItem(parent_item_handle, 6 * size);
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


To get the destination address, call the `getWritableMemory()` method via the `ItemHandle`:


```cpp
void* pwm = item_handle-&gt;getWritableMemory();
```


To insert an item to the cache, call one of the following methods defined in {{#link: https://www.internalfb.com/intern/diffusion/FBS/browsefile/master/fbcode/cachelib/allocator/CacheAllocator.h | label=CacheAllocator.h, target=_blank, style=font-family:monospace; }}:


```cpp
template &lt;typename CacheTrait&gt;
class CacheAllocator : public CacheBase {
  public:
    bool insert(const ItemHandle& handle);
    ItemHandle insertOrReplace(const ItemHandle& handle);
    void addChainedItem(const ItemHandle& parent, ItemHandle child);
  // ...
};
```


For example, the following code writes *new* data (i.e., data associated with a new key) into the cache:


```cpp
string data("Hello world");

// Allocate memory for the data.
auto item_handle = cache-&gt;allocate(pool_id, "key1", data.size());

if (item_handle) {
  // Write the data to the allocated memory.
  std::memcpy(item_handle-&gt;getWritableMemory(), data.data(), data.size());

  // Insert the item handle into the cache.
  cache-&gt;insert(item_handle);
} else {
  // handle allocation failure
}
```


And the following code writes *new* data (i.e., data associated with a new key) to the cache and *replace* the data associated with an existing key in the cache with replacement data:


```cpp
string data("new data");
// Allocate memory for the data.
auto item_handle = cache-&gt;allocate(pool_id, "key2", data.size());

// Write the data to the cache.
std::memcpy(handle-&gt;getWritableMemory(), data.data(), data.size());

// Insert the item handle into the cache.
cache-&gt;insertOrReplace(item_handle);

data = "Repalce the data associated with key key1";
// Allocate memory for the replacement data.
item_handle = cache-&gt;allocate(pool_id, "key1", data.size());

// Write the replacement data to the cache.
std::memcpy(item_handle-&gt;getWritableMemory(), data.data(), data.size());

// Insert the item handle into the cache.
cache-&gt;insertOrReplace(item_handle);
```


To write data to chainded items, do the following:

1. For each chained item, write data to its memory allocated by the `allocateChainedItem()` method.
2. Call the `addChainedItem()` method to add the chained items to the parent item.

For example:


```cpp
std::vector&lt;std::string&gt; chained_items = { "item 1", "item 2", "item 3" };

for (auto itr = chained_items.begin(); itr != chained_items.end(); ++itr) {
  // Allocate memory for the chained item.
  auto item_handle = cache-&gt;allocateChainedItem(parent_handle, size);

  if (!item_handle) {
    // failed to allocate for the chained item.
    throw "error";
  }

  // Write data to the chained item.
  std::memcpy(item_handle-&gt;getWritableMemory(), itr-&gt;data(), itr-&gt;size());

  // Add the chained item to the parent item.
  cache-&gt;addChainedItem(parent_handle, std::move(item_handle));
}
```

A common thing is to try out the biggest allocate-able size for a regular item and chained item. User can find those out by calling the following methods.
```
// Regular item
auto largestSize = (Largest Alloc Granularity: 4MB by default, or user-specified) - (Item::getRequiredSize(key, 0))

// Chained item
auto largestSize = (Largest Alloc Granularity) - (ChainedItem::getRequiredSize(0))
```
