---
id: Read_data_from_cache
title: Read data from cache
---

An item written to cache by cachelib is associated with a key. To read the item from cache, call the `find()` method (defined in allocator/CacheAllocator.h) with the key to look up the item:


```cpp
ItemHandle find(Key key);
```


For example:


```cpp
auto item_handle = cache->find("key1");
```


The `find()` method returns an `ItemHandle` that you use to get the memory location of the data:


```cpp
void* pdata = item_handle->getMemory();
```


The `getMemory()` method returns a `void*` pointer. Use `reinterpret_cast<T*>` to cast it to a pointer of a specific type `T`:


```cpp
auto data = reinterpret_cast<T*>(pdata);
```


For example:


```cpp
auto item_handle = cache->find("key1");
if (item_handle) {
  auto data = reinterpret_cast<const char*>(item_handle->getMemory());
  std::cout << data << '\n';
}
```


You can also use iterators to read all the items written to the cache. See [Visit data in cache](Visit_data_in_cache).

To read data from chained items, start from the parent `ItemHandle`, for example:


```cpp
auto chained_allocs = cache->viewAsChainedAllocs(parent_item_handle);
for (const auto& c : chained_allocs.getChain()) {
  auto data = reinterpret_cast<const char*>(c.getMemory());
  std::cout << data << '\n';
}
```


**Refer to [Chained items](chained_items) to see how chained items are ordered in cache.**

To get the *n*th item in the chain, call the `getNthInChain()` method via `CacheChainedAllocs`:


```cpp
auto chained_allocs = cache->viewAsChainedAllocs(parent_item_handle);
auto item = chained_allocs.getNthInChain(1);
if (item) {
  std::cout << reinterpret_cast<const char*>(item->getMemory()) << '\n';
}
```


Note that the first item has index `0`, second item has index `1`, and so on.
