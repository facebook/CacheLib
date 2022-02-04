---
id: Read_data_from_cache
title: Read data from cache
---

An item written to cache by cachelib is associated with a key. To read the item from cache, call the `find()` method (defined in allocator/CacheAllocator.h) with the key to look up the item:


```cpp
ReadHandle find(Key key);
```


For example:


```cpp
auto handle = cache->find("key1");
```


The `find()` method returns a `ReadHandle` that provides the read-only view of an item. You can use `ReadHandle` to get the read-only memory location of the data:


```cpp
const void* pdata = handle->getMemory();
```
where `handle` is of type `ReadHandle`.


The `getMemory()` method via a `ReadHandle` returns a `const void*` pointer. Use `reinterpret_cast<const T*>` to cast it to a pointer of a specific type `const T`:


```cpp
auto data = reinterpret_cast<const T*>(pdata);
```


For example:


```cpp
auto handle = cache->find("key1");
if (handle) {
  auto data = reinterpret_cast<const char*>(handle->getMemory());
  std::cout << data << '\n';
}
```


You can also use iterators to read all the items written to the cache. See [Visit data in cache](Visit_data_in_cache).

To read data from chained items, start from the parent item handle, for example:


```cpp
auto chainedAllocs = cache->viewAsChainedAllocs(parent_item_handle);
for (auto& c : chainedAllocs.getChain()) {
  auto data = reinterpret_cast<const char*>(c.getMemory());
  std::cout << data << '\n';
}
```


**Refer to [Chained items](chained_items) to see how chained items are ordered in cache.**

To get the *n*th item in the chain, call the `getNthInChain()` method via `CacheChainedAllocs`. The returned *n*th item will be read-only:


```cpp
auto chainedAllocs = cache->viewAsChainedAllocs(parentItemHandle);
auto item = chainedAllocs.getNthInChain(1);
if (item) {
  std::cout << reinterpret_cast<const char*>(item->getMemory()) << '\n';
}
```


Note that the first item has index `0`, second item has index `1`, and so on.
