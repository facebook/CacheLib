---
id: Visit_data_in_cache
title: Visit data in cache
---

Cachelib provides a concurrent iterator to visit unchained data (items) in a cache while other threads are inserting data to or removing data from the cache. At any time, an item visited by the iterator is guaranteed to be valid even if it is concurrently removed by another thread.

The current iterator is *unordered*; that is, the order it visits the items is not the same as the order in which they're written to the cache. During iteration, it guarantees visiting all items that exist in the cache. Iterating the cache does not block any other cache operation like `find()` or `allocate()`. However, before calling the `shutDown()` API to shut down the cache, you must release all iterators.

For example, suppose you write the following three items to cache:


```cpp
std::map<std::string, std::string> dict = {
  { "key1", "item 1" },
  { "key2", "item 2" },
  { "key3", "item 3" },
};
for (const auto& [k, v] : dict) {
  auto item_handle = cache->allocate(pool_id, k, v.size());
  std::memcpy(item_handle->getMemory(), v.data(), v.size());
  cache->insertOrReplace(item_handle);
}
```


To visit these three items in the cache, use an iterator:


```cpp
for (auto itr = cache->begin(); itr != cache->end(); ++itr) {
  auto key = itr->getKey();
  auto data = reinterpret_cast<const char*>(itr->getMemory());
  std::cout << key << " -> " << data << '\n';
}
```


You can also use the shorter `for-each` statement to visit them:


```cpp
for (const auto& itr : *cache) {
  auto key = itr.getKey();
  auto data = reinterpret_cast<const char*>(itr.getMemory());
  std::cout << key << " -> " << data << '\n';
}
```


Chained items are stored in cache as a linked list. For example, suppose you write three chained items to cache:


```cpp
std::string parent_item("parent item");
auto parent_item_handle = cache->allocate(pool_id, "parent key", parent_item.size());
std::memcpy(parent_item_handle->getMemory(), parent_item.c_str(), parent_item.size());
cache->insert(parent_item_handle);

auto size = 100
std::vector<std::string> vitems = { "item 1", "item 2", "item 3" };
for (auto& itr : vitems) {
  auto item_handle = cache->allocateChainedItem(parent_item_handle, size);
  std::memcpy(item_handle->getMemory(), itr.c_str(), itr.size());
  cache->addChainedItem(parent_item_handle, std::move(item_handle));
}
```


To visit the chained items, use the parent `ItemHandle` to traverse the list:


```cpp
auto chained_allocs = cache->viewAsChainedAllocs(parent_item_handle);
for (const auto& c : chained_allocs.getChain()) {
  auto data = reinterpret_cast<const char*>(c.getMemory());
  std::cout << data << '\n';
}
```
