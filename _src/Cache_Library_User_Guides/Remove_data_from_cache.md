---
id: Remove_data_from_cache
title: Remove data from cache
---

To remove data from cache, call these methods declared in allocator/CacheAllocator.h:


```cpp
template <typename CacheTrait>
class CacheAllocator : public CacheBase {
  public:
    // Remove the item with the specified key.
    RemoveRes remove(Key key);

    // Remove the item pointed to by the specified handle.
    RemoveRes remove(const ReadHandle& handle);

    // Remove the first chained item pointed to by the parent handle.
    WriteHandle popChainedItem(WriteHandle& parent)
  ...
};
```


where `RemoveRes` is an enum class defined in allocator/CacheAllocator.h:


```cpp
enum class RemoveRes : uint8_t {
  kSuccess,
  kNotFoundInRam,
};
```


For example, the following code removes an item with key `"key1"`:


```cpp
auto rr = cache->remove("key1");
if (rr == RemoveRes::kSuccess) {
  std::cout << "Removed the item with key \"key1\"" << '\n';
}
```


You can also remove the item as follows:


```cpp
auto handle = cache->find("key1");
if (handle) {
  auto rr = cache->remove(handle);
  if (rr == RemoveRes::kSuccess) {
    std::cout << "Removed the item with key \"key1\"" << '\n';
  }
}
```


If you write the following three items to cache:

```cpp
std::map<std::string, std::string> dict = {
  { "key1", "item 1" },
  { "key2", "item 2" },
  { "key3", "item 3" },
};
for (auto& itr : dict) {
  auto handle = cache->allocate(poolId, itr.first, itr.second.size());
  std::memcpy(handle->getMemory(), itr.first.data(), itr.second.size());
  cache->insertOrReplace(handle);
}
```


you can iterate and remove the three items:

For example:


```cpp
for (auto itr = cache->begin(); itr != cache->end(); ++itr) {
  auto rr = cache->remove(itr.asHandle());
  if (rr == RemoveRes::kSuccess) {
    cout << "Removed the item" << '\n';
  }
}

```

Note that `it.asHandle()` returns the item handle for the  iterator


Consider the following chained items:

<!--
<graphviz>
digraph g {
  rankdir = LR;
  node [shape=box, style=filled, fontsize=10, fontname=Helvetica,];
  p[label="parent"]
  c1[label="item 1"]
  c2[label="item 2"]
  c3[label="item 3"]
  p -> c1;
  c1 -> c2;
  c2 -> c3;
}
</graphviz>
-->
![](remove_data_from_cache_items.png)

To remove the first chained item (`item 1`), call the `popChainedItem()` method:


```cpp
auto handle = cache->popChainedItem(parentItemHandle);
if (handle) {
  cout << "Removed the first chained item" << '\n';
}
```


If you remove the parent item, all its chained items are also removed:


```cpp
rr = cache->remove(parentItemHandle);
if (rr == RemoveRes::kSuccess) {
  parentItemHandle->reset();
}
```
