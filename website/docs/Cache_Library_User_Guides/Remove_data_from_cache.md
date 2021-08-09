---
id: Remove_data_from_cache
title: Remove data from cache
---

To remove data from cache, call these methods declared in [CacheAllocator.h](https://www.internalfb.com/intern/diffusion/FBS/browse/master/fbcode/cachelib/allocator/CacheAllocator.h):


```cpp
template <typename CacheTrait>
class CacheAllocator : public CacheBase {
  public:
    // Remove the item with the specified key.
    RemoveRes remove(Key key);

    // Remove the item pointed to by the specified handle.
    RemoveRes remove(const ItemHandle& handle);

    // Remove the item pointed to by the specified iterator.
    RemoveRes remove(AccessIterator& it);

    // Remove the first chained item pointed to by the parent handle.
    ItemHandle popChainedItem(const ItemHandle& parent)
  ...
};
```


where `RemoveRes` is an enum class defined in [CacheAllocator.h](https://www.internalfb.com/intern/diffusion/FBS/browse/master/fbcode/cachelib/allocator/CacheAllocator.h):


```cpp
enum class RemoveRes : uint8_t {
  kSuccess,
  kNotFoundInRam,
};
```


For example, the following code removes an unchained item with key `"key1"`:


```cpp
auto rr = cache->remove("key1");
if (rr == RemoveRes::kSuccess) {
  std::cout << "Removed the item with key \"key1\"" << '\n';
}
```


You can also remove the item as follows:


```cpp
auto item_handle = cache->find("key1");
if (item_handle) {
  auto rr = cache->remove(item_handle);
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
  auto item_handle = cache->allocate(pool_id, itr.first, itr.second.size());
  std::memcpy(item_handle->getWritableMemory(), itr.first.data(), itr.second.size());
  cache->insertOrReplace(item_handle);
}
```


you can call this method to remove the three items:


```cpp
RemoveRes remove(AccessIterator& it);
```


For example:


```cpp
for (auto itr = cache->begin(); itr != cache->end(); ++itr) {
  auto rr = cache->remove(itr);
  if (rr == RemoveRes::kSuccess) {
    cout << "Removed the item" << '\n';
  }
}

```


Consider the following chained items:

```
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
```

To remove the first chained item (`item 1`), call the `popChainedItem()` method:


```cpp
auto handle = cache->popChainedItem(parent_item_handle);
if (handle) {
  cout << "Removed the first chained item" << '\n';
}
```


If you remove the parent item, all its chained items are also removed:


```cpp
rr = cache->remove(parent_item_handle);
if (rr == RemoveRes::kSuccess) {
  parent_item_handle->reset();
}
```
