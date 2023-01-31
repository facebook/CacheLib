---
id: Set_up_a_simple_cache
title: Set up a simple dram cache
---

Before calling cachelib to cache your data, set up a simple dram cache first.

To set up a simple dram cache, you need to provide the following to cachelib:

- Eviction policy
  - How data is evicted from the cache?
- Cache configuration
  - What's the cache size?
  - What's the name of the cache?
  - What's the access configuration for the cache? The access configuration is used to tune the [hash table](Configure_HashTable) for looking up data in cache.

The allocator/CacheAllocator.cpp file instantiates the following class templates for different eviction policies:


```cpp
template class CacheAllocator<LruCacheTrait>;
template class CacheAllocator<LruCacheWithSpinBucketsTrait>;
template class CacheAllocator<Lru2QCacheTrait>;
template class CacheAllocator<TinyLFUCacheTrait>;
```


where `LruCacheTrait`, `LruCacheWithSpinBucketsTrait`, `Lru2QCacheTrait`, and `TinyLFUCacheTrait` are declared in allocator/CacheTraits.h. You can configure the eviction parameters. For more information, see [Eviction Policy](eviction_policy).

LRU is the most commonly used trait. To use LRU as the eviction policy for your cache, use the following instantiated class:

```cpp
#include "cachelib/allocator/CacheAllocator.h"

using Cache = facebook::cachelib::LruAllocator;
```


Before setting up a simple cache, use an object of a `CacheAllocatorConfig` class to set the cache's configuration:


```cpp
Cache::Config config;
config
    .setCacheSize(1 * 1024 * 1024 * 1024) // 1 GB
    .setCacheName("My cache") // unique identifier for the cache
    .setAccessConfig({25, 10})
    .validate();
```


**Caution!!**
> If you're using multiple cache instances, please consider if you can simplify to using a single instance via multiple cache pools. CacheLib is highly optimized for concurrency; using multiple cachelib instances is typically an anti-pattern unless you have a good reason.

The code to set up a simple cache follows:


```cpp
#include "cachelib/allocator/CacheAllocator.h"

using Cache = facebook::cachelib::LruAllocator;
std::unique_ptr<Cache> cache;
facebook::cachelib::PoolId default_pool;

void initializeCache() {
  Cache::Config config;
  config
      .setCacheSize(1 * 1024 * 1024 * 1024) // 1 GB
      .setCacheName("My cache")
      .setAccessConfig({25, 10})
      .validate();
  cache = std::make_unique<Cache>(config);
  default_pool =
      cache->addPool("default", cache->getCacheMemoryStats().ramCacheSize);
}

```


Before using a cache, you need to partition its memory into pools. You can add many pools to the cache. In this example, only a default pool is added to the `cache`. Because of a fixed overhead needed to manage the cache, the default pool's size is less than 1 GB.

For a complete program to set up a simple cache, see `examples/simple_cache/main.cpp`.

## Test the simple cache setup program

To test the simple cache setup program `examples/simple_cache/main.cpp`:

1. Build `cachelib` with all the dependecies:

    ```sh
    git clone https://github.com/facebook/CacheLib
    cd CacheLib
    ./contrib/build.sh -d -j -v -T
    ```

2. Build the simple cache setup program:

   ```sh
   cd examples/simple_cache
   ./build.sh
   ```
3. Run the program:

   ```sh
   ./build/simple-cache-example
   ```

   When you run `simple-cache-example`, it doesn't produce any output.

**Exercise**: Add code to `examples/simple_cache/main.cpp` to do the following:

- Print the value of the item with key `key`.
- Compare the default pool size with the cache size (1 GB).


<details><summary>Answer</summary>

```cpp
// ...
#include <iostream>  // add this
// ...

int main(int argc, char** argv) {
  // ...
  {
    // ...
    assert(sp == "value");

    // Add this statement to print the value of the item with key "key".
    std::cout << "value = " << sp << '\n';

    // Add the following code to compare the default pool size with the cache size.
    auto cache_size = 1024 * 1024 * 1024; // 1 GB
    auto default_pool_size = gCache_->getCacheMemoryStats().ramCacheSize;
    std::cout << "cache size = " << cache_size << '\n';
    std::cout << "default pool size = " << default_pool_size << '\n';
  }

  destroyCache();
}
```


</details>

## Use cache

After setting up a simple cache, you can do the following:
- [Write data to the cache](Write_data_to_cache).
- [Read data from the cache](Read_data_from_cache).
- [Remove data from the cache](Remove_data_from_cache).
- [Visit data in the cache](Visit_data_in_cache).

## Release and delete cache

When you're done with the cache, to release and delete it, call the `reset()` method:


```cpp
cache.reset();
```
