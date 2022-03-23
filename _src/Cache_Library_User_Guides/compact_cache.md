---
id: compact_cache
title: Compact cache
---

**This feature is in maintenance mode**. All future development plans are dropped. We understand there is value in optimizing for very small payloads, but we think the correct direction is to reduce item overhead in the regular cache instead of adding small, separate caches that offer low space overhead. Please reach out to us directly if you have a strong need for a RAM cache optimized for very small items.

## Overview

Compact cache is optimal to cache *small key-value data*, usually less than tens of bytes per entry. At compile time, the key must be fixed size, while the value can be fixed size or variable size or empty. Compact cache avoids the overhead (32 bytes per entry) incurred to an item stored in a regular cache. Compact cache has almost 0 bytes overhead for fixed size values. For variable sized entries, we pay a 3 byte overhead per bucket, and 4 bytes per entry. Compact cache is also a good alternative for `folly::EvictingCacheMap`, which has an overhead of 24 bytes per entry. In general, these optimizations only matter when your cache size in hundreds of MB or higher.

To optimize for small values, compact cache makes a few trade-offs and API changes compared to regular cache: unlike regular cache where you get or set data in-place in the cache, compact cache always performs a copy. Copying small keys and values that are cache line sized (128 bytes) has similar performance characteristics to not copying and modifying in-place.

For more information, see cachelib/examples/simple_compact_cache/main.cpp

## Defining compact cache instance

To get started, define the compact cache key type and value type:


```cpp
struct MyKey {
  int id; // Or your own POD fields.

  // Key comparator.
  bool operator==(const Key& other) const { return id == other.id; }

  // Determine if an entry is empty. Choose a value that you are not going to cache.
  bool isEmpty() const { return id == 0; }
};

struct MyValue {
  char[10] buf; // This can also be any variable length blob.
};
```


Then define different compact cache types:


```cpp
#include <cachelib/allocator/CCacheAllocator.hpp>
#include <cachelib/compact_cache/CCacheCreator.h>

using FixedValueCCache = CCacheCreator<CCacheAllocator, Key, Value>::type;
using ZeroSizeValueCCache = CCacheCreator<CCacheAllocator, Key>::type;

// compact cache with variable sized records
constexpr size_t kMaxSize = 128;
using VariableValueCCache = CCacheVariableCreator<CCacheAllocator, Key, kMaxSize>::type;
```


The code shows how to create three different types of compact caches:

- `FixedValueCCache`
Values' sizes are fixed and must be defined at compile time by the `Value` Type.
- `VariableValueCCache`
Values' sizes are varied, but less than a predefined maximum size.
- `ZeroSizeValueCCache`
Useful for storing keys that are not associated with any values.

## Creating compact cache instance

After defining a compact cache type, you can start creating an instance of it from the main cache:

Make sure that your cache config has the following enabled:


```cpp
config.enableCompactCache(); // required to use compact cache
```


Then use the defined compact cache type to create a compact cache instance. Assume `MyCacheType` is the type that you defined.


```cpp
// interface detail can be found in cachelib/allocator/CacheAllocator.hpp
auto* ccache = cache.addCompactCache<MyCCacheType>("ccache name", sizeInBytes);
```


## Caching using compact cache

Once you have the cache setup, you can store your keys and values in the compact cache using the following API


```cpp
// get/set/del/exist interface defined in cachelib/compact_cache/CCache.h
Key k{10};
Value v{"foobar"};

// Set the key and value.
auto res = ccache->set(k, &v);

// Look up by key.
Value v2;
auto res = ccache->get(k, &v2 /* copy the value if cache hit */);

// Delete the key. Returns a copy of the deleted value.
Value v3;
auto res = ccache->del(k, &v3 /* copy of the deleted value if present */);
```


The APIs return an enum of the `CCacheReturn` type:


```cpp
enum class CCacheReturn : int {
  TIMEOUT = -2,
  ERROR = -1,
  NOTFOUND = 0,
  FOUND = 1
};
```


## Restoring an instance of compact cache

This is only relevant if your cache is persistent and you want to access the cache that you have previously created in another process. After the compact cache was attached, you should be able to use the instance the same way as you did when you first created it, with all the previous content intact:


```cpp
// For details, see cachelib/allocator/CacheAllocator.hpp.
auto* ccache = cache.attachCompactCache<CCacheType>("ccache name");
```
