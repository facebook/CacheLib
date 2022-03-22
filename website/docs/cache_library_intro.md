---
id: cache_library_intro
title: CacheLib user guide
slug: /
---

Cache library (CacheLib) is a C++ library for accessing and managing cache data. It is a
thread-safe API that enables developers to build and customize scalable, concurrent caches.

## Who should be using CacheLib?

CacheLib is targeted at services that use gigabytes of memory to cache information. It provides an API similar to `malloc` and a concurrent unordered hash table for caching. Consider using cache and cachelib for your service if any of the following applies to the service:

- Caching variable sized objects.
- Concurrent access with zero copy semantics.
- Hard RSS memory constraints for cache to avoid OOMs.
- Variety of caching algorithms like LRU, Segmented LRU, FIFO, 2Q, and TTL.
- Intelligent and automatic tuning of cache for dynamic changes to workload.
- Shared-memory based persistence of cache across process restarts.
- Transparent usage of DRAM and SSD for caching.

## Getting started with CacheLib

To get started with cachelib, read these guides:
- [Set up a simple cache](Cache_Library_User_Guides/Set_up_a_simple_cache)
- [Write data to the cache](Cache_Library_User_Guides/Write_data_to_cache).
- [Read data from the cache](Cache_Library_User_Guides/Read_data_from_cache).
- [Remove data from the cache](Cache_Library_User_Guides/Remove_data_from_cache).
- [Visit data in the cache](Cache_Library_User_Guides/Visit_data_in_cache).

Are you looking for a guide on how CacheLib is designed and how to add new features to it? Please refer [architecture guide](Cache_Library_Architecture_Guide/overview_a_random_walk).
