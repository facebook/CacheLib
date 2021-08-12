---
id: cache_library_intro
title: Cache Library User Guides
slug: /
---



## Cache Library User Guides





Cache Library (cachelib) is a C++ library for accessing and managing cache data. It is a
thread-safe API that enables developers to build and customize scalable, concurrent caches.

## Who should be using cachelib?

CacheLib is targeted at services that use gigabytes of memory to cache information. It provides an API similar to `malloc` and a concurrent unordered hash table for caching. Consider using cache and cachelib for your service if any of the following applies to the service:

- Caching variable sized objects.
- Concurrent access with zero copy semantics.
- Hard RSS memory constraints for cache to avoid OOMs.
- Variety of caching algorithms like LRU, Segmented LRU, FIFO, 2Q, and TTL.
- Intelligent and automatic tuning of cache for dynamic changes to workload.
- Shared-memory based persistence of cache across process restarts.
- Transparent usage of DRAM and SSD for caching.

If you're interested in using cachelib, please go through this exercise and fill  out the [onboarding questionnaire](Cache_Library_User_Guides/Cache_Library_onboarding_questionnaire/ )
and forward it to [cachelib oncall](https://www.internalfb.com/intern/monitor/oncall_management_scheduling?oncall=cachelib).

If you have questions and discussions on cachelib, post them to the [Cache Library Users](https://fb.workplace.com/groups/363899777130504/) group.

## Getting started with CacheLib

To get started with cachelib, read these guides:
- [Set up a simple cache](Cache_Library_User_Guides/Set_up_a_simple_cache/ )
- [Write data to the cache](Cache_Library_User_Guides/Write_data_to_cache/ ).
- [Read data from the cache](Cache_Library_User_Guides/Read_data_from_cache/ ).
- [Remove data from the cache](Cache_Library_User_Guides/Remove_data_from_cache/ ).
- [Visit data in the cache](Cache_Library_User_Guides/Visit_data_in_cache/ ).

Are you looking for a guide on how CacheLib is designed and how to add new features to it? Please refer to our [architecture guide](Cache_Library_Architecture_Guide/).
