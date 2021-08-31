---
id: About_CacheLib
title: About CacheLib
---

# About CacheLib

CacheLib is a C++ library for accessing and managing cache data. It is a thread-safe API that enables developers to build and customize scalable, concurrent caches. It is targeted at applications that dedicate gigabytes of memory to cache information.

To enable this, CacheLib provides a  simple find, insert, and remove APIs for applications to manage Items (key-value pairs) in the cache. CacheLib comes with several caching heuristics to manage evictions when the cache is full.  CacheLib is optimized for both DRAM and NVM caches through Hybrid cache, and empowers applications to achieve large cache capacities for the same or relatively lower cost. When enabled, Items evicted from DRAM will be inserted into NVM  and Items on NVM will be inserted back to DRAM cache upon lookup, and all of these transitions are transparent to users.

CacheLib supports persisting cache data across application process restarts, and enables application developers to restart or update their binary without losing the cache data.

CacheLib is, by default, highly optimized to maximize the system performance, and also provides variety of advanced configuration options for further tuning.

![](cachelib_overview.png)
