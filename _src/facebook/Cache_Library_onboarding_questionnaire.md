---
id: Cache_Library_onboarding_questionnaire
title: Onboarding Questionnaire
---

*To help us understand your use case and provide you effective advice, please answer the following questions and forward your answers to [cachelib oncall](https://www.internalfb.com/intern/monitor/oncall_management_scheduling?oncall=cachelib), or share it in a post to our [user group](https://fb.workplace.com/groups/363899777130504). Doing this will helps us understand your system and cache design, and also hopefully enable you in understanding how to  effectively leverage a cache in your system.*

## Overall System Design

1. What does your service do?
2. What does caching accomplish in your system design?
    1. For example, are you trying to save compute or latency or backend cost ?
3. Can you leverage an existing cache service? (e.g. Memcache, Zinc, etc.)
4. How do you plan on using cachelib in your system?

## Overall Cache Design

1. What is the overall footprint of your system ? (e.g., how many hosts and what type of hardware?)
2. How much space are you willing to use for caching ?
3. What is your system's queries per second (QPS) to cache?
4. How much hit ratio is useful for your system's consideration ?
4. What is the read/write ratio to cache ?
5. Are the keys/values immutable after they are inserted into cache?
6. What is the range of key/value size ? (min/max/P50/P90)?
7. Do you wish to cache binary blobs (e.g., strings) or structured data (e.g., Thrift structure, c++ data structure) ?
8. Is cache persistence across binary restarts important to you ?
9. Do you find it valuable to cache non-existent content to avoid useless backend queries (i.e. negative cache)?
10. Do you intend to use Flash/SSD in addition to DRAM for caching? If so, please answer the below section as well.

## HybridCache Design

NOTE: Answer the following questions if and only if you intend to use flash, in addition to DRAM for caching.

1. How much SSD and DRAM are you able to dedicate for caching ?
2. Do you use the SSD for other purposes or just caching ? If so, how much capacity and write endurance are you able to dedicate for caching (flash drives have limited number of writes before they wear out and need replacing) ?
3. If you plan to use SSDs, which HW SKU are you targetting ? (T10 vs T6F. Boot drives are not suitable for caching)
4. Do you have any expectations around latency from the cache?
    1. Overall Cache (P50/P90/P99)
    2. Reads from just the SSD portion of the Cache (P50/P90/P99)
