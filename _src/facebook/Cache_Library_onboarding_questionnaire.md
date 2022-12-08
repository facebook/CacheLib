---
id: Cache_Library_onboarding_questionnaire
title: Onboarding Questionnaire
---

*To help us understand your use case and provide you effective advice, please answer the following questions and share it in a post to our [user group](https://fb.workplace.com/groups/363899777130504). We will be able to more effectively help you with this information.*

## Overall System Design

1. What is your service?
    1. What is the overall architecture?
2. What role does cache play in your system?
    1. What metrics does cache help in your system?
    2. For example, are you trying to save compute or latency or backend cost?
3. Can you leverage an existing cache service? (e.g. Memcache, ZGateway Cache, VCache, etc.)
    1. If not, why?

## Cache Capacity
1. What is the overall footprint of your system?
    1. e.g., how many hosts and what type of hardware?
2. How much memory or flash are you willing to use for caching per host?
3. What is your system's queries per second (QPS) to cache per host?

## Cache Performance
1. What is the hit rate you require from a cache?
2. What is the range of key/value size? (min/max/P50/P90)?
3. What is the read/write ratio to cache?

## Cache Design
1. How does the cache interact with the rest of your system?
    1. If you have an existing cache, please tell us about it.
2. How can you tell whether or not your cache is performing well?
3. Are you caching immutable data? (e.g. read-only)
4. Do you wish to cache binary blobs (e.g., strings) or structured data (e.g., Thrift structure, c++ data structure)? If so, have you considered [object-cache](https://www.internalfb.com/intern/staticdocs/cachelib/docs/facebook/Object_Cache/Object_Cache_Decision_Guide)?
5. What is the backing store of the data you want to cache?
6. Is cache persistence across binary restarts important to you?
7. Do you find it valuable to cache non-existent content to avoid useless backend queries (i.e. negative cache)?

## HybridCache Design

NOTE: Answer the following questions if and only if you intend to use flash, in addition to DRAM for caching.

1. Operating a hybrid-cache is a large endeavour, have you considered using a remote cache service such as memcache?
2. How much SSD and DRAM are you able to dedicate for caching?
3. Do you use the SSD for other purposes or just caching?
    1. If so, how much capacity and write endurance are you able to dedicate for caching? (Note: flash drives have limited number of writes before they wear out and need replacing)
4. Which HW SKU are you targetting? T6F, T10, T8, or T3? (Note: T1's boot drives are not suitable for caching. Unless you're talking about only a few MB/s read or write rate.)
5. What are your requirements on latency from the cache?
    1. Overall Cache (P50/P90/P99)
    2. Reads from just the SSD portion of the Cache (P50/P90/P99)
