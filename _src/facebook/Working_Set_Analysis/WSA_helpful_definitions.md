---
id: WSA_helpful_definitions
title: Helpful Definitions
---
### Context
This page is intended to provide definitions for people who may be unfamiliar with caches or unfamiliar with some specific vocabulary used when discussing Working Set Analysis.

### Cache Definitions
The cache is an important element of most data storage architectures. Caches store data locally so that future requests for the same data can be served faster. Caches are typically deployed "in front" of a slower data store. When requested data exists and can be served from the cache, we call that request a **cache hit** or simply a **"hit"**. Conversely, requests which cannot be served from the cache are referred to as **"misses"**.

Because a cache itself has finite size, the software component of the cache must decide what to write to cache and what to evict from the cache when space is exhausted. There are many well-known [writing policies](https://en.wikipedia.org/wiki/Cache_(computing)#Writing_policies) and [eviction policies](https://en.wikipedia.org/wiki/Cache_replacement_policies) for handling these cases.

Working Set Analysis focuses primarily on C++ caches at Facebook which are largely built upon Cachelib. Cachelib is a highly performant library which provides a powerful and flexible cache API. Cachelib caches always store data locally in DRAM; some caches also utilize flash storage to increase the overall capacity of the cache. Several teams at Facebook also use systems of caches which support each other. For example, TAO uses DRAM-only caches called "followers" whose misses fall back to DRAM+Flash caches called "leaders"; misses from TAO leaders hit MySQL.

The primary goal of any caching + storage system is to achieve low-latency accesses with as lean of a deployment as possible. The Working Set Analysis toolkit analyzes cache traffic to provide insights into how to best optimize several competing objectives:
- Overall latency (related to overall **hit rate**; hits / (hits + misses))
- Overall size
- Flash I/O (if applicable)

### Working Set Analysis Definitions
These are the terms we most commonly use when talking about Working Set Analysis.

#### Request Trace
A request trace is a time-ordered series of cache operations (SET, GET, DELETE). Every request/operation is attributable to a single **object** identified by a **key** (if applicable, we interpret any multi-object operations as multiple operations for single objects). We most often measure the request trace with respect to a server (i.e. the host/process that is receiving the request) as this has the most clear relationship with cache behavior. However, any time-ordered series of requests can define a request trace. We could, for example, consider all requests originating from a specific client to define a request trace.

The terms “request” and “operation” are mostly interchangeable for our purposes. We typically use “request” to refer to the event that actually gets logged to our raw dataset, which includes both the cache operation as well as some extra metadata.

#### Working Set
The “working set” of a **request trace** is the set of distinct objects referenced within a given time window.

Note that the working set is an attribute of the **request trace** not an attribute of the cache itself. This leads to results that are sometimes unintuitive e.g. a SET operation will add an item to the working set, but a DELETE operation will not remove that same item from the working set. Note as well that we use the term "working set" here because of the natural parallels with mathematical set arithmetic. We often talk about taking the intersection or union of multiple working sets to derive insights about the traffic.

Some metrics we can define on the working set are:
The working set size (measured either in the number of distinct objects, or in the total size of the distinct objects in bytes)
Churn and emergence
Given timestamps t1 < t2 < t3 < t4, we can define two **working sets** as A(t1, t2) and B(t3, t4). **Churn** is a measure of the objects appearing in A but not in B. **Emergence** is a measure of the objects appearing in B but not in A.

#### Tenant
A unifying concept for cache “object type” (e.g. “key prefix” for Memcache, “fbtype” for TAO fbobj, “cdn_object_type” for CDN, etc.). Functionally, we can define tenants as **any disjoint sets of the objects in the cache**. We could, for example, define two tenants “big” and “small“, ”old“ and ”new“, ”bursty“ and ”regular“, etc.

#### Miss Cost
Miss costs are incurred when an item is requested from the cache but not found. For each cache miss there is a CPU cost on the server, a CPU cost on the client, latency costs, and typically a backend cost to refill the item in cache. The relative importance of each of these costs varies depending on the operational environment. Due to the inherent complexity of computing an absolute miss cost, we usually choose to work with relative miss costs that can vary by tenant or by request. In the simplest case, we can specify a relative miss cost of 1 for all requests, in which case “minimizing miss cost” corresponds to a hit rate maximization.

#### Hit Cost
The cache itself intrinsically incurs costs for each operation. Pointers are updated, copies are made, locks are acquired, etc. etc. etc. These costs can add up to be non-trivial especially if the service itself has some non-caching functionality and needs to save CPU to do other work. For now we intentionally assume hit costs are significantly less than miss cost, and are happy to trade one for the other within Cachelib’s scope. However there are opportunities for hit cost savings in the layers “above” the cache e.g. with intelligent load shedding.

#### TTA
TTA (time-to-access) evictions are a pseudo-isolation mechanism that approximates the effect of each tenant getting its own queue. TTA allows much finer granularity than possible with cache pools. Using TTA with static tenant assignment approximates a pseudo-LRU for each tenant. Assigning multiple contextual TTA thresholds (e.g. a “bursty” threshold and a non-bursty threshold) for each tenant can approximate more complex eviction policies like 2Q (note: we do not currently have this capability available). TTA is currently enforced as a short-circuited eviction event that ensures items are expired as soon as their time-since-last-access threshold has elapsed.

#### Item Lifetime
We define an item’s lifetime as how long it is kept in cache until its eviction. We can frame most of our existing heuristics in the form of “extending to an item’s lifetime”, or measuring “hit density over an item’s lifetime”. For example, LRU with an eviction age of 10 minutes will give 10 minutes additional lifetime to each item on access. A ML policy will admit an item that is projected to receive more than 5 accesses in the next hour. **Modeling a policy around lifetime is computationally simple, and it gives us a shared framework to compare and contrast different heuristics**. However, lifetime modeling becomes inaccurate with increasing variation of the lifetime of each individual tenant. (E.g. a flash cache with varying write rate throughout the day and we try to predict # of hits over the lifetime of an item).

#### Item Value
We define an item’s value as its relative rank in relation to other items in the cache. We can view the role of the cache as maintaining an item ranking such that the lowest value items are replaced whenever a new item is added to the queue. With this framing in mind, all eviction policies implicitly assume a model for an item’s value (LRU models item value as monotonically decreasing with age, LFU models item value as increasing with the number of accesses in an observed window, policies like GDSF explicitly maintain a priority queue with a “value” score). **Ranking allows us to model policies that inherently act on features that vary significantly over short period of time**. (e.g. the LRU length and eviction age as traffic peaks and troughs). On the other hand, with ranking we require more complex simulation to compare and contrast different heuristics.
