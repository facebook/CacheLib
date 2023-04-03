---
id: eviction_policy
title: Eviction policy
---

Cachelib offers different eviction policies, suitable for different use cases. To select a specific eviction policy, choose the appropriate allocator type.

## LRU

This is the classic least recently used (LRU) eviction policy with a couple optional modifications. We assume the reader is familiar with the classic LRU. Before we start, let's get familiar a few concepts. **Tail age** is the time the item in the LRU tail spent in the cache. If `Tins(tail)` is the time when an item was inserted into the cache, `tail_age = now() - Tins(tail)`. **Insertion point** (IP) is a position in LRU where a new item is inserted. Classic LRU IP is the head of the list. Moving an item from its current position in LRU to the head is called **promotion**. Now let's look at the following picture:


![](cachelib_LRU.png)


The first modification cachelib made is custom IP, which is not at the head of the list. New items will be inserted at the position `ipos(p) = size(LRU) * p` from the tail, where `p ∈ [0,1]` is a ratio parameter. For example, suppose the regular LRU tail age be 600s, but you want to cache only items that were accessed during the 200s after insertion. Inserting new items at position `ipos(1/3)` from the tail may help you to achieve this. Not that custom IP affects only items that're inserted into the cache the first time; it doesn't affect items that on access are moved to the head of the list.

The second modification is promotion delay. Normally every access item is moved to the LRU head. In fact, you need to be precise about promotions from the tail of LRU and don't bother promoting items close to the head. Before promotion, we check the time since the last promotion and promote only if it is longer than the delay parameter `T`. This is performance optimization. In the setup of the last example, you may want to promote only items with tail age of 300s, roughly when they get to the bottom half of the LRU.

### Configuration

* `lruRefreshTime`
How often does cachelib refresh a previously accessed item. By default this is 60 seconds.

* `updateOnWrite`/`updateOnRead`
Specifies if a LRU promotion happens on read or write or both. As a rule of thumb, for most services that care primarily about read performance, turn on `updateOnRead`. However, if your service cares a lot about retention time of items that are recently written, then turn on `updateOnWrite` as well. By default, `updateOnRead = true` and `updateOnWrite = false`.

* `ipSpec`
This essentially turns the LRU into a two-segmented LRU. Setting this to `1` means every new insertion will be inserted 1/2 from the end of the LRU, `2` means 1/4 from the end of the LRU, and so on.

## LRU 2Q

LRU 2Q deals with bursty accesses. The term *LRU 2Q* is a little misleading. It actually uses 3 LRUs (which are called queue here): hot, warm, and cold. Let us explain how items move between them. Look at the following picture, where the gray arrows indicate promotions:


![](cachelib_2Q.png)


Initially, items are inserted into the hot queue. Items in the hot queue are promoted to the head of hot queue (internal promotion). Items evicted from the hot queue goes to the cold queue. If an item in the cold queue is accessed, it gets promoted to the **warm** queue (cross-queue promotion). An item evicted from the cold queue gets evicted from cache. Similarly to the hot queue, an item in the warm queue s promoted internally and evicted to the cold queue.

The warm queue and the cold queue are effectively an LRU with custom IP (see above) - they're the head of cold queue.

Hot items evicted to the cold queue have quite limited time to get a hit and move to the warm queue. This captures items that are aggressively accessed and then are no longer used.

Usually, the warm queue is about 60%, the cold queue 30%, and the hot queue 10%.

### Configuration

* `lruRefreshTime`
Same as above for memory management LRU (MMLru).
* `updateOnWrite`/`updateOnRead`
Same as above for MMLru.
* `hotSizePercent`
This specifies the ratio of items that will be inserted into the hot queue. Items that are never accessed in the hot queue will be moved to the cold queue, which will be evicted faster. Items that have been accessed while in the hot queue will be moved to the head of the hot queue so they will live in the hot queue for longer.
* `coldSizePercent`
Items that are accessed in the cold queue will be moved to the warm queue. Increasing this ratio can give the items accessed on a longer, but regular period a higher chance to stay in the cache.

## TinyLFU

TinyLFU consists of two parts: frequency estimator (FE) and LRU. FE is an approximate data structure that computes an item's access frequency (Count-Min Sketch used) before inserting it to LRU. Only items that pass frequency threshold get accepted to LRU and evicted otherwise.
