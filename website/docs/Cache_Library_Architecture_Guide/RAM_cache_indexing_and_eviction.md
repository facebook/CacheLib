---
id: ram_cache_indexing_and_eviction
title: "RAM cache indexing and eviction"
---


This article takes a deep look at CacheLib's RAM cache's AccessContainer and
MMContainer, which powers the indexing and eviction of RAM cache.

## **Containers**

The word "container" in the names of AccessContainer and MMContainer refers to
[intrusive containers](https://stackoverflow.com/questions/5004162/what-does-it-mean-for-a-data-structure-to-be-intrusive).
For the purpose of understanding this article, the gist here is that the
container does not store a copy of the element. So an item in CacheLib can be
retrieved by both AccessContainer and MMContainer without duplicating copies.
This is achieved by storing some metadata in the item itself. The details would
be discussed in the sections below.

## **AccessContainer**

The AccessContainer is an intrusive hook based container and supports a
standard interface that works with CacheAllocator. It could be viewed as a
large hash table that serves as the index of the entire CacheLib cache. An
item's accessibility is controlled only by the access container. So an item is
accessible (can be returned by find request) only after it is inserted into the
access container and is no longer accessible once it is removed from the access
container. There is only one access container for the entire CacheLib cache
(see `accessContainer_` in `cachelib/allocator/CacheAllocator.h`).

It has the following major API functions:

* find: Given a string key, return the item handle. This is used when the client calls find on a key.
    * Example call-site: `CacheAllocator::findInternal`.
* insert: Insert an item handle into the AccessContainer, making it accessible
   to future find requests. This is used when the client calls the
   CacheAllocator's inset API.
    * Example call-site: `CacheAllocator::insertImpl`.
* remove: Remove an item from the AccessContainer, making it not accessible to
    future find requests. This is used when the client removes an item from the
    cache. This is also used when moving an item (more details on
    [4. How do we handle chained items](/docs/Cache_Library_Architecture_Guide/slab_rebalancing)).
    * Example call-site: `CacheAllocator::removeImpl`.

The full API can be found in `ChainedHashTable::Container`.

### **CHAINEDHASHTABLE**

This is our only implementation of access container.

The memory payload is a hash table (*ChainedHashTable::Container::ht_*), which
contains a list of compressed pointers (`ChainedHashTable::Impl::hashTable_`).
Each compressed pointer in this list (together with the compressor) points to
the head node of each bucket's chain. In CacheLib cache, this node (the `type T` in
in type parameter for ChainedHashTable::Impl) is a CacheItem
(`allocator/CacheItem.h`). For CacheItem to be eligible as a node here, it needs
to meet the following criterium:

* Carries the hook (`accessHook_` in `cachelib/allocator/CacheItem.h`): This
    hook is defined as `struct CACHELIB_PACKED_ATTR Hook` in
    `cachelib/allocator/ChainedHashTable.h`, which points to the next item in the
    bucket's chain. This adds a compressed pointer to CacheItem's payload.

* Implements `isAccessible()`, `markAccessible()`, `unmarkAccessible()`. This
    is backed by one bit in the CacheItem's flags.

* Implements `getKey()`. This doesn't require extra memory, as the CacheItem
    already store the key for other purpose.

With the intrusive implementation, operations on this hash table is easy:

* `find()`: Hash the key and get the bucket then iterate over the bucket chain
    (`Handle find(Key key)` function in `cachelib/allocator/ChainedHashTable.h`).

* `insert()`: Find the bucket and replace the head with the node to be inserted
    (`bool ChainedHashTable::Container<...::insert(T& node)` in `cachelib/allocator/ChainedHashTable-inl.h`).

* `remove()`: Find the bucket and remove the node from the chain
    (`bool ChainedHashTable::Container<T, HookPtr, LockT>::remove(T& node)` in `cachelib/allocator/ChainedHashTable-inl.h`).

Find and remove requires iteration on the bucket chain but insert does not.
Note that remove requires the iteration on the chain not to locate the node
itself (as we have it from the API) but to find the previous item so that the
node can be removed from the linked list.

## **MMContainer**

RAM evictions are controlled by MMContainers. It can be viewed as an eviction
algorithm powered by intrusive containers. When an item is inserted, it is
added into the MMContainer (e.g. entering the eviction queue). When an item is
accessed, it is recorded (e.g. promoted in LRU). When the cache allocator asks
for an item to evict, it returns an iterator for the next item to be evicted.
When the item is finally removed form the queue, it removes the item form the
queue. There is one MMContainer **per allocation class in each
pool** (see `evictableMMContainers_` in `cachelib/allocator/CacheAllocator.h`).

It has the following major API functions:

* `insert`: Add an item into the eviction queue. This is called when an item is
    inserted into the cache. This can also happen when we are moving an item and we
    need to remove it from one MMContainer and add to another
    (`CacheAllocator<CacheTrait>::insertInMMContainer(Item& item)` in
    `cachelib/allocator/CacheAllocator-inl.h`).
* `record`: Record an access of the item. Typically this means promoting the
    item in some form of LRU queue (`void
    CacheAllocator<CacheTrait>::recordAccessInMMContainer(Item& item, AccessMode
    mode)` in `cachelib/allocator/CacheAllocator-inl.h`).
* `remove`: Remove an item from the eviction queue. This is called when an item
    is removed form the cache (evicted or explicitly removed by the client) (`bool
    CacheAllocator<CacheTrait>::removeFromMMContainer(Item& item)` in
    `cachelib/allocator/CacheAllocator-inl.h`).
* `getEvictionIterator`: Return an iterator of items to be evicted. This is
    called when the cache allocator is looking for eviction. Usually the first item
    that can be evicted (no active handles, not moving, etc) is used (see
    `CacheAllocator<CacheTrait>::findEviction(PoolId pid, ClassId cid)` in
    `cachelib/allocator/CacheAllocator-inl.h`).
* `withEvictionIterator`: Executes callback with eviction iterator passed as a
    parameter. This is alternative for `getEvictionIterator` that offers possibility
    to use combined locking. Combined locking can be turned on by setting:
    `useCombinedLockForIterators` config option.

The full API can be found in `struct Container` in
`cachelib/allocator/MMLru.h`.  This links to MMLru, which is one of the
multiple MMContainers we implemented. All the operations above have constant
time complexity in each of the implementations.

The eviction requires an iterator instead of a single item because it is
possible that the item best for eviction is being referred to, moved, or for
some other reason can not be evicted. In this case, CacheAllocator advances the
iterator until a good victim is found or fails the eviction after too many
retries and further results in an allocation failrue.

### **DLIST**

The multiple implementations of MMContainer all relies on the same intrusive
doubly-linked list implementation (`class DList` in `cachelib/allocator/datastruct/DList.h`).
The *hook* required by this is straightforward: just compressed pointers to the
next and previous item (`struct DListHook` in
`cachelib/allocator/datastruct/DList.h`).

### **LRU**

[MMLru](`class MMLru` in `cachelib/allocator/MMLru.h`) powers the LRU (see [lru](/docs/Cache_Library_User_Guides/eviction_policy/)).

It is powered by a single DList, which holds the head and tail of the queue. It
also has a reference to *the insertion
point* (see ` T* insertionPoint_{nullptr};` in `cachelib/allocator/MMLru.h`)
where new items are inserted. The operations are straightforward standard
linked list operation that takes constant time:

* Insert: A new item is inserted to the insertion point.
* Record: A promoted item is moved to the head of the queue.
* Remove: The item is removed from the queue.
* Eviction iterator: An iterator pointing to the tail of the queue is returned and moves backward.

### **LRU-2Q**

MM2Q powers [LRU2Q](/docs/Cache_Library_User_Guides/eviction_policy/#lru-2q).
It is powered by `MultiDList` (variable `LruList` in `struct Container` in `cachelib/allocator/MM2Q.h`).
Underneath, it contains 5 DLists: Hot, Warm, WarmTail, Cold, ColdTail.

* Insert: A new item is inserted to the head of the Hot queue.
* Record:
   * An item in Hot list is moved to the head of the Hot queue.
   * An item in Warm or WarmTail list is moved to the head of the Warm queue.
   * An item in Cold or ColdTail list is moved to the head of the Warm queue.
* Remove: The item is removed from the current queue.
* Eviction iterator: An iterator goes from the tail of ColdTail backwards, then to Cold, WarmTail, Warm, Hot.

The Tail lists are optional queues designed to supply statistics on access
counts on the last 1 slab worths of items on the Cold and Warm queue. This
stats can help us understand if assigning one more slab to this particular
allocation class would be a good tradeoff. The marginal hits policy of
rebalancing relies on this statistics (more details in [2. How do we pick a slab](/docs/Cache_Library_Architecture_Guide/slab_rebalancing)).

### **TINY-LFU**

`class MMTinyLFU` (in `cachelib/allocator/MMTinyLFU.h`) powers
[Tiny-LFU](/docs/Cache_Library_User_Guides/eviction_policy/#tinylfu). This is an
implementation of W-TinyLFU cache eviction policy described in
[https://arxiv.org/pdf/1512.00727.pdf].
The main cache and the tiny cache. The tiny cache is typically sized to be 1%
of the total cache with the main cache being the rest 99%. Both caches are
implemented using LRUs. New items land in tiny cache. During eviction, the tail
item from the tiny cache is promoted to main cache if its frequency is higher
than the tail item of of main cache, and the tail of main cache is evicted.
This gives the frequency based admission into main cache. Hits in each cache
simply move the item to the head of each LRU cache.

The frequency is maintained by **count-min-sketch** (`cachelib/common/CountMinSketch.h`)

* Insert: A new item is added into the Tiny queue
    * If the Tiny queue's size is larger than expected, promote the tail of the
        Tiny queue into Main queue. Otherwise, swap the tails of the queues, if the
        Tiny queue's tail is more frequently accessed than the Main tail.
    * If the count-min-sketch is too small for the number of items in the queue, recreate the counters with doubled capacity.
        * Note: if recreation happens, the existing frequency counts will be erased.
* Record: A promoted item is move to the head of the current queue.
* Remove: The item is removed from the queue.
* Eviction iterator: The iterator consists of two sub-iterators pointing
    towards the tails of Tiny and Main. The overall iterator returns the less
    frequently used item of the current two sub-iterators.

### **COMMON CONFIGURATIONS**

There are a few common set ups shared by all these three implementations.

#### **THREAD SAFETY**

All of the operations are protected by a mutex.

#### **OPTIONAL PROMOTION**

Configs on whether to update on a read and/or write can be specified. For
example if updateOnRead is set to false, no operation would be taken if the
item is read.

#### **LRU REFRESH TIME**

LRU refresh time is an interval within which consecutive record access won't
re-promote the item. The intention for this mechanism is to reduce lock
contention. LRU refresh time is calculated on the oldest element's age
multiplied by lruRefreshRatio set in config and capped by a max time (currently
900 seconds). It is recalculated once every time interval as specified in the
config (mmReconfigureIntervalSecs). The different MMContainers do have slightly
different implementations:

* In MMLru, items accessed for the first time are not constrained by LRU
    refresh time and will be promoted for sure. It uses the time difference between
    current time and the tail item's last update time as the oldest element age.  *
    In MMTiny-LFU, items accessed for the first time are not constrained by LRU
    refresh time and will be promoted for sure. It uses the time difference between
    current time and the tail of main queue's last update time as the oldest
    element age.
* In MM2Q, the time difference between current time and the tail of wamr queue
    is used as the oldest element age.

#### **EVICTION AGE STATISTICS**

**EvictionAgeStat** (`struct EvictionAgeStat` in
`cachelib/allocator/CacheStats.h`) can be retrieved from all of the
implementations. The projectedAge is calculated when the caller calls eviction
stat with a projectedLength > 0. It is the time difference between current and
the last updated time of the projectedLength'th item from the tail of the major
queue.

* For MMLru, there is only one queue and its statistics is populated into
    warmQueueStat while the other two queue stats are left 0.
* For MM2Q, all the stats are populated. The major queue that is used for
    projection is the warm queue.
* For MMTiny-LFU, only the main queue's stats is populated into warmQueueStat.
    And the main queue is used for projection.

## **Summary**

In this article we went over the indexing (Access Container) and eviction (MM
Container) mechanism of CacheLib. They are implemented by intrusive hooks in
the CacheItem. For public API in CacheAllocator, AccessContainer and
MMContainer works together to perform indexing and eviction management
efficiently.

Usually, when the API takes a key from the client, `AccessContainer::find` is
used to locate the CacheItem. Then the MMContainer can be identified from the
item and operation can be performed in constant time.  The most time overhead
on common cache operations (find, insert, remove) is two iterations on the hash
table's chain, which is still quite efficient (happens on remove).
