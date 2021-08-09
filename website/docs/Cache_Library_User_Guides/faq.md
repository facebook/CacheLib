---
id: faq
title: faq
---

If your question is not listed in this FAQ, post it to the [Cache Library Users](https://fb.workplace.com/groups/363899777130504/) group.

# My service is broken. Help!

## Why my cache is not persisted after process restart?

**Did your service used to recover cache fine? But it didn't recover in the newest release?**

This could be due to cachelib's Format Version changes. Please check our user group and see if there're any pinned post that mention "cold roll" or "format change". In the event of a cache format change, CacheLib Cache cannot persist its cache content across service upgrades.

If you would like to subscribe to cache format changes in the future, you can add your oncall rotation to our butterfly bot. (Or reach out to our oncall and we'll add your rotation to the subscriber list). https://www.internalfb.com/butterfly/rule/2624439420970131

**Did you call `CacheAllocator::shutDown()` explicitly?**


CacheAllocator will destroy all shared memory segments if shutDown is not called before the object is destroyed.


**Do you use Tupperware?**


Tupperware (TW) users need to set up a persistent directory to keep around the cache metadata file between TW restarts. For more information, see [Persistent Directory](Tupperware/The_Hacker's_Guide_to_Tupperware/Task_Local_Storage#Persistent_Directory ). You also need to configure [[Tupperware/Reference/LanguageReference/LxcConfig/ | LXC]] for your container.

## Why do I see allocation failures?

It is normal to see some allocation failures over the lifetime of your cache. However this number should be very low (e.g., less than 0.001% of your allocation attempts). If the rate is high, it can be caused by the following:

1. You do not have any free slabs in certain allocation classes. Please check the [AC stats on scuba](https://our.intern.facebook.com/intern/scuba/query/?dataset=cachelib_admin_ac_stats&pool=uber) to verify this. To remedy this, you must enable slab rebalancing with the default policy (just `RebalancePolicy` by itself). This will rebalance your memory according to the allocation failures.
2. You have very high number of allocation attempts and very low number of slabs in certain allocation classes (think 10K/s and only one or two slabs). Hence rate of evictions cannot keep up with the rate of allocations. To remedy this, you can enable `LruTailAge` rebalance policy which would try to keep all allocation classes around similar eviction age.

## Why do I see invalid allocs?

You're questing for allocation size that is bigger than the biggest allocation size your cache allows. Please check if you're customizing allocation class sizes and the biggest class size you've set. If you're not customizing them, then this means the size your item (size of key + size of value + 32 bytes) is bigger than 4 MB, which is the upper bound of what we allow into our cache. If you absolutely need to store such big values, use [chained items](chained_items/ ).

## Why is my cache so slow?

Have you checked how your hash table is configured? In general the hash table size should be configured to be at least 1.5 times of the number of items you have in cache.

## My service crashed. It takes forever to core dump.

If you're using a cache that's tens of GB or more, it will take a while to finish dumping the core because it's so big. You can turn off dumping core for cache memory by passing in `false` to the `setFullCoredump()` method when you set up the cache. Beware that turning off core dump for cache memory means it will be near impossible to debug any buffer-overflow or use-after-free bugs in cache memory.

## I update certain items in my cache frequently (from some refill logic), but I do not want them to be seen as hotter than other items in my cache which are updated less frequently but read just as frequently. What can I do?

If you know you're doing a mutation, pass in `AccessMode::kWrite` when you call `find()` on CacheAllocator. And configure LRU (or any other eviction policies you're using) to only promote for reads.

# How does `X` work?

## How do I use an item?

Each item has a key and an value associated with it. Both the key and the value can be a string or a POD or anything that's memcpy-safe:


```cpp
auto myHandle = cache->find("I can find my item by using a key");
folly::StringPiece myString{myHandle.getMemory(), myHandle.getSize()};
std::cout << myString << std::endl;
```


An item can also be looked up and used as a user-defined type:


```cpp
struct MyStructure {
  bool aBooleanField;
  uint64_t[10] someIds;
};

auto myHandle = cache->find("I can find my item by using a key");
auto myStruct = myHandle->getMemoryAs<MyStructure>();

if (myStruct->aBooleanField == true) {
  myStruct->someIds[5] == 7777777;
}
```


## How much space does my item take?

When you cache an item, it takes more space than just the number of bytes you intend to cache. That is, if you're trying to cache a structure `Foo` with a key `"HelloKeyForFoo"`, the space required will be bigger or equal to `sizeof(CacheType::Item) + sizeof("HelloKeyForFoo") + sizeof(Foo)`.

The reason the size is at least equal to the above should be obvious. We need space for not just `Foo` but also its key. We also need some additional space for the item header, which contains book-keeping information such as flags and references (yes, internally each item is ref-counted, quite similar to how one would implement a shared pointer).

However, it may not be immediately obvious that sometimes your item can take up more space than it needs. This is because the memory allocator in cachelib does not give true variable sized allocations (not many do). Cachelib's memory allocator can be configured to support from 1 to 127 different allocation sizes (from a minimum of 64 bytes to a maximum of 4 MB). And when you request for N bytes, we will pick the smallest allocation size that will fit the entire space your item needs.

## How does eviction work?

See [Eviction Policy](eviction_policy/ ).

## What is slab rebalancing (and pool rebalancing, and pool resizing)?

See [Pool Rebalance Strategy](pool_rebalance_strategy/ ).

# What does the term mean?

See [Cache Library terminology](terms/ ).
