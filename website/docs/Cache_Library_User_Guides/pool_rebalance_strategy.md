---
id: pool_rebalance_strategy
title: Pool rebalance strategy
---

## When do you need pool rebalancing?

If your cachelib use case always allocates objects of a single size, then
rebalancing is almost always not required for you. Rebalancing of cache
becomes *important only when you store variable sized objects* in cache and
your workload's footprint of access across these objects can potentially
change over time. Often when you cache objects of variable size, the
distribution of `find()` and `allocate()` across object sizes would vary over
time. This leads to poor fragmentation in the cache memory footprint. For
example, imagine you had a cache of 30 GB and store objects of size 100 bytes,
500 bytes, and 1,000 bytes, each occupying 10 GB when warmed up. When your
application workload changes over time, the optimal sizes for these objects
could vary as well requiring more memory for one vs. other. With pool
rebalancing, this kind of workload change would usually result in metrics like
eviction age and hit ratios being sub-optimal over time.

Cachelib offers several rebalancing strategies to offset this behavior by
asking the cache to restructure the underlying memory allocated among objects
of different sizes.

## How does it work?

Internally, cachelib divides up memory into slabs and allocates slabs across
objects of various sizes. When pool rebalancing is enabled, cachelib evicts
objects of one size in favor of other and moves the backing slabs to the other
objects to enable caching more of them. Cachelib can do this automatically and
periodically. However, you will have to pick a strategy that matters to you
and configure how often the rebalancing happens.

Rebalancing is an asynchronous operation and does not impact the latencies of
other cachelib operations like `find()`, `insertOrReplace()`, or `allocate()`.
Rebalancing moves memory at the rate of 4 MB for every interval that you
configure if you would like to estimate a good rate.

## Enabling pool rebalancing

To enable pool rebalancing, specify these two parameters:

1. **Strategy** for re-evaluating metrics about your cache and figuring out a rebalancing action
2. **Interval** of executing the rebalancing

For example:


```cpp
auto rebalanceStrategy =
  std::make_shared<cachelib::LruTailAgeStrategy>(rebalanceConfig);

config.enablePoolRebalancing(
  rebalanceStrategy,
  std::chrono::seconds(kRebalanceIntervalSecs)
);
```


### Picking a strategy

Cachelib offers a few pre-package strategies for rebalancing that you can pick
from. They differ by what they try to optimize for based on traditional wisdom
of large scale caches like social graph caches and general purpose look-aside
key value caches. These are good defaults to start with, but you can also come
up with your own implementation if you have other goals.

#### Lru TailAge

LruTailAge is a fair policy that ensures that objects of different sizes get the same eviction age in cache.  For example, in steady state for your cache, you could have 100 byte objects getting 1 hr lifetime vs 1000 byte objects getting 30 min lifetime. This strategy tries to make the eviction age for various sizes similar. You can configure the following parameters(LruTailAgeStrategy::Config) (whose default values are pretty good to begin with):

* `tailAgeDiffRatio`
This defines how tight the tail age of various object sizes you want them to be. Setting it to 0.1 means that you don't want the min and max age to differ by more than 10%.

* `minTailAgeDifference`
This specifies a threshold of how big the actual diff ratio should be to warrant a rebalancing. For example, your min and max might be more than 10% off, but the real difference is insignificant.

* `minSlabs`
This specifies the minimum amount of memory in slabs that specific object size can not go below while rebalancing. Keep in mind that this is specified in slabs and not in bytes.

* `numSlabsFreeMem`
When you specify rebalancing under this mode, cachlib aggressively moves memory from object sizes that have a lot of free memory. This specifies the threshold for triggering that behavior.

* `slabProjectionLength`
This lets you estimate the min and max by picking a projected eviction age instead of the real eviction age. This can sometimes let you get better results.

For example:


```cpp
cachelib::LruTailAgeStrategy::Config cfg(ratio, kLruTailAgeStrategyMinSlabs);
cfg.slabProjectionLength = 0; // dont project or estimate tail age
cfg.numSlabsFreeMem = 10;     // ok to have ~40 MB free memory in unused allocations
auto rebalanceStrategy = std::make_shared<cachelib::LruTailAgeStrategy>(cfg);

// every 5 seconds, re-evaluate the eviction ages and rebalance the cache.
config.enablePoolRebalancing(std::move(rebalanceStrategy), std::chrono::seconds(5));
```


#### Hit based

HitBased approach tries to optimize the overall hit ratio rather than ensuring a fairness in the cache eviction age. This should result in a relatively higher hit ratio. However, it might potentially make your cache contain more of objects that give hits vs. objects that are expensive to recompute. For example, the cost of miss on objects is not uniform. To control the downsides of such implications, cachelib offers these parameters(HitsPerSlabStrategy::Config). Most of these are similar to the LruTailAge parameters, however, their semantics could slightly differ in the following ways:

* `minDiff`
Like tailAgeDiffRatio, this controls the minimum improvement that should trigger a rebalancing.
* `minLruTailAge`
When using hit based rebalancing, if you want to ensure some level of fairness by guaranteeing some eviction age, you can configure it through this parameter.

#### Marginal hits

This strategy ensures that the marginal hits (estimated by the hits in the tail part of LRU) across different object sizes are similar. Unlike hit based strategy which counts for historical count of hits across the entire cache, this tracks which objects could marginally benefit from getting more memory. To enable this,  you need to use the MM2Q eviction policy and enable tail hits tracking (`Allocator::Config::enableTailHitsTracking()`).

#### Free memory

This strategy frees a slab from an allocation class that satisfies all of the following requirements:
- this allocation class has total slabs above `minSlabs`
- this allocation class has free slabs above `numFreeSlabs`
- this allocation class has the most total free memory among all non-evicting (i.e. no eviction is currently happening) allocation classes in the pool

Note: this strategy does not specify a target allocation class to receive the freed slab.
Here are the parameters to configure this strategy:
* `minSlabs`
The minimum number of slabs to retain in every allocation class. Default is 1.
* `numFreeSlabs`
The threshold of required free slabs. Default is 3.
* `maxUnAllocatedSlabs`
FreeMem strategy will not rebalance anything if the number of free slabs in this pool is more than this number. Default is 1000.

### Writing your own strategy

In addition, if you have some application specific context on how you can improve your cache, you can implement your own strategy and pass it to cachelib for rebalancing. Your rebalancing strategy will have to extend the type `RebalanceStrategy` and implement the following two methods that define where to take memory from and where to give more memory to:


```cpp
virtual RebalanceContext pickVictimAndReceiverImpl(
  const CacheBase& /*cache*/,
  PoolId /*pid*/
) {
  return {};
}

virtual ClassId pickVictimImpl(
  const CacheBase& /*cache*/,
  PoolId /*pid*/
) {
  return Slab::kInvalidClassId;
}
```
