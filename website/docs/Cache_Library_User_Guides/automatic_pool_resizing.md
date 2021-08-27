---
id: automatic_pool_resizing
title: Automatic pool resizing
---

**This feature is incomplete and untested in prod. If you're interested, reach out to us and we can work out a plan to complete it.**

Cachelib requires an initial size to add a new pool or a new compact cache. With pool optimization, the sizes of different pools or compact caches can be automatically adjusted according to a criteria or strategy. This can (1) potentially reduce the efforts to search for a good size for pools and (2) make the pool sizes up to date.

For now we optimize the sizes for regular pools and the sizes for compact caches separately; the total memory for regular pools is constant and the total memory for compact caches is constant. We currently have one supported strategy:

* `MarginalHits`
Similar to rebalancing, this strategy ensures that the marginal hits across different pools or compact caches are the same. To use this strategy, you need to use the MM2Q eviction policy and enable tail hits tracking.
