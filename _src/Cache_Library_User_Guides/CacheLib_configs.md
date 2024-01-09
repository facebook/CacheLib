---
id: CacheLib_configs
title: CacheLib configs
---

This document covers the configs that CacheLib cache consumes. In general there are two types of configs: the static ones that are consumed only when CacheAllocator is constructed, the dynamic ones that can be updated without restarting CacheAllocator.

## Static configs

The most CacheLib configs living in CacheAllocatorConfig are static. These configs are consumed in the constructor of CacheAllocator.

The sections below list out the `config setters` in the order of the config they set gets consumed (the order of that config gets used in the constructor). You do **not** have to call these setters in this order and in fact some setters have to be called in a different order. All the setters here are functions under CacheAllocatorConfig.

### RAM cache configs

* SharedMemoryManager: The component that controls the shared memory (new, attaching, etc)
   * `enableCachePersistence`: setting cache directory.
   * `usePosixForShm`: setting whether to Posix.
* Memory allocator: The components that manages memory allocations. (Carving out slabs, pool managers)
   * `setDefaultAllocSizes`: set the default allocation sizes, by either supplying the sizes directly or specifying min, max, size factor and
   * `enableCachePersistence`: the baseAddr is used here as the slab base address (if supplied).
   * `setCacheSize`: total RAM size.
   * `disableFullCoreDump`: this flag is passed to construct slab allocator.
   * `enableCompactCache`: This setter sets a flag of enableZeroedSlabAllocs, which is copied into MemoryAllocator::Config and is used later when releasing and allocating slabs.
   * `setMemoryLocking`: This flag is passed to construct slab allocator.
* Access container: The component that index the RAM cache.
   * `setAccessConfig`: the access config is used to construct access container.
* Chained items: The components that manage chained item.
   * `configureChainedItems`:
      * chained item's accessConfig constructs the access container for chained item;
      * lockPower: Controls the number of locks to sync chain items operation (move, transfer, etc). Note that this is not the same as the lock power of hash table of access containers.

### NVM Cache Configs

Configs to initialize NVM cache lives in `CacheAllocatorConfig::nvmConfig` and a majority of them are still set via CacheAllocatorConfig's setter functions. Below is the list of setters in CacheAllocatorConfig that sets nvmConfig:

* `enableCachePersistence`: cache directory is used to initialize NVM cache as well.
* `setDropNvmCacheOnShmNew`: This flag is used to determine whether the NVM cache would start truncated.
* `enableNvmCacheEncryption`: This sets CacheAllocatorConfig::nvmConfig::deviceEncryptor.
* `enableNvmCacheTruncateAllocSize`: This sets CacheAllocatorConfig::nvmConfig::truncateItemToOriginalAllocSizeInNvm.
* `setNvmCacheAdmissionPolicy`/`enableRejectFirstAPForNvm`: Sets the NvmAdmissionPolicy. Notice that the field lives with CacheAllocatorConfig.
* `setNvmAdmissionMinTTL`: Sets the NVM admission min TTL. Similarly this lives directly with CacheAllocatorConfig.
* `enableNvmCache`: Sets `CacheAllocatorConfig::nvmConfig` directly. This function should be called first if you intend to turn on NVM cache. And the other functions above would correctly modify the nvmConfig.

### WORKERS

CacheLib has worker threads that run in background for asynchronous jobs. These worker threads are initialized at the end of CacheAllocator's initialization.

* Pool resizers:
   * `setCacheWorkerPostWorkHandler`: This sets a callback that is called on some workers.
   * `enablePoolResizing`: Pool resizing configs.
* [Pool rebalancing](pool_rebalance_strategy):
   * `setCacheWorkerPostWorkHandler`: Similarly, the same callback is used for pool rebalancer. Notice that here a default rebalancing strategy is provided for all pools. It could be overridden later with dynamic config.
   * `enablePoolRebalancing`: Pool rebalancing configs.
* [Memory monitor](oom_protection):
   * `setCacheWorkerPostWorkHandler`: Similarly, the same callback is used for memory monitor.
   * `enableFreeMemoryMonitor`/`enableResidentMemoryMonitor`: Memory monitor configs.
* [Reapers](ttl_reaper/#configure-reaper):
   * `enableItemReaperInBackground`: Reaper configs.
* [Pool optimizer](automatic_pool_resizing):
   * `enablePoolOptimizer`

### Other configs

For the other fields in CacheAllocatorConfig that do not show up above (e.g. `CacheAllocatorConfig::enableFastShutdown`), they are all static configs but just not consumed in the constructor. In CacheAllocator's constructor, a deep copy of CacheAllocatorConfig is made and all the fields are read from that copy. **Changing the copy of CacheAllocatorConfig after the construction of CacheAllocator won't change its behavior.**

## Dynamic configs

Now that static configs are set, CacheAllocator is initialized, you can set some other configs via CacheAllocator directly.

### Pool settings

After the CacheAllocator is constructed, you need to add pools. The pools can be added via addPool, which takes the following arguments and some of them can be overriden later.

* pool name
* size
* allocation sizes: This overrides the allocation size provided in the static config for memory allocator. If this is empty, the allocation sizes in the memory allocator will be used.
* MMConfig: This can be overriden by `CacheAllocator::overridePoolConfig`, which makes use of a config set by `CacheAllcatorConfig::enableTailHitsTracking`.
* rebalanceStrategy: This overrides the one that is specified in the Pool rebalancing workers in the static config. And it could be in turn overridden by `CacheAllocator::overridePoolRebalanceStrategy`.
* resizeStrategy: This overrides the one that is specified in the pool resizer workers in the static config. And it could be overridden by `CacheAllocator::overridePoolResizeStrategy`

### Workers
All the workers can be (re)started by calling the functions below directly.
* Pool resizer: `CacheAllocator::startNewPoolResizer`, `stopPoolResizer`
* pool rebalancer: `CacheAllocator::startNewPoolRebalancer`, `stopPoolRebalancer`
* Memory monitor: `CacheAllocator::startNewMemMonitor`, `stopMemMonitor`
* Reaper: `CacheAllocator::startNewReaper`, `stopReaper`
* Pool optimizer: `CacheAllocator::startNewPoolOptimizer`, `stopPoolOptimizer`

Notice that the rebalancing and resizing specified at the worker level is a default for all pools. If a pool level strategy is specified by addPool or overriding, restarting a worker won't override that.
