---
id: Cache_persistence
title: Cache persistence
---

Cachelib supports *persisting the cache across process restarts*. This is useful when you want to restart your binary that contains a  cache and not lose the cache upon restart.  Cache persistence only works when you restart your process in the same machine. This does not provide persistence across machines.

Unlike a DB where persistent state is stored in a non-volatile medium like Disk or SSD, cachelib persists its cache by retaining the memory associated with the cache across processes. Hence using a persistent cache&mdash;even the size of hundreds of GB&mdash;does not impact the cache shutdown or creation, which is very quick.

## Create a persistent cache

To make your cache persistent in memory, when creating the cache, you need to use the appropriate constructor of type `ShareMemNew` or `SharedMemAttach`. To support persistence, you  need to provide a directory location (cache path) for cachelib to manage the persistence. This will be used to store some *small metadata* about the cache instance that will help us restore the cache. This directory path also uniquely identifies the instance of the cache and only one cache instance can be active at any given time corresponding to a given path.


```cpp
Cache::Config config;
config.setCacheSize(/* size of cache in bytes */);
config.enableCachePersistence(/* directory for shared memory related metadata */);

Cache cache(Cache::SharedMemNew, config);
// ... Use the cache
```

## Shut down a persistent cache

To make sure that your cache is saved in the correct state, *ensure that you have drained all accesses to your cache* and call the `shutdown()` API in your process shutdown path. This ensures that cachelib saves all the relevant information to restore the cache.


```cpp
// Save the metadata and shut down after this point. After that,
// accessing the cache content results in undefined behavior.
//
// Note: If you use cache admin, you must destroy it before calling shutdown
//       (e.g., cacheAdmin->reset();)
auto res = cache.shutDown();
if (res == Cache::ShutDownStatus::kSuccess) {
   // Successfully shut down the cache. Can attempt recovering.
} else {
   // Failure.
}
```


`ShutDownRes` contains three possible states, which can be used to identify the state of the cache upon shutting down:


```cpp
enum class ShutDownRes { kSuccess = 0, kFileDeleted, kFailedWrite }
```


`kSuccess` means the metadata has been successfully saved. `kFileDeleted` indicates that the cache directory was deleted while the cache was running. And `kFailedWrite` indicates any other system error that prevented cachelib from saving the metadata.

## Restore a persistent cache

On start up, try to use the following constructor to attach to your previous instance of cache. Upon failure, you can create a new one like before. A common pattern that many cachelib users do is to try to always attach on startup; if attach fails, which will throw an exception, catch the exception and then try to create a new cache. Also, please note that the pools should not be added again if the attach has been succeeded.


```cpp
Cache::Config config;
config.setCacheSize(/* this must be the same size you specified before */);
config.enableCachePersistence(
    /* this must be the same cache directory you specified before */);

bool attached = false;
std::unique_ptr<Cache> cache;
try {
  cache = std::make_unique<Cache>(Cache::SharedMemAttach, config);
  // Cache is now restored
  attached = true;
} catch (const std::exception& ex) {
  // Attaching failed. Create a new one but make sure that
  // the old cache is destroyed before creating a new one.
  // This allows us to release any held resources (such as
  // open file descriptors and associated fcntl locks).
  cache.reset();
  std::cerr << "Couldn't attach to cache: " << ex.what() << std::endl;
  cache = std::make_unique<Cache>(Cache::SharedMemNew, config);
}

if (!attached) {
  // Add pool only if the cache is not restored above
  cache->addPool("default", cache->getCacheMemoryStats().ramCacheSize);
}
```


## Drop persistent cache

Sometimes you would like your cache to be not persistent when you restart your process. There are two ways to accomplish this:

1. Calling the `SharedMemNew` constructor will always create a new instance and blow away the old one.
2. Indicate to cachelib by blowing away the cache directory that you passed in the `Config`. This will cause `SharedMemAttach` to throw an exception.

## Change configs

When you attach to an existing cache, cachelib will try to incorporate the config changes; however not all configs can be changed while attaching. Notice these two important points:

* The size of the cache is immutable unless you drop the previous instance.
* If using chained items, you must call `config.enableChainedItems()` before constructing the cache. This instructs the cache to save the chained items's hash table so that chained items are accessible on restart.

## Apply best practices

If you haven't done so, *please consider adding a try-catch block in your main or any top level code that will be working with cachelib API*. This is because cachelib APIs can throw exceptions, and when it comes to persisting states, we expect the stack to be properly unwound to ensure the state is not corrupted. Uncaught exception does not unwind the stack properly and can lead to state corruption for cachelib. For more information, see [Is stack unwinding with exceptions guaranteed by C++ standard?](https://stackoverflow.com/questions/39962999/is-stack-unwinding-with-exceptions-guaranteed-by-c-standard) on StackOverflow.

## Common failure scenarios

When using persistent setup, CacheLib locks the path to ensure only one instance can operate corresponding to a given cache path. When you see failures to startup due to *"Can not lock shm metadata file: Device or resource busy"*, this usually means there are more than one processes attempting to have operate a cache with the same path. To resolve this, look for zombie processes or other processes that might be still running with an active cachelib instance corresponding to the same cache path.
