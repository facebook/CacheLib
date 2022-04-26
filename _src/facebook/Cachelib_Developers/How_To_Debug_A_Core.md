---
id: How_To_Debug_A_Core
title: "How To Debug A Core"
---
**[WORK IN PROGRESS]**

GDB is your friend. Here's a quick-start guide to using GDB. http://beej.us/guide/bggdb/

## Find the core file

Find out where the core is located. It's typically in `/var/tmp/cores/` on the host where a crash had happened. You can also manually trigger a coredump via `kill -3` on the process that you want to dump a core. To find the process, you can search for its name. (E.g. `ps aux | grep <name>`).

Alternatively, see if you can find the core via [coredumper](https://fburl.com/scuba/coredumper/s85b6cvg). You can follow [this guide](https://www.internalfb.com/intern/wiki/Coredumper/User_Guide/Remote_Coredumps/#quickstart-guide) to figure out how to download and access the core.

Once you have the location of the core, use fbpkg to fetch the package. You can do this by `fbpkg.fetch <package_name>`. Typically, if you get a task about a crash while oncall, it should contain the package and version for the service that had the crash. Simply `sush` onto the host, fetch the package, and examine the core `gdb <binary> /var/tmp/cores/<core>`. Some times the task itself has the complete command for you to log onto a host and examine the core, so you can copy-paste that as well.

This wiki has a few helper commands for you to navigate around cachelib.

## How to access CacheAllocator

**TAO**

To examine cache, first make sure you have loaded all the debug symbols.
```cpp
fbload auto_debuginfo
auto-load-debuginfo
fbload debuginfo_fbpkg
load_debuginfo
```

Next, navigate to a place where you have visibility to global variables defined in `tao/server/globals.cpp`. A frame that is located in `main.cpp` will give you visibility. After this, you can examine the caches. We have two caches for TAO: gCache and gRecentWritesCache.

```cpp
// Look inside gRecentWritesCache
p *gRecentWritesCache._M_t._M_t._M_head_impl

// Examine cache-allocator object inside gRecentWritesCache
p *gRecentWritesCache._M_t._M_t._M_head_impl->cache_._M_t._M_t._M_head_impl

// Look inside gCache
p *gCache._M_t._M_t._M_head_impl

// Examine cache-allocator object inside gCache
p *gCache._M_t._M_t._M_head_impl->cache_._M_t._M_t._M_head_impl
```

**BigCache**

tbd

**Memcache**

tbd

## How to examine CacheAllocator

First verify if we have a full coredump. You can check that by using our cache allocator config scuba dataset. Filter by the host which the crash happened on. Then, check "disableFullCoredump". If it's true, then we don't have a full core which means we cannot look inside the cache memory. We can still examine other members of the CacheAllocator though. You can look at access container, and mm container for example. You can also examine navy's index and various components if using nvmcache.

**Examine cache memory**

There're several ways. You can examine access container or mm container, and navigate to a cachelib item from there. Or, you can start from the beginning of cache memory and examine slab headers, and then cachelib items one by one.

```cpp
// Look at the beginning of cache memory
<cache ptr>->allocator_._M_t._M_t._M_head_impl->slabAllocator_.slabMemoryStart_
```

tbd
