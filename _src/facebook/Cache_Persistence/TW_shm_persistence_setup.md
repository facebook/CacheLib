---
id: TW_shm_persistence_setup
title: TW shared memory persistence setup
---
## POSIX setup

In the Tupperware config of your jobs, mount `/dev/shm` as a `tmpfs` and ensure it is persisted across task restarts. It looks like this:

```cpp
user_directories=[
  Directory(
    path="/dev/shm",
    persist=True,
    fileSystemMount=FileSystemMount(
      type="tmpfs",
      device="tmpfs_device",
      options="size=100%",
      cleanUpTimeout=persistent_dir_cleanup_timeout,
    ),
  )
],
```

In your cachelib configuration, ensure you have enabled the POSIX allocation mode:

```cpp
// Uses posix shm segments instead of the default sys-v shm segments.
// This allows twshared to more easily clean up shared segments
// when hosts leave our jobs.
 config.usePosixForShm();
```

After following the [Cachelib persistence](../../Cache_Library_User_Guides/Cache_persistence) guide to properly preserve cache on process shutdown and reattach on startup, ensure the directory passed into `enableCachePersistence()` is nested within `/dev/shm` so as to ensure its preservation across process restarts:

```cpp
config.enableCachePersistence("<YOUR_DIRECTORY_PATH>");
```

## Revert safety

If for some reason you ever want to disable shared memory allocation/persistence (e.g., an unrelated SEV that requires cache to be wiped), you must add additional clean up logic in the normal cache setup flow. Use this [Cachelib API](https://fburl.com/diffusion/f6u250yz) to clean up shared memory segments. The code to do so looks like this:

```cpp
const std::string cache_dir = "<YOUR_DIRECTORY_PATH>";  // Your metadata directory
const bool previously_on_posix = false;

using AllocatorType = facebook::cachelib::LruAllocator;

if (persist_cache) {
  config.enableCachePersistence(cache_dir);
  config.usePosixForShm();
  // cache reattach logic here (from https://fburl.com/wiki/8fwbg9qo)
} else {
  // We may have been using POSIX shared memory in the past to allocate cache
  // objects Therefore, try to clean up any remaining data in our cache
  // directory before creating a new cache.
  if (AllocatorType::cleanupStrayShmSegments(cache_dir, previously_on_posix)) {
    LOG(INFO) << "Cleanup of shared memory segments in: "
              << cache_dir << " successful.";
  } else {
    LOG(INFO) << "Cleanup failed";
  }
  cache_ = std::make_unique<AllocatorType>(std::move(config));
}
```

This ensures any cache data left in DRAM associated with `cache_dir` is cleaned up.

## Cleanup safety

To ensure that twshared cleans up the shared memory segments in your hosts when they leave your entitlement, you need to enable a host profile in your capacity dashboard. If you don’t need any particular host profile, you can choose `TWSHARED_CLEANUP`. For more information (including steps required to enable a host profile), see [Host Profiles](https://www.internalfb.com/intern/wiki/Tupperware/Capacity/Host_Profiles_User_Guide/).

In addition, if you are reusing the same `cache-dir` passed to the previous setup, ensure to wipe it clear before switching from sys-v to posix or vice versa. Failure to do so would result in startup crashes where Cachelib detects incompatibility.
