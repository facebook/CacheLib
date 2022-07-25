# Changelog

## V17
In this version, `CacheAllocator::ItemHandle` is removed. Updating to this version will cause compilation error if `ItemHandle` is still used.

## V16

This version is incompatible with versions below 15. Downgrading from this version directly to a version below 15 will require the cache to be dropped. If you need to downgrade from this version, please make sure you downgrade to version 15 first to avoid dropping the cache.

## V15

This version is incompatible with any previous versions.

Updating to this version may cause compilation error because:
- The following APIs are removed:
1. CacheAllocator::allocatePermanent_deprecated.

Updating to this version will not require dropping the cache.

## V14

Inception version when Cachelib goes open source.
