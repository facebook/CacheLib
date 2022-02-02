---
id: common_components
title: "Cachelib Common Components"
---

cachelib/common has a number of helper classes that we use throughout the codebase. You're free to use them in your own projects as well. This page will give an overview to each of the components.

# AccessTracker

This tracks items' past accesses in an approximate manner. User can configure this to be used with a bloom filter or count min sketch implementation. Once a window granularity and number of windows are configured, the tracker will track history for require duration and return approximate past history of accesses. In CacheLib, this is used in conjunction with ML-based admission policies. This can also be used to gather variou statistics regarding item access history.

Refer to `cachelib/common/AccessTracker.h

# DropSet & ApproxSplitSet

DropSet is a set that can drop entries (evict entries) once load factor in its internal hash table exceeds 90% load factor. ApproxSplitSet uses several DropSets internally, and uses each DropSet to represent the keys we've seen in an hour. The goal of ApproxSplitSet is to detect the existence of a key approximately over the recent X hours. We use this in one of our reject first admission policy.

Refer to `cachelib/common/ApproxSplitSet.h`

# BloomFilter

Sharded bloom filters. Internally we have a number of bloom filters and distribute keys between them via their hash. This component also supports serialization for persistence.

Refer to `cachelib/common/BloomFilter.h`

# Cohort

This can be used to divide a set of threads into two groups. This component stores two refcounts in one atomic, and allows switching between them and waiting for them to drain. Used for copy-on-write with memory management for non-blocking read wrapper around thread-unsafe datastructures.

Refer to `cachelib/common/Cohort.h`

# CountDownLatch

This implements the same interface as the standard std::latch (https://en.cppreference.com/w/cpp/thread/latch).

Refer to `cachelib/common/CountDownLatch.h`.

# CountMinSketch

Sharded count min sketch implementation. Internally we have a number of buckets and distribute keys between them via their hash.

Refer to `cachelib/common/CountMinSketch.h`.

# PercentileStats

A sliding window histogram stats. By default the window size is 1 second. Internally it uses folly::SlidingWindowQuantileEstimator which uses TDigest data structure underneath (See [this](https://medium.com/@mani./t-digest-an-interesting-datastructure-to-estimate-quantiles-accurately-b99a50eaf4f7) for an introduction on TDigest).

# PeriodicWorker

Background worker that wakes up every configured period and runs a user specified task. Internally uses std::thread. Inherit from this class and override the work() function to provide user's custom logic.

Refer to `cachelib/common/PeriodicWorker.h`.
