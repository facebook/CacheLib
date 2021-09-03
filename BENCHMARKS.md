# Benchmarking with CacheBench

## Introduction

CacheBench is a benchmark and stress testing tool to evaluate cache
performance with real hardware and real cache workloads. CacheBench takes in a
configuration that describes the cache workload and the cache configuration
and simulates the cache behavior by instantiating a CacheLib cache. It runs
the workload and emits results periodically and at the end. The results
include metrics such as hit rate, evictions, write rate to flash cache,
latency, etc. The workload configs can be hand-written by a human, produced by
a workload analyzer, or backed by raw production cachelib traces.


See [here](https://cachelib.org/docs/Cache_Library_User_Guides/Cachebench_Overview) for details on installing and using `CacheBench`.
