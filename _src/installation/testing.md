---
title: Testing
id: testing
---

# Testing CacheLib

`Cachelib` includes many unit tests for various components
of the cache infrastructure.

## Building CacheLib Unit Tests

To build the cachelib unit tests, use one of the following commands
(see [installation](/docs/installation) instructions for more details):

1. Use `./contrib/build.sh` script with the `-T` option.
2. Use `./contrib/build-package.sh -t cachelib` (with the `-t` option)

The unit test binaries will be installed in `./opt/cachelib/tests`:

```sh
$ git clone https://github.com/facebook/CacheLib
$ cd CacheLib
$ ./contrib/build.sh -T -j
$ cd opt/cachelib/tests
$ ls
allocator-test-AllocationClassTest
allocator-test-AllocatorHitStatsTypeTest
allocator-test-AllocatorResizeTypeTest
allocator-test-AllocatorTypeTest
...
navy-test-ThreadPoolJobSchedulerTest
navy-test-UtilsTest
shm-test-test_page_size
shm-test-test_posix
```


## Running individual unit tests

Running a single unit test binary:

```sh
$ cd opt/cachelib/tests
$ ./allocator-test-ItemTest
[==========] Running 6 tests from 1 test suite.
[----------] Global test environment set-up.
[----------] 6 tests from ItemTest
[ RUN      ] ItemTest.NonStringKey
[       OK ] ItemTest.NonStringKey (0 ms)
[ RUN      ] ItemTest.CreationTime
[       OK ] ItemTest.CreationTime (0 ms)
[ RUN      ] ItemTest.ExpiryTime
[       OK ] ItemTest.ExpiryTime (0 ms)
[ RUN      ] ItemTest.ChainedItemConstruction
[       OK ] ItemTest.ChainedItemConstruction (0 ms)
[ RUN      ] ItemTest.ChangeKey
[       OK ] ItemTest.ChangeKey (0 ms)
[ RUN      ] ItemTest.ToString
[       OK ] ItemTest.ToString (0 ms)
[----------] 6 tests from ItemTest (0 ms total)
[----------] Global test environment tear-down
[==========] 6 tests from 1 test suite ran. (0 ms total)
[  PASSED  ] 6 tests.
```


## Running all  unit tests

Running `make` in the unit tests directory will execute all tests.
Use `make -j` to utilize all available CPUs.

```sh
$ cd opt/cachelib/tests
$ nice make -j
Running common-test-BloomFilterTest
Running common-test-AccessTrackerTest
Running common-test-BloomFilterTest
Running common-test-BytesEqualTest
Running common-test-CohortTests
...
```

Result of each test is stored in a `.log` file:

```sh
$ cat common-test-BloomFilterTest.log
[==========] Running 11 tests from 1 test suite.
[----------] Global test environment set-up.
[----------] 11 tests from BloomFilter
[ RUN      ] BloomFilter.OptimalParams
[       OK ] BloomFilter.OptimalParams (1397 ms)
[ RUN      ] BloomFilter.Default
[       OK ] BloomFilter.Default (0 ms)
[ RUN      ] BloomFilter.Move
[       OK ] BloomFilter.Move (0 ms)
[ RUN      ] BloomFilter.Reset
[       OK ] BloomFilter.Reset (0 ms)
[ RUN      ] BloomFilter.SimpleCollision
[       OK ] BloomFilter.SimpleCollision (0 ms)
[ RUN      ] BloomFilter.SharedCollision
[       OK ] BloomFilter.SharedCollision (0 ms)
[ RUN      ] BloomFilter.InvalidArgs
[       OK ] BloomFilter.InvalidArgs (0 ms)
[ RUN      ] BloomFilter.Clear
[       OK ] BloomFilter.Clear (0 ms)
[ RUN      ] BloomFilter.PersistRecoverWithInvalidParams
[       OK ] BloomFilter.PersistRecoverWithInvalidParams (2 ms)
[ RUN      ] BloomFilter.PersistRecoveryValidLarge
[       OK ] BloomFilter.PersistRecoveryValidLarge (4599 ms)
[ RUN      ] BloomFilter.PersistRecoveryValid
[       OK ] BloomFilter.PersistRecoveryValid (1 ms)
[----------] 11 tests from BloomFilter (6002 ms total)

[----------] Global test environment tear-down
[==========] 11 tests from 1 test suite ran. (6002 ms total)
[  PASSED  ] 11 tests.
```

A summary log file is generated (`cachelib-test-summary.log`) containing test counts,
and logs of all the *failed* tests:

```sh
$ cd opt/cachelib/tests
$ cat cachelib-test-summary.log
=== Cachelib Test Summary ===

107 TESTS PASSED
3 TESTS FAILED:
  allocator-test-NvmCacheTests
  common-test-TimeTests
  shm-test-test_page_size

(system information at the end of this file)


=== FAILED TEST: allocator-test-NvmCacheTests ===

Running main() from /home/assafgordon/cachelib-oss-ci/CacheLib/cachelib/external/googletest/googletest/src/gtest_main.cc
[==========] Running 34 tests from 1 test suite.
[----------] Global test environment set-up.
[----------] 34 tests from NvmCacheTest
[ RUN      ] NvmCacheTest.Config
I0809 06:00:30.146022 1106342 Factory.cpp:272] Cache file: /tmp/nvmcache-cachedir/1106342/navy size: 104857600 truncate: 0
I0809 06:00:30.152341 1106342 NavySetup.cpp:138] metadataSize: 4194304 bigHashCacheOffset: 52428800 bigHashCacheSize: 52428800
I0809 06:00:30.152387 1106342 NavySetup.cpp:166] blockcache: starting offset: 4194304, block cache size: 46137344
I0809 06:00:30.152500 1106342 LruPolicy.cpp:21] LRU policy: expected 11 regions
I0809 06:00:30.158718 1106342 RegionManager.cpp:31] 11 regions, 4194304 bytes each
...
```


Sharing the `cachelib-test-summary.log` file with the CacheLib developers can help us diagnose and fix problems.
