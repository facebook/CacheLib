---
title: Testing
id: testing
---

# Testing CacheLib

CacheLib includes many unit tests for various components
of the cache infrastructure.

## Running tests with `getdeps.py`

The recommended way to build and run all CacheLib tests:

```sh
git clone https://github.com/facebook/CacheLib
cd CacheLib

# Build CacheLib (includes test targets by default)
python3 ./build/fbcode_builder/getdeps.py --allow-system-packages build cachelib

# Run all tests
python3 ./build/fbcode_builder/getdeps.py --allow-system-packages test cachelib
```

## Running individual unit tests

After building with `getdeps.py`, you can run individual test binaries directly.
Use `show-inst-dir` to find the build output directory (see [Locating build output](/docs/installation#locating-build-output)):

```sh
INST_DIR=$(python3 ./build/fbcode_builder/getdeps.py show-inst-dir cachelib)
ls $INST_DIR/bin/
```

Example of running a single test binary:

```sh
$ $INST_DIR/bin/allocator-test-ItemTest
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

## Sharing test results

If you encounter test failures, sharing the test output with the CacheLib developers can help us diagnose and fix problems.
