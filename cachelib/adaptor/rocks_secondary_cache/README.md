# RocksDB CacheLib Secondary Cache

This directory contains files and tests for building Cachelib as a SecondaryCache for RocksDB.

# Build

Currently the build is only supported via cmake.

First, build cachelib through the normal build procedure.

To build under RocksDB, link this directory adaptor/rocksdb_secondary_cache) to the plugins/cachelib directory under RocksDB:
```
$ln -s .../secondary_cache .../plugins/cachelib
```
This will allow RocksDB to find and build the CacheLib plugin code.

Next, under the RocksDB build directory, instruct RocksDB to build and include the cachelib plugin:
```
$cmake -DROCKSDB_PLUGINS=cachelib -DCMAKE_PREFIX_PATH=<Path to CacheLib> -DCMAKE_MODULE_PATH="<Path to CacheLib>"  ..
```
where  the prefix path points to the opt/cachelib directory and module path points to the cachelib/cmake directory.

Finally, build RocksDB using "make".

# Using the RocksDB Cachelib Secondary Cache

The Secondary Cache can be created via either the SecondaryCache::CreateFromString or NewRocksCachelibWrapper APIs.
```
SecondaryCache::CreateFromString(..., "id=RocksCachelibWrapper; ...");

RoksCachelibOptions options;
NewRocksCachelibWrapper(options);
```

When using CreateFromString API, the options can be specified as name-value pairs on the command line. The mapping between the field names and options is found in CacheLibWrapper.cpp.

# Issues and TODO

* There are incompatibilities between RocksDB and Cachelib on the version of GTEST required and supported.  In order to successfully build, you must use the GTEST version in Cachelib and remove the gtest from the RocksDB build.  This requirement is true whether or not you are building tests.  In the RocksDB build, you must remove GTEST from the "include_directories" and "add_subdirectories" and add " -Wno-error=deprecated-declarations" to the CMAKE_CXX_FLAGS.  You may also need to add "-Wno-error=sign-compare" to build the RocksDB tests, depending on your compiler.

* RocksDB does not currently enable plugins to build their own tests (see PR11052).  When the CachelibWrapperTest is added, additional link libraries may be required (See the CMakeLists.txt file).  No investigation as to the reason has been undertaken.



