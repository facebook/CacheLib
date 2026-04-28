---
title: Installation
id: installation
---

# Building and Installation

CacheLib uses `getdeps.py` for building, which is used by many of Meta's OSS tools. It will download and build all of the necessary dependencies first, and will then invoke cmake etc to build CacheLib. This will help ensure that you build with relevant versions of all of the dependent libraries, taking into account what versions are installed locally on your system.

## Dependencies

You can install system dependencies to save building them:

```sh
# Clone the repo
git clone https://github.com/facebook/CacheLib
cd CacheLib

# Install dependencies
sudo python3 ./build/fbcode_builder/getdeps.py install-system-deps --recursive cachelib
```

If you'd like to see the packages before installing them:

```sh
python3 ./build/fbcode_builder/getdeps.py install-system-deps --dry-run --recursive cachelib
```

On other platforms or if on Linux and without system dependencies, `getdeps.py` will mostly download and build them for you during the build step.

Some of the dependencies `getdeps.py` uses and installs are:

  * a version of boost compiled with C++14 support.
  * googletest is required to build and run CacheLib's tests.

## Build

`getdeps.py` currently requires python 3.6+ to be on your path.

```sh
# Clone the repo
git clone https://github.com/facebook/CacheLib
cd CacheLib

# Build, using system dependencies if available
python3 ./build/fbcode_builder/getdeps.py --allow-system-packages build cachelib
```

## Run tests

By default `getdeps.py` will build the tests for CacheLib. To run them:

```sh
python3 ./build/fbcode_builder/getdeps.py --allow-system-packages test cachelib
```

See [Testing](/docs/installation/testing) for more details on running individual tests.

## Locating build output

`getdeps.py` installs build artifacts to a scratch directory. To find where CacheLib was installed:

```sh
python3 ./build/fbcode_builder/getdeps.py show-inst-dir cachelib
```

The install directory contains:

  * `bin/` -- executables (e.g., `cachebench`)
  * `lib/` and `lib64/` -- library files
  * `include/` -- header files
  * `test_configs/` -- sample CacheBench configurations

For example, to run `cachebench`:

```sh
INST_DIR=$(python3 ./build/fbcode_builder/getdeps.py show-inst-dir cachelib)
$INST_DIR/bin/cachebench --help
$INST_DIR/bin/cachebench --json_test_config $INST_DIR/test_configs/simple_test.json
```
