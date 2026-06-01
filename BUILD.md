# Building CacheLib

CacheLib uses `getdeps.py` for building, which is used by many of Meta's OSS tools. This script will download and build all of the necessary dependencies first, and will then invoke cmake to build CacheLib. This ensures that you build with relevant versions of all dependent libraries, taking into account what versions are installed locally on your system.

## Dependencies

CacheLib depends on multiple libraries and programs. Some are available as system packages, and others need to be built from source.

The primary dependencies are:

* a C++20 compiler (tested with GCC, CLANG)
* [https://cmake.org/](CMake)
* [https://github.com/facebook/folly](folly) - Facebook's Open Source library
* [https://github.com/facebook/fbthrift](FBThrift) - Facebook Thrift

These dependencies further require multiple libraries:

* [https://github.com/facebookincubator/fizz](fizz) - Facebook's TLS 1.3 implementation
* [https://github.com/facebook/wangle](wangle) - C++ Networking Library
* [https://github.com/google/glog](glog) - Google Log Library
* [https://github.com/gflags/gflags](gflags) - Google Command-line flags Library
* [https://github.com/google/googletest.git](googletest) - Google Testing Framework
* [https://github.com/fmtlib/fmt.git](fmt) - open-source formatting library
* [https://github.com/Tessil/sparse-map.git](sparse-map) - memory efficient hash map and hash set
* [https://github.com/Cyan4973/xxHash](xxHash) - extremely fast non-cryptographic hash algorithm
* And many more libraries, commonly available as installable packages, e.g:
  `boost`, `libevent`, `lz4`, `snappy`, `zlib`, `ssl`, `libunwind`, `libsodium`

## Building with getdeps.py

### Step 1 - Clone the repository

```sh
git clone https://github.com/facebook/CacheLib
cd CacheLib
```

### Step 2 - Install system dependencies

You can install system dependencies to save building them:

```sh
sudo python3 ./build/fbcode_builder/getdeps.py install-system-deps --recursive cachelib
```

If you'd like to see the packages before installing them:

```sh
python3 ./build/fbcode_builder/getdeps.py install-system-deps --dry-run --recursive cachelib
```

On platforms without system dependencies, `getdeps.py` will download and build them for you during the build step.

### Step 3 - Build CacheLib

`getdeps.py` currently requires python 3.6+ to be on your path.

```sh
# Build, using system dependencies if available
python3 ./build/fbcode_builder/getdeps.py --allow-system-packages build cachelib
```

This command will:
1. Download and build all necessary dependencies (folly, fbthrift, wangle, fizz, etc.)
2. Build CacheLib with the appropriate configuration
3. Install the built artifacts to a scratch directory

The build may take several minutes on the first run as it compiles all dependencies. Subsequent builds will be much faster as only changed components are rebuilt.

## Build Configurations

By default, `getdeps.py` builds in Release mode with debug information. To change the configuration, pass `--build-type` (e.g., `--build-type Debug`) to the `build` command. Run `python3 ./build/fbcode_builder/getdeps.py build --help` for the full list of build options.

## Running Tests

By default `getdeps.py` will build the tests for CacheLib. To run them:

```sh
python3 ./build/fbcode_builder/getdeps.py --allow-system-packages test cachelib
```

This will run the full test suite for CacheLib. Individual tests can be run from the build directory if needed.

## Locating Build Output

`getdeps.py` installs build artifacts to a scratch directory. To find where CacheLib was installed:

```sh
python3 ./build/fbcode_builder/getdeps.py show-inst-dir cachelib
```

The install directory contains:

* `bin/` -- executables (e.g., `cachebench`, `cachebench-util`)
* `lib/` and `lib64/` -- library files (e.g., `libcachelib_allocator.so`)
* `include/` -- header files for CacheLib
* `test_configs/` -- sample CacheBench configurations

For example, to run `cachebench`:

```sh
INST_DIR=$(python3 ./build/fbcode_builder/getdeps.py show-inst-dir cachelib)
$INST_DIR/bin/cachebench --help
$INST_DIR/bin/cachebench --json_test_config $INST_DIR/test_configs/simple_test.json
```

## Development Workflow

When working on CacheLib itself (e.g., tweaking caching algorithms or adding features to `cachebench`), the following is recommended:

1. Make your changes to the source code in the `cachelib/` directory
2. Rebuild CacheLib:

   ```sh
   python3 ./build/fbcode_builder/getdeps.py --allow-system-packages build cachelib
   ```

3. Run relevant tests to verify your changes:

   ```sh
   python3 ./build/fbcode_builder/getdeps.py --allow-system-packages test cachelib
   ```

4. Locate the updated binaries:

   ```sh
   INST_DIR=$(python3 ./build/fbcode_builder/getdeps.py show-inst-dir cachelib)
   $INST_DIR/bin/cachebench --help
   ```

Since `getdeps.py` uses an incremental build system, rebuilding after small changes is typically very fast as only the modified components and their dependents are recompiled.

## Updating to Latest Version

Facebook's internal development cycle tightly couples CacheLib with its dependencies (e.g., folly, fbthrift, wangle, fizz), and all are frequently updated. In particular, the folly library does not provide a stable API, and using mismatched versions can cause compilation errors.

To update to the latest version:

1. Pull the latest changes:

   ```sh
   git pull origin main
   ```

2. Rebuild with getdeps.py:

   ```sh
   python3 ./build/fbcode_builder/getdeps.py --allow-system-packages build cachelib
   ```

The `getdeps.py` script automatically handles updating and rebuilding all dependencies to compatible versions. This ensures that you always have a consistent set of libraries that work together.

If you encounter build issues after pulling, try cleaning the build artifacts and rebuilding:

```sh
# getdeps.py will automatically rebuild dependencies as needed
python3 ./build/fbcode_builder/getdeps.py --allow-system-packages build cachelib
```

## Troubleshooting

### Build Failures

If you encounter build failures:

1. Ensure you have a C++20 compatible compiler (GCC 10+ or Clang 10+)
2. Make sure CMake 3.14 or newer is installed

### Missing System Dependencies

If system dependency installation fails:

1. Try the dry-run option to see what packages would be installed:
   ```sh
   python3 ./build/fbcode_builder/getdeps.py install-system-deps --dry-run --recursive cachelib
   ```

2. Install the packages manually using your system's package manager

3. Re-run the build with `--allow-system-packages` to use the manually installed dependencies

### Platform Support

`getdeps.py` has been tested on:
- Ubuntu 18.04, 20.04, 22.04
- CentOS 8
- Debian 10, 11

For other platforms, you may need to install dependencies manually and use `--allow-system-packages`.

## Legacy Build Scripts

**Note**: The legacy build scripts (`./contrib/build.sh`, `./contrib/build-package.sh`, and related scripts) are deprecated and no longer maintained. Please use `getdeps.py` as documented above for all builds.

## Additional Resources

- [Installation Guide](https://cachelib.org/docs/installation/) - Detailed installation instructions
- [CacheBench Documentation](https://cachelib.org/docs/Cache_Library_User_Guides/Cachebench_Overview) - Guide to using CacheBench
- [GitHub Repository](https://github.com/facebook/CacheLib) - Source code and issue tracker
