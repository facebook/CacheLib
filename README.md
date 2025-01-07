<p align="center">
  <img width="500" height="140" alt="CacheLib" src="website/static/img/CacheLib-Logo-Large-transp.png">
</p>

# CacheLib

Pluggable caching engine to build and scale high performance cache services. See
[www.cachelib.org](https://cachelib.org) for documentation and more information.


## What is CacheLib ?

CacheLib is a C++ library providing in-process high performance caching
mechanism. CacheLib provides a thread safe API to build high throughput,
low overhead caching services, with built-in ability to leverage
DRAM and SSD caching transparently.


## Performance benchmarking

CacheLib provides a standalone executable `CacheBench` that can be used to
evaluate the performance of heuristics and caching hardware platforms against
production workloads. Additionally `CacheBench` enables stress testing
implementation and design changes to CacheLib to catch correctness and
performance issues.

See [CacheBench](https://cachelib.org/docs/Cache_Library_User_Guides/Cachebench_Overview) for usage details
and examples.

## Versioning
CacheLib has one single version number `facebook::cachelib::kCachelibVersion` that can be located at [CacheVersion.h](https://github.com/facebook/CacheLib/blob/main/cachelib/allocator/CacheVersion.h#L31). This version number must be incremented when incompatible changes are introduced. A change is incompatible if it could cause a complication failure due to removing public API or requires dropping the cache. Details about the compatibility information when the version number increases can be found in the [changelog](https://github.com/facebook/CacheLib/blob/main/CHANGELOG.md).


## Building and installation with `getdeps.py`

This script is used by many of Meta's OSS tools.  It will download and build all of the necessary dependencies first, and will then invoke cmake etc to build folly.  This will help ensure that you build with relevant versions of all of the dependent libraries, taking into account what versions are installed locally on your system.

### Dependencies

You can install system dependencies to save building them:

    # Clone the repo
    git clone https://github.com/facebook/CacheLib
    # Install dependencies
    cd CacheLib
    sudo ./build/fbcode_builder/getdeps.py install-system-deps --recursive cachelib

If you'd like to see the packages before installing them:

    ./build/fbcode_builder/getdeps.py install-system-deps --dry-run --recursive

On other platforms or if on Linux and without system dependencies `getdeps.py` will mostly download and build them for you during the build step.

Some of the dependencies `getdeps.py` uses and installs are:

  * a version of boost compiled with C++14 support.
  * googletest is required to build and run folly's tests.

### Build

This script will download and build all of the necessary dependencies first,
and will then invoke cmake etc to build CacheLib.  This will help ensure that you build with relevant versions of all of the dependent libraries, taking into account what versions are installed locally on your system.

`getdeps.py` currently requires python 3.6+ to be on your path.

`getdeps.py` will invoke cmake etc.

    # Clone the repo
    git clone https://github.com/facebook/CacheLib
    cd CacheLib
    # Build, using system dependencies if available
    python3 ./build/fbcode_builder/getdeps.py --allow-system-packages build cachelib

### Run tests

By default `getdeps.py` will build the tests for folly. To run them:

    cd folly
    python3 ./build/fbcode_builder/getdeps.py --allow-system-packages test cachelib



## Contributing

We'd love to have your help in making CacheLib better. If you're interested,
please read our [guide to contributing](CONTRIBUTING.md)



## License

CacheLib is *apache* licensed, as found in the [LICENSE](LICENSE) file.



## Reporting and Fixing Security Issues

Please do not open GitHub issues or pull requests - this makes the problem
immediately visible to everyone, including malicious actors. Security issues in
CacheLib can be safely reported via Facebook's Whitehat Bug Bounty program:

https://www.facebook.com/whitehat

Facebook's security team will triage your report and determine whether or not is
it eligible for a bounty under our program.


## Build status

Clicking on a badge will show you the recent builds for that OS. If your target OS's build is failing, you may check out the latest [release](https://github.com/facebook/CacheLib/releases). If the release is too stale for you, you may wish to check recent issues and PRs for known workarounds.


[![linux](https://github.com/facebook/CacheLib/actions/workflows/getdeps_linux.yml/badge.svg)](https://github.com/facebook/CacheLib/actions/workflows/getdeps_linux.yml)
