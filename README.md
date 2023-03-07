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


## Building and installation

CacheLib provides a build script which prepares and installs all
dependencies and prerequisites, then builds CacheLib.
The build script has been tested to work on CentOS 8,
Ubuntu 18.04, and Debian 10.

```sh
git clone https://github.com/facebook/CacheLib
cd CacheLib
./contrib/build.sh -d -j -v

# The resulting library and executables:
./opt/cachelib/bin/cachebench --help
```

Re-running `./contrib/build.sh` will update CacheLib and its dependencies
to their latest versions and rebuild them.

See [build](https://cachelib.org/docs/installation/) for more details about
the building and installation process.


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

Clicking on a badge will show you the recent builds for that OS. If your target OS's build is failing, you may wish to check recent issues and PRs for known workarounds.

- [![CentOS 8.1](https://github.com/facebook/cachelib/actions/workflows/build-cachelib-centos-8-1.yml/badge.svg?event=schedule)](https://github.com/facebook/cachelib/actions/workflows/build-cachelib-centos-8-1.yml?query=event%3Aschedule)
- [![CentOS 8.5](https://github.com/facebook/cachelib/actions/workflows/build-cachelib-centos-8-5.yml/badge.svg?event=schedule)](https://github.com/facebook/cachelib/actions/workflows/build-cachelib-centos-8-5.yml?query=event%3Aschedule)
- [![Debian 10](https://github.com/facebook/cachelib/actions/workflows/build-cachelib-debian-10.yml/badge.svg?event=schedule)](https://github.com/facebook/cachelib/actions/workflows/build-cachelib-debian-10.yml?query=event%3Aschedule)
- [![Fedora 36](https://github.com/facebook/cachelib/actions/workflows/build-cachelib-fedora-36.yml/badge.svg?event=schedule)](https://github.com/facebook/cachelib/actions/workflows/build-cachelib-fedora-36.yml?query=event%3Aschedule)
- [![Rocky Linux 8](https://github.com/facebook/cachelib/actions/workflows/build-cachelib-rockylinux-8.yml/badge.svg?event=schedule)](https://github.com/facebook/cachelib/actions/workflows/build-cachelib-rockylinux-8.yml?query=event%3Aschedule)
- [![Rocky Linux 9](https://github.com/facebook/cachelib/actions/workflows/build-cachelib-rockylinux-9.yml/badge.svg?event=schedule)](https://github.com/facebook/cachelib/actions/workflows/build-cachelib-rockylinux-9.yml?query=event%3Aschedule)
- [![Ubuntu 18](https://github.com/facebook/cachelib/actions/workflows/build-cachelib-ubuntu-18.yml/badge.svg?event=schedule)](https://github.com/facebook/cachelib/actions/workflows/build-cachelib-ubuntu-18.yml?query=event%3Aschedule)
- [![Ubuntu 20](https://github.com/facebook/cachelib/actions/workflows/build-cachelib-ubuntu-20.yml/badge.svg?event=schedule)](https://github.com/facebook/cachelib/actions/workflows/build-cachelib-ubuntu-20.yml?query=event%3Aschedule)
- [![Ubuntu 22](https://github.com/facebook/cachelib/actions/workflows/build-cachelib-ubuntu-22.yml/badge.svg?event=schedule)](https://github.com/facebook/cachelib/actions/workflows/build-cachelib-ubuntu-22.yml?query=event%3Aschedule)
