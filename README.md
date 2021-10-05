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
CacheLib has one single version number `facebook::cachelib::kCachelibVersion` that can be located at [CacheVersion.h](https://github.com/facebook/CacheLib/blob/main/cachelib/allocator/CacheVersion.h#L31). Any changes that makes API changes or requires dropping the cache should increment this version number. Details about whether a certain version is compatible with the previous one can be found at the [changelog](https://github.com/facebook/CacheLib/blob/main/CHANGELOG.md).


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

See [build](https://cachelib.org/docs/installation/installation) for more details about
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
