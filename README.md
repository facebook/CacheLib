<p align="center">
  <img width="500" height="140" alt="CacheLib" src="CacheLib-Logo.png">
</p>

# CacheLib

Pluggable caching engine for scale high performance cache services


## What is CacheLib ?

CacheLib is a C++ library providing in-process high performance caching
mechanism. CacheLib provides a thread safe API to build high throughput,
low overhead caching services, with built-in ability to leverage
DRAM and SSD caching transparently.


## Hardware Benchmarking

CacheLib provides a standalone executable `cachebench` that can be
used to evaluate heuristics and hardware platforms against production
workloads. Additionally `cachebench` enables a platform to evaluate
and measure new caching techniques.

See [README.benchmarks.md](README.benchmarks.md) for usage details
and examples.



## Building and installation

CacheLib provides a build script which prepares and installs all
dependencies and prerequisites, then builds CacheLib.
The build script has been tested to work on CentOS 8,
Ubuntu 18.04, and Debian 10.

```sh
git clone https://github.com/facebookincubator/CacheLib
cd CacheLib
./contrib/build.sh -d -j -v

# The resulting library and executables:
./build-cachelib/cachebench/cachebench --help
```

Re-running `./contrib/build.sh` will update CacheLib and its dependencies
to their latest versions and rebuild them.

See [README.build.md](README.build.md) for more details about
the building and installation process.



## Contributing

We'd love to have your help in making CacheLib better. If you're interested,
please read our guide to [guide to contributing](CONTRIBUTING.md)



## License

CacheLib is *apache* licensed, as found in the [LICENSE](LICENSE) file.



## Reporting and Fixing Security Issues

Please do not open GitHub issues or pull requests - this makes the problem
immediately visible to everyone, including malicious actors. Security issues in
CacheLib can be safely reported via Facebook's Whitehat Bug Bounty program:

https://www.facebook.com/whitehat

Facebook's security team will triage your report and determine whether or not is
it eligible for a bounty under our program.
