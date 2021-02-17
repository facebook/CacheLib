# Building CacheLib

## Dependencies

CacheLib depends on multiple libraries and programs.
Some are available as system packages, and others need
to be build from source.

The primary dependecies are:

* a C++17 compiler (tested with GCC, CLANG)
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
* And many more libraries, commonly available as installable packages, e.g:
  `boost`, `libevent`, `lz4`, `snappy`, `zlib`, `ssl`, `libunwind`, `libsodium`

Currently, some dependencies can be easily installed using the system's
package manager (e.g. `dnf`/`yum`/`apt`), while others need to be rebuild
from source code.


## Build Script

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

The build script supports the following options:

```sh
$ ./contrib/build.sh -h
CacheLib dependencies builder

usage: build.sh [-BdhijOStv]

options:
  -d    build with DEBUG configuration
        (default is RELEASE with debug information)
  -h    This help screen
  -j    build using all available CPUs ('make -j')
        (default is to use single CPU)
  -O    skip OS package installation (apt/yum/dnf)
  -S    skip git-clone/git-pull step
  -t    build tests
        (default is to skip tests if supported by the package)
  -v    verbose build
```


## Build Process Details

The build process involves the following steps.
These steps can be run manually for troubleshooting and/or
adapting the build to a new system. The wrapper script `./contrib/build.sh`
performs them one by one:

#### Step 1 - System packages

Installs the suitable tools and packages for the operating
system flavor and version (e.g. Debian 10).
This step requires `sudo`, and uses one of the following scripts:
`contrib/prerequisites-centos8.sh`,
`contrib/prerequisites-debian10.sh`,
`contrib/prerequisites-ubuntu18.sh`.

For Debian/Ubuntu it is a simple matter of running `apt-get` with
a known list of packages. For CentOS, the script first adds
the `Power Tools` repository (required for some of the packages).

It is safe to re-run these scripts - if the required packages are
already installed, the script will terminate quickly.


#### Step 2 - Update Git-Submodules

The CacheLib project includes several library as git-submodules
(folly,fbthrift,wangle,fizz).
Due to the way internal facebook projects are
converted to git and exported to github, the updating process
is slightly more complicated than a simple `git submodule update`.

The script `./contrib/update-submodules.sh` performs the required steps
to synchronize the required git revisions.

It is safe to re-run the `update-submodules.sh` script - it will simply
pull the latest changes (if any).


#### Step 3 - Build libraries from source code

Downloads the latest source code version of the following libraries,
builds and installs them (using `sudo`):
`googleflags`, `googlelog`, `sparsemap`, `fmt`, `folly`, `fizz`,
`wangle`, `fbthrift`.

In some cases the operating system has a pre-packaged version of some
of these libraries, but they are too old. In these cases the library
is rebuilt from source code.

Building each library is performed using the following script:

```sh
$ ./contrib/build-package.sh -h
CacheLib dependencies builder

usage: build-package.sh [-BdhijStv] NAME

options:
  -B    skip build step
        (default is to build with cmake & make)
  -d    build with DEBUG configuration
        (default is RELEASE with debug information)
  -h    This help screen
  -i    install after build using 'sudo make install'
        (default is to build but not install)
  -j    build using all available CPUs ('make -j')
        (default is to use single CPU)
  -S    skip git-clone/git-pull step
        (default is to get the latest source)
  -t    build tests
        (default is to skip tests if supported by the package)
  -v    verbose build

NAME: the dependency to build supported values are:
  googlelog, googleflags, googletest,
  fmt, sparsemap,
  folly, fizz, wangle, fbthrift,
  cachelib
```

All the required packages use `cmake`, and will be built in a new subdirectory
named `build-[PACKAGE]` (e.g. running `./contrib/build-package.sh fmt` will
create the `build-fmt` subdirectory).

Example:
Running the command `./contrib/build-package.sh -i -j -d -t fmt`
is equivalent to the following commands:

```sh
cd cachelib/external
git clone https://github.com/fmtlib/fmt.git
cd ../..
mkdir build-fmt
cmake ../cachelib/external/fmt -DCMAKE_BUILD_TYPE=Debug
make -j
sudo make install
```

#### Step 4 - Build CacheLib

Building CacheLib is identical to installing packages (above),
with the exception of system-wide installation - cachelib is *not* installed
by the `build.sh` wrapper script.

To build cachelib, run:

`./contrib/build-package.sh -j -d -v cachelib`.

To install the cachelib files, either add `-i` to the `build-package.sh` script,
or manually install with:

```sh
$ cd build-cachelib
$ sudo make install
```

The installed files will be:
* Header files in `/usr/local/include/cachelib/`
* Library files in `/usr/local/lib/libcachelib_*.so`
* `cachebench` and `cachebench-util` executables in `/usr/local/bin`.



## Development Cycle

When working on CacheLib itself (e.g. tweaking caching algorithms or adding
features to `cachebench`), the following is recommended:

* Run `./contrib/build.sh -j -d -v` to install dependencies and
  build `cachelib`.
* The resulting cachelib files will be stored in the `build-cachelib`
  subdirectory.
* Modify source code files in `./cachelib/`
* Rebuild the modified files in `build-cachelib` using `make`.

Example:

```sh
$ ./contrib/build.sh -d -j -v
[... after build is complete ...]

$ cd build-cachelib
$ make
[... cachelib and cachebench are rebuild ...]

$ touch touch ../cachelib/cachebench/main.cpp
$ make
[... cachelib and cachebench are rebuild ...]
```


## Updating to latest source code version

Facebook internal development cycle tightly couples
cachelib with its dependencies (e.g. folly, fbthrifth, wangle, fizz),
and all are frequently updated.
Particularly, the folly library does not provide stable API,
and using mismatched version can cause compilation errors.

Therefore, it is recommended to *always* update (and rebuild)
all dependencies before updating cachelib. That is,
a simple `git pull` for cachelib alone can often lead to failed builds.

Running the `contrib/build.sh` script takes care of first updating
and rebuilding all dependencies, and then updating and rebuilding cachelib.
Use the `-O` option to skip the installation of system packages, e.g.
`./contrib/build.sh -d -v -j -O`.

As all dependencies use `git/cmake/make`, rebuilding the same code (if there
were no updates) will be very fast.


## Downloading the source code without building

The default `build.sh` wrapper script requires internet connection
(for package installation and github updates).

For special build circumstances where internet connection is not available,
it is possible to download the source code on one machine, then copy it
and build it on another.

Use `build-package.sh -B` option to only download the latest source code
(using `git clone/git pull`) without building.

Example:
```sh
./contrib/build-package.sh -B googlelog
./contrib/build-package.sh -B googleflags
./contrib/build-package.sh -B googletest
./contrib/build-package.sh -B fmt
./contrib/build-package.sh -B sparsemap
./contrib/build-package.sh -B folly
./contrib/build-package.sh -B fizz
./contrib/build-package.sh -B wangle
./contrib/build-package.sh -B fbthrift
./contrib/build-package.sh -B cachelib
```

Will download the latest source code of all libraries under
the `./cachelib/external` subdirectory.

Then the entire build tree can be copied to another machine
(one that does not have internet connectivity).
CacheLib can then be build be adding the `-S` option to `build.sh`
(meaning: skip the `git clone/git pull` step):

```sh
$ ./contrib/build.sh -d -j -v -S
```
