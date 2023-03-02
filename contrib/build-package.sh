#!/bin/sh
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

## Note about shellcheck SC2086
## "Double-quote arguments to prevent globbing and word-splitting"
## https://github.com/koalaman/shellcheck/wiki/SC2086
## In few cases the $XXX_PARAMS argument is NOT double-quoted
## on purpose: it might be empty, and it might have several
## distinct arguments to be passed to other scripts (e.g. "-i -t -D").
##
## As this is done purposfully, and the content of $XXX_PARAMS
## is closely controlled, an explicit 'shellcheck disable SC2086'
## was added to the relevant lines.

set -u

die()
{
  base=$(basename "$0")
  echo "$base: error: $*" >&2
  exit 1
}

show_help_and_exit()
{
  base=$(basename "$0")
  echo "CacheLib dependencies builder

usage: $base [-BdhijStv] NAME

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
  zstd
  googlelog, googleflags, googletest,
  fmt, sparsemap,
  folly, fizz, wangle, fbthrift,
  cachelib

  "
  exit 0
}

###############################
## Parse Commandline arguments
###############################
install=
build=yes
source=yes
debug_build=
build_tests=
show_help=
many_jobs=
verbose=
PREFIX="$PWD/opt/cachelib/"

while getopts :BSdhijtvp: param
do
  case $param in
    i) install=yes ;;
    B) build= ;;
    S) source= ;;
    h) show_help=yes ;;
    d) debug_build=yes ;;
    v) verbose=yes ;;
    j) many_jobs=yes ;;
    t) build_tests=yes ;;
    p) PREFIX=$OPTARG ;;
    ?) die "unknown option. See -h for help."
  esac
done
test -n "$show_help" && show_help_and_exit;
shift $((OPTIND-1))

test "$#" -eq 0 \
    && die "missing dependancy name to build. See -h for help"

######################################
## Check which dependency was requested
######################################

external_git_clone=
external_git_branch=
# external_git_tag can also be used for commit hashes
external_git_tag=
update_submodules=
cmake_custom_params=

case "$1" in
  googlelog)
    NAME=glog
    REPO=https://github.com/google/glog
    REPODIR=cachelib/external/$NAME
    SRCDIR=$REPODIR
    external_git_clone=yes
    external_git_tag="v0.5.0"
    cmake_custom_params="-DBUILD_SHARED_LIBS=ON"
    ;;

  googleflags)
    NAME=gflags
    REPO=https://github.com/gflags/gflags
    REPODIR=cachelib/external/$NAME
    SRCDIR=$REPODIR
    external_git_clone=yes
    external_git_tag="v2.2.2"
    cmake_custom_params="-DGFLAGS_BUILD_SHARED_LIBS=YES"
    if test "$build_tests" = "yes" ; then
        cmake_custom_params="$cmake_custom_params -DGFLAGS_BUILD_TESTING=YES"
    else
        cmake_custom_params="$cmake_custom_params -DGFLAGS_BUILD_TESTING=NO"
    fi
    ;;

  googletest)
    NAME=googletest
    REPO=https://github.com/google/googletest.git
    REPODIR=cachelib/external/$NAME
    SRCDIR=$REPODIR
    # Work-around: build googletest with DEBUG information
    # results in CMake problems in detecting the library.
    # See https://gitlab.kitware.com/cmake/cmake/-/issues/17799
    # Disable debug build, even if requested.
    if test "$debug_build" ; then
        echo "Note: disabling DEBUG mode for googletest build"
        debug_build=
    fi
    cmake_custom_params="-DBUILD_SHARED_LIBS=ON"
    external_git_clone=yes
    ;;

  fmt)
    NAME=fmt
    REPO=https://github.com/fmtlib/fmt.git
    REPODIR=cachelib/external/$NAME
    SRCDIR=$REPODIR
    external_git_clone=yes
    external_git_tag="8.0.1"
    cmake_custom_params="-DBUILD_SHARED_LIBS=ON"
    if test "$build_tests" = "yes" ; then
        cmake_custom_params="$cmake_custom_params -DFMT_TEST=YES"
    else
        cmake_custom_params="$cmake_custom_params -DFMT_TEST=NO"
    fi
    ;;

  zstd)
    NAME=zstd
    REPO=https://github.com/facebook/zstd
    REPODIR=cachelib/external/$NAME
    SRCDIR=$REPODIR/build/cmake
    external_git_clone=yes
    # Previously, we pinned to release branch. v1.5.4 needed
    # CMake >= 3.18, later reverted. While waiting for v1.5.5,
    # pin to the fix: https://github.com/facebook/zstd/pull/3510
    external_git_tag=8420502e
    if test "$build_tests" = "yes" ; then
        cmake_custom_params="-DZSTD_BUILD_TESTS=ON"
    else
        cmake_custom_params="-DZSTD_BUILD_TESTS=OFF"
    fi
    ;;

  sparsemap)
    NAME=sparsemap
    REPO=https://github.com/Tessil/sparse-map.git
    REPODIR=cachelib/external/$NAME
    SRCDIR=$REPODIR
    external_git_clone=yes
    ;;

  folly)
    NAME=folly
    SRCDIR=cachelib/external/$NAME
    update_submodules=yes
    cmake_custom_params="-DBUILD_SHARED_LIBS=ON"
    if test "$build_tests" = "yes" ; then
        cmake_custom_params="$cmake_custom_params -DBUILD_TESTS=ON"
    else
        cmake_custom_params="$cmake_custom_params -DBUILD_TESTS=OFF"
    fi
    ;;

  fizz)
    NAME=fizz
    SRCDIR=cachelib/external/$NAME/$NAME
    update_submodules=yes
    cmake_custom_params="-DBUILD_SHARED_LIBS=ON"
    if test "$build_tests" = "yes" ; then
        cmake_custom_params="$cmake_custom_params -DBUILD_TESTS=ON"
    else
        cmake_custom_params="$cmake_custom_params -DBUILD_TESTS=OFF"
    fi
    ;;

  wangle)
    NAME=wangle
    SRCDIR=cachelib/external/$NAME/$NAME
    update_submodules=yes
    cmake_custom_params="-DBUILD_SHARED_LIBS=ON"
    if test "$build_tests" = "yes" ; then
        cmake_custom_params="$cmake_custom_params -DBUILD_TESTS=ON"
    else
        cmake_custom_params="$cmake_custom_params -DBUILD_TESTS=OFF"
    fi
    ;;

  fbthrift)
    NAME=fbthrift
    SRCDIR=cachelib/external/$NAME
    update_submodules=yes
    cmake_custom_params="-DBUILD_SHARED_LIBS=ON"
    ;;

  cachelib)
    NAME=cachelib
    SRCDIR=cachelib
    if test "$build_tests" = "yes" ; then
        cmake_custom_params="$cmake_custom_params -DBUILD_TESTS=ON"
    else
        cmake_custom_params="$cmake_custom_params -DBUILD_TESTS=OFF"
    fi
    ;;

  *) die "unknown dependency '$1'. See -h for help."
esac

####################################
## Calculate cmake/make parameters
####################################

CMAKE_PARAMS="$cmake_custom_params"
test "$debug_build" \
  && CMAKE_PARAMS="$CMAKE_PARAMS -DCMAKE_BUILD_TYPE=Debug" \
  || CMAKE_PARAMS="$CMAKE_PARAMS -DCMAKE_BUILD_TYPE=RelWithDebInfo"


MAKE_PARAMS=
test "$verbose" && MAKE_PARAMS="$MAKE_PARAMS VERBOSE=YES"

JOBS=$(nproc --ignore 1)
test "$many_jobs" && MAKE_PARAMS="$MAKE_PARAMS -j$JOBS"




####################################
# Real work starts here
####################################


dir=$(dirname "$0")
cd "$dir/.." || die "failed to change-dir into $dir/.."
test -d cachelib || die "expected 'cachelib' directory not found in $PWD"


# After ensuring we are in the correct directory, set the installation prefix"
CMAKE_PARAMS="$CMAKE_PARAMS -DCMAKE_INSTALL_PREFIX=$PREFIX"
CMAKE_PREFIX_PATH="$PREFIX/lib/cmake:$PREFIX/lib64/cmake:$PREFIX/lib:$PREFIX/lib64:$PREFIX:${CMAKE_PREFIX_PATH:-}"
export CMAKE_PREFIX_PATH
PKG_CONFIG_PATH="$PREFIX/lib/pkgconfig:$PREFIX/lib64/pkgconfig:${PKG_CONFIG_PATH:-}"
export PKG_CONFIG_PATH
LD_LIBRARY_PATH="$PREFIX/lib:$PREFIX/lib64:${LD_LIBRARY_PATH:-}"
export LD_LIBRARY_PATH
PATH="$PREFIX/bin:$PATH"
export PATH

##
## Update the latest source code
##

if test "$source" ; then

  if test "$external_git_clone" ; then

    # This is an external (non-facebook) project, clone/pull it.
    if test -d "$SRCDIR" ; then
      # cloned repository already exists, update it, unless we're on a specific tag
      ( cd "$SRCDIR" && git fetch --all ) \
        || die "failed to fetch git repository for '$NAME' in '$SRCDIR'"
    else
      # Clone new repository directory
      git clone "$REPO" "$REPODIR" \
        || die "failed to clone git repository $REPO to '$REPODIR'"
    fi


    # switch to specific branch/tag if needed
    if test "$external_git_branch" ; then
        ( cd "$REPODIR" \
           && git checkout --force "origin/$external_git_branch" ) \
           || die "failed to checkout branch $external_git_branch in $REPODIR"
    elif test "$external_git_tag" ; then
        ( cd "$REPODIR" \
           && git checkout --force "$external_git_tag" ) \
           || die "failed to checkout tag $external_git_tag in $REPODIR"
    fi

  fi

  if test "$update_submodules" ; then
    ./contrib/update-submodules.sh || die "failed to update git-submodules"
  fi
fi


## If we do not build or install (only download the source code),
## exit now.
if test -z "$build" && test -z "$install" ; then
  echo "Source for '$NAME' downloaded/updated"
  exit 0
fi


# The build directory is needed for both build and installation
mkdir -p "build-$NAME" || die "failed to create build-$NAME directory"
cd "build-$NAME" || die "'cd' failed"

##
## Build
##
if test "$build" ; then
  # shellcheck disable=SC2086
  cmake $CMAKE_PARAMS "../$SRCDIR" || die "cmake failed on $SRCDIR"
  # shellcheck disable=SC2086
  nice make $MAKE_PARAMS || die "make failed"
fi

## If no install requested, exit now
if test -z "install" ; then
  echo "'$NAME' is now built in build-$NAME (source in $REPODIR)"
  exit 0
fi


##
## Install
##

if test "$install" ; then
  # shellcheck disable=SC2086
  make $MAKE_PARAMS install || die "make install failed"
fi

echo "'$NAME' is now installed"
