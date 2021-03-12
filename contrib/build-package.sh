#!/bin/sh

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
while getopts :BSdhijtv param
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
    ?) die "unknown option. See -h for help."
  esac
done
test -n "$show_help" && show_help_and_exit;
shift $((OPTIND-1))

test "$#" -eq 0 \
    && die "missing dependancy name to build. See -h for help"



######################################
## Check which dependecy was requested
######################################

external_git_clone=
external_git_branch=
update_submodules=

case "$1" in
  googlelog)
    NAME=glog
    REPO=https://github.com/google/glog
    REPODIR=cachelib/external/$NAME
    SRCDIR=$REPODIR
    external_git_clone=yes
    ;;

  googleflags)
    NAME=gflags
    REPO=https://github.com/gflags/gflags
    REPODIR=cachelib/external/$NAME
    SRCDIR=$REPODIR
    external_git_clone=yes
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
    external_git_clone=yes
    ;;

  fmt)
    NAME=fmt
    REPO=https://github.com/fmtlib/fmt.git
    REPODIR=cachelib/external/$NAME
    SRCDIR=$REPODIR
    external_git_clone=yes
    ;;

  zstd)
    NAME=zstd
    REPO=https://github.com/facebook/zstd
    REPODIR=cachelib/external/$NAME
    SRCDIR=$REPODIR/build/cmake
    external_git_clone=yes
    external_git_branch=release
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
    ;;

  fizz)
    NAME=fizz
    SRCDIR=cachelib/external/$NAME/$NAME
    update_submodules=yes
    ;;

  wangle)
    NAME=wangle
    SRCDIR=cachelib/external/$NAME/$NAME
    update_submodules=yes
    ;;

  fbthrift)
    NAME=fbthrift
    SRCDIR=cachelib/external/$NAME
    update_submodules=yes
    ;;

  cachelib)
    NAME=cachelib
    SRCDIR=cachelib
    install=  # cachelib is NEVER installed automatically
    ;;

  *) die "unknown dependency '$1'. See -h for help."
esac

####################################
## Calculate cmake/make parameters
####################################

CMAKE_PARAMS=
test "$debug_build" \
  && CMAKE_PARAMS="-DCMAKE_BUILD_TYPE=Debug" \
  || CMAKE_PARAMS="-DCMAKE_BUILD_TYPE=RelWithDebInfo"

test -z "$build_tests" \
  && CMAKE_PARAMS="$CMAKE_PARAMS -DBUILD_TESTS=OFF"

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


##
## Update the latest source code
##

if test "$source" ; then

  if test "$external_git_clone" ; then
    # This is an external (non-facebook) project, clone/pull it.
    if test -d "$SRCDIR" ; then
      # cloned repository already exists, update it
      ( cd "$SRCDIR" && git pull ) \
        || die "failed to update git repository for '$NAME' in '$SRCDIR'"
    else
      # Clone new repository directory
      git clone "$REPO" "$REPODIR" \
        || die "failed to clone git repository $REPO to '$REPODIR'"

      # Switch to specific branch if needed
      if test "$external_git_branch" ; then
        ( cd "$REPODIR" \
           && git checkout --track "origin/$external_git_branch" ) \
           || die "failed to checkout branch $external_git_branch in $REPODIR"
      fi
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
  sudo make install || die "make install failed"
fi

echo "'$NAME' is now installed"
