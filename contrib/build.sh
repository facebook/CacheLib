#!/bin/sh

## Note about shellcheck SC2086
## "Double-quote arguments to prevent globbing and word-splitting"
## https://github.com/koalaman/shellcheck/wiki/SC2086
## In few cases the $pass_params argument is NOT double-quoted
## on purpose: it might be empty, and it might have several
## distinct arguments to be passed to other scripts (e.g. "-i -t -D").
##
## As this is done purposfully, and the content of $pass_params
## is closely controlled, an explicit 'shellcheck disable SC2086'
## was added to the relevant lines.

set -u

die()
{
  base=$(basename "$0")
  echo "$base: error: $*" >&2
  exit 1
}

detect_os()
{
  ### Detect by /etc/os-release (if exists)
  if test -e /etc/os-release ; then
    DETECTED=$(sh -c '. /etc/os-release ; echo ${ID}${VERSION_ID} | tr A-Z a-z')
  fi


  if test -z "$DETECTED" ; then
    ### Detect by lsb_release (if exists)
    if command -v lsb_release >/dev/null 2>&1 ; then
      DETECTED=$(lsb_release -s -ir | tr -d '\n' | tr '[:upper:]' '[:lower:]')
    fi
  fi

  test -n "$DETECTED" || die "failed to detect operating system type"
}

build_debian_10()
{
  if test -z "$skip_os_pkgs" ; then
    ./contrib//prerequisites-debian10.sh \
      || die "failed to install packages for Debian"
  fi

  for pkg in zstd sparsemap fmt folly fizz wangle fbthrift ;
  do
    # shellcheck disable=SC2086
    ./contrib/build-package.sh -i $pass_params "$pkg" \
      || die "failed to build dependency '$pkg'"
  done
}

build_centos_8()
{
  if test -z "$skip_os_pkgs" ; then
    ./contrib/prerequisites-centos8.sh \
      || die "failed to install packages for CentOS"
  fi

  for pkg in zstd googleflags googlelog sparsemap fmt folly fizz wangle fbthrift ;
  do
    # shellcheck disable=SC2086
    ./contrib/build-package.sh -i $pass_params "$pkg" \
      || die "failed to build dependency '$pkg'"
  done
}

build_ubuntu_18()
{
  if test -z "$skip_os_pkgs" ; then
    ./contrib//prerequisites-ubuntu18.sh \
      || die "failed to install packages for Ubuntu 18.04"
  fi

  for pkg in zstd googletest sparsemap fmt folly fizz wangle fbthrift ;
  do
    # shellcheck disable=SC2086
    ./contrib/build-package.sh -i $pass_params "$pkg" \
      || die "failed to build dependency '$pkg'"
  done
}

show_help_and_exit()
{
  base=$(basename "$0")
  echo "CacheLib dependencies builder

usage: $base [-BdhijOStv]

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

  "
  exit 0
}


###############################
## Parse Commandline arguments
###############################
pass_params=
skip_os_pkgs=
show_help=
while getopts dhjOStv param
do
  case $param in
  h)  show_help=yes ;;
  O)  skip_os_pkgs=yes ;;
  d|j|S|t|v) pass_params="$pass_params -$param" ;;
  ?)      die "unknown option. See -h for help."
  esac
done
test -n "$show_help" && show_help_and_exit;

# Nothing detected so far
DETECTED=
detect_os

case "$DETECTED" in
  debian10) build_debian_10 ;;
  ubuntu18.04) build_ubuntu_18 ;;
  centos8) build_centos_8 ;;
  *) die "No build recipe for detected operating system '$DETECTED'" ;;
esac

# shellcheck disable=SC2086
./contrib/build-package.sh $pass_params cachelib \
  || die "failed to build cachelib"
