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
  ./contrib//prerequisites-debian10.sh \
    || die "failed to install packages for Debian"
}

build_centos_8()
{
  ./contrib/prerequisites-centos8.sh \
    || die "failed to install packages for CentOS"
}

build_rocky_9()
{
  ./contrib/prerequisites-rocky9.sh \
    || die "failed to install packages for RockyLinux 9"
}

build_ubuntu_18()
{
  ./contrib//prerequisites-ubuntu18.sh \
    || die "failed to install packages for Ubuntu"
}

build_fedora_34()
{
  ./contrib//prerequisites-fedora34.sh \
    || die "failed to install packages for Fedora"
}

build_arch()
{
  ./contrib//prerequisites-arch.sh \
    || die "failed to install packages for ArchLinux"
}

build_dependencies()
{
  for pkg in zstd googleflags googlelog googletest sparsemap fmt folly fizz wangle fbthrift ;
  do
    # shellcheck disable=SC2086
    ./contrib/build-package.sh $pass_params "$pkg" \
      || die "failed to build dependency '$pkg'"
  done
}

show_help_and_exit()
{
  base=$(basename "$0")
  echo "CacheLib dependencies builder

usage: $base [-BdhijOStv]

options:
  -B    skip build - just download packages and git source
  -d    build with DEBUG configuration
        (default is RELEASE with debug information)
  -h    This help screen
  -j    build using all available CPUs ('make -j')
        (default is to use single CPU)
  -O    skip OS package installation (apt/yum/dnf)
  -S    skip git-clone/git-pull step
  -t    build tests
        (default is to skip tests if supported by the package)
  -T    build only CacheLib tests
  -v    verbose build

  "
  exit 0
}


###############################
## Parse Commandline arguments
###############################
pass_params=
skip_os_pkgs=
skip_build=
show_help=
build_cachelib_tests=
while getopts BdhjOStvTp: param
do
  case $param in
  h)  show_help=yes ;;
  O)  skip_os_pkgs=yes ;;
  B)  skip_build=yes ;;
  d|j|S|t|v) pass_params="$pass_params -$param" ;;
  T)  build_cachelib_tests=yes ;;
  p)  pass_params="$pass_params -$param $OPTARG" ;;
  ?)      die "unknown option. See -h for help."
  esac
done
test -n "$show_help" && show_help_and_exit;

test -n "$skip_build" \
  && pass_params="$pass_params -B" \
  || pass_params="$pass_params -i"


# Nothing detected so far
DETECTED=
detect_os

if test -z "$skip_os_pkgs" ; then
  case "$DETECTED" in
    debian10|debian11) build_debian_10 ;;
    ubuntu18.04|ubuntu20.04|ubuntu21.04|ubuntu22.04) build_ubuntu_18 ;;
    centos8|rocky8.?) build_centos_8 ;;
    rocky9.?) build_rocky_9 ;;
    fedora3[456]) build_fedora_34 ;;
    arch*|manjaro*) build_arch ;;
    *) die "No build recipe for detected operating system '$DETECTED'" ;;
  esac
fi

build_dependencies

test -n "$build_cachelib_tests" \
  && pass_params="$pass_params -t"

# shellcheck disable=SC2086
./contrib/build-package.sh $pass_params cachelib \
  || die "failed to build cachelib"
