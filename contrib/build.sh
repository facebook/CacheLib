#!/bin/sh

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
    if which lsb_release >/dev/null 2>&1 ; then
  	DETECTED=$(lsb_release -s -ir | tr -d '\n' | tr 'A-Z' 'a-z')
    fi
  fi
  
  test -n "$DETECTED" || die "failed to detect operating system type"
}

build_debian_10()
{
./contrib//prerequisites-debian10.sh \
	&& ./contrib/install-fmt.sh \
	&& ./contrib/install-folly.sh \
	&& ./contrib/install-fizz.sh \
	&& ./contrib/install-wangle.sh \
	&& ./contrib/install-fbthrift.sh \
	&& ./contrib/install-sparsemap.sh \
	|| die "failed to install/build dependencies for Debian"
}

build_centos_8()
{
./contrib/prerequisites-centos8.sh \
   &&./contrib/install-googleflags.sh \
   && ./contrib/install-googlelog.sh \
   && ./contrib/install-fmt.sh \
   && ./contrib/install-folly.sh \
   && ./contrib/install-fizz.sh \
   && ./contrib/install-wangle.sh \
   && ./contrib/install-fbthrift.sh \
   && ./contrib/install-sparsemap.sh \
  || die "failed to install/build dependencies for CentOS-8"
}

build_ubuntu_18()
{
  ./contrib//prerequisites-ubuntu18.sh \
   && ./contrib/install-googletest.sh \
   && ./contrib/install-fmt.sh \
   && ./contrib/install-folly.sh \
   && ./contrib/install-fizz.sh \
   && ./contrib/install-wangle.sh \
   && ./contrib/install-fbthrift.sh \
   && ./contrib/install-sparsemap.sh \
  || die "failed to install/build dependencies for Ubuntu-18"
}

# Nothing detected so far
DETECTED=

detect_os

case "$DETECTED" in
  debian10) build_debian_10 ;;
  ubuntu18.04) build_ubuntu_18 ;;
  centos8) build_centos_8 ;;
  *) die "No build recipe for detected operating system '$DETECTED'" ;;
esac
