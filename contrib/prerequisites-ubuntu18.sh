#!/bin/sh

sudo apt-get update
sudo apt-get install -y \
  build-essential \
  git \
  g++ \
  cmake \
  bison flex \
  libboost-all-dev \
  libevent-dev \
  libdouble-conversion-dev \
  libgoogle-glog-dev \
  libgflags-dev \
  libiberty-dev \
  liblz4-dev \
  liblzma-dev \
  libbz2-dev \
  libsnappy-dev \
  make \
  zlib1g-dev \
  binutils-dev \
  libjemalloc-dev \
  libssl-dev \
  pkg-config \
  libunwind-dev \
  libzstd-dev \
  libelf-dev \
  libdwarf-dev \
  libsodium-dev

# NOTE:
# GoogleTest/GoogleMock libraries are available in Ubuntu as
# Standard packages, but the do not contain the required CMAKE
# files, and aren't detected when building CacheLib.
# So on Ubuntu-18 - build googletest from source.
### googletest libgtest-dev google-mock
