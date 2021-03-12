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
  libelf-dev \
  libdwarf-dev \
  libsodium-dev \
  libgmock-dev \
  libgtest-dev googletest
