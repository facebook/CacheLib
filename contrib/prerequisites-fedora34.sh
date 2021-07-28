#!/bin/sh

sudo dnf -y update
sudo dnf groupinstall -y "Development Tools"
sudo dnf -y install bison flex patch bzip2 cmake \
  double-conversion double-conversion-devel make g++ \
  boost-devel libevent-devel openssl-devel libunwind-devel \
  zlib-devel lz4-devel xz-devel bzip2-devel \
  jemalloc-devel snappy-devel libsodium-devel libdwarf-devel libaio-devel
