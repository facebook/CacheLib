#!/bin/sh

sudo dnf -y update
sudo dnf groupinstall -y "Development Tools"
sudo dnf -y install bison flex patch bzip2 cmake \
	double-conversion double-conversion-devel \
	boost-devel libevent-devel openssl-devel libunwind-devel \
	zlib-devel lz4-devel libzstd-devel xz-devel bzip2-devel \
	jemalloc-devel snappy-devel libsodium-devel libdwarf-devel libaio-devel \
	gmock-devel gflags-devel gtest gtest-devel \
	fmt fmt-devel

# DO NOT INSTALL glog-devel - need to build from source for the glog-*.cmake files
