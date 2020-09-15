#!/bin/sh

sudo dnf install -y epel-release
sudo dnf groupinstall -y "Development Tools"
sudo dnf install -y \
	libtool bison flex cmake \
	double-conversion double-conversion-devel \
	boost-devel \
	libevent-devel \
	openssl-devel \
	libunwind-devel \
	zlib-devel \
	lz4-devel \
	libzstd-devel \
	xz-devel \
	bzip2-devel \
	jemalloc-devel \
	libsodium-devel \
	libaio-devel \
	binutils-devel

dnf --enablerepo=PowerTools install \
	libdwarf-devel \
	snappy-devel \
	gmock-devel \
	gtest-devel \
	libsodium-static \
	libdwarf-static \
	boost-static \
	double-conversion-static

	#gflags-devel \
	#glog \
	#fmt \
