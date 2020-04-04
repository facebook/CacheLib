#!/bin/bash
BUILD_DIR=${1:-"$HOME"}

pushd ${BUILD_DIR}
#basic install
apt -y update
apt -y htop
apt-get install -y software-properties-common
add-apt-repository -y ppa:ubuntu-toolchain-r/test
apt -y update
apt -y install g++
apt install g++-7 -y
apt install -y make

#cmake
curl -O -L https://github.com/Kitware/CMake/releases/download/v3.17.0/cmake-3.17.0-Linux-x86_64.tar.gz
curl -O -L https://github.com/Kitware/CMake/releases/download/v3.17.0/cmake-3.17.0-Linux-x86_64.sh 
chmod +x cmake-3.17.0-Linux-x86_64.sh && ./cmake-3.17.0-Linux-x86_64.sh --skip-license --prefix=/usr/local

#gtest
wget https://github.com/google/googletest/archive/release-1.8.0.tar.gz && \
tar zxf release-1.8.0.tar.gz && \
rm -f release-1.8.0.tar.gz && \
pushd googletest-release-1.8.0 && \
cmake . && \
make && \
make install
popd


#folly deps
sudo apt-get -y install \
    g++ \
    libboost-all-dev \
    libevent-dev \
    libdouble-conversion-dev \
    libgoogle-glog-dev \
    libgflags-dev \
    libiberty-dev \
    liblz4-dev \
    liblzma-dev \
    libsnappy-dev \
    make \
    zlib1g-dev \
    binutils-dev \
    libjemalloc-dev \
    libssl-dev \
    pkg-config \
    libunwind-dev

#fmt
##fmt depts
apt -y install libsodium-dev
##
[[ -d fmt ]] || git clone https://github.com/fmtlib/fmt.git || exit 1
pushd fmt

[[ -d _build ]] || mkdir _build
cd _build
cmake ..

make -j$(nproc)
make install
popd
#rm -rf fmt

#folly
[[ -d folly ]] || git clone https://github.com/facebook/folly.git || exit 1
pushd folly
[[ -d _build ]] || mkdir _build
cd _build
cmake -DCMAKE_CXX_COMPILER=g++-7 -DCMAKE_CXX_STANDARD=17 -DCMAKE_CXX_STANDARD_REQUIRED=ON ..
make -j $(nproc)
make install
popd

fbthrift deps
#glog
apt -y install bison libboost-all-dev flex zlib1g libgflags-dev
[[ -d glog ]] || git clone https://github.com/google/glog.git || exit 1
pushd glog
./autogen.sh && ./configure && make -j $(nproc) && make install
popd

##rsocket-cpp
[[ -d rsocket-cpp ]] || git clone https://github.com/rsocket/rsocket-cpp.git | exit 1
pushd rsocket-cpp
mkdir -p build
cd build
cmake ..
make -j $(nproc)
make install
popd

## fizz
sudo apt-get -y install \
    g++ \
    libboost-all-dev \
    libevent-dev \
    libdouble-conversion-dev \
    libgoogle-glog-dev \
    libgflags-dev \
    libiberty-dev \
    liblz4-dev \
    liblzma-dev \
    libsnappy-dev \
    make \
    zlib1g-dev \
    binutils-dev \
    libjemalloc-dev \
    libssl-dev \
    pkg-config \
    libsodium-dev

[[ -d fizz ]] || git clone https://github.com/facebookincubator/fizz || exit 1
pushd fizz
mkdir build_ && cd build_
cmake ../fizz
make -j $(nproc)
sudo make install
popd

##wangle
[[ -d wangle ]] || git clone https://github.com/facebook/wangle.git || exit 1
pushd wangle/wangle
cmake -DCMAKE_CXX_COMPILER=g++-7 .
make -j $(nproc)
sudo make install
popd

##
[[ -d zstd ]] || git clone https://github.com/facebook/zstd.git || exit 1
pushd zstd
make -j $(nproc)
make install
popd

#fbthrift
[[ -d fbthrift ]] || git clone https://github.com/facebook/fbthrift || exit 1
pushd fbthrift
cd build
cmake -DCMAKE_CXX_COMPILER=g++-7 ..
make -j $(nproc)
make install
popd

#build cachelib!

popd
[[ -d build ]] || mkdir build
pushd build
cmake -Wno-dev -DCMAKE_CXX_COMPILER=g++-7 .. && make -j $(nproc)
popd
