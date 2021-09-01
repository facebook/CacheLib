#!/bin/sh

set -e

# Root directory for the CacheLib project
CLBASE="$PWD/../.."

# Additional "FindXXX.cmake" files are here (e.g. FindSodium.cmake)
CLCMAKE="$CLBASE/cachelib/cmake"

# After ensuring we are in the correct directory, set the installation prefix"
PREFIX="$CLBASE/opt/cachelib/"

CMAKE_PARAMS="-DCMAKE_INSTALL_PREFIX='$PREFIX' -DCMAKE_MODULE_PATH='$CLCMAKE'"

CMAKE_PREFIX_PATH="$PREFIX/lib/cmake:$PREFIX/lib64/cmake:$PREFIX/lib:$PREFIX/lib64:$PREFIX:${CMAKE_PREFIX_PATH:-}"
export CMAKE_PREFIX_PATH
PKG_CONFIG_PATH="$PREFIX/lib/pkgconfig:$PREFIX/lib64/pkgconfig:${PKG_CONFIG_PATH:-}"
export PKG_CONFIG_PATH
LD_LIBRARY_PATH="$PREFIX/lib:$PREFIX/lib64:${LD_LIBRARY_PATH:-}"
export LD_LIBRARY_PATH

mkdir -p build
cd build
cmake $CMAKE_PARAMS ..
make
