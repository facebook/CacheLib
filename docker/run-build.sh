#!/usr/bin/env bash
# SPDX-License-Identifier: BSD-3-Clause
# Copyright 2022, Intel Corporation

set -e

function sudo_password() {
	echo ${USERPASS} | sudo -Sk $*
}

cd ..
mkdir build
cd build
cmake ../cachelib -DBUILD_TESTS=ON -DCMAKE_INSTALL_PREFIX=/opt -DCMAKE_BUILD_TYPE=Debug
sudo_password make install -j$(nproc)

cd /opt/tests && $WORKDIR/run_tests.sh
