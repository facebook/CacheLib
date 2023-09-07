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

sudo dnf install -y epel-release
sudo dnf groupinstall -y "Development Tools"
sudo dnf install -y \
  libtool bison flex cmake \
  double-conversion \
  boost-devel \
  libevent-devel \
  openssl-devel \
  libunwind-devel \
  zlib-devel \
  lz4-devel \
  xz-devel \
  bzip2-devel \
  jemalloc-devel \
  libsodium-devel \
  libaio-devel \
  binutils-devel \
  libdwarf-devel \
  gmock-devel \
  gtest-devel \
  libsodium-static \
  libdwarf-static \
  numactl-devel


# Enable the "PowerTools" repository
# CentOS Repository name for "PowerTools" is "crb" for Centos Stream 9

# Show list of repos (for debugging)
sudo dnf repolist --all

POWERTOOLS_REPO=crb
sudo dnf --enablerepo="$POWERTOOLS_REPO" install -y \
  boost-static \
  snappy-devel \
  double-conversion-devel

#Do not install these from OS packages - they are typically outdated.
#gflags-devel \
#glog \
#fmt \
