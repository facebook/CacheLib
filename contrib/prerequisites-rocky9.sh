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


# RockyLinux 9 diverged from CentOS, instead of "Powertools" repo,
# use CRB (CodeBuild Ready).
# see: https://wiki.rockylinux.org/rocky/repo/

sudo dnf update -y
sudo dnf install dnf-plugins-core
sudo dnf install -y epel-release
sudo dnf config-manager --set-enabled crb

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
  xz-devel \
  bzip2-devel \
  jemalloc-devel \
  libsodium-devel \
  libaio-devel \
  binutils-devel \
  numactl-devel


sudo dnf install -y \
  libdwarf-devel \
  snappy-devel \
  gmock-devel \
  gtest-devel \
  libsodium-static \
  libdwarf-static \
  boost-static \
  double-conversion-devel
