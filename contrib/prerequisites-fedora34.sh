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

sudo dnf -y update
sudo dnf groupinstall -y "Development Tools"
sudo dnf -y install bison flex patch bzip2 cmake \
  double-conversion double-conversion-devel make g++ \
  boost-devel libevent-devel openssl-devel libunwind-devel \
  zlib-devel lz4-devel xz-devel bzip2-devel \
  jemalloc-devel snappy-devel libsodium-devel libdwarf-devel libaio-devel \
  numactl-devel
