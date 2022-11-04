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

NAME=cachelib

die()
{
  base=$(basename "0")
  echo "$base: error: $*" >&2
  exit 1
}

dir=$(dirname "$0")
cd "$dir/.." || die "failed to change-dir into $dir/.."
test -d cachelib || die "failed to change-dir to expected root directory"


mkdir -p "build-$NAME" || die "failed to create build-$NAME directory"
cd "build-$NAME" || die "'cd' failed"

cmake ../cachelib/ || die "cmake failed"
make -j --keep-going || die "make failed"
#sudo make install || die "make install failed"

#echo "$NAME library is now installed"
