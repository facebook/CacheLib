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

die()
{
  base=$(basename "$0")
  echo "$base: error: $*" >&2
  exit 1
}

dir=$(dirname "$0")
cd "$dir/.." || die "failed to change-dir into $dir/.."
test -d cachelib || die "failed to change-dir to expected root directory"

git submodule update --init --checkout --force
git submodule foreach git fetch

root=./cachelib/external

# Temporary hack:
# After updating, checkout the required version based on fbthrift's files.
file="$root/fbthrift/build/deps/github_hashes/facebook/wangle-rev.txt"
wangle_rev=$(awk '{print $3}' $file) \
  || die "failed to detect required wangle revision"

( cd "$root/wangle" ; git checkout --force "$wangle_rev" ) \
  || die "failed to checkout required wangle revision '$wangle_rev'"

# Based on Wangle, checkout fizz
file="$root/wangle/build/deps/github_hashes/facebookincubator/fizz-rev.txt"
fizz_rev=$(awk '{print $3}' $file) \
  || die "failed to detect required fizz revision"

( cd "$root/fizz" ; git checkout --force "$fizz_rev" ) \
  || die "failed to checkout required fizz revision '$fizz_rev'"
