#!/usr/bin/env bash

set -o pipefail

die()
{
  base=$(basename "0")
  echo "$base: error: $*" >&2
  exit 1
}

dir=$(dirname "$0")
cd "$dir/.." || die "failed to change-dir into $dir/.."
test -d cachelib || die "failed to change-dir to expected root directory"

test -d build-cachelib || die "the 'build-cachelib' directory wasn't found (try ./contrib/build-cachelib.sh)"

d=$(date +%F-%H%M%S)

dst=dist-$d

mkdir -p "$dst" "$dst/bin" "$dst/lib/cachelib" || die "failed to create destination directory '$dst'"

cp build-cachelib/cachebench/cachebench "$dst/bin" \
  || die "failed to copy cachebench binary to '$dst/bin'"

ldd build-cachelib/cachebench/cachebench | awk '/=>/ { print $3 }' | sort -u | xargs cp -t "$dst/lib/cachelib/" \
  || die "failed to extract and copy shared-object dependencies"

tar -czf "cachelib-rpath-binary-$d.tar.gz" "$dst" \
  || die "failed to create tarball"


echo "
  Created RPATH binary:
     $dst/bin/cachebench

  Tarball in:
     cachelib-rpath-binary-$d.tar.gz
"
