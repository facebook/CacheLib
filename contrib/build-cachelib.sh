#!/bin/sh

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
