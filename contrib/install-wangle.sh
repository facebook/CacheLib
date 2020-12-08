#!/bin/sh

NAME=wangle

die()
{
  base=$(basename "0")
  echo "$base: error: $*" >&2
  exit 1
}

dir=$(dirname "$0")
cd "$dir/.." || die "failed to change-dir into $dir/.."
test -d cachelib || die "failed to change-dir to expected root directory"


./contrib/update-submodules.sh || die "failed to update git-submodules"

mkdir -p "build-$NAME" || die "failed to create build-$NAME directory"
cd "build-$NAME" || die "'cd' failed"

cmake ../cachelib/external/$NAME/$NAME || die "cmake failed"
make -j || die "make failed"
sudo make install || die "make install failed"

echo "$NAME library is now installed"
