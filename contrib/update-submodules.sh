#!/bin/sh

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
