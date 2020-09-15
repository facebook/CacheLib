#!/bin/sh

NAME=sparsemap
REPO=https://github.com/Tessil/sparse-map.git

die()
{
	base=$(basename "0")
	echo "$base: error: $* (tmpdir = $tmpdir)" >&2
	exit 1
}

tmpdir=$(mktemp -t -d  cachelib.prereqs.$NAME.XXXXXX) || die "failed to create temporary directory"
cd "$tmpdir" || die "faied to CD into $tmpdir"
git clone  "$REPO" || die "failed to clone '$NAME' repository $REPO"
basedir=$(basename "$REPO" .git)
cd "$basedir" && mkdir _build && cd _build \
	|| die "failed to create build directory"
cmake .. || die "cmake failed"
make -j || die "make failed"
sudo make install || die "make install failed"

cd /tmp
rm -r "$tmpdir" || die "failed to remove temporary build directory"

echo "$NAME library is now installed"
