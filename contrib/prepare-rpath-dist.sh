#!/usr/bin/env bash

set -o pipefail
export LC_ALL=C

# In case these directories were not included
export LD_LIBRARY_PATH=/usr/local/lib64:/usr/local/lib:$LD_LIBRARY_PATH

# Default installation path
BINARY=opt/cachelib/bin/cachebench



die()
{
  base=$(basename "$0")
  echo "$base: error: $*" >&2
  exit 1
}

dir=$(dirname "$0")
cd "$dir/.." || die "failed to change-dir into $dir/.."
test -d cachelib || die "failed to change-dir to expected root directory"

test -d build-cachelib || die "the 'build-cachelib' directory wasn't found (try ./contrib/build-cachelib.sh)"

test -e "$BINARY" || die "the executable '$BINARY' was not found"

# Ensure all libraries are found
if ldd "$BINARY" 2>&1 | grep -q "not found" ; then
  die "some shared-object/libraries not found for '$BINARY' - check \$LD_LIBRARY_PATH"
fi

d=$(date +%F-%H%M%S)

dst=dist-$d

# Find parent directory of the binary
BASE_DIR=$(dirname "$BINARY")
BASE_DIR=$(dirname "$BASE_DIR")
LIB_OS_DIR="$BASE_DIR/lib-os"

mkdir -p "$LIB_OS_DIR" \
  || die "failed to create directory '$LIB_OS_DIR'"

ldd "$BINARY" \
    | awk '/=>/ { print $3 }' \
    | sort -u \
    | xargs -I% realpath --no-symlinks --relative-base="$BASE_DIR" "%" \
    | grep "^/" \
    | grep -vE "lib(c|m|dl|rt|pthread)[-.]" \
    | sed -E 's/\.so\.[0-9.]*$/\.so*/' \
    | xargs echo cp --no-dereference -t "$LIB_OS_DIR" \
    | sh \
   || die "failed to extract and copy shared-object dependencies"


tar -C opt -czf "cachelib-rpath-binary-$d.tar.gz" cachelib \
  || die "failed to create tarball"


echo "
  Tarball in:
     cachelib-rpath-binary-$d.tar.gz
"
