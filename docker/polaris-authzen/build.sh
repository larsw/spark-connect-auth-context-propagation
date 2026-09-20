#!/usr/bin/env bash
#
# Builds the Polaris server image from the AuthZEN fork and tags it for compose.
#
# This is a FORK of Apache Polaris, not a release: larsw/polaris @ feat/authzen-pdp-support, one
# commit on top of Polaris main that adds an AuthZEN Policy Decision Point authorizer. There is no
# published image, so it is built from source -- same idea as docker/polaris-console, which builds
# the console from a pinned upstream commit rather than vendoring it.
#
# Needs java21 and docker, both of which install.sh already checks for. The build is Gradle +
# Quarkus and takes a couple of minutes on a warm cache.
#
#   ./docker/polaris-authzen/build.sh [--clean]
#
set -euo pipefail

FORK_URL="${POLARIS_FORK_URL:-https://github.com/larsw/polaris.git}"
FORK_REF="${POLARIS_FORK_REF:-24702b214ae07fe738eca968e5165dffce3662ed}"
FORK_BRANCH="${POLARIS_FORK_BRANCH:-feat/authzen-pdp-support}"

# What compose.yaml expects. Polaris's own build tags apache/polaris:<version>; we retag so it is
# obvious in `docker images` that this is not the released Apache image.
IMAGE="${POLARIS_IMAGE:-spark-connect-propagation/polaris-authzen:1.8.0-SNAPSHOT}"

BUILD_DIR="${POLARIS_BUILD_DIR:-${TMPDIR:-/tmp}/polaris-authzen-src}"

if [ "${1:-}" = "--clean" ]; then
  echo "removing $BUILD_DIR"
  rm -rf "$BUILD_DIR"
fi

if [ ! -d "$BUILD_DIR/.git" ]; then
  echo "--- cloning $FORK_URL ($FORK_BRANCH) into $BUILD_DIR ---"
  git clone --depth 30 --branch "$FORK_BRANCH" "$FORK_URL" "$BUILD_DIR"
fi

echo "--- checking out $FORK_REF ---"
git -C "$BUILD_DIR" fetch --depth 30 origin "$FORK_BRANCH"
# reset rather than checkout: the patches below leave the tree dirty, and this has to be
# re-runnable.
git -C "$BUILD_DIR" reset --hard --quiet "$FORK_REF"

# Fixes carried on top of the pinned fork commit, applied at build time rather than by moving
# the pin, so it stays obvious which part is upstream's and which is ours. See patches/README.md.
PATCH_DIR="$(cd "$(dirname "$0")" && pwd)/patches"
if compgen -G "$PATCH_DIR/*.patch" >/dev/null; then
  for patch in "$PATCH_DIR"/*.patch; do
    echo "--- applying $(basename "$patch") ---"
    git -C "$BUILD_DIR" apply --whitespace=nowarn "$patch"
  done
fi

echo "--- building the Polaris server image (gradle + quarkus) ---"
make -C "$BUILD_DIR" build-server

BUILT="apache/polaris:$(cat "$BUILD_DIR/version.txt")"
echo "--- tagging $BUILT as $IMAGE ---"
docker tag "$BUILT" "$IMAGE"

echo
echo "built $IMAGE from $(git -C "$BUILD_DIR" rev-parse --short HEAD)"
