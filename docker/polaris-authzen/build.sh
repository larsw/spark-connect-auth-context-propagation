#!/usr/bin/env bash
#
# Builds the Polaris server image from the AuthZEN fork and tags it for compose.
#
# This is a FORK of Apache Polaris, not a release: larsw/polaris @ feat/authzen-pdp-support, which
# adds an AuthZEN Policy Decision Point authorizer on top of Polaris main. There is no published
# image, so it is built from source -- same idea as docker/polaris-console, which builds the console
# from a pinned upstream commit rather than vendoring it.
#
# Gradle runs in an eclipse-temurin:21-jdk container. Polaris's own `make build-server` refuses any
# `java` that is not exactly 21, and Gradle needs a full JDK, not the Java 21 runtime without a
# compiler that some distributions install as "java 21". Running Gradle directly in the container
# sidesteps both, so the host needs only git and docker. The Gradle cache
# lives in a docker volume, so a second build takes a fraction of the first. The image itself is
# then built on the host from Polaris's own Dockerfile.jvm.
#
#   ./docker/polaris-authzen/build.sh [--clean]
#
set -euo pipefail

FORK_URL="${POLARIS_FORK_URL:-https://github.com/larsw/polaris.git}"
FORK_REF="${POLARIS_FORK_REF:-24702b214ae07fe738eca968e5165dffce3662ed}"
FORK_BRANCH="${POLARIS_FORK_BRANCH:-feat/authzen-pdp-support}"

# What compose.yaml expects. Tagged so that `docker images` shows this is not the released image.
IMAGE="${POLARIS_IMAGE:-spark-connect-propagation/polaris-authzen:1.8.0-SNAPSHOT}"

BUILD_DIR="${POLARIS_BUILD_DIR:-${TMPDIR:-/tmp}/polaris-authzen-src}"
JDK_IMAGE="${POLARIS_JDK_IMAGE:-eclipse-temurin:21-jdk}"
GRADLE_CACHE_VOLUME="${POLARIS_GRADLE_CACHE:-spark-connect-propagation-polaris-gradle}"

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
git -C "$BUILD_DIR" checkout --quiet "$FORK_REF"

echo "--- building the Polaris server in $JDK_IMAGE ---"
# A new volume belongs to root; the build runs as you, so the sources it writes stay yours.
docker run --rm -v "$GRADLE_CACHE_VOLUME:/gradle" "$JDK_IMAGE" chown "$(id -u):$(id -g)" /gradle
docker run --rm \
  --user "$(id -u):$(id -g)" \
  -e HOME=/tmp \
  -e GRADLE_USER_HOME=/gradle \
  -v "$GRADLE_CACHE_VOLUME:/gradle" \
  -v "$BUILD_DIR:/src" \
  -w /src \
  "$JDK_IMAGE" \
  ./gradlew --no-daemon :polaris-server:assemble :polaris-server:quarkusAppPartsBuild --rerun \
    -Dquarkus.container-image.build=false

echo "--- building $IMAGE ---"
docker build -f "$BUILD_DIR/runtime/server/src/main/docker/Dockerfile.jvm" -t "$IMAGE" \
  "$BUILD_DIR/runtime/server"

echo
echo "built $IMAGE from $(git -C "$BUILD_DIR" rev-parse --short HEAD)"
