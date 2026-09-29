#!/bin/bash
# Build the earth2studio downloader image, and optionally push it.
#
# The repo root is not a usable build context (planetary_datasets/ can hold terabytes of
# downloaded data), so the modules the image runs are staged into a temporary directory
# and built from there.
#
#   ./build.sh                                      # local image only
#   REGISTRY=ghcr.io/jacobbieker PUSH=1 ./build.sh  # also push
#
# The Dagster assets run $EARTH2STUDIO_IMAGE, which defaults to the local tag below.
set -euo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$HERE/../.." && pwd)

IMAGE_NAME=${IMAGE_NAME:-planetary-datasets/earth2studio}
TAG=${TAG:-$(date -u +%Y%m%d)}

CTX=$(mktemp -d "${TMPDIR:-/tmp}/earth2studio-ctx.XXXXXX")
trap 'rm -rf "$CTX"' EXIT

mkdir -p "$CTX/planetary_datasets/providers"
: > "$CTX/planetary_datasets/__init__.py"
: > "$CTX/planetary_datasets/providers/__init__.py"
for f in opera_download.py earth2studio_download.py; do
  cp "$REPO_ROOT/planetary_datasets/providers/$f" "$CTX/planetary_datasets/providers/$f"
done
cp "$HERE/environment.yml" "$HERE/requirements.txt" "$HERE/Dockerfile" "$HERE/run.sh" "$CTX/"

docker build -t "$IMAGE_NAME:$TAG" -t "$IMAGE_NAME:latest" "$CTX"

if [ "${PUSH:-0}" = "1" ]; then
  : "${REGISTRY:?set REGISTRY to push, e.g. ghcr.io/<owner>}"
  for tag in "$TAG" latest; do
    docker tag "$IMAGE_NAME:$tag" "$REGISTRY/$IMAGE_NAME:$tag"
    docker push "$REGISTRY/$IMAGE_NAME:$tag"
  done
fi
echo "built $IMAGE_NAME:$TAG"
