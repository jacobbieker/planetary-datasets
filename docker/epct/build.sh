#!/bin/bash
# Build the Data Tailor image from a staged context holding only epct_download.py.
#   ./build.sh                                      # local image only
#   REGISTRY=ghcr.io/jacobbieker PUSH=1 ./build.sh  # also push
set -euo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$HERE/../.." && pwd)
IMAGE_NAME=${IMAGE_NAME:-planetary-datasets/epct}
TAG=${TAG:-$(date -u +%Y%m%d)}

CTX=$(mktemp -d)
trap 'rm -rf "$CTX"' EXIT
mkdir -p "$CTX/planetary_datasets/providers/polar"
touch "$CTX/planetary_datasets/__init__.py" "$CTX/planetary_datasets/providers/__init__.py" \
      "$CTX/planetary_datasets/providers/polar/__init__.py"
cp "$REPO_ROOT/planetary_datasets/providers/polar/epct_download.py" \
   "$CTX/planetary_datasets/providers/polar/"
cp "$HERE/environment.yml" "$HERE/Dockerfile" "$CTX/"

docker build -t "$IMAGE_NAME:$TAG" -t "$IMAGE_NAME:latest" "$CTX"

if [ "${PUSH:-0}" = "1" ]; then
  : "${REGISTRY:?set REGISTRY to push, e.g. ghcr.io/<owner>}"
  for tag in "$TAG" latest; do
    docker tag "$IMAGE_NAME:$tag" "$REGISTRY/$IMAGE_NAME:$tag"
    docker push "$REGISTRY/$IMAGE_NAME:$tag"
  done
fi
echo "built $IMAGE_NAME:$TAG"
