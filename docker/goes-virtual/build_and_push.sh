#!/bin/bash
# Build the GOES virtual-ingest image and push it to ECR.
#
# The repo root cannot be used as a build context: planetary_datasets/ holds
# ~1.8TB of downloaded data. This stages the handful of files the image needs
# into a temporary directory and builds from there, so the context is ~600KB.
#
# The image is pushed to every region in $REGIONS. The ingest boxes run in
# us-east-1, next to the NOAA source buckets, and their launch config names the
# us-east-1 registry — so a tag pushed only to us-west-2 is one they cannot
# pull. A cross-region pull is possible but pays transfer cost and latency on
# every launch, so it is cheaper to hold the tag in both.
#
# ECR credentials come from the environment (or the ambient AWS config); none
# are written into the image.
#
#   AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... ./build_and_push.sh
#   REGIONS=us-east-1 ./build_and_push.sh          # just the one
set -euo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$HERE/../.." && pwd)

# AWS_REGION still works, and names the region used to resolve the account.
REGIONS=${REGIONS:-"us-west-2 us-east-1"}
REGION=${AWS_REGION:-${REGIONS%% *}}
IMAGE_NAME=${IMAGE_NAME:-goes-virtual-ingest}
TAG=${TAG:-$(date -u +%Y%m%d)}
PLATFORM=linux/amd64
LIFECYCLE_POLICY="$HERE/ecr_lifecycle_policy.json"

CTX=$(mktemp -d /tmp/goes-virtual-ctx.XXXXXX)
trap 'rm -rf "$CTX"' EXIT

echo "==> staging build context in $CTX"
mkdir -p "$CTX/planetary_datasets/providers/virtualized"
# Empty package markers.
: > "$CTX/planetary_datasets/__init__.py"
: > "$CTX/planetary_datasets/providers/__init__.py"
: > "$CTX/planetary_datasets/providers/virtualized/__init__.py"
# Shared config and memory helpers. The ingests read their store location,
# source buckets and memory settings from these, so the image needs them even
# though it does not install the rest of the package.
for f in config.py memory.py; do
  cp "$REPO_ROOT/planetary_datasets/$f" "$CTX/planetary_datasets/$f"
done
# The modules the ingests import: the shared core, the four GOES satellite
# definitions, and the CLI.
for f in goes_radf_common.py goes_16_radf.py goes_17_radf.py goes_18_radf.py \
         goes_19_radf.py ingest_goes_radf.py; do
  cp "$REPO_ROOT/planetary_datasets/providers/virtualized/$f" \
     "$CTX/planetary_datasets/providers/virtualized/$f"
done
# The other missions this image can also run. Optional: the GOES tiers work
# without them, and `set -e` would otherwise abort the whole build on a
# checkout that does not have them yet.
for f in gk2a_ami_fd.py ingest_gk2a_fd.py \
         himawari_isatss.py ingest_himawari_isatss.py; do
  src="$REPO_ROOT/planetary_datasets/providers/virtualized/$f"
  if [ -f "$src" ]; then
    cp "$src" "$CTX/planetary_datasets/providers/virtualized/$f"
  else
    echo "    note: $f not present, image will run GOES only"
  fi
done
cp "$HERE/environment.yml" "$HERE/Dockerfile" "$HERE/run_goes_virtual.sh" \
   "$HERE/memory_watchdog.py" "$CTX/"
echo "    context size: $(du -sh "$CTX" | cut -f1)"

echo "==> resolving account"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --region "$REGION")
echo "    account $ACCOUNT_ID, regions: $REGIONS"

# Build once against the first region's URI, then retag per region. The layers
# are identical, so every region ends up with the same digest.
PRIMARY_REGION=${REGIONS%% *}
BUILD_IMAGE="${ACCOUNT_ID}.dkr.ecr.${PRIMARY_REGION}.amazonaws.com/${IMAGE_NAME}"

echo "==> building $PLATFORM"
docker buildx build \
  --platform "$PLATFORM" \
  --tag "${BUILD_IMAGE}:${TAG}" \
  --load \
  "$CTX"

pushed=()
for r in $REGIONS; do
  registry="${ACCOUNT_ID}.dkr.ecr.${r}.amazonaws.com"
  image="${registry}/${IMAGE_NAME}"
  echo
  echo "==> $r"

  aws ecr describe-repositories --repository-names "$IMAGE_NAME" --region "$r" >/dev/null 2>&1 \
    || aws ecr create-repository \
         --repository-name "$IMAGE_NAME" \
         --region "$r" \
         --image-scanning-configuration scanOnPush=true \
         --query 'repository.repositoryUri' --output text

  # Keep retention consistent everywhere, including a freshly created repo.
  if [ -f "$LIFECYCLE_POLICY" ]; then
    aws ecr put-lifecycle-policy --repository-name "$IMAGE_NAME" --region "$r" \
      --lifecycle-policy-text "file://${LIFECYCLE_POLICY}" >/dev/null \
      || echo "    warning: could not apply lifecycle policy in $r"
  fi

  aws ecr get-login-password --region "$r" \
    | docker login --username AWS --password-stdin "$registry" 2>/dev/null

  docker tag "${BUILD_IMAGE}:${TAG}" "${image}:${TAG}"
  docker tag "${BUILD_IMAGE}:${TAG}" "${image}:latest"
  docker push "${image}:${TAG}"
  docker push "${image}:latest"
  pushed+=("${image}:${TAG}")
done

echo
echo "pushed:"
for p in "${pushed[@]}"; do echo "  $p"; done
echo
echo "digests (should match across regions):"
for r in $REGIONS; do
  d=$(aws ecr describe-images --repository-name "$IMAGE_NAME" --region "$r" \
        --image-ids "imageTag=${TAG}" --query 'imageDetails[0].imageDigest' --output text)
  printf '  %-12s %s\n' "$r" "$d"
done
