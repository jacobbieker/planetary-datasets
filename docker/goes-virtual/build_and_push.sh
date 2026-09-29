#!/bin/bash
# Build the GOES virtual-ingest image and push it to ECR and GHCR.
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
# It is also pushed to GHCR, which anyone can pull from without an AWS
# account. The credential is GHCR_TOKEN, GITHUB_TOKEN, or `gh auth token`, and
# needs write:packages — a shortfall there is reported but does not fail the
# ECR push, since that is what the running fleet pulls.
#
# Registry credentials come from the environment (or the ambient AWS/gh
# config); none are written into the image.
#
#   AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... ./build_and_push.sh
#   REGIONS=us-east-1 ./build_and_push.sh          # just the one
#   PUSH_GHCR=0 ./build_and_push.sh                # ECR only
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
GHCR_OWNER=${GHCR_OWNER:-jacobbieker}
GHCR_IMAGE=${GHCR_IMAGE:-ghcr.io/${GHCR_OWNER}/${IMAGE_NAME}}

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
# planetary_datasets.common, whole. config.py imports common.paths, and
# common/__init__.py eagerly re-exports its dataset/download/store modules, so
# importing any one of them pulls in the package. It is ~200KB — all the bulk
# under planetary_datasets/ lives in providers/, which is why that one is
# copied file by file and this one is not.
cp -R "$REPO_ROOT/planetary_datasets/common" "$CTX/planetary_datasets/common"
find "$CTX/planetary_datasets/common" -name __pycache__ -type d -exec rm -rf {} +
# The modules the ingests import: the shared core, the Icechunk repository
# helpers that it and both non-GOES CLIs import at module scope, the four GOES
# satellite definitions, and the CLI. A module missing from this list fails at
# *import* time inside the container rather than at build time, which is what
# the smoke check below exists to catch.
for f in goes_radf_common.py virtual_repo.py goes_16_radf.py goes_17_radf.py \
         goes_18_radf.py goes_19_radf.py ingest_goes_radf.py; do
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

# Import every CLI the entrypoint can dispatch to, before anything is pushed.
# The staged context is a hand-maintained subset of the package, so a new
# module-scope import in the source tree turns into an ImportError that only
# surfaces when a box pulls the tag and the ingest dies on startup. Importing
# here costs seconds and makes that a build failure instead.
echo "==> smoke check: importing the ingest CLIs, and the flags the orchestrator passes"
docker run --rm --entrypoint /usr/local/bin/_entrypoint.sh "${BUILD_IMAGE}:${TAG}" \
  python -c '
import importlib, pathlib, re, subprocess, sys

# The orchestrator and the CLIs are versioned together but refactored apart:
# a CLI that drops a flag the orchestrator still passes builds and imports
# cleanly, then exits on argparse the moment a box runs it. Check the actual
# call sites against the actual parsers.
LAUNCHERS = {
    "launch": "ingest_goes_radf",
    "launch_gk2a": "ingest_gk2a_fd",
    "launch_himawari": "ingest_himawari_isatss",
}
src = pathlib.Path("/usr/local/bin/run_goes_virtual").read_text()
failed = False
for fn, name in LAUNCHERS.items():
    mod = importlib.import_module(
        f"planetary_datasets.providers.virtualized.{name}"
    )
    print("    ok import", name)
    # Read the accepted flags from --help rather than a parser factory: only
    # two of the three expose one, and --help is what every CLI has.
    help_text = subprocess.run(
        [sys.executable, "-m", mod.__name__, "--help"],
        capture_output=True, text=True, check=True,
    ).stdout
    known = set(re.findall(r"(?<![\w-])--[a-z][a-z0-9-]*", help_text))
    # Just this launcher function body: anchoring on the command variable
    # instead would also match the pass-through `exec` near the top of the
    # file and run on past the function that follows.
    body = re.search(rf"^{fn}\(\) \{{[^\n]*\n(.*?)^\}}", src, re.S | re.M)
    if body is None:
        print(f"    FAIL could not find {fn}() in the orchestrator")
        failed = True
        continue
    # Comments in the body mention flags they do not pass.
    code = re.sub(r"(?m)^\s*#.*$", "", body.group(1))
    for flag in sorted(set(re.findall(r"(?<![\w-])--[a-z][a-z0-9-]*", code))):
        if flag not in known:
            print(f"    FAIL {name} does not accept {flag}")
            failed = True
    print("    ok flags", name)
sys.exit(1 if failed else 0)
'

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

# GHCR, as a registry that does not need an AWS account to pull from. The
# ingest boxes still pull from ECR in their own region; this is for everyone
# else. Skipped rather than fatal when there is no credential, so a push to
# ECR is never held up by a GitHub token.
if [ "${PUSH_GHCR:-1}" = "1" ]; then
  echo
  echo "==> ghcr"
  ghcr_token=${GHCR_TOKEN:-${GITHUB_TOKEN:-$(gh auth token 2>/dev/null || true)}}
  if [ -z "$ghcr_token" ]; then
    echo "    no GHCR_TOKEN/GITHUB_TOKEN and no gh login, skipping"
  else
    if printf '%s' "$ghcr_token" \
         | docker login ghcr.io --username "$GHCR_OWNER" --password-stdin >/dev/null 2>&1; then
      docker tag "${BUILD_IMAGE}:${TAG}" "${GHCR_IMAGE}:${TAG}"
      docker tag "${BUILD_IMAGE}:${TAG}" "${GHCR_IMAGE}:latest"
      # write:packages is a separate scope from repo, so a token that logs in
      # fine can still be refused here. Report it and keep the ECR push.
      if docker push "${GHCR_IMAGE}:${TAG}" && docker push "${GHCR_IMAGE}:latest"; then
        pushed+=("${GHCR_IMAGE}:${TAG}")
      else
        echo "    warning: GHCR push refused — the token likely lacks write:packages"
      fi
    else
      echo "    warning: could not log in to ghcr.io, skipping"
    fi
  fi
fi

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
