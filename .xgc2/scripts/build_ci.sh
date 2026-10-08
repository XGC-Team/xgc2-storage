#!/usr/bin/env bash
set -euo pipefail
arch="$(dpkg --print-architecture)"
root="$(pwd)"
out="${RUNNER_TEMP:?}/storage-${GITHUB_RUN_ID:?}-${arch}"
mkdir -p "$out"
for suite in focal jammy noble; do
  deb="$out/$suite/debs"
  mkdir -p "$deb"
  python3 scripts/build-package.py --architecture "$arch" --distribution "$suite" --output "$deb/xgc2-storage.deb"
  version="$(awk -F': *' '/^version:/ {print $2;exit}' .xgc2/product.yml)"
  python3 .xgc2/scripts/xgc2_artifact_manifest.py build --deb-dir "$deb" --output-dir "$out/$suite/manifests" --product xgc2-storage --product-version "$version" --distribution "$suite" --architecture "$arch" --source-sha "$GITHUB_SHA" --ci-run-id "$GITHUB_RUN_ID" --ci-workflow "$GITHUB_WORKFLOW" --ci-workflow-ref "$GITHUB_WORKFLOW_REF"
  dpkg-deb -x "$deb/xgc2-storage.deb" "$out/$suite/installed"
  "$out/$suite/installed/usr/bin/xgc2-storage" --help
  mkdir -p "$root/.ci/artifacts/$suite"
  cp -a "$deb" "$out/$suite/manifests" "$root/.ci/artifacts/$suite/"
done
