#!/usr/bin/env bash
set -euo pipefail

IMAGE="${OMNIKV_IMAGE:-omnikv:smoke}"
OUTPUT="${OMNIKV_SBOM_OUTPUT:-target/release/omnikv-sbom.spdx.json}"

if ! command -v syft >/dev/null 2>&1; then
  cat >&2 <<'MSG'
syft is required to generate an SBOM.

Install: https://github.com/anchore/syft
Example:
  syft packages "$OMNIKV_IMAGE" -o spdx-json > target/release/omnikv-sbom.spdx.json
MSG
  exit 2
fi

mkdir -p "$(dirname "$OUTPUT")"
syft packages "$IMAGE" -o spdx-json > "$OUTPUT"
echo "SBOM written to $OUTPUT"
