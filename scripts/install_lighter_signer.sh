#!/usr/bin/env bash
set -euo pipefail

# Installs the Lighter native signer shared library from lighter-go releases into libs/lighter/.
#
# Usage:
#   scripts/install_lighter_signer.sh                 # installs latest for current OS/arch
#   LIGHTER_SIGNER_VERSION=v1.0.1 scripts/install_lighter_signer.sh
#   LIGHTER_SIGNER_VERSION=v1.0.1 LIGHTER_SIGNER_OUT=libs/lighter/signer-arm64.so scripts/install_lighter_signer.sh
#
# Source: https://github.com/elliottech/lighter-go/releases

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/.." && pwd)"

OS="$(uname -s)"
ARCH="$(uname -m)"

VERSION="${LIGHTER_SIGNER_VERSION:-latest}"

out_default() {
  case "${OS}:${ARCH}" in
    Darwin:arm64) echo "${ROOT}/libs/lighter/signer-arm64.dylib" ;;
    Linux:aarch64) echo "${ROOT}/libs/lighter/signer-arm64.so" ;;
    Linux:x86_64) echo "${ROOT}/libs/lighter/signer-amd64.so" ;;
    *)
      echo "unsupported OS/arch: ${OS}/${ARCH}" >&2
      exit 2
      ;;
  esac
}

OUT="${LIGHTER_SIGNER_OUT:-$(out_default)}"

asset_name() {
  case "${OS}:${ARCH}" in
    Darwin:arm64) echo "lighter-signer-darwin-arm64.dylib" ;;
    Linux:aarch64) echo "lighter-signer-linux-arm64.so" ;;
    Linux:x86_64) echo "lighter-signer-linux-amd64.so" ;;
    *)
      echo "unsupported OS/arch: ${OS}/${ARCH}" >&2
      exit 2
      ;;
  esac
}

ASSET="$(asset_name)"

release_json() {
  if [[ "${VERSION}" == "latest" ]]; then
    curl -fsSL "https://api.github.com/repos/elliottech/lighter-go/releases/latest"
  else
    curl -fsSL "https://api.github.com/repos/elliottech/lighter-go/releases/tags/${VERSION}"
  fi
}

url_from_release() {
  python3 - "$ASSET" <<'PY'
import json
import sys

asset = sys.argv[1]
d = json.load(sys.stdin)
for a in d.get("assets", []):
    if a.get("name") == asset:
        print(a["browser_download_url"])
        raise SystemExit(0)
raise SystemExit(f"asset not found in release: {asset}")
PY
}

mkdir -p "$(dirname "${OUT}")"

URL="$(release_json | url_from_release)"
TMP="${OUT}.tmp"

echo "[lighter-signer] downloading ${URL}"
curl -fL --retry 3 --retry-delay 1 -o "${TMP}" "${URL}"
chmod +x "${TMP}" || true
mv -f "${TMP}" "${OUT}"

echo "[lighter-signer] installed -> ${OUT}"
if command -v file >/dev/null 2>&1; then
  file "${OUT}" || true
fi

