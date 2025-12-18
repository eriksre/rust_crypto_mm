# Lighter Native Signer

This repo uses Lighter's native signer shared library (compiled from `lighter-go`) to sign transactions.

**Supported assets (from `elliottech/lighter-go` releases):**
- `lighter-signer-linux-amd64.so` (Linux x86_64)
- `lighter-signer-linux-arm64.so` (Linux aarch64 / Graviton)
- `lighter-signer-darwin-arm64.dylib` (macOS arm64)

**How this repo expects them to be named:**
- Linux x86_64: `libs/lighter/signer-amd64.so`
- Linux aarch64: `libs/lighter/signer-arm64.so`
- macOS arm64: `libs/lighter/signer-arm64.dylib`

## Install

Use the helper script:
- `scripts/install_lighter_signer.sh`

Or manually download from:
- `https://github.com/elliottech/lighter-go/releases`

