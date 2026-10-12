#!/usr/bin/env bash
# Builds dapr-wasm-components-go-outbound into the components workspace's
# target dir, next to the Rust providers:
#   components/target/wasm32-wasip2/release/dapr_wasm_components_go_outbound.wasm
#
# Needs Go >= 1.27.1, componentize-go and python3 on PATH. The Dapr
# components compiled in are listed in scripts/gen-components.py.
set -euo pipefail
cd "$(dirname "$0")/.."
out=../target/wasm32-wasip2/release/dapr_wasm_components_go_outbound.wasm
mkdir -p "$(dirname "$out")"
python3 overlay/gen-overlay.py target/overlay >/dev/null
GOFLAGS="-overlay=$PWD/target/overlay/overlay.json -trimpath" \
  componentize-go -d wit --world go-outbound build -o "$out"
echo "$out"
