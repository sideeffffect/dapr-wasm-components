#!/usr/bin/env bash
# Builds dapr-wasm-components-go-outbound into the components workspace's
# target dir, next to the Rust providers:
#   components/target/wasm32-wasip2/release/dapr_wasm_components_go_outbound.wasm
#
# Needs Go >= 1.27.1, componentize-go and python3 on PATH. The Dapr
# components compiled in are listed in scripts/gen-components.py.
#
# go-outbound is an async component (WASI 0.3: `events.run`/`next-event`),
# which needs a Go with runtime.wasiOnIdle (golang/go#76775, not upstream
# yet). Set GO to such a Go, or let componentize-go download its patched one.
set -euo pipefail
cd "$(dirname "$0")/.."
GO=$(scripts/patched-go.sh)
export GO
out=../target/wasm32-wasip2/release/dapr_wasm_components_go_outbound.wasm
mkdir -p "$(dirname "$out")"
python3 overlay/gen-overlay.py ../target/go-overlay >/dev/null
GOFLAGS="-overlay=$PWD/../target/go-overlay/overlay.json -trimpath" \
  componentize-go -d wit --world go-outbound build --go "$GO" -o "$out"
echo "$out"
