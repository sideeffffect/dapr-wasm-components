#!/usr/bin/env bash
# Reproduces the go-async spikes (see README.md). Needs: componentize-go
# >= 0.5.3 (downloads a patched Go 1.27.1 with runtime.wasiOnIdle), cargo with
# the wasm32-wasip2 target, wac >= 0.10 and wasmtime >= 49.
set -euo pipefail
cd "$(dirname "$0")"
here=$PWD

build_go() { # <dir> <world> <out>
  # `bindings` rewrites go.mod; keep the checked-in one.
  local mod sum
  mod=$(cat "$1/go/go.mod"); sum=$(cat "$1/go/go.sum")
  (cd "$1/go" && componentize-go -d ../wit -w "$2" bindings >/dev/null)
  printf '%s\n' "$mod" >"$1/go/go.mod"; printf '%s\n' "$sum" >"$1/go/go.sum"
  (cd "$1/go" && componentize-go -d ../wit -w "$2" build -o "$here/$1/$3" 2>&1 |
    { grep -v '^Note\|^See\|^Using\|^Downloading\|^Extracting' || true; })
}

echo "== spike 1: background goroutines of an async Go component"
build_go 1-background go-bg go-bg.wasm
(cd 1-background && cargo build -q --release --target wasm32-wasip2)
r=1-background/target/wasm32-wasip2/release
wac plug "$r/driver.wasm" --plug 1-background/go-bg.wasm -o 1-background/spike1.wasm
wasmtime run 1-background/spike1.wasm

echo "== spike 3: a sync-typed callback that waits on a p3 import (expected trap)"
wac compose -d spike:gobg=1-background/go-bg.wasm -d spike:app3=$r/app3.wasm \
  -d spike:driver3=$r/driver3.wasm 1-background/c3.wac -o 1-background/spike3.wasm
wasmtime run 1-background/spike3.wasm 2>&1 | grep -m1 'wasm trap' || true

echo "== spike 2: app -> Go -> same app, async-typed functions (expect 103)"
build_go 2-reentrance provider provider.wasm
(cd 2-reentrance && cargo build -q --release --target wasm32-wasip2)
r=2-reentrance/target/wasm32-wasip2/release
wac compose -d spike:provider=2-reentrance/provider.wasm -d spike:app=$r/app.wasm \
  -d spike:pump=$r/pump.wasm 2-reentrance/compose.wac -o 2-reentrance/spike2.wasm
wasmtime run 2-reentrance/spike2.wasm

echo "== spike 2 with sync-typed functions (expected Go deadlock)"
wasmtime run 2-reentrance/spike2.wasm sync 2>&1 | grep -m1 'fatal error' || true
