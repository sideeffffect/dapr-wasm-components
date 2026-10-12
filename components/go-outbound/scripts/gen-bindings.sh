#!/usr/bin/env bash
# Regenerates the checked-in Go bindings in internal/wit from wit/.
# Only generated files (wit_bindings.go, empty.s, wit_exports/) are replaced;
# the hand-written export implementations (internal/wit/export_*/export.go)
# are kept.
set -euo pipefail
cd "$(dirname "$0")/.."
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
componentize-go -d wit --world go-outbound bindings \
  --pkg-name github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit \
  --format -o "$tmp" >/dev/null
mkdir -p internal/wit
find internal/wit \( -name wit_bindings.go -o -name empty.s -o -name wit_exports.go \) -delete
(cd "$tmp" && find . -type f) | while read -r f; do
  mkdir -p "internal/wit/$(dirname "$f")"
  cp "$tmp/$f" "internal/wit/$f"
done
find internal/wit -type d -empty -delete
python3 scripts/fix-bindings.py
