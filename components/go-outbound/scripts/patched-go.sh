#!/usr/bin/env bash
# Prints the path of a Go binary with runtime.wasiOnIdle, which async
# components need (golang/go#76775): $GO if set, else `go` on PATH if it is
# patched, else the patched Go that `componentize-go install-go` downloads.
set -euo pipefail
has_hook() { grep -q wasiOnIdle "$("$1" env GOROOT)/src/runtime/lock_wasip1.go" 2>/dev/null; }
if [ -n "${GO:-}" ]; then
  has_hook "$GO" || { echo "patched-go: $GO has no runtime.wasiOnIdle" >&2; exit 1; }
  echo "$GO"; exit 0
fi
if command -v go >/dev/null && has_hook go; then command -v go; exit 0; fi
componentize-go install-go >/dev/null 2>&1
for cache in "${XDG_CACHE_HOME:-$HOME/.cache}" "$HOME/Library/Caches"; do
  for go in "$cache"/componentize-go/*/go-*-bootstrap/bin/go; do
    if [ -x "$go" ] && has_hook "$go"; then echo "$go"; exit 0; fi
  done
done
echo "patched-go: no Go with runtime.wasiOnIdle found" >&2
exit 1
