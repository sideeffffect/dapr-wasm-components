#!/usr/bin/env bash
# compose.sh — turn the three-dependency `wac compose` invocation into a
# one-liner. Composes your app with a dapr-wasm-components outbound + inbound
# provider into one runnable server component (`outbound → app → inbound`).
#
# For the sidecar transports the two directions are independent, so they are
# chosen separately (gRPC out + HTTP in is valid). The `go` family embeds Dapr
# itself and comes as a pair (`--out go` implies `--in go`). Providers are
# resolved automatically: a local release build under components/target/ is
# used when present, otherwise the published modules are pulled from the OCI
# registry with `wkg`.
#
#   ./compose.sh my_app.wasm                       # http out + http in -> composed.wasm
#   ./compose.sh my_app.wasm --out grpc --in http  # mixed transports
#   ./compose.sh my_app.wasm --out go              # Dapr embedded, no sidecar
#   ./compose.sh my_app.wasm -o server.wasm --tag 0.7.5
#   ./compose.sh my_app.wasm --outbound out.wasm --inbound in.wasm   # explicit paths
#
# Run the result next to a Dapr sidecar:
#   dapr run --app-id my-app -- wasmtime serve -S cli composed.wasm
# or, for `--out go`, on its own (the arguments are daprd's flags):
#   wasmtime run -S inherit-network -S tcp -S allow-ip-name-lookup \
#     --dir ./resources::/resources composed.wasm --app-id my-app --resources-path /resources
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

app=""
output="composed.wasm"
out_transport="http"
in_transport=""          # defaults to $out_transport once parsed
outbound_path=""
inbound_path=""
tag="latest"
org="${DAPR_WASM_COMPONENTS_ORG:-ghcr.io/sideeffffect}"
local_dir="${DAPR_WASM_COMPONENTS_DIR:-$here/components/target/wasm32-wasip2/release}"

die() { echo "compose.sh: $*" >&2; exit 1; }

usage() {
  # Print the header comment block (every leading-# line after the shebang),
  # stripping the comment marker; stop at the first non-comment line.
  awk 'NR==1{next} /^#/{sub(/^# ?/,""); print; next} {exit}' "${BASH_SOURCE[0]}"
  exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -o|--output)   output="$2"; shift 2 ;;
    --out)         out_transport="$2"; shift 2 ;;
    --in)          in_transport="$2"; shift 2 ;;
    --outbound)    outbound_path="$2"; shift 2 ;;
    --inbound)     inbound_path="$2"; shift 2 ;;
    --tag)         tag="$2"; shift 2 ;;
    --org)         org="$2"; shift 2 ;;
    --local)       local_dir="$2"; shift 2 ;;
    -h|--help)     usage 0 ;;
    -*)            die "unknown option: $1 (try --help)" ;;
    *)             [[ -z "$app" ]] || die "unexpected argument: $1"; app="$1"; shift ;;
  esac
done

[[ -n "$app" ]] || usage 1
[[ -f "$app" ]] || die "app component not found: $app"
in_transport="${in_transport:-$out_transport}"
for t in "$out_transport" "$in_transport"; do
  [[ "$t" == "http" || "$t" == "grpc" || "$t" == "go" ]] || die "transport must be 'http', 'grpc' or 'go', got '$t'"
done
if [[ "$out_transport" == "go" || "$in_transport" == "go" ]] && [[ "$out_transport" != "$in_transport" ]]; then
  die "the go family is a pair (one embedded Dapr runtime): use --out go --in go"
fi
command -v wac >/dev/null || die "wac not found on PATH (see https://github.com/bytecodealliance/wac)"

# Created here, in the main shell: `resolve` runs inside `$(...)` subshells,
# so a temp dir created (or a variable set) there would be invisible to this
# shell and its EXIT trap — the dirs would leak. `cleanup` must also end with
# status 0: when the script falls off its end, the EXIT trap's last status
# becomes the script's exit status.
tmp="$(mktemp -d)"
cleanup() { rm -rf "$tmp"; }
trap cleanup EXIT

# Resolve a provider wasm: explicit path wins, then a local release build,
# then an OCI pull of the published module.
resolve() {
  local direction="$1" transport="$2" explicit="$3"
  if [[ -n "$explicit" ]]; then
    [[ -f "$explicit" ]] || die "provider not found: $explicit"
    echo "$explicit"; return
  fi
  # go-* are named without the wasi- prefix: they are not a sidecar transport.
  local family="wasi_${transport}" package="wasi-${transport}"
  if [[ "$transport" == "go" ]]; then family="go"; package="go"; fi
  local file="dapr_wasm_components_${family}_${direction}.wasm"
  if [[ -f "$local_dir/$file" ]]; then
    echo "$local_dir/$file"; return
  fi
  command -v wkg >/dev/null || die "wkg not found and no local $direction build at $local_dir/$file (see https://github.com/bytecodealliance/wasm-pkg-tools)"
  # Every module lives under one OCI namespace since 0.7.4:
  # <org>/dapr-wasm-components/<module>. The old flat names
  # (<org>/dapr-wasm-components-<module>) were deleted from ghcr; all their
  # versions (0.6.0+; go-* from 0.7.1) were copied to the new names.
  local ref="$org/dapr-wasm-components/${package}-${direction}:${tag}"
  echo "compose.sh: pulling $ref" >&2
  # Explicit `|| die`: `set -e` is not inherited by the `$(...)` subshell this
  # runs in, so a failed pull would otherwise hand wac a missing file.
  wkg oci pull "$ref" -o "$tmp/$file" >&2 || die "could not pull $ref (does tag '$tag' exist?)"
  echo "$tmp/$file"
}

outbound="$(resolve outbound "$out_transport" "$outbound_path")"
inbound="$(resolve inbound "$in_transport" "$inbound_path")"

echo "compose.sh: $out_transport-outbound + app + $in_transport-inbound -> $output" >&2
wac compose \
  --dep dapr:app="$app" \
  --dep dapr:outbound="$outbound" \
  --dep dapr:inbound="$inbound" \
  "$here/compose.wac" -o "$output"
echo "compose.sh: wrote $output" >&2
