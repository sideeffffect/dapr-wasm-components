#!/usr/bin/env bash
# compose.sh — turn the three-dependency `wac compose` invocation into a
# one-liner. Composes your app with a dapr-wasm-components outbound + inbound
# provider into one runnable server component (`outbound → app → inbound`).
#
# The two directions are independent, so the transports are chosen separately
# (gRPC out + HTTP in is valid). Providers are resolved automatically: a local
# release build under components/target/ is used when present, otherwise the
# published modules are pulled from the OCI registry with `wkg`.
#
#   ./compose.sh my_app.wasm                       # http out + http in -> composed.wasm
#   ./compose.sh my_app.wasm --out grpc --in http  # mixed transports
#   ./compose.sh my_app.wasm -o server.wasm --tag 0.4.0
#   ./compose.sh my_app.wasm --outbound out.wasm --inbound in.wasm   # explicit paths
#
# Run the result next to a Dapr sidecar:
#   dapr run --app-id my-app -- wasmtime serve -S cli composed.wasm
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
  [[ "$t" == "http" || "$t" == "grpc" ]] || die "transport must be 'http' or 'grpc', got '$t'"
done
command -v wac >/dev/null || die "wac not found on PATH (see https://github.com/bytecodealliance/wac)"

tmp=""
cleanup() { [[ -n "$tmp" ]] && rm -rf "$tmp"; }
trap cleanup EXIT

# Resolve a provider wasm: explicit path wins, then a local release build,
# then an OCI pull of the published module.
resolve() {
  local direction="$1" transport="$2" explicit="$3"
  if [[ -n "$explicit" ]]; then
    [[ -f "$explicit" ]] || die "provider not found: $explicit"
    echo "$explicit"; return
  fi
  local file="dapr_wasm_components_wasi_${transport}_${direction}.wasm"
  if [[ -f "$local_dir/$file" ]]; then
    echo "$local_dir/$file"; return
  fi
  command -v wkg >/dev/null || die "wkg not found and no local $direction build at $local_dir/$file (see https://github.com/bytecodealliance/wasm-pkg-tools)"
  [[ -n "$tmp" ]] || tmp="$(mktemp -d)"
  local ref="$org/dapr-wasm-components-wasi-${transport}-${direction}:${tag}"
  echo "compose.sh: pulling $ref" >&2
  wkg oci pull "$ref" -o "$tmp/$file" >&2
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
