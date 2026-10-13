// dapr-wasm-components-go-outbound: the Dapr runtime itself
// (github.com/dapr/dapr), embedded in a WebAssembly component. It exports
// every building block of dapr-wasm-components:interfaces plus the private
// go-events queue that dapr-wasm-components-go-inbound pumps. Build with
// scripts/build.sh (it needs the net and dapr overlays).
package main

import (
	_ "golang.org/x/crypto/x509roots/fallback" // CA roots: wasm has no system trust store

	_ "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/wit_exports"
	_ "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/wasisock"
)

func main() {}
