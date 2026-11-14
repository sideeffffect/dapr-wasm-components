# proposal: all: add wasip3/wasm port (golang/go#77141)

> Source: https://github.com/golang/go/issues/77141
> Collected: 2026-11-13
> Published: 2026-01-09

Status on 2026-11-13: open, labels Proposal + ToolProposal, milestone Proposal (not accepted). Last comment 2026-06-30. Step 0 PR golang/go#76775 (`runtime.wasiOnIdle`) is still open, so componentize-go ships a patched Go.

## Proposal (summary of the issue text)

- Problem: componentize-go builds with `GOOS=wasip1` and the P1→P2 adapter. The adapter cannot talk to the Go scheduler, so a std call that ends in a P1 import such as `poll_oneoff` blocks all goroutines. Until wasip3, the only way out is P3 bindings that bypass std.
- Step 0: temporary `runtime.wasiOnIdle` so wit-bindgen-go can bridge CM concurrency with the Go scheduler (or use a temporary fork of the runtime).
- Step 1: `GOOS=wasip3`; default buildmode targets `wasi:cli/command`. I/O only blocks the calling goroutine. Full TCP/UDP. Built on the current CPS transform (unwind the Go stack, yield to the host). Custom handling of `main` for the CM async export ABI.
- Future: `net/http` client over `wasi:http`; `net/http` server subset over `wasi:http/service` (different compile mode); native stack switching / CM cooperative threads instead of the CPS transform ("dramatically improve performance"); WASI 1.0 as `GOOS=wasi`.

## Discussion highlights

- 2026-02-18: jellevandenhooff prototype (`GOARCH=wasm32`, `GOOS=wasip3`, LLM-written): scheduler integration and linker work, no deep std integration.
- Open design question: how CM (sub)tasks map to goroutines (who owns goroutines spawned by an export call that already returned). dicej: the scheduler sees one "soup" of goroutines; a task only tracks the channels its goroutines block on.
- johanbrandhorst (Go team) cautions that Go is generally not the place for experiments.
- 2026-06: WASI 0.3 released; a commenter notes the Component Model proposal is still Phase 1 in the W3C process, which some use as an argument to wait.
