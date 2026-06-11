# wasm-pkg-tools (wkg)

> Sources: Bytecode Alliance, Unknown
> Raw: [wasm-pkg-tools-readme](../../raw/wasm-tooling/wasm-pkg-tools-readme.md)

## Overview

`wasm-pkg-tools` is the Bytecode Alliance project for fetching and publishing Wasm components (including WIT packages encoded as components) to OCI or Warg registries. The `wkg` CLI exposes everything: building WIT packages, fetching dependencies, and publishing. Install via `cargo install wkg` or `cargo binstall wkg`, or grab a release binary.

## Configuration

Config lives at `$XDG_CONFIG_HOME/wasm-pkg/config.toml` (Linux: `~/.config/wasm-pkg/config.toml`), overridable with `--config`. Key pieces:

```toml
default_registry = "..."

[namespace_registries]
# maps the "dapr" of "dapr:client" to a registry; value can be inline metadata:
dapr = { registry = "dapr-wasm", metadata = { preferredProtocol = "oci", oci = { registry = "ghcr.io", namespacePrefix = "sideeffffect/" } } }

[registry."dapr-wasm".oci]
auth = { username = "...", password = "..." }  # falls back to docker config.json if unset
```

- A namespace maps to a registry; the registry's OCI config has `registry` (e.g. `ghcr.io`) and `namespacePrefix`.
- Public registries can instead serve `/.well-known/wasm-pkg/registry.json`.
- Without config, fallback namespace mappings exist for `wasi` → wasi.dev and `ba` → bytecodealliance.org (both backed by ghcr.io OCI).

## OCI naming convention

Package `ns:pkg@x.y.z` is stored at `<registry>/<namespacePrefix><ns>/<pkg>:x.y.z`, e.g. `wasi:http@0.2.1` → `ghcr.io/webassembly/wasi/http:0.2.1`. The tag **must** be valid semver. So for this project, `dapr:client@0.1.0` with prefix `sideeffffect/` → `ghcr.io/sideeffffect/dapr/client:0.1.0`.

## wkg.toml and wkg.lock

- `wkg.lock` — auto-generated lock of fetched WIT dependencies (name, registry, version, sha256 digest).
- `wkg.toml` — optional; `[overrides]` to point a dependency to a local path, `[metadata]` (authors, description, license, homepage, repository) which `wkg wit build` embeds into the WIT package and `wkg publish` maps to OCI annotations (`org.opencontainers.image.*`).

## Typical commands

- `wkg wit build --wit-dir wit` — build a `wit/` directory into a WIT package `.wasm` named `ns:pkg@x.y.z.wasm` (embedding wkg.toml metadata).
- `wkg wit fetch` — fetch dependencies declared in `wit/` into `wit/deps/`, writing `wkg.lock`.
- `wkg publish ns:pkg@x.y.z.wasm` — publish a WIT package; `wkg publish component.wasm --package ns:pkg@x.y.z` for a component binary. Registry resolved through namespace config.
- `wkg oci push ghcr.io/user/name:0.1.0 file.wasm` / `wkg oci pull` — raw OCI operations with explicit references, no config needed.
- `wkg get --format wit ns:pkg@x.y.z --output pkg.wit` — fetch a published package as WIT text.
- `wkg config --default-registry ...` / `wkg config --edit` — manage config.

## Authentication (ghcr.io)

wkg falls back to `~/.docker/config.json` credentials when config.toml has no `auth`. Locally: `docker login ghcr.io -u USER -p <PAT with write:packages>`. In GitHub Actions: `permissions: { packages: write, contents: read }` + `docker/login-action` with `username: ${{ github.actor }}`, `password: ${{ secrets.GITHUB_TOKEN }}`. Packages published this way are private by default; the `org.opencontainers.image.source` annotation (from wkg.toml `repository`) links them to the repo.

## See Also

- [WIT Format](../wasm-component-model/wit-format.md)
