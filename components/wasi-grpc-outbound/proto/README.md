# Vendored Dapr protos

`dapr/proto/**/*.proto` are copied verbatim from
[dapr/dapr](https://github.com/dapr/dapr) at tag **v1.18.0** (the E2E
tests run daprd 1.18.7 by default, see `DAPRD_IMAGE_TAG`).

The tonic/prost client code generated from them is checked in under
`../src/proto/` (like the Dapr Rust SDK does), so building this crate
needs neither `protoc` nor a codegen build script. `wasi-grpc-inbound`
reuses the same generated messages (via `#[path]`), so a regen affects both.

To regenerate after bumping the protos, run from this directory:

```sh
cargo new --bin /tmp/protogen
cd /tmp/protogen
cargo add tonic-prost-build@0.14
cargo add protox@0.9
cat > src/main.rs <<'EOF'
fn main() {
    std::fs::create_dir_all("out").unwrap();
    let fds = protox::compile(["dapr/proto/runtime/v1/dapr.proto"], ["<this proto dir>"]).unwrap();
    tonic_prost_build::configure()
        .build_server(false)
        .build_transport(false)
        .out_dir("out")
        .compile_fds(fds)
        .unwrap();
}
EOF
cargo run
cp out/dapr.proto.*.rs <repo>/components/wasi-grpc-outbound/src/proto/
