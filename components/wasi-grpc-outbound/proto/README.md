# Vendored Dapr protos

`dapr/proto/**/*.proto` are copied verbatim from
[dapr/dapr](https://github.com/dapr/dapr) at tag **v1.18.0** (the daprd
version the E2E tests run against).

The tonic/prost client code generated from them is checked in under
`../src/proto/` (like the Dapr Rust SDK does), so building this crate
needs neither `protoc` nor a codegen build script.

To regenerate after bumping the protos, run from this directory:

```sh
cargo new --bin /tmp/protogen
cd /tmp/protogen
cargo add tonic-build@0.13.1 --features prost
cargo add protox@0.7
cat > src/main.rs <<'EOF'
fn main() {
    std::fs::create_dir_all("out").unwrap();
    let fds = protox::compile(["dapr/proto/runtime/v1/dapr.proto"], ["<this proto dir>"]).unwrap();
    tonic_build::configure()
        .build_server(false)
        .build_transport(false)
        .out_dir("out")
        .compile_fds(fds)
        .unwrap();
}
EOF
cargo run
cp out/dapr.proto.*.rs <repo>/components/wasi-grpc/src/proto/
