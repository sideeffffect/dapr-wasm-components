wit_bindgen::generate!({ world: "app3", path: "../wit", generate_all,
    sync: ["import:wasi:clocks/monotonic-clock@0.3.0#wait-for"] });
struct A;
impl exports::spike::bg::work::Guest for A {
    fn on_work() {
        // sync-lowered call to an async-typed (p3) import
        wasi::clocks::monotonic_clock::wait_for(300_000_000);
    }
}
export!(A);
