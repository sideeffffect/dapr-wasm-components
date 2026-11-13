wit_bindgen::generate!({ world: "driver3", path: "../wit", generate_all });
use spike::bg::{bg, work};
struct D;
impl exports::wasi::cli::run::Guest for D {
    async fn run() -> Result<(), ()> {
        let _bg = wit_bindgen::spawn_local(async { bg::run().await });
        wasi::clocks::monotonic_clock::wait_for(50_000_000).await;
        let c0 = bg::count();
        work::on_work();
        let c1 = bg::count();
        eprintln!(
            "during sync-typed callback doing sync-lowered p3 wait 300ms: +{}",
            c1 - c0
        );
        Ok(())
    }
}
export!(D);
