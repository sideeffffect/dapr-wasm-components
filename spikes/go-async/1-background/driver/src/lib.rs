wit_bindgen::generate!({ world: "driver", path: "../wit", generate_all });

use spike::bg::bg;
use wasi::clocks::monotonic_clock as mc;

struct D;
fn now_ms() -> u64 {
    mc::now() / 1_000_000
}

impl exports::wasi::cli::run::Guest for D {
    async fn run() -> Result<(), ()> {
        let _bg = wit_bindgen::spawn_local(async { bg::run().await });
        let c0 = bg::count();
        eprintln!("[{}] start count={c0}", now_ms());
        mc::wait_for(300_000_000).await;
        let c1 = bg::count();
        eprintln!("[{}] after p3 wait_for 300ms: +{}", now_ms(), c1 - c0);
        std::thread::sleep(std::time::Duration::from_millis(300));
        let c2 = bg::count();
        eprintln!("[{}] after p2 sleep 300ms: +{}", now_ms(), c2 - c1);
        let t = now_ms();
        let mut x = 0u64;
        while now_ms() - t < 300 {
            x = x.wrapping_add(1);
        }
        let c3 = bg::count();
        eprintln!("[{}] after busy 300ms: +{} ({x})", now_ms(), c3 - c2);
        mc::wait_for(300_000_000).await;
        eprintln!(
            "[{}] after p3 wait_for 300ms: +{}",
            now_ms(),
            bg::count() - c3
        );
        Ok(())
    }
}
export!(D);
