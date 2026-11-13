wit_bindgen::generate!({ world: "pump", path: "../wit", generate_all });
use spike::reent::{cb, prov};
use std::cell::Cell;
use std::rc::Rc;
struct P;
thread_local!(static SYNC: Cell<bool> = Cell::new(false));
impl exports::wasi::cli::run::Guest for P {
    async fn run() -> Result<(), ()> {
        let sync = std::env::args().any(|a| a == "sync");
        SYNC.with(|s| s.set(sync));
        let _daemon = wit_bindgen::spawn_local(async { prov::run().await });
        let stop = Rc::new(Cell::new(false));
        let s2 = stop.clone();
        let _loop = wit_bindgen::spawn_local(async move {
            while !s2.get() {
                let (id, depth) = prov::next_event().await;
                eprintln!("pump: event {id} depth={depth}");
                let _h = wit_bindgen::spawn_local(async move {
                    let r = if SYNC.with(|s| s.get()) {
                        cb::on_invoke_sync(depth)
                    } else {
                        cb::on_invoke(depth).await
                    };
                    prov::complete_event(id, r);
                });
                std::mem::forget(_h);
            }
        });
        let r = if sync {
            cb::on_invoke_sync(0)
        } else {
            cb::on_invoke(0).await
        };
        eprintln!("pump: top-level result = {r} (expect 103)");
        stop.set(true);
        std::process::exit(if r == 103 { 0 } else { 1 });
    }
}
export!(P);
