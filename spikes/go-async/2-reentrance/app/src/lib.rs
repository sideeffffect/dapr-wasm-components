wit_bindgen::generate!({ world: "app", path: "../wit", generate_all });
use spike::reent::prov;
struct A;
impl exports::spike::reent::cb::Guest for A {
    async fn on_invoke(depth: u32) -> u32 {
        eprintln!("app: on-invoke depth={depth} enter");
        let r = if depth < 3 {
            prov::invoke(depth + 1).await + 1
        } else {
            100
        };
        eprintln!("app: on-invoke depth={depth} -> {r}");
        r
    }
    fn on_invoke_sync(depth: u32) -> u32 {
        eprintln!("app: on-invoke-sync depth={depth} enter");
        let r = if depth < 3 {
            prov::invoke_sync(depth + 1) + 1
        } else {
            100
        };
        eprintln!("app: on-invoke-sync depth={depth} -> {r}");
        r
    }
}
export!(A);
