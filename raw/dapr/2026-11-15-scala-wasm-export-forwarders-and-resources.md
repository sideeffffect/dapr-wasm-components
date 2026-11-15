# scala-wasm / wit-bindgen-scala: export forwarders and exported resources (PR notes)

- Source: own work in scala-wasm/wit-bindgen-scala#9, #10 and scala-wasm/scala-wasm#274
- Collected: 2026-11-15
- Published: 2026-11-15

## wit-bindgen-scala#9: `--export-impl-package` (stacked on #8)

- tanishiking dropped generated export traits on 2026-08-30 (wit-bindgen-scala#5); scala-wasm deprecated `@WitExportInterface`/`@WitImplementation` in 1.22.1-wasm.4. The WIT meaning stays on static `@WitExport` methods only.
- New opt-in option generates `@WitExport` forwarders that call a user object by a fixed name convention that includes the WIT package: `ns:pkg/iface` -> `<prefix>.ns.pkg.Iface`; inline `foo` of world `w` -> `<prefix>.ns.pkg.w.Foo`; world-level functions -> `<prefix>.ns.pkg.W`. WIT forbids an interface and a world (or two world items) with the same name, so there are no clashes. No traits.
- World-level forwarders must qualify world-imported types (`use iface.{t}` in a world): their alias lives in `<base>.root`, the forwarders in `<base>.exports.root`.
- CI pin: wit-bindgen-test compiles upstream Rust fixtures with a hard-coded `-Dwarnings`; Rust 1.99 denies `i64::min_value()` (fixture `numbers`), 1.98.0 accepts it. CI now pins 1.98.0.
- Fork PRs need maintainer approval before CI runs (`action_required`).

## scala-wasm#274: exported resources

- API: `@WitResourceExport(scope, name)` on an ordinary class; resource functions are `@WitExport(scope, "[constructor]r" | "[method]r.f" | "[static]r.f")`; `wit.Borrow[R]` gives `borrow<r>`.
- The CABI rep of an own-defined resource is an i32; a GC reference cannot be one. The linker keeps one rep->instance table (anyref array, free slots chained as i31ref): own out = alloc + `[resource-new]`; borrow in = rep directly (CABI passes the rep when the callee defines the resource); own in = `[resource-rep]` + `[resource-drop]` (drop of an own-defined resource calls the dtor directly, no re-entrance check); `[dtor]r` export frees the slot. No user destructor callback: the GC owns instances after the last handle.
- Core names follow wit-component: intrinsics imported from module `[export]<iface>`, dtor exported as `<iface>#[dtor]<r>`. Component type needed no change: the class carries the existing `WitTypeDef.Resource`.
- Existing bug fixed: an export that receives `borrow<r>` of an *imported* resource must drop the borrow handle before returning ("borrow handles still remain at the end of the call"). scala-wasm never did; now lifted borrow handles are recorded on a stack and dropped after the method returns.
- Known limit: a world that imports and exports the same interface with a resource (fixtures `resource-import-and-export`, `resource_with_lists`) collides, because scala-wasm keys resource typedefs by (scope, name).
- scalafmt `--list/--test` only checks git-tracked files: format new files before the first commit.

## wit-bindgen-scala#10 (draft, stacked on #9)

- Resource `r` of an exported interface = class `<impl object>.R` (constructor = WIT constructor, companion = statics); forwarders `r$constructor`, `r$method$f`, `r$static$f`.
- Harness: `rust-to-scala` now replaces any upstream Rust component with a same-named Scala overlay, so upstream Rust runners drive Scala resource exporters. 51/51 fixture runs pass against a local publish of #274. Draft until a scala-wasm snapshot with #274 is published (the pinned one lacks `@WitResourceExport`).
