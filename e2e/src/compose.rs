//! Compose an application (`app` world) with the split provider components
//! using wac-graph — the programmatic equivalent of `wac plug`.
//!
//! The graph is acyclic by construction: `outbound → app → inbound`.
//!   - the **outbound** provider (`dapr-outbound`) has no app-facing imports,
//!     so it instantiates first and satisfies the app's building-block imports;
//!   - the **app** exports the callbacks;
//!   - the **inbound** provider (`dapr-inbound`) instantiates last, taking the
//!     callbacks from the app and its type-only building-block imports
//!     (`state`/`invocation`/`types`, pulled in by the callback interfaces)
//!     from the outbound provider.
//!
//! A single bidirectional provider would form an `app ↔ provider` cycle, which
//! the component model forbids; splitting into two components avoids it.

use wac_graph::types::Package;
use wac_graph::{CompositionGraph, EncodeOptions, PackageId};

fn imports_of(graph: &CompositionGraph, id: PackageId) -> Vec<String> {
    graph.types()[graph[id].ty()]
        .imports
        .keys()
        .cloned()
        .collect()
}

fn exports_of(graph: &CompositionGraph, id: PackageId) -> Vec<String> {
    graph.types()[graph[id].ty()]
        .exports
        .keys()
        .cloned()
        .collect()
}

/// Plug an outbound-only app (a `wasi:cli` command) with the outbound provider
/// and re-export the app's exports (e.g. `wasi:cli/run`). Used for command
/// apps that only call Dapr and never receive deliveries.
pub fn plug(app_bytes: Vec<u8>, outbound_bytes: Vec<u8>) -> anyhow::Result<Vec<u8>> {
    let mut graph = CompositionGraph::new();

    let outbound_pkg = Package::from_bytes("outbound", None, outbound_bytes, graph.types_mut())?;
    let outbound_id = graph.register_package(outbound_pkg)?;
    let app_pkg = Package::from_bytes("app", None, app_bytes, graph.types_mut())?;
    let app_id = graph.register_package(app_pkg)?;

    let outbound = graph.instantiate(outbound_id);
    let app = graph.instantiate(app_id);

    let outbound_exports = exports_of(&graph, outbound_id);
    for import in imports_of(&graph, app_id) {
        if outbound_exports.contains(&import) {
            let export = graph.alias_instance_export(outbound, &import)?;
            graph.set_instantiation_argument(app, &import, export)?;
        }
    }

    // Re-export only the command entry point (`wasi:cli/run`). The app's
    // callback exports reference building-block types and are meaningless for a
    // command run; re-exporting them produces an invalid component.
    for export in exports_of(&graph, app_id) {
        if export.starts_with("wasi:cli/") {
            let aliased = graph.alias_instance_export(app, &export)?;
            graph.export(aliased, &export)?;
        }
    }

    Ok(graph.encode(EncodeOptions::default())?)
}

/// Plug an app with both provider directions, producing a server component
/// that exports `wasi:http/incoming-handler` (from the inbound provider). Used
/// for reactor apps that receive deliveries from the sidecar.
pub fn plug_full(
    app_bytes: Vec<u8>,
    outbound_bytes: Vec<u8>,
    inbound_bytes: Vec<u8>,
) -> anyhow::Result<Vec<u8>> {
    let mut graph = CompositionGraph::new();

    let outbound_pkg = Package::from_bytes("outbound", None, outbound_bytes, graph.types_mut())?;
    let outbound_id = graph.register_package(outbound_pkg)?;
    let app_pkg = Package::from_bytes("app", None, app_bytes, graph.types_mut())?;
    let app_id = graph.register_package(app_pkg)?;
    let inbound_pkg = Package::from_bytes("inbound", None, inbound_bytes, graph.types_mut())?;
    let inbound_id = graph.register_package(inbound_pkg)?;

    let outbound = graph.instantiate(outbound_id);
    let app = graph.instantiate(app_id);
    let inbound = graph.instantiate(inbound_id);

    let outbound_exports = exports_of(&graph, outbound_id);
    let app_exports = exports_of(&graph, app_id);

    // app's building-block imports ← outbound's exports.
    for import in imports_of(&graph, app_id) {
        if outbound_exports.contains(&import) {
            let export = graph.alias_instance_export(outbound, &import)?;
            graph.set_instantiation_argument(app, &import, export)?;
        }
    }

    // inbound's imports ← the app's callback exports, with the type-only
    // building-block imports satisfied from the outbound provider.
    for import in imports_of(&graph, inbound_id) {
        if app_exports.contains(&import) {
            let export = graph.alias_instance_export(app, &import)?;
            graph.set_instantiation_argument(inbound, &import, export)?;
        } else if outbound_exports.contains(&import) {
            let export = graph.alias_instance_export(outbound, &import)?;
            graph.set_instantiation_argument(inbound, &import, export)?;
        }
    }

    // The composed server exports the inbound provider's app-channel entry
    // point, plus any command entry point the app itself exports.
    for export in exports_of(&graph, inbound_id) {
        let aliased = graph.alias_instance_export(inbound, &export)?;
        graph.export(aliased, &export)?;
    }
    for export in app_exports {
        if export.starts_with("wasi:cli/") {
            let aliased = graph.alias_instance_export(app, &export)?;
            graph.export(aliased, &export)?;
        }
    }

    Ok(graph.encode(EncodeOptions::default())?)
}
