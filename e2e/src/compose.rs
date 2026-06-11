//! Compose an application component with the wasi-http provider using
//! wac-graph — the programmatic equivalent of `wac plug`.

use wac_graph::types::Package;
use wac_graph::{CompositionGraph, EncodeOptions};

/// Plug every import of `app` that `provider` exports, and re-export the
/// app's exports (e.g. `wasi:cli/run`). Returns the composed component.
pub fn plug(app_bytes: Vec<u8>, provider_bytes: Vec<u8>) -> anyhow::Result<Vec<u8>> {
    let mut graph = CompositionGraph::new();

    let provider = Package::from_bytes("provider", None, provider_bytes, graph.types_mut())?;
    let provider_id = graph.register_package(provider)?;
    let app = Package::from_bytes("app", None, app_bytes, graph.types_mut())?;
    let app_id = graph.register_package(app)?;

    let provider_instance = graph.instantiate(provider_id);
    let app_instance = graph.instantiate(app_id);

    // Satisfy each of the app's imports with the provider's matching export.
    let app_imports: Vec<String> = graph.types()[graph[app_id].ty()]
        .imports
        .keys()
        .cloned()
        .collect();
    let provider_exports: Vec<String> = graph.types()[graph[provider_id].ty()]
        .exports
        .keys()
        .cloned()
        .collect();
    for import in app_imports {
        if provider_exports.contains(&import) {
            let export = graph.alias_instance_export(provider_instance, &import)?;
            graph.set_instantiation_argument(app_instance, &import, export)?;
        }
    }

    // Re-export the app's exports.
    let app_exports: Vec<String> = graph.types()[graph[app_id].ty()]
        .exports
        .keys()
        .cloned()
        .collect();
    for export in app_exports {
        let aliased = graph.alias_instance_export(app_instance, &export)?;
        graph.export(aliased, &export)?;
    }

    Ok(graph.encode(EncodeOptions::default())?)
}
