//! A component's import list is its authority manifest.
//!
//! The `app` world imports both halves of each split building block
//! (`state-read` + `state`, `secrets-read` + `secrets`, …), but `wit-component`
//! keeps only the functions an app actually calls. These tests read the
//! imports of the built fixture apps and check that the read/write split shows
//! up there: an app that only reads state imports no `state` function.

use std::collections::BTreeMap;

use wasmtime::component::types::ComponentItem;
use wasmtime::component::Component;

use dapr_wasm_components_e2e::{engine, microservice_path, state_reader_path};

const PACKAGE: &str = "dapr-wasm-components:interfaces/";

/// The `dapr-wasm-components:interfaces` imports of a component, as
/// interface name (without package and version) -> the functions imported
/// from it, sorted.
fn dapr_imports(path: std::path::PathBuf) -> BTreeMap<String, Vec<String>> {
    let engine = engine().unwrap();
    let component = Component::from_file(&engine, &path)
        .unwrap_or_else(|e| panic!("cannot load {}: {e}", path.display()));
    let mut imports = BTreeMap::new();
    for (name, item) in component.component_type().imports(&engine) {
        let Some(interface) = name.strip_prefix(PACKAGE) else {
            continue;
        };
        let interface = interface.split('@').next().unwrap().to_string();
        let ComponentItem::ComponentInstance(instance) = item.ty else {
            panic!("{name} is not an instance import");
        };
        let mut functions: Vec<String> = instance
            .exports(&engine)
            .filter(|(_, item)| matches!(item.ty, ComponentItem::ComponentFunc(_)))
            .map(|(name, _)| name.to_string())
            .collect();
        functions.sort();
        imports.insert(interface, functions);
    }
    imports
}

#[test]
fn read_only_app_imports_only_the_read_side() {
    let imports = dapr_imports(state_reader_path());
    assert_eq!(imports.get("state-read"), Some(&vec!["get".to_string()]));
    assert!(
        !imports.contains_key("state"),
        "a read-only app must not import `state`: {imports:?}"
    );
    // Type-only imports from the exported callbacks carry no functions.
    for (interface, functions) in &imports {
        if interface != "state-read" {
            assert!(
                functions.is_empty(),
                "unexpected functions imported from {interface}: {functions:?}"
            );
        }
    }
}

#[test]
fn microservice_imports_only_what_it_calls() {
    let imports = dapr_imports(microservice_path());
    assert_eq!(imports.get("state-read"), Some(&vec!["get".to_string()]));
    assert_eq!(
        imports.get("state"),
        Some(&vec!["delete".to_string(), "save".to_string()])
    );
    // It reads one named secret, so it has the narrow `secrets-read` and not
    // the bulk-enumerating `secrets`.
    assert_eq!(
        imports.get("secrets-read"),
        Some(&vec!["get-secret".to_string()])
    );
    for absent in [
        "secrets",
        "configuration",
        "jobs-read",
        "jobs",
        "workflow-read",
        "workflow",
    ] {
        assert!(
            !imports.contains_key(absent),
            "unexpected import {absent}: {imports:?}"
        );
    }
}
