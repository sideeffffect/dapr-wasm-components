//! Distributed lock (alpha) — https://docs.dapr.io/reference/api/distributed_lock_api/

use serde::Deserialize;
use serde_json::json;
use wstd::http::Method;

use crate::exports::lock::{Guest, LockError, UnlockStatus};
use crate::sidecar::{seg, DaprFailure, Sidecar};
use crate::Component;

/// Map a recoverable failure to the lock setup/config error.
fn lock_error(f: DaprFailure) -> LockError {
    if f.is_permission() {
        LockError::PermissionDenied(f.message)
    } else {
        LockError::StoreNotFound(f.message)
    }
}

impl Guest for Component {
    fn try_lock(
        store_name: String,
        resource_id: String,
        lock_owner: String,
        expiry_in_seconds: u32,
    ) -> Result<bool, LockError> {
        let sidecar = Sidecar::from_env();
        let path = format!("/v1.0-alpha1/lock/{}", seg(&store_name));
        let body = json!({
            "resourceId": resource_id,
            "lockOwner": lock_owner,
            "expiryInSeconds": expiry_in_seconds,
        });
        let response = sidecar
            .json(Method::POST, &path, &body)
            .map_err(lock_error)?;

        #[derive(Deserialize)]
        struct LockJson {
            #[serde(default)]
            success: bool,
        }
        let parsed: LockJson = serde_json::from_slice(&response.body)
            .unwrap_or_else(|e| panic!("unexpected lock response: {e}"));
        Ok(parsed.success)
    }

    fn unlock(
        store_name: String,
        resource_id: String,
        lock_owner: String,
    ) -> Result<UnlockStatus, LockError> {
        let sidecar = Sidecar::from_env();
        let path = format!("/v1.0-alpha1/unlock/{}", seg(&store_name));
        let body = json!({
            "resourceId": resource_id,
            "lockOwner": lock_owner,
        });
        let response = sidecar
            .json(Method::POST, &path, &body)
            .map_err(lock_error)?;

        #[derive(Deserialize)]
        struct UnlockJson {
            #[serde(default)]
            status: u8,
        }
        let parsed: UnlockJson = serde_json::from_slice(&response.body)
            .unwrap_or_else(|e| panic!("unexpected unlock response: {e}"));
        Ok(match parsed.status {
            0 => UnlockStatus::Success,
            1 => UnlockStatus::LockDoesNotExist,
            2 => UnlockStatus::LockBelongsToOthers,
            _ => panic!("unexpected unlock status: {}", parsed.status),
        })
    }
}
