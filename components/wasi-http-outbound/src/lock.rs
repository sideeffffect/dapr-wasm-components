//! Distributed lock (alpha) — https://docs.dapr.io/reference/api/distributed_lock_api/

use serde::Deserialize;
use serde_json::json;
use wstd::http::Method;

use crate::exports::lock::{Guest, LockError, TryLockError, UnlockError};
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

/// Map a recoverable setup/config failure of `try-lock`. (The `not-acquired`
/// case is produced from the `success == false` branch.)
fn try_lock_error(f: DaprFailure) -> TryLockError {
    TryLockError::Lock(lock_error(f))
}

/// Map a recoverable setup/config failure of `unlock`. (The
/// `lock-does-not-exist`/`lock-belongs-to-others` cases come from the unlock
/// status code.)
fn unlock_error(f: DaprFailure) -> UnlockError {
    UnlockError::Lock(lock_error(f))
}

impl Guest for Component {
    fn try_lock(
        store_name: String,
        resource_id: String,
        lock_owner: String,
        expiry_in_seconds: u32,
    ) -> Result<(), TryLockError> {
        let sidecar = Sidecar::from_env();
        let path = format!("/v1.0-alpha1/lock/{}", seg(&store_name));
        let body = json!({
            "resourceId": resource_id,
            "lockOwner": lock_owner,
            "expiryInSeconds": expiry_in_seconds,
        });
        let response = sidecar
            .json(Method::POST, &path, &body)
            .map_err(try_lock_error)?;

        #[derive(Deserialize)]
        struct LockJson {
            #[serde(default)]
            success: bool,
        }
        let parsed: LockJson = serde_json::from_slice(&response.body)
            .unwrap_or_else(|e| panic!("unexpected lock response: {e}"));
        if parsed.success {
            Ok(())
        } else {
            Err(TryLockError::NotAcquired)
        }
    }

    fn unlock(
        store_name: String,
        resource_id: String,
        lock_owner: String,
    ) -> Result<(), UnlockError> {
        let sidecar = Sidecar::from_env();
        let path = format!("/v1.0-alpha1/unlock/{}", seg(&store_name));
        let body = json!({
            "resourceId": resource_id,
            "lockOwner": lock_owner,
        });
        let response = sidecar
            .json(Method::POST, &path, &body)
            .map_err(unlock_error)?;

        #[derive(Deserialize)]
        struct UnlockJson {
            #[serde(default)]
            status: u8,
        }
        let parsed: UnlockJson = serde_json::from_slice(&response.body)
            .unwrap_or_else(|e| panic!("unexpected unlock response: {e}"));
        match parsed.status {
            0 => Ok(()),
            1 => Err(UnlockError::LockDoesNotExist),
            2 => Err(UnlockError::LockBelongsToOthers),
            // INTERNAL_ERROR (3) / any unknown status is an unrecoverable
            // sidecar fault, so trap.
            _ => panic!("unexpected unlock status: {}", parsed.status),
        }
    }
}
