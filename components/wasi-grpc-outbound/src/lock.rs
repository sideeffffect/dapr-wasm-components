//! Distributed lock (alpha) over gRPC — `TryLockAlpha1`, `UnlockAlpha1`.

use crate::exports::lock::{Guest, LockError, UnlockStatus};
use crate::proto::runtime as pb;
use crate::proto::runtime::unlock_response;
use crate::sidecar::{DaprFailure, Sidecar};
use crate::Component;

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
        // The proto field is int32; a value that does not fit is a
        // programming error from the caller, not a recoverable failure.
        let expiry_in_seconds = i32::try_from(expiry_in_seconds)
            .unwrap_or_else(|_| panic!("expiry-in-seconds {expiry_in_seconds} exceeds i32"));
        let response = sidecar
            .unary(
                pb::TryLockRequest {
                    store_name,
                    resource_id,
                    lock_owner,
                    expiry_in_seconds,
                },
                |mut client, request| async move { client.try_lock_alpha1(request).await },
            )
            .map_err(lock_error)?;
        Ok(response.success)
    }

    fn unlock(
        store_name: String,
        resource_id: String,
        lock_owner: String,
    ) -> Result<UnlockStatus, LockError> {
        let sidecar = Sidecar::from_env();
        let response = sidecar
            .unary(
                pb::UnlockRequest {
                    store_name,
                    resource_id,
                    lock_owner,
                },
                |mut client, request| async move { client.unlock_alpha1(request).await },
            )
            .map_err(lock_error)?;
        Ok(match unlock_response::Status::try_from(response.status) {
            Ok(unlock_response::Status::Success) => UnlockStatus::Success,
            Ok(unlock_response::Status::LockDoesNotExist) => UnlockStatus::LockDoesNotExist,
            Ok(unlock_response::Status::LockBelongsToOthers) => UnlockStatus::LockBelongsToOthers,
            // The WIT enum has no internal-error case: an unknown/internal
            // status is an unrecoverable sidecar fault, so trap.
            _ => panic!("unexpected unlock status: {}", response.status),
        })
    }
}
