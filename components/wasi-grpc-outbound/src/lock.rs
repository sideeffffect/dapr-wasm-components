//! Distributed lock (alpha) over gRPC — `TryLockAlpha1`, `UnlockAlpha1`.

use crate::exports::lock::{Guest, UnlockStatus};
use crate::proto::runtime as pb;
use crate::proto::runtime::unlock_response;
use crate::sidecar::Sidecar;
use crate::types::Error;
use crate::Component;

impl Guest for Component {
    fn try_lock(
        store_name: String,
        resource_id: String,
        lock_owner: String,
        expiry_in_seconds: u32,
    ) -> Result<bool, Error> {
        let sidecar = Sidecar::from_env()?;
        // The proto field is int32; don't let a huge u32 wrap negative.
        let expiry_in_seconds = i32::try_from(expiry_in_seconds).map_err(|_| {
            Error::InvalidArgument(format!("expiry-in-seconds {expiry_in_seconds} exceeds i32"))
        })?;
        let response = sidecar.unary(
            pb::TryLockRequest {
                store_name,
                resource_id,
                lock_owner,
                expiry_in_seconds,
            },
            |mut client, request| async move { client.try_lock_alpha1(request).await },
        )?;
        Ok(response.success)
    }

    fn unlock(
        store_name: String,
        resource_id: String,
        lock_owner: String,
    ) -> Result<UnlockStatus, Error> {
        let sidecar = Sidecar::from_env()?;
        let response = sidecar.unary(
            pb::UnlockRequest {
                store_name,
                resource_id,
                lock_owner,
            },
            |mut client, request| async move { client.unlock_alpha1(request).await },
        )?;
        Ok(match unlock_response::Status::try_from(response.status) {
            Ok(unlock_response::Status::Success) => UnlockStatus::Success,
            Ok(unlock_response::Status::LockDoesNotExist) => UnlockStatus::LockDoesNotExist,
            Ok(unlock_response::Status::LockBelongsToOthers) => UnlockStatus::LockBelongsToOthers,
            // InternalError plus any enum value this client doesn't know.
            _ => UnlockStatus::InternalError,
        })
    }
}
