// Distributed lock (alpha), served by the embedded runtime's API —
// TryLockAlpha1, UnlockAlpha1. Mirrors wasi-grpc-outbound's lock.rs.
package export_dapr_wasm_components_interfaces_lock

import (
	"context"
	"fmt"
	"math"

	daprgrpc "github.com/dapr/dapr/pkg/api/grpc"
	runtimev1pb "github.com/dapr/dapr/pkg/proto/runtime/v1"
	witTypes "go.bytecodealliance.org/pkg/wit/types"

	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/call"
	lk "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_lock"
)

func lockError(f *call.Failure) lk.LockError {
	if f.IsPermission() {
		return lk.MakeLockErrorPermissionDenied(f.Message)
	}
	return lk.MakeLockErrorStoreNotFound(f.Message)
}

func TryLock(storeName string, resourceID string, lockOwner string, expiryInSeconds uint32) witTypes.Result[witTypes.Unit, lk.TryLockError] {
	// The proto field is int32; a value that does not fit is a programming
	// error of the caller, not a recoverable failure.
	if expiryInSeconds > math.MaxInt32 {
		panic(fmt.Sprintf("expiry-in-seconds %d exceeds i32", expiryInSeconds))
	}
	r, f := call.Do(func(ctx context.Context, api daprgrpc.API) (*runtimev1pb.TryLockResponse, error) {
		return api.TryLockAlpha1(ctx, &runtimev1pb.TryLockRequest{
			StoreName: storeName, ResourceId: resourceID, LockOwner: lockOwner, ExpiryInSeconds: int32(expiryInSeconds),
		})
	})
	if f != nil {
		return witTypes.Err[witTypes.Unit](lk.MakeTryLockErrorLock(lockError(f)))
	}
	if !r.GetSuccess() {
		return witTypes.Err[witTypes.Unit](lk.MakeTryLockErrorNotAcquired())
	}
	return witTypes.Ok[witTypes.Unit, lk.TryLockError](witTypes.Unit{})
}

func Unlock(storeName string, resourceID string, lockOwner string) witTypes.Result[witTypes.Unit, lk.UnlockError] {
	r, f := call.Do(func(ctx context.Context, api daprgrpc.API) (*runtimev1pb.UnlockResponse, error) {
		return api.UnlockAlpha1(ctx, &runtimev1pb.UnlockRequest{StoreName: storeName, ResourceId: resourceID, LockOwner: lockOwner})
	})
	if f != nil {
		return witTypes.Err[witTypes.Unit](lk.MakeUnlockErrorLock(lockError(f)))
	}
	switch r.GetStatus() {
	case runtimev1pb.UnlockResponse_SUCCESS:
		return witTypes.Ok[witTypes.Unit, lk.UnlockError](witTypes.Unit{})
	case runtimev1pb.UnlockResponse_LOCK_DOES_NOT_EXIST:
		return witTypes.Err[witTypes.Unit](lk.MakeUnlockErrorLockDoesNotExist())
	case runtimev1pb.UnlockResponse_LOCK_BELONGS_TO_OTHERS:
		return witTypes.Err[witTypes.Unit](lk.MakeUnlockErrorLockBelongsToOthers())
	}
	// INTERNAL_ERROR or an unknown status is an unrecoverable runtime fault.
	panic(fmt.Sprintf("unexpected unlock status: %v", r.GetStatus()))
}
