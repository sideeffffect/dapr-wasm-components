// Jobs / distributed scheduler writes, served by the embedded runtime's API
// — ScheduleJob, DeleteJob (the stable RPCs). Job data is JSON in the WIT
// contract but google.protobuf.Any in the proto — packed as
// google.protobuf.Value via anyjson. The reads live in jobs-read. Mirrors
// wasi-grpc-outbound's jobs.rs (the jobs half).
package export_dapr_wasm_components_interfaces_jobs

import (
	"context"
	"fmt"
	"strings"
	"time"

	daprgrpc "github.com/dapr/dapr/pkg/api/grpc"
	commonv1pb "github.com/dapr/dapr/pkg/proto/common/v1"
	runtimev1pb "github.com/dapr/dapr/pkg/proto/runtime/v1"
	witTypes "go.bytecodealliance.org/pkg/wit/types"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/anyjson"
	jbblk "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/blocks/jobs"
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/call"
	jb "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_jobs"
	jr "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_jobs_read"
)

func scheduleError(f *call.Failure) jb.ScheduleError {
	if f.Status == 409 || strings.Contains(f.ErrorCode, "ALREADY_EXISTS") {
		return jb.MakeScheduleErrorAlreadyExists(f.Message)
	}
	if f.Status == 400 {
		return jb.MakeScheduleErrorInvalidSchedule(f.Message)
	}
	return jb.MakeScheduleErrorJobs(jbblk.Error(f))
}

// intervalPb parses the WIT retry interval, a Go duration string ("10s").
// Unlike the Rust provider (humantime), this is exactly Go's grammar. A
// duration the app cannot express is a programming error, so it panics.
func intervalPb(interval string) *durationpb.Duration {
	d, err := time.ParseDuration(interval)
	if err != nil {
		panic(fmt.Sprintf("invalid failure-policy interval %q: %v", interval, err))
	}
	return durationpb.New(d)
}

func failurePolicyPb(p jr.JobFailurePolicy) *commonv1pb.JobFailurePolicy {
	if p.Tag() == jr.JobFailurePolicyTagConstant {
		c := p.Constant()
		constant := &commonv1pb.JobFailurePolicyConstant{MaxRetries: call.OptPtr(c.MaxRetries)}
		if c.Interval.IsSome() {
			constant.Interval = intervalPb(c.Interval.Some())
		}
		return &commonv1pb.JobFailurePolicy{Policy: &commonv1pb.JobFailurePolicy_Constant{Constant: constant}}
	}
	return &commonv1pb.JobFailurePolicy{Policy: &commonv1pb.JobFailurePolicy_Drop{Drop: &commonv1pb.JobFailurePolicyDrop{}}}
}

func jobPb(name string, job jr.Job) *runtimev1pb.Job {
	pb := &runtimev1pb.Job{
		Name:     name,
		Schedule: call.OptPtr(job.Schedule),
		Repeats:  call.OptPtr(job.Repeats),
		DueTime:  call.OptPtr(job.DueTime),
		Ttl:      call.OptPtr(job.Ttl),
	}
	if job.Data.IsSome() {
		// Job data that is not valid JSON is a programming error, so packing
		// panics rather than surfacing a recoverable failure.
		data, err := anyjson.PackJSON(job.Data.Some())
		if err != nil {
			panic(err.Error())
		}
		pb.Data = data
	}
	if job.FailurePolicy.IsSome() {
		pb.FailurePolicy = failurePolicyPb(job.FailurePolicy.Some())
	}
	return pb
}

func Schedule(name string, job jr.Job, overwrite bool) witTypes.Result[witTypes.Unit, jb.ScheduleError] {
	req := &runtimev1pb.ScheduleJobRequest{Job: jobPb(name, job), Overwrite: overwrite}
	_, f := call.Do(func(ctx context.Context, api daprgrpc.API) (any, error) {
		return api.ScheduleJob(ctx, req)
	})
	if f != nil {
		return witTypes.Err[witTypes.Unit](scheduleError(f))
	}
	return witTypes.Ok[witTypes.Unit, jb.ScheduleError](witTypes.Unit{})
}

func Delete(name string) witTypes.Result[witTypes.Unit, jr.JobsError] {
	_, f := call.Do(func(ctx context.Context, api daprgrpc.API) (any, error) {
		return api.DeleteJob(ctx, &runtimev1pb.DeleteJobRequest{Name: name})
	})
	if f != nil {
		return witTypes.Err[witTypes.Unit](jbblk.Error(f))
	}
	return witTypes.Ok[witTypes.Unit, jr.JobsError](witTypes.Unit{})
}
