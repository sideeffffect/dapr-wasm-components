// Jobs / distributed scheduler, served by the embedded runtime's API —
// ScheduleJob, GetJob, DeleteJob (the stable RPCs). Job data is JSON in the
// WIT contract but google.protobuf.Any in the proto — packed as
// google.protobuf.Value via anyjson. Mirrors wasi-grpc-outbound's jobs.rs.
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
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/call"
	jb "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_jobs"
)

func jobsError(f *call.Failure) jb.JobsError {
	return jb.MakeJobsErrorPermissionDenied(f.Message)
}

func scheduleError(f *call.Failure) jb.ScheduleError {
	if f.Status == 409 || strings.Contains(f.ErrorCode, "ALREADY_EXISTS") {
		return jb.MakeScheduleErrorAlreadyExists(f.Message)
	}
	if f.Status == 400 {
		return jb.MakeScheduleErrorInvalidSchedule(f.Message)
	}
	return jb.MakeScheduleErrorJobs(jobsError(f))
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

// intervalWit renders "90s" / "1.5s" like the strings the HTTP API returns.
// Retry intervals are never negative; clamp rather than panic.
func intervalWit(d *durationpb.Duration) string {
	seconds := max(d.GetSeconds(), 0)
	nanos := max(d.GetNanos(), 0)
	if nanos == 0 {
		return fmt.Sprintf("%ds", seconds)
	}
	return fmt.Sprintf("%d.%ss", seconds, strings.TrimRight(fmt.Sprintf("%09d", nanos), "0"))
}

func failurePolicyPb(p jb.JobFailurePolicy) *commonv1pb.JobFailurePolicy {
	if p.Tag() == jb.JobFailurePolicyTagConstant {
		c := p.Constant()
		constant := &commonv1pb.JobFailurePolicyConstant{MaxRetries: call.OptPtr(c.MaxRetries)}
		if c.Interval.IsSome() {
			constant.Interval = intervalPb(c.Interval.Some())
		}
		return &commonv1pb.JobFailurePolicy{Policy: &commonv1pb.JobFailurePolicy_Constant{Constant: constant}}
	}
	return &commonv1pb.JobFailurePolicy{Policy: &commonv1pb.JobFailurePolicy_Drop{Drop: &commonv1pb.JobFailurePolicyDrop{}}}
}

func failurePolicyWit(p *commonv1pb.JobFailurePolicy) witTypes.Option[jb.JobFailurePolicy] {
	switch x := p.GetPolicy().(type) {
	case *commonv1pb.JobFailurePolicy_Drop:
		return witTypes.Some(jb.MakeJobFailurePolicyDrop())
	case *commonv1pb.JobFailurePolicy_Constant:
		c := jb.JobFailurePolicyConstant{MaxRetries: optFromPtr(x.Constant.MaxRetries), Interval: witTypes.None[string]()}
		if x.Constant.GetInterval() != nil {
			c.Interval = witTypes.Some(intervalWit(x.Constant.GetInterval()))
		}
		return witTypes.Some(jb.MakeJobFailurePolicyConstant(c))
	}
	return witTypes.None[jb.JobFailurePolicy]()
}

func optFromPtr[T any](p *T) witTypes.Option[T] {
	if p == nil {
		return witTypes.None[T]()
	}
	return witTypes.Some(*p)
}

func jobPb(name string, job jb.Job) *runtimev1pb.Job {
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

func jobWit(job *runtimev1pb.Job) jb.Job {
	data := witTypes.None[string]()
	if job.GetData() != nil {
		data = witTypes.Some(anyjson.UnpackJSON(job.GetData()))
	}
	policy := witTypes.None[jb.JobFailurePolicy]()
	if job.GetFailurePolicy() != nil {
		policy = failurePolicyWit(job.GetFailurePolicy())
	}
	return jb.Job{
		Schedule:      optFromPtr(job.Schedule),
		Repeats:       optFromPtr(job.Repeats),
		DueTime:       optFromPtr(job.DueTime),
		Ttl:           optFromPtr(job.Ttl),
		Data:          data,
		FailurePolicy: policy,
	}
}

func Schedule(name string, job jb.Job, overwrite bool) witTypes.Result[witTypes.Unit, jb.ScheduleError] {
	req := &runtimev1pb.ScheduleJobRequest{Job: jobPb(name, job), Overwrite: overwrite}
	_, f := call.Do(func(ctx context.Context, api daprgrpc.API) (any, error) {
		return api.ScheduleJob(ctx, req)
	})
	if f != nil {
		return witTypes.Err[witTypes.Unit](scheduleError(f))
	}
	return witTypes.Ok[witTypes.Unit, jb.ScheduleError](witTypes.Unit{})
}

func Get(name string) witTypes.Result[jb.Job, jb.GetJobError] {
	r, f := call.Do(func(ctx context.Context, api daprgrpc.API) (*runtimev1pb.GetJobResponse, error) {
		return api.GetJob(ctx, &runtimev1pb.GetJobRequest{Name: name})
	})
	if f != nil {
		if f.Status == 404 {
			return witTypes.Err[jb.Job](jb.MakeGetJobErrorJobNotFound())
		}
		return witTypes.Err[jb.Job](jb.MakeGetJobErrorJobs(jobsError(f)))
	}
	// A successful GetJob with no job also means the job is absent.
	if r.GetJob() == nil {
		return witTypes.Err[jb.Job](jb.MakeGetJobErrorJobNotFound())
	}
	return witTypes.Ok[jb.Job, jb.GetJobError](jobWit(r.GetJob()))
}

func Delete(name string) witTypes.Result[witTypes.Unit, jb.JobsError] {
	_, f := call.Do(func(ctx context.Context, api daprgrpc.API) (any, error) {
		return api.DeleteJob(ctx, &runtimev1pb.DeleteJobRequest{Name: name})
	})
	if f != nil {
		return witTypes.Err[witTypes.Unit](jobsError(f))
	}
	return witTypes.Ok[witTypes.Unit, jb.JobsError](witTypes.Unit{})
}
