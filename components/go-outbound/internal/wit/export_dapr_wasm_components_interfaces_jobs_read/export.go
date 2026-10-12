// Jobs / distributed scheduler reads, served by the embedded runtime's API
// — GetJob (the stable RPC). Job data is google.protobuf.Any in the proto,
// unpacked to JSON via anyjson. Mirrors wasi-grpc-outbound's jobs.rs (the
// jobs-read half).
package export_dapr_wasm_components_interfaces_jobs_read

import (
	"context"
	"fmt"
	"strings"

	daprgrpc "github.com/dapr/dapr/pkg/api/grpc"
	commonv1pb "github.com/dapr/dapr/pkg/proto/common/v1"
	runtimev1pb "github.com/dapr/dapr/pkg/proto/runtime/v1"
	witTypes "go.bytecodealliance.org/pkg/wit/types"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/anyjson"
	jbblk "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/blocks/jobs"
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/call"
	jr "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_jobs_read"
)

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

func failurePolicyWit(p *commonv1pb.JobFailurePolicy) witTypes.Option[jr.JobFailurePolicy] {
	switch x := p.GetPolicy().(type) {
	case *commonv1pb.JobFailurePolicy_Drop:
		return witTypes.Some(jr.MakeJobFailurePolicyDrop())
	case *commonv1pb.JobFailurePolicy_Constant:
		c := jr.JobFailurePolicyConstant{MaxRetries: optFromPtr(x.Constant.MaxRetries), Interval: witTypes.None[string]()}
		if x.Constant.GetInterval() != nil {
			c.Interval = witTypes.Some(intervalWit(x.Constant.GetInterval()))
		}
		return witTypes.Some(jr.MakeJobFailurePolicyConstant(c))
	}
	return witTypes.None[jr.JobFailurePolicy]()
}

func optFromPtr[T any](p *T) witTypes.Option[T] {
	if p == nil {
		return witTypes.None[T]()
	}
	return witTypes.Some(*p)
}

func jobWit(job *runtimev1pb.Job) jr.Job {
	data := witTypes.None[string]()
	if job.GetData() != nil {
		data = witTypes.Some(anyjson.UnpackJSON(job.GetData()))
	}
	policy := witTypes.None[jr.JobFailurePolicy]()
	if job.GetFailurePolicy() != nil {
		policy = failurePolicyWit(job.GetFailurePolicy())
	}
	return jr.Job{
		Schedule:      optFromPtr(job.Schedule),
		Repeats:       optFromPtr(job.Repeats),
		DueTime:       optFromPtr(job.DueTime),
		Ttl:           optFromPtr(job.Ttl),
		Data:          data,
		FailurePolicy: policy,
	}
}

func Get(name string) witTypes.Result[jr.Job, jr.GetJobError] {
	r, f := call.Do(func(ctx context.Context, api daprgrpc.API) (*runtimev1pb.GetJobResponse, error) {
		return api.GetJob(ctx, &runtimev1pb.GetJobRequest{Name: name})
	})
	if f != nil {
		if f.Status == 404 {
			return witTypes.Err[jr.Job](jr.MakeGetJobErrorJobNotFound())
		}
		return witTypes.Err[jr.Job](jr.MakeGetJobErrorJobs(jbblk.Error(f)))
	}
	// A successful GetJob with no job also means the job is absent.
	if r.GetJob() == nil {
		return witTypes.Err[jr.Job](jr.MakeGetJobErrorJobNotFound())
	}
	return witTypes.Ok[jr.Job, jr.GetJobError](jobWit(r.GetJob()))
}
