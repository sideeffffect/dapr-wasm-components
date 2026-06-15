//! Jobs / distributed scheduler over gRPC — `ScheduleJob`, `GetJob`,
//! `DeleteJob` (the stable RPCs; this proto version also still carries the
//! Alpha1 variants).
//! Job data is JSON in the WIT contract but `google.protobuf.Any` on the
//! wire — packed as `google.protobuf.Value` via `anyjson`.

use crate::anyjson::{pack_json, unpack_json};
use crate::exports::jobs::{
    Guest, Job, JobFailurePolicy, JobFailurePolicyConstant, JobsError, ScheduleError,
};
use crate::proto::common as pbc;
use crate::proto::runtime as pb;
use crate::sidecar::{DaprFailure, Sidecar};
use crate::Component;

fn jobs_error(f: DaprFailure) -> JobsError {
    JobsError::PermissionDenied(f.message)
}

fn schedule_error(f: DaprFailure) -> ScheduleError {
    if matches!(f.status, 409)
        || f.error_code
            .as_deref()
            .is_some_and(|c| c.contains("ALREADY_EXISTS"))
    {
        return ScheduleError::AlreadyExists(f.message);
    }
    if f.status == 400 {
        return ScheduleError::InvalidSchedule(f.message);
    }
    ScheduleError::Jobs(jobs_error(f))
}

/// WIT carries the retry interval as a Go-style duration string ("10s");
/// the proto wants a `google.protobuf.Duration`. The parser is humantime,
/// whose grammar is close to but not exactly Go's `time.ParseDuration`
/// (it accepts "1d", rejects fractions like ".5s") — unlike the HTTP
/// provider, which passes the string to the sidecar verbatim. A duration the
/// app cannot express is a programming error, so it traps rather than being
/// surfaced as a recoverable failure.
fn interval_pb(interval: &str) -> prost_types::Duration {
    let duration = humantime::parse_duration(interval)
        .unwrap_or_else(|e| panic!("invalid failure-policy interval {interval:?}: {e}"));
    prost_types::Duration {
        seconds: i64::try_from(duration.as_secs())
            .unwrap_or_else(|_| panic!("failure-policy interval {interval:?} is out of range")),
        nanos: duration.subsec_nanos() as i32,
    }
}

/// Render Go-parseable ("90s", "1.5s") like the strings the HTTP API
/// returns — not humantime's word form ("1m 30s"). Retry intervals are
/// never negative; clamp rather than panic if the sidecar ever sends one.
fn interval_wit(duration: &prost_types::Duration) -> String {
    let seconds = u64::try_from(duration.seconds).unwrap_or(0);
    let nanos = u32::try_from(duration.nanos).unwrap_or(0);
    if nanos == 0 {
        format!("{seconds}s")
    } else {
        let fraction = format!("{nanos:09}");
        format!("{seconds}.{}s", fraction.trim_end_matches('0'))
    }
}

fn failure_policy_pb(policy: &JobFailurePolicy) -> pbc::JobFailurePolicy {
    use pbc::job_failure_policy::Policy;
    let policy = match policy {
        JobFailurePolicy::Drop => Policy::Drop(pbc::JobFailurePolicyDrop {}),
        JobFailurePolicy::Constant(constant) => Policy::Constant(pbc::JobFailurePolicyConstant {
            interval: constant.interval.as_deref().map(interval_pb),
            max_retries: constant.max_retries,
        }),
    };
    pbc::JobFailurePolicy {
        policy: Some(policy),
    }
}

fn failure_policy_wit(policy: pbc::JobFailurePolicy) -> Option<JobFailurePolicy> {
    use pbc::job_failure_policy::Policy;
    Some(match policy.policy? {
        Policy::Drop(_) => JobFailurePolicy::Drop,
        Policy::Constant(constant) => JobFailurePolicy::Constant(JobFailurePolicyConstant {
            max_retries: constant.max_retries,
            interval: constant.interval.as_ref().map(interval_wit),
        }),
    })
}

fn job_pb(name: String, job: &Job) -> pb::Job {
    pb::Job {
        name,
        schedule: job.schedule.clone(),
        repeats: job.repeats,
        due_time: job.due_time.clone(),
        ttl: job.ttl.clone(),
        // Job data the app supplies that is not valid JSON is a programming
        // error, so packing traps rather than surfacing a recoverable failure.
        data: job
            .data
            .as_deref()
            .map(|data| pack_json(data).unwrap_or_else(|e| panic!("{e}"))),
        failure_policy: job.failure_policy.as_ref().map(failure_policy_pb),
    }
}

fn job_wit(job: pb::Job) -> Job {
    Job {
        schedule: job.schedule,
        repeats: job.repeats,
        due_time: job.due_time,
        ttl: job.ttl,
        data: job.data.as_ref().map(unpack_json),
        failure_policy: job.failure_policy.and_then(failure_policy_wit),
    }
}

impl Guest for Component {
    fn schedule(name: String, job: Job, overwrite: bool) -> Result<(), ScheduleError> {
        let sidecar = Sidecar::from_env();
        sidecar
            .unary(
                pb::ScheduleJobRequest {
                    job: Some(job_pb(name, &job)),
                    overwrite,
                },
                |mut client, request| async move { client.schedule_job(request).await },
            )
            .map_err(schedule_error)?;
        Ok(())
    }

    fn get(name: String) -> Result<Option<Job>, JobsError> {
        let sidecar = Sidecar::from_env();
        let response = match sidecar.unary(
            pb::GetJobRequest { name },
            |mut client, request| async move { client.get_job(request).await },
        ) {
            Ok(response) => response,
            // A missing job is absence, not an error.
            Err(f) if f.status == 404 => return Ok(None),
            Err(f) => return Err(jobs_error(f)),
        };
        // A successful GetJob with no job means the job is absent.
        match response.job {
            Some(job) => Ok(Some(job_wit(job))),
            None => Ok(None),
        }
    }

    fn delete(name: String) -> Result<(), JobsError> {
        let sidecar = Sidecar::from_env();
        sidecar
            .unary(
                pb::DeleteJobRequest { name },
                |mut client, request| async move { client.delete_job(request).await },
            )
            .map_err(jobs_error)?;
        Ok(())
    }
}
