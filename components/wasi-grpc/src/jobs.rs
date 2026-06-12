//! Jobs / distributed scheduler over gRPC — `ScheduleJob`, `GetJob`,
//! `DeleteJob` (the stable RPCs; this proto version also still carries the
//! Alpha1 variants).
//! Job data is JSON in the WIT contract but `google.protobuf.Any` on the
//! wire — packed as `google.protobuf.Value` via `anyjson`.

use crate::anyjson::{pack_json, unpack_json};
use crate::exports::jobs::{Guest, Job, JobFailurePolicy, JobFailurePolicyConstant};
use crate::proto::common as pbc;
use crate::proto::runtime as pb;
use crate::sidecar::Sidecar;
use crate::types::Error;
use crate::Component;

/// WIT carries the retry interval as a Go-style duration string ("10s");
/// the proto wants a `google.protobuf.Duration`. The parser is humantime,
/// whose grammar is close to but not exactly Go's `time.ParseDuration`
/// (it accepts "1d", rejects fractions like ".5s") — unlike the HTTP
/// provider, which passes the string to the sidecar verbatim.
fn interval_pb(interval: &str) -> Result<prost_types::Duration, Error> {
    let duration = humantime::parse_duration(interval).map_err(|e| {
        Error::InvalidArgument(format!("invalid failure-policy interval {interval:?}: {e}"))
    })?;
    Ok(prost_types::Duration {
        seconds: i64::try_from(duration.as_secs()).map_err(|_| {
            Error::InvalidArgument(format!(
                "failure-policy interval {interval:?} is out of range"
            ))
        })?,
        nanos: duration.subsec_nanos() as i32,
    })
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

fn failure_policy_pb(policy: &JobFailurePolicy) -> Result<pbc::JobFailurePolicy, Error> {
    use pbc::job_failure_policy::Policy;
    let policy = match policy {
        JobFailurePolicy::Drop => Policy::Drop(pbc::JobFailurePolicyDrop {}),
        JobFailurePolicy::Constant(constant) => Policy::Constant(pbc::JobFailurePolicyConstant {
            interval: constant.interval.as_deref().map(interval_pb).transpose()?,
            max_retries: constant.max_retries,
        }),
    };
    Ok(pbc::JobFailurePolicy {
        policy: Some(policy),
    })
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

fn job_pb(name: String, job: &Job) -> Result<pb::Job, Error> {
    Ok(pb::Job {
        name,
        schedule: job.schedule.clone(),
        repeats: job.repeats,
        due_time: job.due_time.clone(),
        ttl: job.ttl.clone(),
        data: job.data.as_deref().map(pack_json).transpose()?,
        failure_policy: job
            .failure_policy
            .as_ref()
            .map(failure_policy_pb)
            .transpose()?,
    })
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
    fn schedule(name: String, job: Job, overwrite: bool) -> Result<(), Error> {
        let sidecar = Sidecar::from_env()?;
        sidecar.unary(
            pb::ScheduleJobRequest {
                job: Some(job_pb(name, &job)?),
                overwrite,
            },
            |mut client, request| async move { client.schedule_job(request).await },
        )?;
        Ok(())
    }

    fn get(name: String) -> Result<Job, Error> {
        let sidecar = Sidecar::from_env()?;
        let response = sidecar.unary(
            pb::GetJobRequest { name },
            |mut client, request| async move { client.get_job(request).await },
        )?;
        let job = response
            .job
            .ok_or_else(|| Error::Internal("GetJob returned no job".to_string()))?;
        Ok(job_wit(job))
    }

    fn delete(name: String) -> Result<(), Error> {
        let sidecar = Sidecar::from_env()?;
        sidecar.unary(
            pb::DeleteJobRequest { name },
            |mut client, request| async move { client.delete_job(request).await },
        )?;
        Ok(())
    }
}
