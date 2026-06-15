//! Jobs (distributed scheduler) — https://docs.dapr.io/reference/api/jobs_api/

use serde::Deserialize;
use serde_json::json;
use wstd::http::Method;

use crate::exports::jobs::{
    GetJobError, Guest, Job, JobFailurePolicy, JobFailurePolicyConstant, JobsError, ScheduleError,
};
use crate::sidecar::{seg, DaprFailure, Sidecar};
use crate::Component;

/// Map a recoverable failure to the jobs error (only permission-denied).
fn jobs_error(f: DaprFailure) -> JobsError {
    JobsError::PermissionDenied(f.message)
}

/// Map a recoverable failure of `get` through the jobs error.
/// (The `job-not-found` case is produced from the 404 branch.)
fn get_job_error(f: DaprFailure) -> GetJobError {
    GetJobError::Jobs(jobs_error(f))
}

/// Map a recoverable failure of a schedule.
fn schedule_error(f: DaprFailure) -> ScheduleError {
    if f.status == 409
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

impl Guest for Component {
    fn schedule(name: String, job: Job, overwrite: bool) -> Result<(), ScheduleError> {
        let sidecar = Sidecar::from_env();
        let path = format!("/v1.0/jobs/{}", seg(&name));

        let mut body = json!({});
        if let Some(schedule) = &job.schedule {
            body["schedule"] = json!(schedule);
        }
        if let Some(repeats) = job.repeats {
            body["repeats"] = json!(repeats);
        }
        if let Some(due_time) = &job.due_time {
            body["dueTime"] = json!(due_time);
        }
        if let Some(ttl) = &job.ttl {
            body["ttl"] = json!(ttl);
        }
        if let Some(data) = &job.data {
            body["data"] = serde_json::from_str(data)
                .unwrap_or_else(|e| panic!("job data is not valid JSON: {e}"));
        }
        if overwrite {
            body["overwrite"] = json!(true);
        }
        if let Some(policy) = &job.failure_policy {
            body["failure_policy"] = match policy {
                JobFailurePolicy::Drop => json!({ "drop": {} }),
                JobFailurePolicy::Constant(constant) => {
                    let mut object = json!({});
                    if let Some(max_retries) = constant.max_retries {
                        object["max_retries"] = json!(max_retries);
                    }
                    if let Some(interval) = &constant.interval {
                        object["interval"] = json!(interval);
                    }
                    json!({ "constant": object })
                }
            };
        }

        sidecar
            .json(Method::POST, &path, &body)
            .map_err(schedule_error)?;
        Ok(())
    }

    fn get(name: String) -> Result<Job, GetJobError> {
        let sidecar = Sidecar::from_env();
        let path = format!("/v1.0/jobs/{}", seg(&name));
        let response = match sidecar.expect_success(Method::GET, &path, &[], Vec::new()) {
            Ok(r) => r,
            Err(f) if f.status == 404 => return Err(GetJobError::JobNotFound),
            Err(f) => return Err(get_job_error(f)),
        };

        #[derive(Deserialize)]
        struct JobJson {
            #[serde(default)]
            schedule: Option<String>,
            #[serde(default)]
            repeats: Option<u32>,
            #[serde(default, rename = "dueTime")]
            due_time: Option<String>,
            #[serde(default)]
            ttl: Option<String>,
            #[serde(default)]
            data: Option<serde_json::Value>,
            #[serde(default)]
            failure_policy: Option<FailurePolicyJson>,
        }
        #[derive(Deserialize)]
        struct FailurePolicyJson {
            #[serde(default)]
            drop: Option<serde_json::Value>,
            #[serde(default)]
            constant: Option<ConstantJson>,
        }
        #[derive(Deserialize)]
        struct ConstantJson {
            #[serde(default)]
            max_retries: Option<u32>,
            #[serde(default)]
            interval: Option<String>,
        }

        let parsed: JobJson = serde_json::from_slice(&response.body)
            .unwrap_or_else(|e| panic!("unexpected job response: {e}"));
        Ok(Job {
            schedule: parsed.schedule,
            repeats: parsed.repeats,
            due_time: parsed.due_time,
            ttl: parsed.ttl,
            data: parsed.data.map(|value| value.to_string()),
            failure_policy: parsed.failure_policy.map(|policy| {
                if let Some(constant) = policy.constant {
                    JobFailurePolicy::Constant(JobFailurePolicyConstant {
                        max_retries: constant.max_retries,
                        interval: constant.interval,
                    })
                } else {
                    let _ = policy.drop;
                    JobFailurePolicy::Drop
                }
            }),
        })
    }

    fn delete(name: String) -> Result<(), JobsError> {
        let sidecar = Sidecar::from_env();
        let path = format!("/v1.0/jobs/{}", seg(&name));
        sidecar
            .expect_success(Method::DELETE, &path, &[], Vec::new())
            .map_err(jobs_error)?;
        Ok(())
    }
}
