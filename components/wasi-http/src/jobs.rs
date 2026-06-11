//! Jobs (distributed scheduler) — https://docs.dapr.io/reference/api/jobs_api/

use serde::Deserialize;
use serde_json::json;
use wstd::http::Method;

use crate::exports::jobs::{Guest, Job, JobFailurePolicy, JobFailurePolicyConstant};
use crate::sidecar::{seg, Sidecar};
use crate::types::Error;
use crate::Component;

impl Guest for Component {
    fn schedule(name: String, job: Job, overwrite: bool) -> Result<(), Error> {
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
                .map_err(|e| Error::InvalidArgument(format!("job data is not valid JSON: {e}")))?;
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

        sidecar.json(Method::POST, &path, &body)?;
        Ok(())
    }

    fn get(name: String) -> Result<Job, Error> {
        let sidecar = Sidecar::from_env();
        let path = format!("/v1.0/jobs/{}", seg(&name));
        let response = sidecar.expect_success(Method::GET, &path, &[], Vec::new())?;

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
            .map_err(|e| Error::Internal(format!("unexpected job response: {e}")))?;
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

    fn delete(name: String) -> Result<(), Error> {
        let sidecar = Sidecar::from_env();
        let path = format!("/v1.0/jobs/{}", seg(&name));
        sidecar.expect_success(Method::DELETE, &path, &[], Vec::new())?;
        Ok(())
    }
}
