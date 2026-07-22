use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{ErrorKind, RepoboxError, Result};

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobKind {
    Initialize,
    EnvironmentCreate,
    EnvironmentDelete,
    EnvironmentPull,
    EnvironmentPrune,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Pending,
    Running,
    Degraded,
    Succeeded,
    Failed,
    Canceled,
}

impl JobStatus {
    pub const fn terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Canceled)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StepStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Skipped,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
pub struct JobStep {
    pub name: String,
    pub status: StepStatus,
    #[serde(default)]
    pub attempts: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub resource: Value,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
pub struct JobRecord {
    pub schema_version: u32,
    pub id: Uuid,
    pub sequence: u64,
    pub kind: JobKind,
    pub status: JobStatus,
    pub project_id: Uuid,
    pub environment: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub steps: Vec<JobStep>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
}

impl JobRecord {
    pub fn new(
        kind: JobKind,
        project_id: Uuid,
        environment: impl Into<String>,
        steps: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        let now = Utc::now();
        Self {
            schema_version: 1,
            id: Uuid::now_v7(),
            sequence: 0,
            kind,
            status: JobStatus::Pending,
            project_id,
            environment: environment.into(),
            created_at: now,
            updated_at: now,
            steps: steps
                .into_iter()
                .map(|name| JobStep {
                    name: name.into(),
                    status: StepStatus::Pending,
                    attempts: 0,
                    message: None,
                    resource: Value::Null,
                })
                .collect(),
            error_code: None,
        }
    }

    pub fn update_step(
        &mut self,
        name: &str,
        status: StepStatus,
        message: Option<String>,
    ) -> Result<()> {
        let step = self
            .steps
            .iter_mut()
            .find(|step| step.name == name)
            .ok_or_else(|| {
                RepoboxError::new(
                    ErrorKind::NotFound,
                    "job_step_not_found",
                    format!("job step `{name}` does not exist"),
                )
            })?;
        if status == StepStatus::Running {
            step.attempts += 1;
        }
        step.status = status;
        step.message = message;
        self.sequence += 1;
        self.updated_at = Utc::now();
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct JobStore {
    path: PathBuf,
}

impl JobStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn append(&self, job: &JobRecord) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        serde_json::to_writer(&mut file, job).map_err(|error| {
            RepoboxError::new(ErrorKind::Runtime, "job_encode_failed", error.to_string())
        })?;
        file.write_all(b"\n")?;
        file.sync_data()?;
        Ok(())
    }

    pub fn list(&self) -> Result<Vec<JobRecord>> {
        let file = match fs::File::open(&self.path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
            Err(error) => return Err(error.into()),
        };
        let mut latest = std::collections::BTreeMap::new();
        for (index, line) in BufReader::new(file).lines().enumerate() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let record: JobRecord = serde_json::from_str(&line).map_err(|error| {
                RepoboxError::new(
                    ErrorKind::Runtime,
                    "job_ledger_corrupt",
                    format!("invalid job ledger record on line {}: {error}", index + 1),
                )
            })?;
            latest.insert(record.id, record);
        }
        let mut records: Vec<_> = latest.into_values().collect();
        records.sort_by_key(|record| record.created_at);
        Ok(records)
    }

    pub fn get(&self, id: Uuid) -> Result<JobRecord> {
        self.list()?
            .into_iter()
            .find(|record| record.id == id)
            .ok_or_else(|| {
                RepoboxError::new(
                    ErrorKind::NotFound,
                    "job_not_found",
                    format!("job `{id}` was not found"),
                )
            })
    }

    pub fn latest(&self) -> Result<JobRecord> {
        self.list()?.pop().ok_or_else(|| {
            RepoboxError::new(
                ErrorKind::NotFound,
                "job_not_found",
                "no Repobox jobs exist",
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ledger_returns_latest_snapshot_per_job() {
        let temp = tempfile::tempdir().unwrap();
        let store = JobStore::new(temp.path().join("jobs.jsonl"));
        let mut job = JobRecord::new(
            JobKind::EnvironmentCreate,
            Uuid::new_v4(),
            "feature",
            ["create_branch"],
        );
        store.append(&job).unwrap();
        job.status = JobStatus::Running;
        job.update_step("create_branch", StepStatus::Running, None)
            .unwrap();
        store.append(&job).unwrap();
        let jobs = store.list().unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].status, JobStatus::Running);
        assert_eq!(jobs[0].steps[0].attempts, 1);
    }
}
