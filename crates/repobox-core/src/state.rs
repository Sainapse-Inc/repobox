use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{ErrorKind, RepoboxError, Result};

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvironmentStatus {
    Provisioning,
    Ready,
    Degraded,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct DatabaseBinding {
    pub service: String,
    pub provider: String,
    pub organization: String,
    pub database: String,
    pub branch: String,
    pub role_id: String,
    pub role_name: String,
    pub ready: bool,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct EnvironmentRecord {
    pub name: String,
    pub provider_branch: String,
    pub status: EnvironmentStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub databases: BTreeMap<String, DatabaseBinding>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failures: Vec<String>,
}

impl EnvironmentRecord {
    pub fn new(name: impl Into<String>, provider_branch: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            name: name.into(),
            provider_branch: provider_branch.into(),
            status: EnvironmentStatus::Provisioning,
            created_at: now,
            updated_at: now,
            databases: BTreeMap::new(),
            failures: vec![],
        }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct ProjectState {
    pub schema_version: u32,
    pub project_id: Uuid,
    #[serde(default)]
    pub environments: BTreeMap<String, EnvironmentRecord>,
    #[serde(default)]
    pub bootstrapped_services: BTreeSet<String>,
}

impl ProjectState {
    pub fn new(project_id: Uuid) -> Self {
        Self {
            schema_version: 1,
            project_id,
            environments: BTreeMap::new(),
            bootstrapped_services: BTreeSet::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct StateStore {
    path: PathBuf,
}

impl StateStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn load(&self, project_id: Uuid) -> Result<ProjectState> {
        let bytes = match fs::read(&self.path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(ProjectState::new(project_id));
            }
            Err(error) => return Err(error.into()),
        };
        let state: ProjectState = serde_json::from_slice(&bytes).map_err(|error| {
            RepoboxError::new(
                ErrorKind::Runtime,
                "state_decode_failed",
                format!("could not decode {}: {error}", self.path.display()),
            )
        })?;
        if state.project_id != project_id {
            return Err(RepoboxError::new(
                ErrorKind::Conflict,
                "state_project_mismatch",
                "local state belongs to a different Repobox project",
            ));
        }
        Ok(state)
    }

    pub fn save(&self, state: &ProjectState) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let temporary = self.path.with_extension("json.tmp");
        let bytes = serde_json::to_vec_pretty(state).map_err(|error| {
            RepoboxError::new(ErrorKind::Runtime, "state_encode_failed", error.to_string())
        })?;
        let mut options = OpenOptions::new();
        options.create(true).truncate(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, &self.path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_round_trips_atomically() {
        let directory = tempfile::tempdir().unwrap();
        let project_id = Uuid::new_v4();
        let store = StateStore::new(directory.path().join("state.json"));
        let mut state = ProjectState::new(project_id);
        state.environments.insert(
            "feature/a".to_owned(),
            EnvironmentRecord::new("feature/a", "rbx-feature-a"),
        );
        store.save(&state).unwrap();
        let loaded = store.load(project_id).unwrap();
        assert!(loaded.environments.contains_key("feature/a"));
    }
}
