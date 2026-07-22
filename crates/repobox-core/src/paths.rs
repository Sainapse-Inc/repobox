use std::path::{Path, PathBuf};

use directories::ProjectDirs;
use uuid::Uuid;

use crate::{ErrorKind, RepoboxError, Result};

#[derive(Clone, Debug)]
pub struct RepoboxPaths {
    pub config_dir: PathBuf,
    pub state_dir: PathBuf,
    pub cache_dir: PathBuf,
}

impl RepoboxPaths {
    pub fn discover() -> Result<Self> {
        let dirs = ProjectDirs::from("dev", "abhirupghosh", "repobox").ok_or_else(|| {
            RepoboxError::new(
                ErrorKind::Runtime,
                "home_directory_unavailable",
                "could not determine platform configuration directories",
            )
        })?;
        Ok(Self {
            config_dir: dirs.config_dir().to_path_buf(),
            state_dir: dirs
                .state_dir()
                .unwrap_or_else(|| dirs.data_local_dir())
                .to_path_buf(),
            cache_dir: dirs.cache_dir().to_path_buf(),
        })
    }

    pub fn project_state(&self, project_id: Uuid) -> PathBuf {
        self.state_dir.join("projects").join(project_id.to_string())
    }

    pub fn jobs(&self, project_id: Uuid) -> PathBuf {
        self.project_state(project_id).join("jobs.jsonl")
    }

    pub fn state(&self, project_id: Uuid) -> PathBuf {
        self.project_state(project_id).join("state.json")
    }

    pub fn credentials_file(&self) -> PathBuf {
        self.config_dir.join("credentials.json")
    }

    pub fn user_config(&self) -> PathBuf {
        self.config_dir.join("config.yml")
    }

    pub fn ensure_parent(path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        Ok(())
    }
}
