use std::env;
use std::path::{Path, PathBuf};

use repobox_core::config::{self, RepoboxConfig};
use repobox_core::paths::RepoboxPaths;
use repobox_core::{ErrorKind, RepoboxError, Result};

use crate::git;

#[derive(Clone, Debug)]
pub struct ProjectContext {
    pub repository: PathBuf,
    pub config_path: PathBuf,
    pub config: RepoboxConfig,
    pub paths: RepoboxPaths,
}

impl ProjectContext {
    pub fn load(start: &Path) -> Result<Self> {
        let start = canonical_directory(start)?;
        let config_path = config::discover(&start).ok_or_else(|| {
            RepoboxError::new(
                ErrorKind::NotFound,
                "config_not_found",
                format!(
                    "no {} exists at or above {}",
                    config::CONFIG_FILE_NAME,
                    start.display()
                ),
            )
            .with_suggestion(
                "Run `repobox config detect --json` to inspect the repository, then `repobox init`.",
            )
        })?;
        let repository = config_path
            .parent()
            .expect("a config file has a parent")
            .to_path_buf();
        let config = RepoboxConfig::load(&config_path)?;
        Ok(Self {
            repository,
            config_path,
            config,
            paths: RepoboxPaths::discover()?,
        })
    }

    pub async fn environment(&self, explicit: Option<&str>) -> Result<String> {
        if let Some(value) = explicit {
            repobox_core::identity::validate_environment_name(value)?;
            return Ok(value.to_owned());
        }
        if let Ok(value) = env::var("REPOBOX_ENV")
            && !value.is_empty()
        {
            repobox_core::identity::validate_environment_name(&value)?;
            return Ok(value);
        }
        git::current_environment(&self.repository).await
    }
}

pub fn requested_repository(path: Option<&PathBuf>) -> Result<PathBuf> {
    let path = match path {
        Some(path) => path.clone(),
        None => env::current_dir()?,
    };
    canonical_directory(&path)
}

fn canonical_directory(path: &Path) -> Result<PathBuf> {
    let canonical = path.canonicalize().map_err(|error| {
        RepoboxError::new(
            ErrorKind::NotFound,
            "repository_path_not_found",
            format!("could not access {}: {error}", path.display()),
        )
    })?;
    if !canonical.is_dir() {
        return Err(RepoboxError::new(
            ErrorKind::Usage,
            "repository_path_not_directory",
            format!("{} is not a directory", canonical.display()),
        ));
    }
    Ok(canonical)
}
