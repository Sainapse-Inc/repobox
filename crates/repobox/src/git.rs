use std::path::{Path, PathBuf};

use repobox_core::{ErrorKind, RepoboxError, Result};
use tokio::process::Command;

pub async fn repository_root(start: &Path) -> Result<PathBuf> {
    let output = git(start, &["rev-parse", "--show-toplevel"]).await?;
    Ok(PathBuf::from(output))
}

pub async fn current_environment(repository: &Path) -> Result<String> {
    let branch = git(repository, &["branch", "--show-current"]).await?;
    if !branch.is_empty() {
        return Ok(branch);
    }
    let commit = git(repository, &["rev-parse", "--short=12", "HEAD"]).await?;
    Ok(format!("detached-{commit}"))
}

pub async fn merged_branches(repository: &Path, base: &str, fetch: bool) -> Result<Vec<String>> {
    if fetch {
        let status = Command::new("git")
            .current_dir(repository)
            .args(["fetch", "--prune"])
            .status()
            .await?;
        if !status.success() {
            return Err(RepoboxError::new(
                ErrorKind::Runtime,
                "git_fetch_failed",
                format!("git fetch --prune exited with {status}"),
            ));
        }
    }
    let output = git(
        repository,
        &["branch", "--merged", base, "--format=%(refname:short)"],
    )
    .await?;
    Ok(output
        .lines()
        .map(str::trim)
        .filter(|branch| !branch.is_empty() && *branch != base)
        .map(str::to_owned)
        .collect())
}

pub async fn is_repository(path: &Path) -> bool {
    Command::new("git")
        .current_dir(path)
        .args(["rev-parse", "--is-inside-work-tree"])
        .output()
        .await
        .is_ok_and(|output| output.status.success())
}

async fn git(repository: &Path, args: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .current_dir(repository)
        .args(args)
        .output()
        .await
        .map_err(|error| {
            RepoboxError::new(
                ErrorKind::Runtime,
                "git_unavailable",
                format!("could not execute Git: {error}"),
            )
        })?;
    if !output.status.success() {
        return Err(RepoboxError::new(
            ErrorKind::Usage,
            "git_command_failed",
            String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}
