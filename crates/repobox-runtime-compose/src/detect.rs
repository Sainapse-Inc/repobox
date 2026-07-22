use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use repobox_core::runtime::{DetectedService, DetectedServiceKind, RuntimeDetection};
use repobox_core::{ErrorKind, RepoboxError, Result};
use tokio::process::Command;

use crate::model::ComposeProject;

const COMPOSE_NAMES: &[&str] = &[
    "compose.yaml",
    "compose.yml",
    "docker-compose.yaml",
    "docker-compose.yml",
];

pub fn detect_compose_files(repository: &Path) -> Vec<PathBuf> {
    COMPOSE_NAMES
        .iter()
        .map(|name| repository.join(name))
        .filter(|path| path.is_file())
        .collect()
}

pub async fn detect_repository(repository: &Path) -> Result<RuntimeDetection> {
    let files = detect_compose_files(repository);
    if files.is_empty() {
        return Ok(RuntimeDetection {
            driver: "none".to_owned(),
            warnings: vec![
                "No Docker Compose configuration was found; configure a native argv command."
                    .to_owned(),
            ],
            ..RuntimeDetection::default()
        });
    }
    detect_configuration(repository, &files, &[], &BTreeMap::new()).await
}

pub async fn detect_configuration(
    repository: &Path,
    files: &[PathBuf],
    profiles: &[String],
    environment: &BTreeMap<String, String>,
) -> Result<RuntimeDetection> {
    let project = resolve_project(repository, files, profiles, environment).await?;
    let services = project
        .services
        .into_iter()
        .map(|(name, service)| {
            let environment = service.environment.as_map();
            let kind = if is_postgres(service.image.as_deref(), &environment) {
                DetectedServiceKind::Postgres
            } else {
                DetectedServiceKind::Other
            };
            let dependencies = service.dependencies();
            DetectedService {
                name,
                image: service.image,
                kind,
                dependencies,
                environment,
            }
        })
        .collect();
    Ok(RuntimeDetection {
        driver: "compose".to_owned(),
        files: files
            .iter()
            .map(|path| path.strip_prefix(repository).unwrap_or(path).to_path_buf())
            .collect(),
        services,
        warnings: vec![],
    })
}

pub(crate) async fn resolve_project(
    repository: &Path,
    files: &[PathBuf],
    profiles: &[String],
    environment: &BTreeMap<String, String>,
) -> Result<ComposeProject> {
    let output = run_compose_config(repository, files, profiles, environment, false).await?;
    let output = if output.status.success() {
        output
    } else {
        let interpolated_error = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        let fallback = run_compose_config(repository, files, profiles, environment, true).await?;
        if !fallback.status.success() {
            return Err(RepoboxError::new(
                ErrorKind::Usage,
                "compose_config_failed",
                format!(
                    "{interpolated_error}; without interpolation: {}",
                    String::from_utf8_lossy(&fallback.stderr).trim()
                ),
            )
            .with_suggestion(
                "Run `docker compose config` and fix the reported configuration error.",
            ));
        }
        fallback
    };
    serde_json::from_slice(&output.stdout).map_err(|error| {
        RepoboxError::new(
            ErrorKind::Runtime,
            "compose_output_invalid",
            format!("Docker Compose returned invalid JSON: {error}"),
        )
    })
}

async fn run_compose_config(
    repository: &Path,
    files: &[PathBuf],
    profiles: &[String],
    environment: &BTreeMap<String, String>,
    no_interpolate: bool,
) -> Result<std::process::Output> {
    let mut command = Command::new("docker");
    command.current_dir(repository).arg("compose");
    for profile in profiles {
        command.arg("--profile").arg(profile);
    }
    for file in files {
        command.arg("-f").arg(file);
    }
    command.envs(environment);
    command.args(["config", "--format", "json"]);
    if no_interpolate {
        command.arg("--no-interpolate");
    }
    command.output().await.map_err(|error| {
        RepoboxError::new(
            ErrorKind::Runtime,
            "compose_unavailable",
            format!("could not execute Docker Compose: {error}"),
        )
        .with_suggestion("Install Docker Compose v2 and run `repobox doctor`.")
    })
}

fn is_postgres(
    image: Option<&str>,
    environment: &std::collections::BTreeMap<String, String>,
) -> bool {
    image.is_some_and(|image| {
        let base = image.rsplit('/').next().unwrap_or(image);
        let repository = base.split([':', '@']).next().unwrap_or(base);
        matches!(repository, "postgres" | "postgis")
    }) || environment.contains_key("POSTGRES_DB")
        || environment.contains_key("POSTGRES_USER")
        || environment.contains_key("POSTGRES_PASSWORD")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_postgres_images_and_environment() {
        assert!(is_postgres(
            Some("postgres:18"),
            &std::collections::BTreeMap::default()
        ));
        assert!(is_postgres(
            Some("postgis/postgis:17"),
            &std::collections::BTreeMap::default()
        ));
        assert!(is_postgres(
            Some("custom-db"),
            &[("POSTGRES_DB".to_owned(), "app".to_owned())]
                .into_iter()
                .collect()
        ));
        assert!(!is_postgres(
            Some("redis:8"),
            &std::collections::BTreeMap::default()
        ));
    }
}
