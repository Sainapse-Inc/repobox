use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Stdio;

use async_trait::async_trait;
use repobox_core::runtime::{RuntimeDetection, RuntimeDriver, RuntimeServiceStatus, RuntimeStatus};
use repobox_core::{ErrorKind, RepoboxError, Result};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

use crate::detect::{detect_repository, resolve_project};
use crate::model::ComposeProject;

#[derive(Clone, Debug)]
pub struct ComposeRuntime {
    repository: PathBuf,
    files: Vec<PathBuf>,
    profiles: Vec<String>,
    project_name: String,
    remote_services: BTreeSet<String>,
    environment_by_service: BTreeMap<String, BTreeMap<String, String>>,
    global_environment: BTreeMap<String, String>,
}

impl ComposeRuntime {
    pub fn new(
        repository: impl Into<PathBuf>,
        files: Vec<PathBuf>,
        profiles: Vec<String>,
        project_name: impl Into<String>,
        remote_services: BTreeSet<String>,
        environment_by_service: BTreeMap<String, BTreeMap<String, String>>,
        global_environment: BTreeMap<String, String>,
    ) -> Self {
        Self {
            repository: repository.into(),
            files,
            profiles,
            project_name: project_name.into(),
            remote_services,
            environment_by_service,
            global_environment,
        }
    }

    async fn transformed_yaml(
        &self,
        global_environment: &BTreeMap<String, String>,
    ) -> Result<String> {
        let absolute_files: Vec<_> = self
            .files
            .iter()
            .map(|path| {
                if path.is_absolute() {
                    path.clone()
                } else {
                    self.repository.join(path)
                }
            })
            .collect();
        let mut interpolation_environment = self.global_environment.clone();
        interpolation_environment.extend(global_environment.clone());
        let project = transform_project(
            resolve_project(
                &self.repository,
                &absolute_files,
                &self.profiles,
                &interpolation_environment,
            )
            .await?,
            &self.remote_services,
            &self.environment_by_service,
            &interpolation_environment,
        );
        serde_yaml_ng::to_string(&project).map_err(|error| {
            RepoboxError::new(
                ErrorKind::Runtime,
                "compose_transform_failed",
                error.to_string(),
            )
        })
    }

    pub fn project_name(&self) -> &str {
        &self.project_name
    }

    pub fn spawn_logs(
        &self,
        service: Option<&str>,
        follow: bool,
        tail: usize,
    ) -> Result<tokio::process::Child> {
        let mut command = Command::new("docker");
        command.current_dir(&self.repository).arg("compose");
        self.add_profiles(&mut command);
        self.add_files(&mut command);
        command
            .args(["-p", &self.project_name, "logs", "--no-color"])
            .arg(format!("--tail={tail}"));
        if follow {
            command.arg("--follow");
        }
        if let Some(service) = service {
            command.arg(service);
        }
        command
            .envs(&self.global_environment)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command.spawn().map_err(|error| {
            RepoboxError::new(
                ErrorKind::Runtime,
                "compose_logs_failed",
                format!("could not start Docker Compose logs: {error}"),
            )
        })
    }

    async fn compose_with_stdin(
        &self,
        args: &[&str],
        environment: &BTreeMap<String, String>,
        quiet: bool,
    ) -> Result<std::process::ExitStatus> {
        let yaml = self.transformed_yaml(environment).await?;
        let mut command = Command::new("docker");
        command.current_dir(&self.repository).arg("compose");
        self.add_profiles(&mut command);
        command
            .args(["-p", &self.project_name, "-f", "-"])
            .args(args)
            .envs(&self.global_environment)
            .envs(environment)
            .stdin(Stdio::piped());
        if quiet {
            command.stdout(Stdio::null()).stderr(Stdio::null());
        } else {
            command.stdout(Stdio::inherit()).stderr(Stdio::inherit());
        }
        let mut child = command.spawn().map_err(|error| {
            RepoboxError::new(
                ErrorKind::Runtime,
                "compose_start_failed",
                format!("could not start Docker Compose: {error}"),
            )
        })?;
        child
            .stdin
            .take()
            .expect("piped stdin is available")
            .write_all(yaml.as_bytes())
            .await?;
        child.wait().await.map_err(Into::into)
    }

    pub async fn start_quiet(&self, environment: &BTreeMap<String, String>) -> Result<()> {
        self.start_internal(environment, true, true).await
    }

    pub async fn stop_quiet(&self) -> Result<()> {
        self.stop_internal(true).await
    }

    pub async fn restart_quiet(&self, service: Option<&str>) -> Result<()> {
        self.restart_internal(service, true).await
    }

    async fn start_internal(
        &self,
        environment: &BTreeMap<String, String>,
        detach: bool,
        quiet: bool,
    ) -> Result<()> {
        let args = if detach {
            vec!["up", "--detach", "--remove-orphans"]
        } else {
            vec!["up", "--remove-orphans"]
        };
        let status = self.compose_with_stdin(&args, environment, quiet).await?;
        if status.success() {
            Ok(())
        } else {
            Err(RepoboxError::new(
                ErrorKind::Runtime,
                "compose_runtime_failed",
                format!("Docker Compose exited with {status}"),
            ))
        }
    }

    async fn stop_internal(&self, quiet: bool) -> Result<()> {
        let mut command = Command::new("docker");
        command.current_dir(&self.repository).arg("compose");
        self.add_profiles(&mut command);
        self.add_files(&mut command);
        command
            .args(["-p", &self.project_name, "stop"])
            .envs(&self.global_environment);
        if quiet {
            command.stdout(Stdio::null()).stderr(Stdio::null());
        }
        let status = command.status().await?;
        if status.success() {
            Ok(())
        } else {
            Err(RepoboxError::new(
                ErrorKind::Runtime,
                "compose_stop_failed",
                format!("Docker Compose exited with {status}"),
            ))
        }
    }

    async fn restart_internal(&self, service: Option<&str>, quiet: bool) -> Result<()> {
        let mut command = Command::new("docker");
        command.current_dir(&self.repository).arg("compose");
        self.add_profiles(&mut command);
        self.add_files(&mut command);
        command
            .args(["-p", &self.project_name, "restart"])
            .envs(&self.global_environment);
        if let Some(service) = service {
            command.arg(service);
        }
        if quiet {
            command.stdout(Stdio::null()).stderr(Stdio::null());
        }
        let status = command.status().await?;
        if status.success() {
            Ok(())
        } else {
            Err(RepoboxError::new(
                ErrorKind::Runtime,
                "compose_restart_failed",
                format!("Docker Compose exited with {status}"),
            ))
        }
    }

    fn add_profiles(&self, command: &mut Command) {
        for profile in &self.profiles {
            command.arg("--profile").arg(profile);
        }
    }

    fn add_files(&self, command: &mut Command) {
        for file in &self.files {
            command.arg("-f").arg(file);
        }
    }
}

fn transform_project(
    mut project: ComposeProject,
    remote_services: &BTreeSet<String>,
    environment_by_service: &BTreeMap<String, BTreeMap<String, String>>,
    global_environment: &BTreeMap<String, String>,
) -> ComposeProject {
    for remote in remote_services {
        project.services.remove(remote);
    }
    for (name, service) in &mut project.services {
        service.remove_dependencies(remote_services);
        if let Some(environment) = environment_by_service.get(name) {
            for (key, source_key) in environment {
                if let Some(value) = global_environment.get(source_key) {
                    service.environment.insert(key.clone(), value.clone());
                }
            }
        }
    }
    project
}

#[async_trait]
impl RuntimeDriver for ComposeRuntime {
    fn name(&self) -> &'static str {
        "compose"
    }

    async fn detect(&self, repository: &Path) -> Result<RuntimeDetection> {
        detect_repository(repository).await
    }

    async fn status(&self) -> Result<RuntimeStatus> {
        let mut command = Command::new("docker");
        command.current_dir(&self.repository).arg("compose");
        self.add_profiles(&mut command);
        self.add_files(&mut command);
        let output = command
            .args(["-p", &self.project_name, "ps", "--format", "json", "--all"])
            .envs(&self.global_environment)
            .output()
            .await?;
        if !output.status.success() {
            return Ok(RuntimeStatus {
                running: false,
                services: vec![],
            });
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut services = vec![];
        let values = parse_status_values(&stdout)?;
        for value in values {
            services.push(RuntimeServiceStatus {
                name: value
                    .get("Service")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown")
                    .to_owned(),
                state: value
                    .get("State")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown")
                    .to_owned(),
                health: value
                    .get("Health")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned),
            });
        }
        Ok(RuntimeStatus {
            running: services.iter().any(|service| service.state == "running"),
            services,
        })
    }

    async fn start(&self, environment: &BTreeMap<String, String>, detach: bool) -> Result<()> {
        self.start_internal(environment, detach, false).await
    }

    async fn stop(&self) -> Result<()> {
        self.stop_internal(false).await
    }

    async fn restart(&self, service: Option<&str>) -> Result<()> {
        self.restart_internal(service, false).await
    }
}

fn parse_status_values(stdout: &str) -> Result<Vec<serde_json::Value>> {
    let trimmed = stdout.trim();
    if trimmed.is_empty() {
        return Ok(vec![]);
    }
    match serde_json::from_str::<serde_json::Value>(trimmed) {
        Ok(serde_json::Value::Array(values)) => Ok(values),
        Ok(value @ serde_json::Value::Object(_)) => Ok(vec![value]),
        Ok(_) => Ok(vec![]),
        Err(_) => trimmed
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                serde_json::from_str(line).map_err(|error| {
                    RepoboxError::new(
                        ErrorKind::Runtime,
                        "compose_status_invalid",
                        format!("invalid Docker Compose status JSON: {error}"),
                    )
                })
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ComposeEnvironment, ComposeService};

    #[test]
    fn transformation_removes_remote_db_and_injects_process_environment() {
        let mut project = ComposeProject::default();
        project
            .services
            .insert("db".to_owned(), ComposeService::default());
        project.services.insert(
            "app".to_owned(),
            ComposeService {
                depends_on: serde_json::json!({"db": {"condition": "service_healthy"}}),
                ..ComposeService::default()
            },
        );
        let remote = ["db".to_owned()].into_iter().collect();
        let mappings = [(
            "app".to_owned(),
            [("DATABASE_URL".to_owned(), "DATABASE_URL".to_owned())]
                .into_iter()
                .collect(),
        )]
        .into_iter()
        .collect();
        let secrets = [(
            "DATABASE_URL".to_owned(),
            "postgresql://user:secret@example.test/app".to_owned(),
        )]
        .into_iter()
        .collect();
        let transformed = transform_project(project, &remote, &mappings, &secrets);
        assert!(!transformed.services.contains_key("db"));
        let app = &transformed.services["app"];
        assert!(app.dependencies().is_empty());
        assert_eq!(
            app.environment.as_map()["DATABASE_URL"],
            "postgresql://user:secret@example.test/app"
        );
        assert!(matches!(app.environment, ComposeEnvironment::Map(_)));
    }

    #[test]
    fn status_parser_accepts_array_and_json_lines() {
        let array = r#"[{"Service":"web","State":"running"}]"#;
        let lines = "{\"Service\":\"web\",\"State\":\"running\"}\n{\"Service\":\"worker\",\"State\":\"exited\"}\n";
        assert_eq!(parse_status_values(array).unwrap().len(), 1);
        assert_eq!(parse_status_values(lines).unwrap().len(), 2);
    }
}
