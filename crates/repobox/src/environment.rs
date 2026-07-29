use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use chrono::Utc;
use repobox_core::config::{
    BootstrapMode, RemoteServiceConfig, RepoboxConfig, RuntimeConfig, ServiceConfig,
};
use repobox_core::identity::provider_branch_name;
use repobox_core::jobs::{JobKind, JobRecord, JobStatus, JobStore, StepStatus};
use repobox_core::output::{DryRunPlan, PlannedCall};
use repobox_core::provider::{
    Backup, Branch, CreateBranchRequest, CreateDatabaseRequest, CreateRoleRequest,
    DatabaseProvider, connection_urls,
};
use repobox_core::state::{
    DatabaseBinding, EnvironmentRecord, EnvironmentStatus, ProjectState, StateStore,
};
use repobox_core::{ErrorKind, RepoboxError, Result};
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, Command};
use tokio::sync::watch;
use url::Url;

use crate::credentials::CredentialStore;
use crate::output::Output;

/// `PlanetScale` Postgres branches have taken almost twelve minutes to become ready in live smoke
/// tests. Keep a single named budget for every asynchronous provider resource so a slow-but-valid
/// operation remains resumable without encouraging a duplicate create request.
const PROVIDER_READINESS_TIMEOUT: Duration = Duration::from_mins(15);
const PROVIDER_READINESS_POLL_INTERVAL: Duration = Duration::from_secs(2);
const PROCESS_STDERR_TAIL_BYTES: usize = 64 * 1024;
const FAILED_STREAM_EXIT_GRACE_PERIOD: Duration = Duration::from_secs(2);
const DOCKER_CLEANUP_COMMAND_TIMEOUT: Duration = Duration::from_secs(1);
const LOCAL_POSTGRES_READINESS_TIMEOUT: Duration = Duration::from_mins(1);
const LOCAL_POSTGRES_READINESS_POLL_INTERVAL: Duration = Duration::from_millis(500);
const CONTROLLED_COMMAND_SCRIPT: &str = r#"
child=
watcher=
cleanup() {
  [ -z "$watcher" ] || kill -KILL "$watcher" 2>/dev/null || true
  [ -z "$child" ] || kill -KILL "$child" 2>/dev/null || true
}
trap cleanup EXIT HUP INT TERM
exec 3<&0
"$@" </dev/null 3<&- &
child=$!
(
  while IFS= read -r line; do :; done
  kill -KILL "$child" 2>/dev/null || true
) <&3 &
watcher=$!
exec 3<&-
wait "$child"
status=$?
kill -KILL "$watcher" 2>/dev/null || true
wait "$watcher" 2>/dev/null || true
trap - EXIT HUP INT TERM
exit "$status"
"#;

#[derive(Clone, Debug)]
pub struct OperationCancellation {
    sender: watch::Sender<bool>,
}

impl Default for OperationCancellation {
    fn default() -> Self {
        let (sender, _) = watch::channel(false);
        Self { sender }
    }
}

impl OperationCancellation {
    pub fn cancel(&self) {
        self.sender.send_replace(true);
    }

    pub fn is_cancelled(&self) -> bool {
        *self.sender.borrow()
    }

    pub async fn cancelled(&self) {
        let mut receiver = self.sender.subscribe();
        if *receiver.borrow() {
            return;
        }
        while receiver.changed().await.is_ok() {
            if *receiver.borrow_and_update() {
                return;
            }
        }
    }

    pub(crate) fn check(&self) -> Result<()> {
        if self.is_cancelled() {
            Err(operation_interrupted())
        } else {
            Ok(())
        }
    }
}

fn operation_interrupted() -> RepoboxError {
    RepoboxError::new(
        ErrorKind::Runtime,
        "operation_interrupted",
        "the operation was interrupted after its latest durable checkpoint; cleanup completed",
    )
    .with_suggestion("Run `repobox job view latest --json`, then resume the exact job UUID.")
}

fn interruption_error(error: &RepoboxError) -> bool {
    matches!(
        error.code.as_str(),
        "operation_interrupted" | "operation_interrupted_cleanup_incomplete"
    )
}

fn finish_with_cleanup<T>(
    operation: Result<T>,
    cleanup: Result<()>,
    residual: impl AsRef<str>,
) -> Result<T> {
    match (operation, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(cleanup_error)) => Err(RepoboxError::new(
            ErrorKind::Runtime,
            "operation_cleanup_failed",
            format!(
                "operation completed but cleanup failed; {} may remain: {}",
                residual.as_ref(),
                cleanup_error.message
            ),
        )
        .with_suggestion(
            "Inspect the named residual resource, remove it safely, then view the durable job.",
        )),
        (Err(error), Err(cleanup_error)) => {
            let code = if interruption_error(&error) {
                "operation_interrupted_cleanup_incomplete"
            } else {
                "operation_cleanup_failed"
            };
            Err(RepoboxError::new(
                ErrorKind::Runtime,
                code,
                format!(
                    "{}; cleanup failed and {} may remain: {}",
                    error.message,
                    residual.as_ref(),
                    cleanup_error.message
                ),
            )
            .with_suggestion(
                "Inspect the named residual resource, remove it safely, then resume the exact durable job.",
            ))
        }
    }
}

async fn finish_failed_compose_start<Cleanup>(
    start_error: RepoboxError,
    service: &str,
    cleanup: Cleanup,
) -> Result<()>
where
    Cleanup: Future<Output = Result<()>>,
{
    finish_with_cleanup(
        Err(start_error),
        cleanup.await,
        format!("Compose source service `{service}`"),
    )
}

#[derive(Debug)]
struct DockerContainerCleanup {
    name: String,
    armed: bool,
}

impl DockerContainerCleanup {
    fn new(name: String) -> Self {
        Self { name, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }

    fn run(&mut self) -> std::io::Result<()> {
        if !self.armed {
            return Ok(());
        }
        self.armed = false;
        for attempt in 0..5 {
            let name = self.name.clone();
            let (sender, receiver) = std::sync::mpsc::sync_channel(1);
            std::thread::spawn(move || {
                let remove = std::process::Command::new("docker")
                    .args(["rm", "--force", "--volumes", &name])
                    .stdin(Stdio::null())
                    .output();
                let outcome = match remove {
                    Ok(output) if output.status.success() => DockerCleanupAttempt::Succeeded,
                    Ok(_) => {
                        let inspection = std::process::Command::new("docker")
                            .args(["container", "inspect", &name])
                            .stdin(Stdio::null())
                            .stdout(Stdio::null())
                            .output();
                        match inspection {
                            Ok(output) => classify_docker_cleanup_commands(
                                Some(false),
                                Some(output.status.success()),
                                &String::from_utf8_lossy(&output.stderr),
                            ),
                            Err(_) => classify_docker_cleanup_commands(Some(false), None, ""),
                        }
                    }
                    Err(_) => classify_docker_cleanup_commands(None, None, ""),
                };
                let _ = sender.send(outcome);
            });
            let outcome = match receiver.recv_timeout(DOCKER_CLEANUP_COMMAND_TIMEOUT) {
                Ok(outcome) => outcome,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => DockerCleanupAttempt::TimedOut,
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    DockerCleanupAttempt::Disconnected
                }
            };
            if let Some(result) = docker_cleanup_attempt_result(&self.name, outcome, attempt == 4) {
                return result;
            }
            if attempt < 4 {
                std::thread::sleep(Duration::from_millis(50));
            }
        }
        unreachable!("the final Docker cleanup attempt always returns a result")
    }
}

impl Drop for DockerContainerCleanup {
    fn drop(&mut self) {
        let _ = self.run();
    }
}

#[derive(Clone, Copy, Debug)]
enum DockerCleanupAttempt {
    Succeeded,
    Failed,
    TimedOut,
    Disconnected,
}

fn classify_docker_cleanup_commands(
    remove_succeeded: Option<bool>,
    inspect_succeeded: Option<bool>,
    inspect_stderr: &str,
) -> DockerCleanupAttempt {
    match (remove_succeeded, inspect_succeeded) {
        (Some(true), _) => DockerCleanupAttempt::Succeeded,
        (Some(false), Some(false))
            if {
                let stderr = inspect_stderr.to_ascii_lowercase();
                stderr.contains("no such object") || stderr.contains("no such container")
            } =>
        {
            DockerCleanupAttempt::Succeeded
        }
        (Some(false), Some(true)) => DockerCleanupAttempt::Failed,
        (Some(false), Some(false) | None) | (None, _) => DockerCleanupAttempt::Disconnected,
    }
}

fn docker_cleanup_attempt_result(
    name: &str,
    outcome: DockerCleanupAttempt,
    final_attempt: bool,
) -> Option<std::io::Result<()>> {
    match outcome {
        DockerCleanupAttempt::Succeeded => Some(Ok(())),
        DockerCleanupAttempt::TimedOut => Some(Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!(
                "timed out removing managed Docker container `{name}`; the cleanup command may still be running"
            ),
        ))),
        DockerCleanupAttempt::Failed | DockerCleanupAttempt::Disconnected if final_attempt => {
            Some(Err(std::io::Error::other(format!(
                "could not remove managed Docker container `{name}` after 5 attempts"
            ))))
        }
        DockerCleanupAttempt::Failed | DockerCleanupAttempt::Disconnected => None,
    }
}

struct ManagedChild {
    child: Child,
    cleanup: Option<DockerContainerCleanup>,
    #[cfg(unix)]
    process_group: Option<nix::unistd::Pid>,
    active: bool,
}

impl ManagedChild {
    fn spawn(
        mut command: Command,
        cleanup: Option<DockerContainerCleanup>,
    ) -> std::io::Result<Self> {
        command.kill_on_drop(true);
        #[cfg(unix)]
        command.process_group(0);
        let child = command.spawn()?;
        #[cfg(unix)]
        let process_group = child
            .id()
            .and_then(|id| i32::try_from(id).ok())
            .map(nix::unistd::Pid::from_raw);
        Ok(Self {
            child,
            cleanup,
            #[cfg(unix)]
            process_group,
            active: true,
        })
    }

    fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
        let status = self.child.try_wait()?;
        if let Some(status) = status {
            self.finish(status);
        }
        Ok(status)
    }

    async fn wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
        let status = self.child.wait().await?;
        self.finish(status);
        Ok(status)
    }

    fn start_kill(&mut self) -> std::io::Result<()> {
        let process_group_result = self.kill_process_group();
        let child_result = match self.child.start_kill() {
            Err(error) if error.kind() == std::io::ErrorKind::InvalidInput => Ok(()),
            result => result,
        };
        let cleanup_result = self
            .cleanup
            .as_mut()
            .map_or(Ok(()), DockerContainerCleanup::run);
        cleanup_result.and(process_group_result).and(child_result)
    }

    fn finish(&mut self, _status: std::process::ExitStatus) {
        self.active = false;
        if let Some(cleanup) = &mut self.cleanup {
            // A normally reaped `docker run --rm` process has already completed
            // Docker's own container removal, regardless of the inner psql exit status.
            cleanup.disarm();
        }
        #[cfg(unix)]
        {
            self.process_group = None;
        }
    }

    #[cfg(unix)]
    fn kill_process_group(&self) -> std::io::Result<()> {
        let Some(process_group) = self.process_group else {
            return Ok(());
        };
        match nix::sys::signal::killpg(process_group, nix::sys::signal::Signal::SIGKILL) {
            Ok(()) | Err(nix::errno::Errno::ESRCH) => Ok(()),
            Err(error) => Err(std::io::Error::from_raw_os_error(error as i32)),
        }
    }

    #[cfg(not(unix))]
    fn kill_process_group(&self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Drop for ManagedChild {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let _ = self.kill_process_group();
        let _ = self.child.start_kill();
        if let Some(cleanup) = &mut self.cleanup {
            let _ = cleanup.run();
        }
    }
}

struct PsqlCommand {
    command: Command,
    cleanup: Option<DockerContainerCleanup>,
}

impl PsqlCommand {
    fn command_mut(&mut self) -> &mut Command {
        &mut self.command
    }

    fn spawn(mut self) -> std::io::Result<ManagedChild> {
        ManagedChild::spawn(self.command, self.cleanup.take())
    }

    async fn output(mut self) -> std::io::Result<std::process::Output> {
        self.command.stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = self.spawn()?;
        let mut stdout = child.child.stdout.take().expect("psql stdout is piped");
        let mut stderr = child.child.stderr.take().expect("psql stderr is piped");
        let mut stdout_bytes = Vec::new();
        let mut stderr_bytes = Vec::new();
        let (status, stdout_result, stderr_result) = tokio::join!(
            child.wait(),
            stdout.read_to_end(&mut stdout_bytes),
            stderr.read_to_end(&mut stderr_bytes),
        );
        stdout_result?;
        stderr_result?;
        Ok(std::process::Output {
            status: status?,
            stdout: stdout_bytes,
            stderr: stderr_bytes,
        })
    }
}

#[derive(Clone, Copy, Debug)]
struct ProviderReadinessPolicy {
    timeout: Duration,
    poll_interval: Duration,
}

impl Default for ProviderReadinessPolicy {
    fn default() -> Self {
        Self {
            timeout: PROVIDER_READINESS_TIMEOUT,
            poll_interval: PROVIDER_READINESS_POLL_INTERVAL,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ProvisionOptions {
    pub create_backup: bool,
    pub wait_for_backup: bool,
    pub selected_services: BTreeSet<String>,
}

#[derive(Clone, Copy)]
struct RemoteDatabaseRef<'a> {
    organization: &'a str,
    database: &'a str,
    branch: &'a str,
}

async fn replay_extensions(target: &Url, extensions: &[String], resource: &str) -> Result<()> {
    const MANAGED: &[&str] = &[
        "plpgsql",
        "hypopg",
        "pgextwlist",
        "pginsights",
        "pg_pscale_utils",
        "pg_strict",
    ];
    const RESTART_REQUIRED: &[&str] = &[
        "pg_cron",
        "pg_duckdb",
        "pg_hint_plan",
        "pg_partman_bgw",
        "pg_squeeze",
        "pg_stat_statements",
        "timescaledb",
    ];
    let blocked = extensions
        .iter()
        .filter(|extension| {
            RESTART_REQUIRED
                .iter()
                .any(|required| extension.eq_ignore_ascii_case(required))
        })
        .cloned()
        .collect::<Vec<_>>();
    if !blocked.is_empty() {
        return Err(RepoboxError::new(
            ErrorKind::Conflict,
            "extensions_require_dashboard",
            format!(
                "{resource} requires dashboard/restart extensions: {}",
                blocked.join(", ")
            ),
        )
        .with_suggestion(
            "Enable the listed extensions for the target branch in PlanetScale, then resume the job.",
        )
        .with_doc_url("https://planetscale.com/docs/postgres/extensions"));
    }
    let mut extensions = extensions
        .iter()
        .filter(|extension| !MANAGED.contains(&extension.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    extensions.sort_by(|left, right| {
        extension_priority(left)
            .cmp(&extension_priority(right))
            .then_with(|| left.cmp(right))
    });
    for extension in extensions {
        let identifier = extension.replace('"', "\"\"");
        run_psql(
            target,
            &format!("CREATE EXTENSION IF NOT EXISTS \"{identifier}\""),
        )
        .await
        .map_err(|error| {
            RepoboxError::new(
                error.kind,
                "extension_replay_failed",
                format!("could not enable extension `{extension}` on {resource}: {}", error.message),
            )
            .with_suggestion(
                "Verify PlanetScale supports the extension and that the Repobox role inherits `postgres`.",
            )
            .with_doc_url("https://planetscale.com/docs/postgres/extensions")
        })?;
    }
    Ok(())
}

fn extension_priority(extension: &str) -> u8 {
    match extension {
        "cube" | "postgis" | "vector" => 0,
        "earthdistance" | "address_standardizer" | "postgis_topology" | "vectorscale" => 1,
        _ => 2,
    }
}

async fn run_psql(url: &Url, sql: &str) -> Result<String> {
    let mut command = psql_command(url)?;
    command
        .command_mut()
        .args([
            "--no-psqlrc",
            "--set",
            "ON_ERROR_STOP=1",
            "--tuples-only",
            "--no-align",
            "--command",
            sql,
        ])
        .stdin(Stdio::null());
    let output = command.output().await?;
    if output.status.success() {
        return Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned());
    }
    let mut redactor = repobox_core::redaction::SecretRedactor::default();
    if let Some(password) = url.password() {
        redactor.add(password);
    }
    Err(RepoboxError::new(
        ErrorKind::Runtime,
        "psql_failed",
        redactor.redact(String::from_utf8_lossy(&output.stderr).trim()),
    ))
}

fn psql_command(url: &Url) -> Result<PsqlCommand> {
    let host = url.host_str().ok_or_else(|| {
        RepoboxError::new(
            ErrorKind::Runtime,
            "database_url_host_missing",
            "database connection URL has no host",
        )
    })?;
    let database = url.path().trim_start_matches('/');
    if database.is_empty() || url.username().is_empty() {
        return Err(RepoboxError::new(
            ErrorKind::Runtime,
            "database_url_invalid",
            "database connection URL must include username and database name",
        ));
    }
    let mut environment = BTreeMap::from([
        ("PGHOST", host.to_owned()),
        (
            "PGPORT",
            url.port_or_known_default().unwrap_or(5432).to_string(),
        ),
        ("PGUSER", url.username().to_owned()),
        ("PGDATABASE", database.to_owned()),
    ]);
    if let Some(password) = url.password() {
        environment.insert("PGPASSWORD", password.to_owned());
    }
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "sslmode" => {
                environment.insert("PGSSLMODE", value.into_owned());
            }
            "sslrootcert" => {
                environment.insert("PGSSLROOTCERT", value.into_owned());
            }
            _ => {}
        }
    }
    let requires_system_root_cert = url
        .query_pairs()
        .any(|(key, value)| key == "sslrootcert" && value == "system");
    let local_psql = std::process::Command::new("psql")
        .arg("--version")
        .stdin(Stdio::null())
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| {
            parse_psql_major_version(&output.stdout)
                .or_else(|| parse_psql_major_version(&output.stderr))
        })
        .is_some_and(|major| psql_major_version_is_compatible(major, requires_system_root_cert));
    let (mut command, cleanup) = if local_psql {
        (Command::new("psql"), None)
    } else {
        let id = uuid::Uuid::now_v7().simple().to_string();
        let container_name = format!("repobox-psql-{}", &id[id.len() - 12..]);
        let mut command = Command::new("docker");
        command.args([
            "run",
            "--rm",
            "--name",
            &container_name,
            "--label",
            "io.repobox.managed=psql",
            "-i",
        ]);
        for key in environment.keys() {
            command.arg("-e").arg(key);
        }
        command.args(["postgres:18", "psql"]);
        (command, Some(DockerContainerCleanup::new(container_name)))
    };
    command.envs(environment);
    Ok(PsqlCommand { command, cleanup })
}

fn parse_psql_major_version(output: &[u8]) -> Option<u32> {
    std::str::from_utf8(output)
        .ok()?
        .split_whitespace()
        .find_map(|component| {
            let digits = component
                .chars()
                .take_while(char::is_ascii_digit)
                .collect::<String>();
            if digits.is_empty() {
                None
            } else {
                digits.parse().ok()
            }
        })
}

const fn psql_major_version_is_compatible(major: u32, requires_system_root_cert: bool) -> bool {
    !requires_system_root_cert || major >= 16
}

#[derive(Clone, Debug, Serialize)]
pub struct EnvironmentMutation {
    pub environment: EnvironmentRecord,
    pub job: JobRecord,
    pub resumed: bool,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct BranchDeletionTarget {
    organization: String,
    database: String,
    branch: String,
}

type EnvironmentDeletionTargets = BTreeMap<String, BTreeMap<BranchDeletionTarget, bool>>;

pub struct EnvironmentManager<'a> {
    config: &'a RepoboxConfig,
    repository: PathBuf,
    provider: &'a dyn DatabaseProvider,
    credentials: &'a CredentialStore,
    state_store: StateStore,
    jobs: JobStore,
    output: &'a Output,
    readiness: ProviderReadinessPolicy,
    cancellation: OperationCancellation,
}

impl<'a> EnvironmentManager<'a> {
    pub fn new(
        config: &'a RepoboxConfig,
        repository: impl Into<PathBuf>,
        provider: &'a dyn DatabaseProvider,
        credentials: &'a CredentialStore,
        state_store: StateStore,
        jobs: JobStore,
        output: &'a Output,
    ) -> Self {
        Self {
            config,
            repository: repository.into(),
            provider,
            credentials,
            state_store,
            jobs,
            output,
            readiness: ProviderReadinessPolicy::default(),
            cancellation: OperationCancellation::default(),
        }
    }

    pub fn with_cancellation(mut self, cancellation: OperationCancellation) -> Self {
        self.cancellation = cancellation;
        self
    }

    #[cfg(test)]
    fn with_readiness_policy(mut self, readiness: ProviderReadinessPolicy) -> Self {
        self.readiness = readiness;
        self
    }

    pub fn create_plan(&self, environment: &str, options: &ProvisionOptions) -> Result<DryRunPlan> {
        let branch = provider_branch_name(self.config.project.id, environment)?;
        let services = self.selected_services(options)?;
        let mut calls = vec![];
        for (name, service) in services {
            let RemoteServiceConfig::Planetscale {
                organization,
                database,
                base_branch,
                ..
            } = &service.remote;
            calls.extend([
                PlannedCall {
                    provider: "planetscale".to_owned(),
                    action: "ensure_database".to_owned(),
                    resource: format!("{organization}/{database}"),
                },
                PlannedCall {
                    provider: "planetscale".to_owned(),
                    action: "restore_latest_backup".to_owned(),
                    resource: format!("{organization}/{database}/{base_branch} -> {branch}"),
                },
                PlannedCall {
                    provider: "planetscale".to_owned(),
                    action: "ensure_role".to_owned(),
                    resource: format!("{organization}/{database}/{branch} ({name})"),
                },
            ]);
        }
        Ok(DryRunPlan {
            operation: "environment_create".to_owned(),
            environment: environment.to_owned(),
            provider_calls: calls,
            warnings: vec![
                "Each restored PlanetScale Postgres branch has its own billable cluster."
                    .to_owned(),
            ],
            estimated_cost: Some(
                "Provider pricing depends on the selected smallest eligible SKU.".to_owned(),
            ),
            rollback_available: true,
        })
    }

    pub fn delete_plan(&self, environment: &str) -> Result<DryRunPlan> {
        let state = self.state_store.load(self.config.project.id)?;
        let record = state.environments.get(environment).ok_or_else(|| {
            RepoboxError::new(
                ErrorKind::NotFound,
                "environment_not_found",
                format!("environment `{environment}` is not known locally"),
            )
        })?;
        let mutation_lineages =
            nonterminal_environment_mutation_jobs(&self.jobs, self.config.project.id, environment)?;
        let checkpoint_jobs = unreconciled_environment_checkpoint_jobs(
            &self.jobs,
            self.config.project.id,
            environment,
        )?;
        let cleanup_jobs = mutation_jobs_to_reconcile(&mutation_lineages, &checkpoint_jobs);
        let targets = environment_deletion_targets(record, &cleanup_jobs)?;
        let provider_calls = targets
            .into_iter()
            .flat_map(|(service, targets)| {
                targets.into_keys().map(move |target| PlannedCall {
                    provider: "planetscale".to_owned(),
                    action: "delete_branch".to_owned(),
                    resource: format!(
                        "{}/{}/{} ({service})",
                        target.organization, target.database, target.branch
                    ),
                })
            })
            .collect();
        Ok(DryRunPlan {
            operation: "environment_delete".to_owned(),
            environment: environment.to_owned(),
            provider_calls,
            warnings: vec!["Provider branch deletion is irreversible.".to_owned()],
            estimated_cost: None,
            rollback_available: false,
        })
    }

    pub async fn ensure(
        &mut self,
        environment: &str,
        options: &ProvisionOptions,
    ) -> Result<EnvironmentMutation> {
        self.ensure_with_job(environment, options, None).await
    }

    pub async fn resume_create(
        &mut self,
        job_id: uuid::Uuid,
        environment: &str,
        options: &ProvisionOptions,
    ) -> Result<EnvironmentMutation> {
        let job = self.jobs.get(job_id)?;
        self.ensure_with_job(environment, options, Some(job)).await
    }

    async fn ensure_with_job(
        &mut self,
        environment: &str,
        options: &ProvisionOptions,
        exact_job: Option<JobRecord>,
    ) -> Result<EnvironmentMutation> {
        repobox_core::identity::validate_environment_name(environment)?;
        let provider_branch = provider_branch_name(self.config.project.id, environment)?;
        let mut state = self.state_store.load(self.config.project.id)?;
        let selected = self.selected_services(options)?;
        let (mut job, resumed) = if let Some(job) = exact_job {
            (
                self.prepare_resumable_job(
                    job,
                    JobKind::EnvironmentCreate,
                    environment,
                    "provision:",
                    selected.keys(),
                )?,
                true,
            )
        } else {
            self.resumable_job(
                JobKind::EnvironmentCreate,
                environment,
                "provision:",
                selected.keys(),
            )?
        };
        if let Some(record) = state.environments.get(environment) {
            validate_selected_binding_identity(record, &selected, &provider_branch, false)?;
        }
        prepare_create_job_resources(&mut job, &selected, &provider_branch, resumed)?;
        job.status = JobStatus::Running;
        self.jobs.append(&job)?;

        let record = state
            .environments
            .entry(environment.to_owned())
            .or_insert_with(|| EnvironmentRecord::new(environment, &provider_branch));
        record.status = EnvironmentStatus::Provisioning;
        record.failures.clear();
        record.updated_at = Utc::now();
        self.state_store.save(&state)?;

        let mut failures = vec![];
        let mut interruption = None;
        for (name, service) in selected {
            let step = format!("provision:{name}");
            job.update_step(&step, StepStatus::Running, None)?;
            self.jobs.append(&job)?;
            self.event(
                "step_started",
                &serde_json::json!({"job_id": job.id, "step": step}),
            )?;
            let bootstrap_marker = bootstrap_service_marker(&name, &service);
            let result = if service.bootstrap.mode == BootstrapMode::Import {
                match self.import_local_service(&name, &service).await {
                    Ok(()) => {
                        state.bootstrapped_services.insert(bootstrap_marker);
                        self.state_store.save(&state)?;
                        self.provision_service(&provider_branch, &name, &service, options)
                            .await
                    }
                    Err(error) => Err(error),
                }
            } else {
                self.provision_service(&provider_branch, &name, &service, options)
                    .await
            };
            match result {
                Ok(binding) => {
                    state
                        .environments
                        .get_mut(environment)
                        .expect("environment was inserted")
                        .databases
                        .insert(name.clone(), binding.clone());
                    self.state_store.save(&state)?;
                    job.update_step(
                        &step,
                        StepStatus::Succeeded,
                        Some(format!("{}/{}", binding.database, binding.branch)),
                    )?;
                    update_create_step_binding(&mut job, &step, &binding)?;
                    self.event("step_succeeded", &binding)?;
                }
                Err(error) => {
                    let interrupted = interruption_error(&error);
                    if interrupted {
                        interruption = Some(error.clone());
                    }
                    failures.push(format!("{name}: {}", error.message));
                    job.error_code = Some(error.code.clone());
                    job.update_step(&step, StepStatus::Failed, Some(error.message.clone()))?;
                    self.event(
                        "step_failed",
                        &serde_json::json!({"service": name, "error": error}),
                    )?;
                }
            }
            self.jobs.append(&job)?;
            if interruption.is_some() {
                break;
            }
        }

        if failures.is_empty() && interruption.is_none() {
            let incomplete_steps = job
                .steps
                .iter()
                .filter(|step| step.status != StepStatus::Succeeded)
                .map(|step| step.name.clone())
                .collect::<Vec<_>>();
            let incomplete_services = incomplete_environment_services(
                state
                    .environments
                    .get(environment)
                    .expect("environment existence was checked"),
                self.config,
                &provider_branch,
            );
            if !incomplete_steps.is_empty() || !incomplete_services.is_empty() {
                failures.push(format!(
                    "durable provision is incomplete; unfinished steps: {}; incomplete bindings: {}",
                    if incomplete_steps.is_empty() {
                        "none".to_owned()
                    } else {
                        incomplete_steps.join(", ")
                    },
                    if incomplete_services.is_empty() {
                        "none".to_owned()
                    } else {
                        incomplete_services.join(", ")
                    }
                ));
                job.error_code = Some("environment_provision_incomplete".to_owned());
            }
        }

        let finished_record = {
            let record = state
                .environments
                .get_mut(environment)
                .expect("environment exists");
            record.updated_at = Utc::now();
            if failures.is_empty() {
                record.status = EnvironmentStatus::Ready;
                job.status = JobStatus::Succeeded;
                job.error_code = None;
            } else {
                record.status = EnvironmentStatus::Degraded;
                record.failures.clone_from(&failures);
                job.status = JobStatus::Degraded;
            }
            record.clone()
        };
        self.state_store.save(&state)?;
        self.jobs.append(&job)?;
        let mutation = EnvironmentMutation {
            environment: finished_record,
            job: job.clone(),
            resumed,
        };
        if let Some(error) = interruption {
            Err(error)
        } else if failures.is_empty() {
            Ok(mutation)
        } else {
            Err(RepoboxError::new(
                ErrorKind::Runtime,
                "environment_provision_degraded",
                format!(
                    "environment `{environment}` is degraded: {}",
                    failures.join("; ")
                ),
            )
            .with_suggestion(format!(
                "Fix the reported provider issue and run `repobox job resume {}`.",
                job.id
            )))
        }
    }

    pub async fn delete(
        &mut self,
        environment: &str,
        keep_state: bool,
    ) -> Result<EnvironmentMutation> {
        self.cancellation.check()?;
        let mut state = self.state_store.load(self.config.project.id)?;
        let record = state
            .environments
            .get(environment)
            .cloned()
            .ok_or_else(|| {
                RepoboxError::new(
                    ErrorKind::NotFound,
                    "environment_not_found",
                    format!("environment `{environment}` is not known locally"),
                )
            })?;
        let mutation_lineages =
            nonterminal_environment_mutation_jobs(&self.jobs, self.config.project.id, environment)?;
        let checkpoint_jobs = unreconciled_environment_checkpoint_jobs(
            &self.jobs,
            self.config.project.id,
            environment,
        )?;
        let cleanup_jobs = mutation_jobs_to_reconcile(&mutation_lineages, &checkpoint_jobs);
        let deletion_targets = environment_deletion_targets(&record, &cleanup_jobs)?;
        let mut job = JobRecord::new(
            JobKind::EnvironmentDelete,
            self.config.project.id,
            environment,
            deletion_targets
                .keys()
                .map(|service| format!("delete:{service}")),
        );
        job.status = JobStatus::Running;
        self.jobs.append(&job)?;
        let mut failures = vec![];
        let mut interruption = None;
        for (service, targets) in &deletion_targets {
            if let Err(error) = self.cancellation.check() {
                failures.push(error.message.clone());
                job.error_code = Some(error.code.clone());
                interruption = Some(error);
                break;
            }
            let step = format!("delete:{service}");
            job.update_step(&step, StepStatus::Running, None)?;
            self.jobs.append(&job)?;
            let mut service_failures = vec![];
            let mut absent_branches = 0_usize;
            for (target, checkpointed_credentials) in targets {
                let result = self
                    .provider
                    .delete_branch(&target.organization, &target.database, &target.branch)
                    .await;
                match result {
                    Ok(()) => {}
                    Err(error) if error.kind == ErrorKind::NotFound => {
                        absent_branches += 1;
                    }
                    Err(error) => {
                        service_failures.push(format!("{}: {}", target.branch, error.message));
                        continue;
                    }
                }
                let key =
                    CredentialStore::database_key(self.config.project.id, &target.branch, service);
                let binding_has_credentials = record
                    .databases
                    .get(service)
                    .is_some_and(|binding| binding.branch == target.branch);
                let credential_result = if *checkpointed_credentials
                    || binding_has_credentials
                    || self.credentials.has_database_url_evidence(&key)?
                {
                    self.credentials.remove_database_urls(&key)
                } else {
                    Ok(())
                };
                if let Err(error) = credential_result {
                    service_failures.push(format!("{}: {}", target.branch, error.message));
                }
            }
            if service_failures.is_empty() {
                let provider_message = match (targets.len(), absent_branches) {
                    (_, 0) => None,
                    (1, 1) => Some("provider branch was already absent".to_owned()),
                    (_, count) => Some(format!("{count} provider branches were already absent")),
                };
                job.update_step(&step, StepStatus::Succeeded, provider_message)?;
            } else {
                failures.extend(service_failures.iter().cloned());
                job.update_step(&step, StepStatus::Failed, Some(service_failures.join("; ")))?;
            }
            self.jobs.append(&job)?;
        }
        if failures.is_empty() && interruption.is_none() {
            self.reconcile_environment_mutation_jobs(&cleanup_jobs)?;
            job.status = JobStatus::Succeeded;
            job.error_code = None;
            if !keep_state {
                state.environments.remove(environment);
                self.state_store.save(&state)?;
            }
        } else {
            job.status = JobStatus::Degraded;
        }
        self.jobs.append(&job)?;
        if let Some(error) = interruption {
            return Err(error);
        }
        if !failures.is_empty() {
            return Err(RepoboxError::new(
                ErrorKind::Runtime,
                "environment_delete_degraded",
                failures.join("; "),
            ));
        }
        Ok(EnvironmentMutation {
            environment: record,
            job,
            resumed: false,
        })
    }

    pub async fn delete_many(
        &mut self,
        environments: &[String],
    ) -> Result<Vec<EnvironmentMutation>> {
        let mut deleted = Vec::with_capacity(environments.len());
        for environment in environments {
            self.cancellation.check()?;
            deleted.push(self.delete(environment, false).await?);
        }
        Ok(deleted)
    }

    pub fn pull_plan(&self, environment: &str, options: &ProvisionOptions) -> Result<DryRunPlan> {
        let branch = provider_branch_name(self.config.project.id, environment)?;
        let mut calls = vec![];
        for (_, service) in self.selected_services(options)? {
            let RemoteServiceConfig::Planetscale {
                organization,
                database,
                base_branch,
                ..
            } = &service.remote;
            calls.extend([
                PlannedCall {
                    provider: "planetscale".to_owned(),
                    action: "restore_staging_branch".to_owned(),
                    resource: format!("{organization}/{database}/{base_branch} -> {branch}-next-*"),
                },
                PlannedCall {
                    provider: "planetscale".to_owned(),
                    action: "delete_environment_branch".to_owned(),
                    resource: format!("{organization}/{database}/{branch}"),
                },
                PlannedCall {
                    provider: "planetscale".to_owned(),
                    action: "rename_staging_branch".to_owned(),
                    resource: format!("{organization}/{database}/{branch}"),
                },
            ]);
        }
        Ok(DryRunPlan {
            operation: "environment_pull".to_owned(),
            environment: environment.to_owned(),
            provider_calls: calls,
            warnings: vec![
                "This permanently replaces environment-local database data.".to_owned(),
                "Local services stay stopped if any database swap is incomplete.".to_owned(),
                "No rollback backup of the old environment branch is created.".to_owned(),
            ],
            estimated_cost: Some(
                "A staging branch temporarily overlaps the existing branch during the swap."
                    .to_owned(),
            ),
            rollback_available: false,
        })
    }

    pub async fn pull(
        &mut self,
        environment: &str,
        options: &ProvisionOptions,
    ) -> Result<EnvironmentMutation> {
        self.pull_with_job(environment, options, None).await
    }

    pub async fn resume_pull(
        &mut self,
        job_id: uuid::Uuid,
        environment: &str,
        options: &ProvisionOptions,
    ) -> Result<EnvironmentMutation> {
        let job = self.jobs.get(job_id)?;
        self.pull_with_job(environment, options, Some(job)).await
    }

    async fn pull_with_job(
        &mut self,
        environment: &str,
        options: &ProvisionOptions,
        exact_job: Option<JobRecord>,
    ) -> Result<EnvironmentMutation> {
        let canonical = provider_branch_name(self.config.project.id, environment)?;
        let selected = self.selected_services(options)?;
        let mut state = self.state_store.load(self.config.project.id)?;
        let initial_status = state
            .environments
            .get(environment)
            .map(|record| record.status)
            .ok_or_else(|| {
                RepoboxError::new(
                    ErrorKind::NotFound,
                    "environment_not_found",
                    format!("environment `{environment}` does not exist"),
                )
                .with_suggestion("Run `repobox env create --yes` first.")
            })?;
        let (mut job, resumed) = if let Some(job) = exact_job {
            (
                self.prepare_resumable_job(
                    job,
                    JobKind::EnvironmentPull,
                    environment,
                    "refresh:",
                    selected.keys(),
                )?,
                true,
            )
        } else {
            self.resumable_job(
                JobKind::EnvironmentPull,
                environment,
                "refresh:",
                selected.keys(),
            )?
        };
        if !resumed && initial_status != EnvironmentStatus::Ready {
            return Err(RepoboxError::new(
                ErrorKind::Conflict,
                "environment_not_ready_for_pull",
                format!(
                    "environment `{environment}` is {initial_status:?}; a new pull cannot replace data until its incomplete operation is resolved"
                ),
            )
            .with_suggestion(
                "Resume the exact degraded create or pull job, or delete the environment and recreate it before starting a new pull.",
            ));
        }
        if !resumed {
            validate_selected_binding_identity(
                state
                    .environments
                    .get(environment)
                    .expect("environment existence was checked"),
                &selected,
                &canonical,
                true,
            )?;
        }
        prepare_pull_job_resources(&mut job, &selected, &canonical, resumed)?;
        job.status = JobStatus::Running;
        self.jobs.append(&job)?;
        {
            let record = state
                .environments
                .get_mut(environment)
                .expect("environment existence was checked");
            record.status = EnvironmentStatus::Provisioning;
            record.failures.clear();
            record.updated_at = Utc::now();
        }
        self.state_store.save(&state)?;

        let mut failures = vec![];
        let mut interruption = None;
        for (name, service) in selected {
            let step = format!("refresh:{name}");
            if job
                .steps
                .iter()
                .find(|candidate| candidate.name == step)
                .is_some_and(|step| step.status == StepStatus::Succeeded)
            {
                continue;
            }
            if let Err(error) = self.cancellation.check()
                && !pull_step_may_require_forward_repair(&job, &step)
            {
                failures.push(format!("{name}: {}", error.message));
                job.error_code = Some(error.code.clone());
                interruption = Some(error);
                break;
            }
            job.update_step(&step, StepStatus::Running, None)?;
            self.jobs.append(&job)?;
            self.event(
                "step_started",
                &serde_json::json!({"job_id": job.id, "step": step}),
            )?;
            match self
                .pull_service(&canonical, &name, &service, options, &mut job, &step)
                .await
            {
                Ok(binding) => {
                    state
                        .environments
                        .get_mut(environment)
                        .expect("environment existence was checked")
                        .databases
                        .insert(name.clone(), binding.clone());
                    self.state_store.save(&state)?;
                    job.update_step(
                        &step,
                        StepStatus::Succeeded,
                        Some("provider branch swapped forward".to_owned()),
                    )?;
                    update_pull_step_phase(
                        &mut job,
                        &step,
                        "complete",
                        serde_json::json!({"binding": binding}),
                    )?;
                    self.event("step_succeeded", &binding)?;
                }
                Err(error) => {
                    let interrupted = interruption_error(&error);
                    if interrupted {
                        interruption = Some(error.clone());
                    }
                    failures.push(format!("{name}: {}", error.message));
                    job.error_code = Some(error.code.clone());
                    job.update_step(&step, StepStatus::Failed, Some(error.message.clone()))?;
                    self.event(
                        "step_failed",
                        &serde_json::json!({"service": name, "error": error}),
                    )?;
                }
            }
            self.jobs.append(&job)?;
            if interruption.is_some() {
                break;
            }
        }

        if failures.is_empty() && interruption.is_none() {
            let incomplete_steps = job
                .steps
                .iter()
                .filter(|step| step.status != StepStatus::Succeeded)
                .map(|step| step.name.clone())
                .collect::<Vec<_>>();
            let incomplete_services = incomplete_environment_services(
                state
                    .environments
                    .get(environment)
                    .expect("environment existence was checked"),
                self.config,
                &canonical,
            );
            if !incomplete_steps.is_empty() || !incomplete_services.is_empty() {
                failures.push(format!(
                    "durable pull is incomplete; unfinished steps: {}; incomplete bindings: {}",
                    if incomplete_steps.is_empty() {
                        "none".to_owned()
                    } else {
                        incomplete_steps.join(", ")
                    },
                    if incomplete_services.is_empty() {
                        "none".to_owned()
                    } else {
                        incomplete_services.join(", ")
                    }
                ));
                job.error_code = Some("environment_pull_incomplete".to_owned());
            }
        }

        let finished_record = {
            let record = state
                .environments
                .get_mut(environment)
                .expect("environment existence was checked");
            record.updated_at = Utc::now();
            if failures.is_empty() && interruption.is_none() {
                record.status = EnvironmentStatus::Ready;
                job.status = JobStatus::Succeeded;
                job.error_code = None;
            } else {
                record.status = EnvironmentStatus::Degraded;
                record.failures.clone_from(&failures);
                job.status = JobStatus::Degraded;
            }
            record.clone()
        };
        self.state_store.save(&state)?;
        self.jobs.append(&job)?;
        let mutation = EnvironmentMutation {
            environment: finished_record,
            job: job.clone(),
            resumed,
        };
        if let Some(error) = interruption {
            Err(error)
        } else if failures.is_empty() {
            Ok(mutation)
        } else {
            Err(RepoboxError::new(
                ErrorKind::Runtime,
                "environment_pull_degraded",
                format!("data refresh is incomplete: {}", failures.join("; ")),
            )
            .with_suggestion(format!(
                "Services remain stopped. Resume forward with `repobox job resume {}`.",
                job.id
            )))
        }
    }

    async fn provision_service(
        &mut self,
        provider_branch: &str,
        service_name: &str,
        service: &ServiceConfig,
        options: &ProvisionOptions,
    ) -> Result<DatabaseBinding> {
        self.cancellation.check()?;
        let RemoteServiceConfig::Planetscale {
            organization,
            database,
            base_branch,
            cluster_size,
        } = &service.remote;
        let sizes = if cluster_size == "auto-smallest" {
            self.provider.list_cluster_sizes(organization).await?
        } else {
            vec![cluster_size.clone()]
        };
        let size = select_smallest_size(&sizes)?;
        self.cancellation.check()?;

        let database_exists = self
            .wait_for_existing_database(organization, database)
            .await?;
        self.cancellation.check()?;
        if !database_exists {
            match service.bootstrap.mode {
                BootstrapMode::Attach => {
                    return Err(RepoboxError::new(
                        ErrorKind::NotFound,
                        "planetscale_database_not_found",
                        format!(
                            "PlanetScale database `{organization}/{database}` does not exist"
                        ),
                    )
                    .with_suggestion(
                        "Choose `bootstrap: { mode: empty }` for this service, or attach an existing database.",
                    ));
                }
                BootstrapMode::Empty | BootstrapMode::Import => {
                    self.cancellation.check()?;
                    self.output.progress(&format!(
                        "creating PlanetScale database {organization}/{database}"
                    ));
                    self.provider
                        .create_database(&CreateDatabaseRequest {
                            organization: organization.clone(),
                            name: database.clone(),
                            region: None,
                            cluster_size: size.clone(),
                            major_version: None,
                        })
                        .await?;
                    self.wait_for_database(organization, database).await?;
                    self.cancellation.check()?;
                }
            }
        }

        let branch_exists = self
            .wait_for_existing_branch(organization, database, provider_branch)
            .await?;
        self.cancellation.check()?;
        let key =
            CredentialStore::database_key(self.config.project.id, provider_branch, service_name);
        if branch_exists
            && optional_database_urls(self.credentials.database_urls(&key))?.is_some()
            && let Some(role) = self
                .provider
                .list_roles(organization, database, provider_branch)
                .await?
                .into_iter()
                .find(|role| role.name == role_name(provider_branch, service_name))
        {
            return Ok(DatabaseBinding {
                service: service_name.to_owned(),
                provider: "planetscale".to_owned(),
                organization: organization.clone(),
                database: database.clone(),
                branch: provider_branch.to_owned(),
                role_id: role.id,
                role_name: role.name,
                ready: true,
                updated_at: Utc::now(),
            });
        }

        if !branch_exists {
            self.cancellation.check()?;
            let backup = self
                .latest_backup(
                    organization,
                    database,
                    base_branch,
                    options.create_backup
                        || !database_exists
                        || service.bootstrap.mode == BootstrapMode::Import,
                    options.wait_for_backup
                        || !database_exists
                        || service.bootstrap.mode == BootstrapMode::Import,
                )
                .await?;
            self.cancellation.check()?;
            self.output.progress(&format!(
                "restoring {organization}/{database}/{provider_branch} from backup {}",
                backup.name
            ));
            self.provider
                .create_branch(&CreateBranchRequest {
                    organization: organization.clone(),
                    database: database.clone(),
                    name: provider_branch.to_owned(),
                    parent_branch: base_branch.clone(),
                    backup_id: Some(backup.id),
                    cluster_size: Some(size),
                })
                .await?;
            self.wait_for_branch(organization, database, provider_branch)
                .await?;
            self.cancellation.check()?;
        }

        self.cancellation.check()?;
        let desired_role_name = role_name(provider_branch, service_name);
        let existing = self
            .provider
            .list_roles(organization, database, provider_branch)
            .await?
            .into_iter()
            .find(|role| role.name == desired_role_name);
        if let Some(existing) = existing {
            self.provider
                .delete_role(
                    organization,
                    database,
                    provider_branch,
                    &existing.id,
                    Some("postgres"),
                )
                .await?;
        }
        let role = self
            .provider
            .create_role(&CreateRoleRequest {
                organization: organization.clone(),
                database: database.clone(),
                branch: provider_branch.to_owned(),
                name: desired_role_name.clone(),
                inherited_roles: vec!["postgres".to_owned()],
            })
            .await?;
        if role.password.is_none() {
            return Err(RepoboxError::new(
                ErrorKind::Runtime,
                "planetscale_role_password_missing",
                "PlanetScale did not return the one-time password for a new role",
            ));
        }
        let urls = connection_urls(&role)?;
        self.replay_extensions_from_base(
            organization,
            database,
            base_branch,
            provider_branch,
            &urls.direct,
        )
        .await?;
        self.credentials
            .store_database_urls(&key, urls.pooled.as_str(), urls.direct.as_str())?;
        Ok(DatabaseBinding {
            service: service_name.to_owned(),
            provider: "planetscale".to_owned(),
            organization: organization.clone(),
            database: database.clone(),
            branch: provider_branch.to_owned(),
            role_id: role.id,
            role_name: role.name,
            ready: true,
            updated_at: Utc::now(),
        })
    }

    async fn pull_service(
        &mut self,
        canonical: &str,
        service_name: &str,
        service: &ServiceConfig,
        options: &ProvisionOptions,
        job: &mut JobRecord,
        step: &str,
    ) -> Result<DatabaseBinding> {
        let RemoteServiceConfig::Planetscale {
            organization,
            database,
            base_branch,
            cluster_size,
        } = &service.remote;
        let staging = staging_branch_name(canonical, job.id);
        let mut phase = job
            .steps
            .iter()
            .find(|candidate| candidate.name == step)
            .and_then(|step| step.resource.get("phase"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("planned")
            .to_owned();
        if !matches!(phase.as_str(), "credentialed" | "old_deleted") {
            self.cancellation.check()?;
        }

        if !matches!(phase.as_str(), "credentialed" | "old_deleted") {
            if !self
                .wait_for_existing_database(organization, database)
                .await?
            {
                return Err(RepoboxError::new(
                    ErrorKind::NotFound,
                    "planetscale_database_not_found",
                    format!("PlanetScale database `{organization}/{database}` does not exist"),
                ));
            }
            self.cancellation.check()?;
        }

        if matches!(phase.as_str(), "planned" | "staged") {
            if !self
                .wait_for_existing_branch(organization, database, &staging)
                .await?
            {
                if phase != "planned" {
                    return Err(RepoboxError::new(
                        ErrorKind::Conflict,
                        "pull_staging_branch_missing",
                        format!(
                            "pull step `{step}` is {phase}, but its recorded staging branch `{staging}` is missing"
                        ),
                    )
                    .with_suggestion(
                        "Do not recreate the staging branch implicitly. Inspect the durable job and provider branches, then restart the pull only after reconciling credentials.",
                    ));
                }
                let backup = self
                    .latest_backup(
                        organization,
                        database,
                        base_branch,
                        options.create_backup,
                        options.wait_for_backup,
                    )
                    .await?;
                self.cancellation.check()?;
                let size = if cluster_size == "auto-smallest" {
                    select_smallest_size(&self.provider.list_cluster_sizes(organization).await?)?
                } else {
                    cluster_size.clone()
                };
                self.cancellation.check()?;
                self.provider
                    .create_branch(&CreateBranchRequest {
                        organization: organization.clone(),
                        database: database.clone(),
                        name: staging.clone(),
                        parent_branch: base_branch.clone(),
                        backup_id: Some(backup.id),
                        cluster_size: Some(size),
                    })
                    .await?;
                self.wait_for_branch(organization, database, &staging)
                    .await?;
                self.cancellation.check()?;
            }
            if phase == "planned" {
                "staged".clone_into(&mut phase);
                update_pull_step_phase(job, step, &phase, serde_json::Value::Null)?;
                self.jobs.append(job)?;
            }
            self.cancellation.check()?;
        }

        let staging_key =
            CredentialStore::database_key(self.config.project.id, &staging, service_name);
        let canonical_key =
            CredentialStore::database_key(self.config.project.id, canonical, service_name);
        let desired_role_name = role_name(canonical, service_name);
        if phase == "staged" {
            self.cancellation.check()?;
            let role = self
                .ensure_pull_role(
                    organization,
                    database,
                    &staging,
                    canonical,
                    service_name,
                    &staging_key,
                )
                .await?;
            self.cancellation.check()?;
            let (_, staging_direct) = self.credentials.database_urls(&staging_key)?;
            let staging_direct = Url::parse(&staging_direct).map_err(|error| {
                RepoboxError::new(
                    ErrorKind::Runtime,
                    "database_credential_invalid",
                    error.to_string(),
                )
            })?;
            self.replay_extensions_from_base(
                organization,
                database,
                base_branch,
                &staging,
                &staging_direct,
            )
            .await?;
            "credentialed".clone_into(&mut phase);
            update_pull_step_phase(
                job,
                step,
                &phase,
                serde_json::json!({"role_id": role.id, "role_name": role.name}),
            )?;
            self.jobs.append(job)?;
            self.cancellation.check()?;
        }

        if phase == "credentialed" {
            let branches = self.provider.list_branches(organization, database).await?;
            if !branches.iter().any(|branch| branch.name == staging) {
                return Err(RepoboxError::new(
                    ErrorKind::Conflict,
                    "pull_staging_branch_missing",
                    format!(
                        "credentialed pull step `{step}` has no recorded staging branch `{staging}`"
                    ),
                )
                .with_suggestion(
                    "Do not delete the canonical branch. Inspect the durable job, provider branches, and staging credentials before restarting the pull.",
                ));
            }
            if branches.iter().any(|branch| branch.name == canonical) {
                self.cancellation.check()?;
                self.provider
                    .delete_branch(organization, database, canonical)
                    .await?;
            }
            "old_deleted".clone_into(&mut phase);
            update_pull_step_phase(job, step, &phase, serde_json::Value::Null)?;
            self.jobs.append(job)?;
        }

        if phase == "old_deleted" {
            let branches = self.provider.list_branches(organization, database).await?;
            if branches.iter().any(|branch| branch.name == staging) {
                self.provider
                    .rename_branch(organization, database, &staging, canonical)
                    .await?;
            } else if !branches.iter().any(|branch| branch.name == canonical) {
                return Err(RepoboxError::new(
                    ErrorKind::Conflict,
                    "pull_swap_missing_branches",
                    "neither the staging nor canonical branch exists during a forward-only swap",
                ));
            }
            "renamed".clone_into(&mut phase);
            update_pull_step_phase(job, step, &phase, serde_json::Value::Null)?;
            self.jobs.append(job)?;
        }

        if phase == "renamed" {
            self.cancellation.check()?;
            self.wait_for_branch(organization, database, canonical)
                .await?;
            "swapped".clone_into(&mut phase);
            update_pull_step_phase(job, step, &phase, serde_json::Value::Null)?;
            self.jobs.append(job)?;
        }

        self.cancellation.check()?;
        let (pooled, direct) =
            database_urls_or_fallback(self.credentials.database_urls(&staging_key), || {
                self.credentials.database_urls(&canonical_key)
            })?;
        self.credentials
            .store_database_urls(&canonical_key, &pooled, &direct)?;
        self.credentials.remove_database_urls(&staging_key)?;
        let roles = self
            .provider
            .list_roles(organization, database, canonical)
            .await?;
        let role = roles
            .into_iter()
            .find(|role| role.name == desired_role_name)
            .ok_or_else(|| {
                RepoboxError::new(
                    ErrorKind::NotFound,
                    "planetscale_role_not_found",
                    "the swapped branch's Repobox role was not found",
                )
            })?;
        Ok(DatabaseBinding {
            service: service_name.to_owned(),
            provider: "planetscale".to_owned(),
            organization: organization.clone(),
            database: database.clone(),
            branch: canonical.to_owned(),
            role_id: role.id,
            role_name: role.name,
            ready: true,
            updated_at: Utc::now(),
        })
    }

    async fn ensure_pull_role(
        &self,
        organization: &str,
        database: &str,
        staging_branch: &str,
        canonical_branch: &str,
        service_name: &str,
        credential_key: &str,
    ) -> Result<repobox_core::provider::DatabaseRole> {
        let desired_role_name = role_name(canonical_branch, service_name);
        let existing = self
            .provider
            .list_roles(organization, database, staging_branch)
            .await?
            .into_iter()
            .find(|role| role.name == desired_role_name);
        if optional_database_urls(self.credentials.database_urls(credential_key))?.is_some() {
            return existing.ok_or_else(|| {
                RepoboxError::new(
                    ErrorKind::Conflict,
                    "staging_role_missing",
                    "staging credentials exist but the provider role is missing",
                )
            });
        }
        if let Some(existing) = existing {
            self.provider
                .delete_role(
                    organization,
                    database,
                    staging_branch,
                    &existing.id,
                    Some("postgres"),
                )
                .await?;
        }
        let role = self
            .provider
            .create_role(&CreateRoleRequest {
                organization: organization.to_owned(),
                database: database.to_owned(),
                branch: staging_branch.to_owned(),
                name: desired_role_name,
                inherited_roles: vec!["postgres".to_owned()],
            })
            .await?;
        let urls = connection_urls(&role)?;
        self.credentials.store_database_urls(
            credential_key,
            urls.pooled.as_str(),
            urls.direct.as_str(),
        )?;
        Ok(role)
    }

    async fn latest_backup(
        &self,
        organization: &str,
        database: &str,
        base_branch: &str,
        create: bool,
        wait: bool,
    ) -> Result<Backup> {
        let backups = self
            .provider
            .list_backups(organization, database, base_branch)
            .await?;
        let mut successful = backups
            .iter()
            .filter(|backup| backup.state == "success")
            .cloned()
            .collect::<Vec<_>>();
        successful.sort_by_key(|backup| backup.completed_at.unwrap_or(backup.created_at));
        if let Some(backup) = successful.pop() {
            return Ok(backup);
        }
        if let Some(pending) = backups
            .iter()
            .find(|backup| matches!(backup.state.as_str(), "pending" | "running"))
        {
            if !wait {
                return Err(RepoboxError::new(
                    ErrorKind::Conflict,
                    "planetscale_backup_in_progress",
                    format!("backup `{}` is still {}", pending.name, pending.state),
                )
                .with_suggestion("Rerun with `--wait`, or retry after the backup completes."));
            }
            return self
                .wait_for_backup(
                    organization,
                    database,
                    base_branch,
                    &pending.id,
                    &pending.name,
                )
                .await;
        }
        if !create {
            return Err(RepoboxError::new(
                ErrorKind::Conflict,
                "planetscale_backup_required",
                format!(
                    "no successful backup exists for {organization}/{database}/{base_branch}"
                ),
            )
            .with_suggestion(
                "Create one in PlanetScale, or rerun with `--create-backup` to request an immediate backup.",
            ));
        }
        let name = format!("repobox-{}", Utc::now().format("%Y%m%d%H%M%S"));
        let created = self
            .provider
            .create_backup(organization, database, base_branch, &name)
            .await?;
        self.wait_for_backup(organization, database, base_branch, &created.id, &name)
            .await
    }

    async fn import_local_service(
        &self,
        service_name: &str,
        service: &ServiceConfig,
    ) -> Result<()> {
        self.cancellation.check()?;
        if !self.config.data.allow_copy {
            return Err(RepoboxError::new(
                ErrorKind::Conflict,
                "data_copy_not_approved",
                "local database import requires data.allow_copy: true",
            ));
        }
        let RuntimeConfig::Compose { compose } = &self.config.runtime else {
            return Err(RepoboxError::new(
                ErrorKind::Usage,
                "import_requires_compose",
                "automatic local Postgres import currently requires Docker Compose",
            ));
        };
        let RemoteServiceConfig::Planetscale {
            organization,
            database,
            base_branch,
            cluster_size,
        } = &service.remote;
        let size = if cluster_size == "auto-smallest" {
            select_smallest_size(&self.provider.list_cluster_sizes(organization).await?)?
        } else {
            cluster_size.clone()
        };
        self.cancellation.check()?;
        if !self
            .wait_for_existing_database(organization, database)
            .await?
        {
            self.provider
                .create_database(&CreateDatabaseRequest {
                    organization: organization.clone(),
                    name: database.clone(),
                    region: None,
                    cluster_size: size,
                    major_version: None,
                })
                .await?;
            self.wait_for_database(organization, database).await?;
        }
        self.cancellation.check()?;

        let project_digest =
            Sha256::digest(format!("{}\0{service_name}", self.config.project.id).as_bytes());
        let backup_prefix = format!("repobox-import-{}", &hex::encode(project_digest)[..12]);
        let backups = self
            .provider
            .list_backups(organization, database, base_branch)
            .await?;
        if backups
            .iter()
            .any(|backup| backup.name.starts_with(&backup_prefix) && backup.state == "success")
        {
            self.output.progress(&format!(
                "reusing completed import backup for {organization}/{database}"
            ));
            self.cancellation.check()?;
            return Ok(());
        }
        if let Some(pending) = backups.iter().find(|backup| {
            backup.name.starts_with(&backup_prefix)
                && matches!(backup.state.as_str(), "pending" | "running")
        }) {
            self.wait_for_backup(
                organization,
                database,
                base_branch,
                &pending.id,
                &pending.name,
            )
            .await?;
            self.output.progress(&format!(
                "reusing completed import backup for {organization}/{database}"
            ));
            self.cancellation.check()?;
            return Ok(());
        }

        self.cancellation.check()?;
        let role_name = backup_prefix.clone();
        if let Some(role) = self
            .provider
            .list_roles(organization, database, base_branch)
            .await?
            .into_iter()
            .find(|role| role.name == role_name)
        {
            self.provider
                .delete_role(
                    organization,
                    database,
                    base_branch,
                    &role.id,
                    Some("postgres"),
                )
                .await?;
        }
        self.cancellation.check()?;
        let role = self
            .provider
            .create_role(&CreateRoleRequest {
                organization: organization.clone(),
                database: database.clone(),
                branch: base_branch.clone(),
                name: role_name,
                inherited_roles: vec!["postgres".to_owned()],
            })
            .await?;
        let import_result = async {
            let direct = connection_urls(&role)?.direct;
            let remote = RemoteDatabaseRef {
                organization,
                database,
                branch: base_branch,
            };
            self.copy_compose_database(
                &service.local.compose_service,
                &compose.files,
                &compose.profiles,
                &direct,
                remote,
            )
            .await
        }
        .await;
        let cleanup_result = self
            .provider
            .delete_role(
                organization,
                database,
                base_branch,
                &role.id,
                Some("postgres"),
            )
            .await;
        finish_with_cleanup(
            import_result,
            cleanup_result,
            format!(
                "temporary PlanetScale role `{}` on {organization}/{database}/{base_branch}",
                role.name
            ),
        )?;
        self.cancellation.check()?;
        let backup_name = format!("{backup_prefix}-{}", Utc::now().format("%Y%m%d%H%M%S"));
        let backup = self
            .provider
            .create_backup(organization, database, base_branch, &backup_name)
            .await?;
        self.wait_for_backup(
            organization,
            database,
            base_branch,
            &backup.id,
            &backup_name,
        )
        .await?;
        self.cancellation.check()?;
        self.output.progress(&format!(
            "imported local `{service_name}` into {organization}/{database} and captured backup `{backup_name}`"
        ));
        Ok(())
    }

    async fn copy_compose_database(
        &self,
        compose_service: &str,
        files: &[PathBuf],
        profiles: &[String],
        target: &Url,
        remote: RemoteDatabaseRef<'_>,
    ) -> Result<()> {
        self.cancellation.check()?;
        let was_running = self
            .compose_service_running(files, profiles, compose_service)
            .await?;
        let mut compose = self.compose_command(files, profiles);
        if !was_running {
            self.cancellation.check()?;
            let status = compose
                .args(["up", "--detach", compose_service])
                .status()
                .await?;
            if !status.success() {
                let start_error = RepoboxError::new(
                    ErrorKind::Runtime,
                    "local_postgres_start_failed",
                    format!("Docker Compose exited with {status}"),
                );
                return finish_failed_compose_start(
                    start_error,
                    compose_service,
                    self.stop_compose_service(files, profiles, compose_service),
                )
                .await;
            }
        }

        let copy_result = async {
            self.cancellation.check()?;
            if !was_running {
                wait_for_local_postgres_readiness(
                    &self.cancellation,
                    compose_service,
                    LOCAL_POSTGRES_READINESS_TIMEOUT,
                    LOCAL_POSTGRES_READINESS_POLL_INTERVAL,
                    || {
                        let mut command = self.compose_command(files, profiles);
                        command.args(["exec", "-T", compose_service, "pg_isready", "--quiet"]);
                        async move {
                            let output = command.output().await?;
                            Ok(output.status.success())
                        }
                    },
                )
                .await?;
            }
            self.copy_running_compose_database(compose_service, files, profiles, target, remote)
                .await
        }
        .await;
        let cleanup_result = if was_running {
            Ok(())
        } else {
            self.stop_compose_service(files, profiles, compose_service)
                .await
        };
        finish_with_cleanup(
            copy_result,
            cleanup_result,
            format!("Compose source service `{compose_service}`"),
        )
    }

    async fn copy_running_compose_database(
        &self,
        compose_service: &str,
        files: &[PathBuf],
        profiles: &[String],
        target: &Url,
        remote: RemoteDatabaseRef<'_>,
    ) -> Result<()> {
        let detection = repobox_runtime_compose::detect_configuration(
            &self.repository,
            files,
            profiles,
            &BTreeMap::new(),
        )
        .await?;
        let detected = detection
            .services
            .into_iter()
            .find(|candidate| candidate.name == compose_service)
            .ok_or_else(|| {
                RepoboxError::new(
                    ErrorKind::NotFound,
                    "local_postgres_service_not_found",
                    format!("Compose service `{compose_service}` was not detected"),
                )
            })?;
        let user = detected
            .environment
            .get("POSTGRES_USER")
            .cloned()
            .unwrap_or_else(|| "postgres".to_owned());
        let local_database = detected
            .environment
            .get("POSTGRES_DB")
            .cloned()
            .unwrap_or_else(|| user.clone());

        let extensions = self
            .local_extensions(files, profiles, compose_service, &user, &local_database)
            .await?;
        replay_extensions(
            target,
            &extensions,
            &format!(
                "{}/{}/{}",
                remote.organization, remote.database, remote.branch
            ),
        )
        .await?;

        let mut dump = self.compose_command(files, profiles);
        dump.args([
            "exec",
            "-T",
            compose_service,
            "sh",
            "-c",
            CONTROLLED_COMMAND_SCRIPT,
            "repobox-pg-dump",
            "pg_dump",
            "--clean",
            "--if-exists",
            "--no-owner",
            "--no-acl",
            "--username",
            &user,
            "--dbname",
            &local_database,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
        let dump = ManagedChild::spawn(dump, None)?;
        let mut restore = psql_command(target)?;
        restore
            .command_mut()
            .args(["--no-psqlrc", "--set", "ON_ERROR_STOP=1"])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        let restore = restore.spawn()?;
        stream_database_copy(dump, restore, target, &self.cancellation).await
    }

    async fn compose_service_running(
        &self,
        files: &[PathBuf],
        profiles: &[String],
        service: &str,
    ) -> Result<bool> {
        let mut command = self.compose_command(files, profiles);
        let output = command
            .args(["ps", "--status", "running", "--quiet", service])
            .output()
            .await?;
        if !output.status.success() {
            return Err(RepoboxError::new(
                ErrorKind::Runtime,
                "local_postgres_status_failed",
                String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            ));
        }
        Ok(!output.stdout.is_empty())
    }

    async fn stop_compose_service(
        &self,
        files: &[PathBuf],
        profiles: &[String],
        service: &str,
    ) -> Result<()> {
        let mut command = self.compose_command(files, profiles);
        let status = command.args(["stop", service]).status().await?;
        if status.success() {
            Ok(())
        } else {
            Err(RepoboxError::new(
                ErrorKind::Runtime,
                "local_postgres_stop_failed",
                format!("Docker Compose exited with {status}"),
            ))
        }
    }

    async fn local_extensions(
        &self,
        files: &[PathBuf],
        profiles: &[String],
        service: &str,
        user: &str,
        database: &str,
    ) -> Result<Vec<String>> {
        let mut command = self.compose_command(files, profiles);
        let output = command
            .args([
                "exec",
                "-T",
                service,
                "psql",
                "--username",
                user,
                "--dbname",
                database,
                "--tuples-only",
                "--no-align",
                "--command",
                "SELECT extname FROM pg_extension ORDER BY extname",
            ])
            .output()
            .await?;
        if !output.status.success() {
            return Err(RepoboxError::new(
                ErrorKind::Runtime,
                "postgres_extension_inspection_failed",
                String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            ));
        }
        Ok(String::from_utf8_lossy(&output.stdout)
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(str::to_owned)
            .collect())
    }

    fn compose_command(&self, files: &[PathBuf], profiles: &[String]) -> Command {
        let mut command = Command::new("docker");
        command.current_dir(&self.repository).arg("compose");
        for profile in profiles {
            command.arg("--profile").arg(profile);
        }
        for file in files {
            command.arg("-f").arg(file);
        }
        command
    }

    async fn replay_extensions_from_base(
        &self,
        organization: &str,
        database: &str,
        base_branch: &str,
        target_branch: &str,
        target_url: &Url,
    ) -> Result<()> {
        let inspect_id = uuid::Uuid::now_v7().simple().to_string();
        let name = format!("repobox-inspect-{}", &inspect_id[inspect_id.len() - 12..]);
        let source_role = self
            .provider
            .create_role(&CreateRoleRequest {
                organization: organization.to_owned(),
                database: database.to_owned(),
                branch: base_branch.to_owned(),
                name,
                inherited_roles: vec!["postgres".to_owned()],
            })
            .await?;
        let source_url = connection_urls(&source_role)?.direct;
        let replay = async {
            let output = run_psql(
                &source_url,
                "SELECT extname FROM pg_extension ORDER BY extname",
            )
            .await?;
            let extensions = output
                .lines()
                .map(str::trim)
                .filter(|line| !line.is_empty())
                .map(str::to_owned)
                .collect::<Vec<_>>();
            replay_extensions(
                target_url,
                &extensions,
                &format!("{organization}/{database}/{target_branch}"),
            )
            .await
        }
        .await;
        let cleanup = self
            .provider
            .delete_role(organization, database, base_branch, &source_role.id, None)
            .await;
        finish_with_cleanup(
            replay,
            cleanup,
            format!(
                "temporary PlanetScale inspection role `{}` on {organization}/{database}/{base_branch}",
                source_role.name
            ),
        )
    }

    async fn wait_for_backup(
        &self,
        organization: &str,
        database: &str,
        base_branch: &str,
        backup_id: &str,
        name: &str,
    ) -> Result<Backup> {
        let started = tokio::time::Instant::now();
        loop {
            let backups = self
                .provider
                .list_backups(organization, database, base_branch)
                .await?;
            if let Some(backup) = backups.into_iter().find(|backup| backup.id == backup_id) {
                match backup.state.as_str() {
                    "success" => return Ok(backup),
                    "failed" | "canceled" | "ignored" => {
                        return Err(RepoboxError::new(
                            ErrorKind::Runtime,
                            "planetscale_backup_failed",
                            format!("backup `{name}` ended in state `{}`", backup.state),
                        ));
                    }
                    _ => {}
                }
            }
            if !self.wait_for_next_provider_poll(started).await? {
                break;
            }
        }
        Err(provider_timeout("backup", name))
    }

    async fn wait_for_existing_database(&self, organization: &str, database: &str) -> Result<bool> {
        let existing = self
            .provider
            .list_databases(organization)
            .await?
            .into_iter()
            .find(|candidate| candidate.name == database);
        let Some(existing) = existing else {
            return Ok(false);
        };
        if !existing.ready {
            self.output.progress(&format!(
                "waiting for existing PlanetScale database {organization}/{database}"
            ));
            self.wait_for_database(organization, database).await?;
        }
        Ok(true)
    }

    async fn wait_for_existing_branch(
        &self,
        organization: &str,
        database: &str,
        branch: &str,
    ) -> Result<bool> {
        let existing = self
            .provider
            .list_branches(organization, database)
            .await?
            .into_iter()
            .find(|candidate| candidate.name == branch);
        let Some(existing) = existing else {
            return Ok(false);
        };
        if !provider_branch_ready(&existing) {
            self.output.progress(&format!(
                "waiting for existing PlanetScale branch {organization}/{database}/{branch}"
            ));
            self.wait_for_branch(organization, database, branch).await?;
        }
        Ok(true)
    }

    async fn wait_for_branch(
        &self,
        organization: &str,
        database: &str,
        branch: &str,
    ) -> Result<()> {
        let started = tokio::time::Instant::now();
        loop {
            match self
                .provider
                .get_branch(organization, database, branch)
                .await
            {
                Ok(value) if provider_branch_ready(&value) => return Ok(()),
                Ok(_) => {}
                Err(error) if error.kind == ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            if !self.wait_for_next_provider_poll(started).await? {
                break;
            }
        }
        Err(provider_timeout("branch", branch))
    }

    async fn wait_for_database(&self, organization: &str, database: &str) -> Result<()> {
        let started = tokio::time::Instant::now();
        loop {
            if self
                .provider
                .list_databases(organization)
                .await?
                .into_iter()
                .any(|candidate| candidate.name == database && candidate.ready)
            {
                return Ok(());
            }
            if !self.wait_for_next_provider_poll(started).await? {
                break;
            }
        }
        Err(provider_timeout("database", database))
    }

    async fn wait_for_next_provider_poll(&self, started: tokio::time::Instant) -> Result<bool> {
        let Some(remaining) = self.readiness.timeout.checked_sub(started.elapsed()) else {
            return Ok(false);
        };
        if remaining.is_zero() {
            return Ok(false);
        }
        tokio::select! {
            biased;
            () = self.cancellation.cancelled() => Err(operation_interrupted()),
            () = tokio::time::sleep(self.readiness.poll_interval.min(remaining)) => Ok(true),
        }
    }

    fn selected_services(
        &self,
        options: &ProvisionOptions,
    ) -> Result<BTreeMap<String, ServiceConfig>> {
        if options.selected_services.is_empty() {
            return Ok(self.config.services.clone());
        }
        let mut selected = BTreeMap::new();
        for name in &options.selected_services {
            let service = self.config.services.get(name).ok_or_else(|| {
                RepoboxError::new(
                    ErrorKind::NotFound,
                    "service_not_found",
                    format!("configured service `{name}` does not exist"),
                )
            })?;
            selected.insert(name.clone(), service.clone());
        }
        Ok(selected)
    }

    fn mutation_lineages(&self, environment: &str) -> Result<Vec<JobRecord>> {
        nonterminal_environment_mutation_jobs(&self.jobs, self.config.project.id, environment)
    }

    fn validate_unique_mutation_lineage(
        &self,
        selected: &JobRecord,
        environment: &str,
    ) -> Result<()> {
        let terminal_residuals = unreconciled_environment_checkpoint_jobs(
            &self.jobs,
            self.config.project.id,
            environment,
        )?
        .into_iter()
        .filter(|job| job.id != selected.id && job.status.terminal())
        .collect::<Vec<_>>();
        match terminal_residuals.as_slice() {
            [] => {}
            [job] => {
                return Err(environment_recovery_required(
                    job,
                    "resume another environment mutation",
                ));
            }
            _ => return Err(mutation_lineage_conflict(environment, &terminal_residuals)),
        }
        let lineages = self.mutation_lineages(environment)?;
        if lineages.iter().all(|job| job.id == selected.id) {
            return Ok(());
        }
        Err(mutation_lineage_conflict(environment, &lineages))
    }

    fn reconcile_environment_mutation_jobs(&self, lineages: &[JobRecord]) -> Result<()> {
        for lineage in lineages {
            let mut reconciled = lineage.clone();
            if !reconciled.status.terminal() {
                reconciled.status = JobStatus::Canceled;
            }
            reconciled.error_code = Some("environment_deleted".to_owned());
            reconciled.sequence += 1;
            reconciled.updated_at = Utc::now();
            self.jobs.append(&reconciled)?;
        }
        Ok(())
    }

    fn validate_resumable_job(
        &self,
        job: &JobRecord,
        kind: JobKind,
        environment: &str,
    ) -> Result<()> {
        if job.project_id != self.config.project.id {
            return Err(RepoboxError::new(
                ErrorKind::Conflict,
                "job_project_mismatch",
                format!(
                    "job {} belongs to project {}, not {}",
                    job.id, job.project_id, self.config.project.id
                ),
            ));
        }
        if job.kind != kind {
            return Err(RepoboxError::new(
                ErrorKind::Conflict,
                "job_kind_mismatch",
                format!("job {} is {:?}, not {:?}", job.id, job.kind, kind),
            ));
        }
        if job.environment != environment {
            return Err(RepoboxError::new(
                ErrorKind::Conflict,
                "job_environment_mismatch",
                format!(
                    "job {} targets environment `{}`, not `{environment}`",
                    job.id, job.environment
                ),
            ));
        }
        if job.status.terminal() {
            return Err(RepoboxError::new(
                ErrorKind::Conflict,
                "job_already_terminal",
                format!("job {} is already {:?}", job.id, job.status),
            ));
        }
        Ok(())
    }

    fn prepare_resumable_job<'b>(
        &self,
        mut job: JobRecord,
        kind: JobKind,
        environment: &str,
        step_prefix: &str,
        services: impl Iterator<Item = &'b String>,
    ) -> Result<JobRecord> {
        self.validate_resumable_job(&job, kind, environment)?;
        self.validate_unique_mutation_lineage(&job, environment)?;
        for service in services {
            let name = format!("{step_prefix}{service}");
            if !job.steps.iter().any(|step| step.name == name) {
                job.steps.push(repobox_core::jobs::JobStep {
                    name,
                    status: StepStatus::Pending,
                    attempts: 0,
                    message: None,
                    resource: serde_json::Value::Null,
                });
            }
        }
        Ok(job)
    }

    fn resumable_job<'b>(
        &self,
        kind: JobKind,
        environment: &str,
        step_prefix: &str,
        services: impl Iterator<Item = &'b String>,
    ) -> Result<(JobRecord, bool)> {
        let services = services.cloned().collect::<Vec<_>>();
        let unreconciled_checkpoints = unreconciled_environment_checkpoint_jobs(
            &self.jobs,
            self.config.project.id,
            environment,
        )?;
        let terminal_checkpoints = unreconciled_checkpoints
            .iter()
            .filter(|job| job.status.terminal())
            .collect::<Vec<_>>();
        match terminal_checkpoints.as_slice() {
            [] => {}
            [job] => {
                return Err(environment_recovery_required(
                    job,
                    "start an environment mutation",
                ));
            }
            _ => {
                return Err(mutation_lineage_conflict(
                    environment,
                    &unreconciled_checkpoints,
                ));
            }
        }
        let lineages = self.mutation_lineages(environment)?;
        if lineages.len() > 1 {
            return Err(mutation_lineage_conflict(environment, &lineages));
        }
        if let Some(job) = lineages.into_iter().next() {
            if job.kind != kind || !job_scope_matches(&job, step_prefix, &services) {
                return Err(environment_recovery_required(
                    &job,
                    "start a different environment mutation",
                ));
            }
            return Ok((
                self.prepare_resumable_job(job, kind, environment, step_prefix, services.iter())?,
                true,
            ));
        }
        Ok((
            JobRecord::new(
                kind,
                self.config.project.id,
                environment,
                services
                    .into_iter()
                    .map(|service| format!("{step_prefix}{service}")),
            ),
            false,
        ))
    }

    fn event<T: Serialize>(&self, event: &str, data: &T) -> Result<()> {
        if self.output.json() {
            self.output.stream(event, data)
        } else {
            Ok(())
        }
    }
}

enum StreamDatabaseCopyExit {
    Completed {
        transfer_result: std::io::Result<()>,
        dump_status: Option<std::process::ExitStatus>,
        restore_status: std::process::ExitStatus,
        restore_was_terminated: bool,
    },
    RestoreExitedEarly {
        restore_status: std::process::ExitStatus,
    },
    Cancelled,
}

enum StreamDatabaseCopyRace {
    Transfer(std::io::Result<()>),
    Restore(std::io::Result<std::process::ExitStatus>),
    Cancelled,
}

async fn wait_for_local_postgres_readiness<Probe, ProbeFuture>(
    cancellation: &OperationCancellation,
    service: &str,
    timeout: Duration,
    poll_interval: Duration,
    mut probe: Probe,
) -> Result<()>
where
    Probe: FnMut() -> ProbeFuture,
    ProbeFuture: Future<Output = Result<bool>>,
{
    let started = tokio::time::Instant::now();
    loop {
        cancellation.check()?;
        if probe().await? {
            return Ok(());
        }
        let Some(remaining) = timeout.checked_sub(started.elapsed()) else {
            break;
        };
        if remaining.is_zero() {
            break;
        }
        tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(operation_interrupted()),
            () = tokio::time::sleep(poll_interval.min(remaining)) => {}
        }
    }
    Err(RepoboxError::new(
        ErrorKind::Runtime,
        "local_postgres_readiness_timeout",
        format!(
            "Compose service `{service}` did not accept PostgreSQL connections within {} seconds",
            timeout.as_secs()
        ),
    )
    .with_suggestion(
        "Inspect the service with `docker compose logs`, verify its health configuration, then resume the exact Repobox job.",
    ))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FailedStreamFailureSource {
    Transfer,
    StalledRestore,
}

fn failed_stream_failure_source(
    restore_was_terminated: bool,
    transfer_error: Option<&std::io::Error>,
) -> Option<FailedStreamFailureSource> {
    if !restore_was_terminated {
        return None;
    }
    if transfer_error.is_some_and(|error| error.kind() != std::io::ErrorKind::BrokenPipe) {
        Some(FailedStreamFailureSource::Transfer)
    } else {
        Some(FailedStreamFailureSource::StalledRestore)
    }
}

async fn stream_database_copy(
    mut dump: ManagedChild,
    mut restore: ManagedChild,
    target: &Url,
    cancellation: &OperationCancellation,
) -> Result<()> {
    let mut dump_stdout = dump.child.stdout.take().expect("pg_dump stdout is piped");
    let dump_stderr = dump.child.stderr.take().expect("pg_dump stderr is piped");
    let dump_control = dump.child.stdin.take();
    let mut restore_stdin = restore.child.stdin.take().expect("psql stdin is piped");
    let restore_stderr = restore.child.stderr.take().expect("psql stderr is piped");

    let stream = Box::pin(async move {
        let dump_control = dump_control;
        let mut transfer = Box::pin(async move {
            let copy_result = tokio::io::copy(&mut dump_stdout, &mut restore_stdin)
                .await
                .map(|_| ());
            let shutdown_result = restore_stdin.shutdown().await;
            copy_result.and(shutdown_result)
        });
        let race = tokio::select! {
            biased;
            () = cancellation.cancelled() => StreamDatabaseCopyRace::Cancelled,
            transfer_result = &mut transfer => StreamDatabaseCopyRace::Transfer(transfer_result),
            restore_status = restore.wait() => StreamDatabaseCopyRace::Restore(restore_status),
        };
        match race {
            StreamDatabaseCopyRace::Transfer(transfer_result) => {
                drop(transfer);
                drop(dump_control);
                if transfer_result.is_err() {
                    let dump_status = dump.try_wait()?;
                    if dump_status.is_none() {
                        dump.start_kill()?;
                        let _ = dump.wait().await?;
                    }
                    let (restore_status, restore_was_terminated) = if let Ok(status) =
                        tokio::time::timeout(FAILED_STREAM_EXIT_GRACE_PERIOD, restore.wait()).await
                    {
                        (status?, false)
                    } else {
                        restore.start_kill()?;
                        (restore.wait().await?, true)
                    };
                    Ok::<StreamDatabaseCopyExit, RepoboxError>(StreamDatabaseCopyExit::Completed {
                        transfer_result,
                        dump_status,
                        restore_status,
                        restore_was_terminated,
                    })
                } else {
                    let (dump_status, restore_status) = tokio::join!(dump.wait(), restore.wait());
                    Ok::<StreamDatabaseCopyExit, RepoboxError>(StreamDatabaseCopyExit::Completed {
                        transfer_result,
                        dump_status: Some(dump_status?),
                        restore_status: restore_status?,
                        restore_was_terminated: false,
                    })
                }
            }
            StreamDatabaseCopyRace::Restore(restore_status) => {
                drop(transfer);
                drop(dump_control);
                let _ = dump.start_kill();
                let _ = dump.wait().await;
                Ok::<StreamDatabaseCopyExit, RepoboxError>(
                    StreamDatabaseCopyExit::RestoreExitedEarly {
                        restore_status: restore_status?,
                    },
                )
            }
            StreamDatabaseCopyRace::Cancelled => {
                drop(transfer);
                drop(dump_control);
                let dump_kill = dump.start_kill();
                let restore_kill = restore.start_kill();
                let (dump_status, restore_status) = tokio::join!(dump.wait(), restore.wait());
                interrupted_database_copy_cleanup(
                    dump_kill,
                    restore_kill,
                    dump_status.map(|_| ()),
                    restore_status.map(|_| ()),
                )
            }
        }
    });
    let dump_stderr = Box::pin(collect_stderr_tail(dump_stderr));
    let restore_stderr = Box::pin(collect_stderr_tail(restore_stderr));
    let (stream_result, dump_stderr_result, restore_stderr_result) =
        tokio::join!(stream, dump_stderr, restore_stderr,);

    let stream_exit = stream_result?;
    let dump_stderr = dump_stderr_result?;
    let restore_stderr = restore_stderr_result?;
    let (transfer_result, dump_status, restore_status, restore_was_terminated) = match stream_exit {
        StreamDatabaseCopyExit::Completed {
            transfer_result,
            dump_status,
            restore_status,
            restore_was_terminated,
        } => (
            transfer_result,
            dump_status,
            restore_status,
            restore_was_terminated,
        ),
        StreamDatabaseCopyExit::RestoreExitedEarly { restore_status } => {
            if !restore_status.success() {
                return Err(planetscale_import_failure(
                    target,
                    restore_status,
                    &restore_stderr,
                ));
            }
            return Err(RepoboxError::new(
                ErrorKind::Runtime,
                "database_stream_interrupted",
                "psql exited successfully before pg_dump finished",
            ));
        }
        StreamDatabaseCopyExit::Cancelled => return Err(operation_interrupted()),
    };
    let mut transfer_error = transfer_result.err();
    match failed_stream_failure_source(restore_was_terminated, transfer_error.as_ref()) {
        Some(FailedStreamFailureSource::Transfer) => {
            return Err(transfer_error
                .take()
                .expect("transfer error was classified")
                .into());
        }
        Some(FailedStreamFailureSource::StalledRestore) => {
            return Err(database_stream_interrupted(
                target,
                format!(
                    "database stream failed and psql did not exit within {} seconds",
                    FAILED_STREAM_EXIT_GRACE_PERIOD.as_secs()
                ),
                &restore_stderr,
            ));
        }
        None => {}
    }
    let transfer_broken_pipe = transfer_error
        .as_ref()
        .is_some_and(|error| error.kind() == std::io::ErrorKind::BrokenPipe);

    if transfer_broken_pipe && !restore_status.success() {
        return Err(planetscale_import_failure(
            target,
            restore_status,
            &restore_stderr,
        ));
    }
    if let Some(dump_status) = dump_status.filter(|status| !status.success()) {
        return Err(RepoboxError::new(
            ErrorKind::Runtime,
            "local_postgres_dump_failed",
            process_failure_message("pg_dump", dump_status, &dump_stderr),
        ));
    }
    if transfer_error
        .as_ref()
        .is_some_and(|error| error.kind() != std::io::ErrorKind::BrokenPipe)
    {
        return Err(transfer_error.expect("transfer error was checked").into());
    }
    if !restore_status.success() {
        return Err(planetscale_import_failure(
            target,
            restore_status,
            &restore_stderr,
        ));
    }
    if let Some(error) = transfer_error {
        return Err(error.into());
    }
    Ok(())
}

fn interrupted_database_copy_cleanup(
    dump_kill: std::io::Result<()>,
    restore_kill: std::io::Result<()>,
    dump_reap: std::io::Result<()>,
    restore_reap: std::io::Result<()>,
) -> Result<StreamDatabaseCopyExit> {
    let mut cleanup_failures = Vec::new();
    if let Err(error) = dump_kill {
        cleanup_failures.push(format!("pg_dump process group kill failed: {error}"));
    }
    if let Err(error) = restore_kill {
        cleanup_failures.push(format!("psql process group kill failed: {error}"));
    }
    if let Err(error) = dump_reap {
        cleanup_failures.push(format!("pg_dump reap failed: {error}"));
    }
    if let Err(error) = restore_reap {
        cleanup_failures.push(format!("psql reap failed: {error}"));
    }
    if cleanup_failures.is_empty() {
        Ok(StreamDatabaseCopyExit::Cancelled)
    } else {
        Err(RepoboxError::new(
            ErrorKind::Runtime,
            "operation_interrupted_cleanup_incomplete",
            format!(
                "database import was interrupted, but managed pg_dump or psql processes may remain: {}",
                cleanup_failures.join("; ")
            ),
        )
        .with_suggestion(
            "Inspect managed child process groups and Docker containers before resuming the exact durable job.",
        ))
    }
}

fn database_stream_interrupted(
    target: &Url,
    message: impl Into<String>,
    stderr: &[u8],
) -> RepoboxError {
    let mut message = message.into();
    let stderr = String::from_utf8_lossy(stderr);
    let stderr = stderr.trim();
    if !stderr.is_empty() {
        message.push_str(": ");
        message.push_str(stderr);
    }
    let mut redactor = repobox_core::redaction::SecretRedactor::default();
    if let Some(password) = target.password() {
        redactor.add(password);
    }
    RepoboxError::new(
        ErrorKind::Runtime,
        "database_stream_interrupted",
        redactor.redact(&message),
    )
}

async fn collect_stderr_tail<R>(mut stderr: R) -> std::io::Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut tail = Vec::with_capacity(PROCESS_STDERR_TAIL_BYTES);
    let mut total_bytes = 0_usize;
    let mut buffer = [0_u8; 8 * 1024];
    loop {
        let read = stderr.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        total_bytes = total_bytes.saturating_add(read);
        tail.extend_from_slice(&buffer[..read]);
        if tail.len() > PROCESS_STDERR_TAIL_BYTES {
            let excess = tail.len() - PROCESS_STDERR_TAIL_BYTES;
            tail.copy_within(excess.., 0);
            tail.truncate(PROCESS_STDERR_TAIL_BYTES);
        }
    }
    if total_bytes > tail.len() {
        let marker = format!(
            "[stderr truncated: retained last {} of {total_bytes} bytes]\n",
            tail.len()
        );
        let mut retained = Vec::with_capacity(marker.len() + tail.len());
        retained.extend_from_slice(marker.as_bytes());
        retained.extend_from_slice(&tail);
        return Ok(retained);
    }
    Ok(tail)
}

fn planetscale_import_failure(
    target: &Url,
    status: std::process::ExitStatus,
    stderr: &[u8],
) -> RepoboxError {
    let mut redactor = repobox_core::redaction::SecretRedactor::default();
    if let Some(password) = target.password() {
        redactor.add(password);
    }
    let message = process_failure_message("psql", status, stderr);
    RepoboxError::new(
        ErrorKind::Runtime,
        "planetscale_import_failed",
        redactor.redact(&message),
    )
}

fn process_failure_message(
    process: &str,
    status: std::process::ExitStatus,
    stderr: &[u8],
) -> String {
    let stderr = String::from_utf8_lossy(stderr);
    let stderr = stderr.trim();
    if stderr.is_empty() {
        format!("{process} exited with {status}")
    } else {
        format!("{process} exited with {status}: {stderr}")
    }
}

pub fn environment_variables(
    config: &RepoboxConfig,
    record: &EnvironmentRecord,
    credentials: &CredentialStore,
) -> Result<BTreeMap<String, String>> {
    if record.status != EnvironmentStatus::Ready {
        return Err(RepoboxError::new(
            ErrorKind::Conflict,
            "environment_not_ready",
            format!("environment `{}` is not ready", record.name),
        )
        .with_suggestion(
            "Run `repobox job view latest --json`, then resume the failed operation.",
        ));
    }
    let expected_branch = provider_branch_name(config.project.id, &record.name)?;
    if record.provider_branch != expected_branch {
        return Err(RepoboxError::new(
            ErrorKind::Conflict,
            "environment_binding_config_mismatch",
            format!(
                "environment `{}` records provider branch `{}`, but current configuration resolves `{expected_branch}`",
                record.name, record.provider_branch
            ),
        )
        .with_suggestion(
            "Do not start the runtime. Reconcile the project/environment identity or recreate the Repobox environment.",
        ));
    }
    let mut variables = BTreeMap::new();
    for (name, service) in &config.services {
        let binding = record.databases.get(name).ok_or_else(|| {
            RepoboxError::new(
                ErrorKind::NotFound,
                "database_binding_not_found",
                format!("environment has no binding for service `{name}`"),
            )
        })?;
        let RemoteServiceConfig::Planetscale {
            organization,
            database,
            ..
        } = &service.remote;
        if binding.organization != *organization
            || binding.database != *database
            || binding.branch != expected_branch
            || !binding.ready
        {
            return Err(RepoboxError::new(
                ErrorKind::Conflict,
                "environment_binding_config_mismatch",
                format!(
                    "stored binding for service `{name}` does not match configured {organization}/{database}/{expected_branch}"
                ),
            )
            .with_suggestion(
                "Do not start the runtime with stale credentials. Resume the matching durable job or delete and recreate the environment.",
            ));
        }
        let key = CredentialStore::database_key(config.project.id, &record.provider_branch, name);
        let (pooled, direct) = credentials.database_urls(&key)?;
        variables.insert(service.env.pooled.clone(), pooled);
        variables.insert(service.env.direct.clone(), direct);
    }
    Ok(variables)
}

pub fn stored_environment_variables(
    config: &RepoboxConfig,
    record: &EnvironmentRecord,
    credentials: &CredentialStore,
) -> Result<BTreeMap<String, String>> {
    let mut variables = BTreeMap::new();
    for (name, service) in &config.services {
        if !record.databases.contains_key(name) {
            continue;
        }
        let key = CredentialStore::database_key(config.project.id, &record.provider_branch, name);
        if let Some((pooled, direct)) = optional_database_urls(credentials.database_urls(&key))? {
            variables.insert(service.env.pooled.clone(), pooled);
            variables.insert(service.env.direct.clone(), direct);
        }
    }
    Ok(variables)
}

fn optional_database_urls(result: Result<(String, String)>) -> Result<Option<(String, String)>> {
    match result {
        Ok(urls) => Ok(Some(urls)),
        Err(error) if error.code == "database_credentials_not_found" => Ok(None),
        Err(error) => Err(error),
    }
}

fn database_urls_or_fallback<F>(
    primary: Result<(String, String)>,
    fallback: F,
) -> Result<(String, String)>
where
    F: FnOnce() -> Result<(String, String)>,
{
    match primary {
        Ok(urls) => Ok(urls),
        Err(error) if error.code == "database_credentials_not_found" => fallback(),
        Err(error) => Err(error),
    }
}

pub fn state_store(
    config: &RepoboxConfig,
    paths: &repobox_core::paths::RepoboxPaths,
) -> StateStore {
    StateStore::new(paths.state(config.project.id))
}

pub fn job_store(config: &RepoboxConfig, paths: &repobox_core::paths::RepoboxPaths) -> JobStore {
    JobStore::new(paths.jobs(config.project.id))
}

fn select_smallest_size(sizes: &[String]) -> Result<String> {
    sizes
        .iter()
        .filter_map(|name| {
            name.trim_start_matches("PS_")
                .trim_start_matches("PS-")
                .chars()
                .take_while(char::is_ascii_digit)
                .collect::<String>()
                .parse::<u32>()
                .ok()
                .map(|size| (size, name))
        })
        .min_by_key(|(size, _)| *size)
        .map(|(_, name)| name.clone())
        .or_else(|| sizes.first().cloned())
        .ok_or_else(|| {
            RepoboxError::new(
                ErrorKind::Conflict,
                "planetscale_cluster_size_unavailable",
                "PlanetScale returned no eligible PostgreSQL cluster sizes",
            )
        })
}

fn provider_branch_ready(branch: &Branch) -> bool {
    branch.ready || branch.state == "ready"
}

fn role_name(provider_branch: &str, service: &str) -> String {
    let digest = Sha256::digest(format!("{provider_branch}\0{service}").as_bytes());
    let suffix = &hex::encode(digest)[..12];
    let mut service = service.to_owned();
    service.truncate(63 - "repobox--".len() - suffix.len());
    format!("repobox-{service}-{suffix}")
}

fn bootstrap_service_marker(service_name: &str, service: &ServiceConfig) -> String {
    let RemoteServiceConfig::Planetscale {
        organization,
        database,
        base_branch,
        ..
    } = &service.remote;
    format!("{service_name}@{organization}/{database}/{base_branch}")
}

fn incomplete_environment_services(
    record: &EnvironmentRecord,
    config: &RepoboxConfig,
    canonical: &str,
) -> Vec<String> {
    config
        .services
        .iter()
        .filter_map(|(service_name, service)| {
            let RemoteServiceConfig::Planetscale {
                organization,
                database,
                ..
            } = &service.remote;
            let complete = record.databases.get(service_name).is_some_and(|binding| {
                binding.organization == *organization
                    && binding.database == *database
                    && binding.branch == canonical
                    && binding.ready
            });
            (!complete).then(|| service_name.clone())
        })
        .collect()
}

fn validate_selected_binding_identity(
    record: &EnvironmentRecord,
    selected: &BTreeMap<String, ServiceConfig>,
    canonical: &str,
    require_all: bool,
) -> Result<()> {
    if (require_all || !record.databases.is_empty()) && record.provider_branch != canonical {
        return Err(RepoboxError::new(
            ErrorKind::Conflict,
            "environment_binding_config_mismatch",
            format!(
                "environment `{}` records provider branch `{}`, but the current project identity resolves `{canonical}`",
                record.name, record.provider_branch
            ),
        )
        .with_suggestion(
            "Do not mutate a different provider target. Restore the matching configuration, or explicitly delete and recreate the environment.",
        ));
    }
    for (service_name, service) in selected {
        let Some(binding) = record.databases.get(service_name) else {
            if require_all {
                return Err(RepoboxError::new(
                    ErrorKind::Conflict,
                    "environment_binding_config_mismatch",
                    format!(
                        "environment `{}` has no durable binding for selected service `{service_name}`",
                        record.name
                    ),
                )
                .with_suggestion(
                    "Do not start a new pull from incomplete state. Resume the exact durable job, or explicitly delete and recreate the environment.",
                ));
            }
            continue;
        };
        let RemoteServiceConfig::Planetscale {
            organization,
            database,
            ..
        } = &service.remote;
        if binding.organization != *organization
            || binding.database != *database
            || binding.branch != canonical
            || !binding.ready
        {
            return Err(RepoboxError::new(
                ErrorKind::Conflict,
                "environment_binding_config_mismatch",
                format!(
                    "stored binding for service `{service_name}` points to {}/{}/{}, not configured {organization}/{database}/{canonical}",
                    binding.organization, binding.database, binding.branch
                ),
            )
            .with_suggestion(
                "Do not overwrite the stored binding or credentials. Restore the matching configuration, or explicitly delete and recreate the environment.",
            ));
        }
    }
    Ok(())
}

fn staging_branch_name(canonical: &str, job_id: uuid::Uuid) -> String {
    let job_id = job_id.simple().to_string();
    let suffix = format!("-next-{}", &job_id[job_id.len() - 8..]);
    let mut base = canonical.to_owned();
    base.truncate(63_usize.saturating_sub(suffix.len()));
    format!("{base}{suffix}")
}

fn nonterminal_environment_mutation_jobs(
    jobs: &JobStore,
    project_id: uuid::Uuid,
    environment: &str,
) -> Result<Vec<JobRecord>> {
    Ok(jobs
        .list()?
        .into_iter()
        .filter(|job| {
            job.project_id == project_id
                && job.environment == environment
                && matches!(
                    job.kind,
                    JobKind::EnvironmentCreate | JobKind::EnvironmentPull
                )
                && !job.status.terminal()
        })
        .collect())
}

fn pull_job_has_residual_checkpoint(job: &JobRecord) -> bool {
    job.steps.iter().any(|step| {
        let phase = step
            .resource
            .get("phase")
            .and_then(serde_json::Value::as_str);
        phase != Some("complete")
            && (phase.is_some()
                || step
                    .resource
                    .get("staging")
                    .and_then(serde_json::Value::as_str)
                    .is_some())
    })
}

fn create_job_has_residual_checkpoint(job: &JobRecord) -> bool {
    job.steps.iter().any(|step| {
        step.resource
            .get("phase")
            .and_then(serde_json::Value::as_str)
            != Some("complete")
            && ["organization", "database", "canonical"]
                .into_iter()
                .all(|key| {
                    step.resource
                        .get(key)
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|value| !value.is_empty())
                })
    })
}

fn unreconciled_environment_create_jobs(
    jobs: &JobStore,
    project_id: uuid::Uuid,
    environment: &str,
) -> Result<Vec<JobRecord>> {
    Ok(jobs
        .list()?
        .into_iter()
        .filter(|job| {
            job.project_id == project_id
                && job.environment == environment
                && job.kind == JobKind::EnvironmentCreate
                && job.error_code.as_deref() != Some("environment_deleted")
                && match job.status {
                    JobStatus::Succeeded => false,
                    JobStatus::Pending | JobStatus::Running | JobStatus::Degraded => true,
                    JobStatus::Failed | JobStatus::Canceled => {
                        create_job_has_residual_checkpoint(job)
                    }
                }
        })
        .collect())
}

fn unreconciled_environment_pull_jobs(
    jobs: &JobStore,
    project_id: uuid::Uuid,
    environment: &str,
) -> Result<Vec<JobRecord>> {
    Ok(jobs
        .list()?
        .into_iter()
        .filter(|job| {
            job.project_id == project_id
                && job.environment == environment
                && job.kind == JobKind::EnvironmentPull
                && job.error_code.as_deref() != Some("environment_deleted")
                && match job.status {
                    JobStatus::Succeeded => false,
                    JobStatus::Pending | JobStatus::Running | JobStatus::Degraded => true,
                    JobStatus::Failed | JobStatus::Canceled => {
                        pull_job_has_residual_checkpoint(job)
                    }
                }
        })
        .collect())
}

fn unreconciled_environment_checkpoint_jobs(
    jobs: &JobStore,
    project_id: uuid::Uuid,
    environment: &str,
) -> Result<Vec<JobRecord>> {
    let mut checkpoints = BTreeMap::new();
    for job in unreconciled_environment_create_jobs(jobs, project_id, environment)?
        .into_iter()
        .chain(unreconciled_environment_pull_jobs(
            jobs,
            project_id,
            environment,
        )?)
    {
        checkpoints.insert(job.id, job);
    }
    Ok(checkpoints.into_values().collect())
}

fn mutation_jobs_to_reconcile(
    lineages: &[JobRecord],
    cleanup_pull_jobs: &[JobRecord],
) -> Vec<JobRecord> {
    let mut jobs = BTreeMap::new();
    for job in lineages.iter().chain(cleanup_pull_jobs) {
        jobs.insert(job.id, job.clone());
    }
    jobs.into_values().collect()
}

fn environment_recovery_required(job: &JobRecord, action: &str) -> RepoboxError {
    let error = RepoboxError::new(
        ErrorKind::Conflict,
        "environment_recovery_required",
        format!(
            "{action} is blocked because environment `{}` has unresolved {:?} job {}",
            job.environment, job.kind, job.id
        ),
    );
    if job.status.terminal() {
        error.with_suggestion(format!(
            "Job {} is terminal and cannot be resumed. Run `repobox env delete {} --yes` to reconcile its persisted branches before starting over.",
            job.id, job.environment
        ))
    } else {
        error.with_suggestion(format!(
            "Resume the exact lineage with `repobox job resume {} --yes`, or explicitly delete environment `{}` before starting over.",
            job.id, job.environment
        ))
    }
}

fn mutation_lineage_conflict(environment: &str, lineages: &[JobRecord]) -> RepoboxError {
    let ids = lineages
        .iter()
        .map(|job| format!("{:?} {}", job.kind, job.id))
        .collect::<Vec<_>>()
        .join(", ");
    RepoboxError::new(
        ErrorKind::Conflict,
        "environment_mutation_lineage_conflict",
        format!(
            "environment `{environment}` has multiple unresolved mutation lineages: {ids}"
        ),
    )
    .with_suggestion(format!(
        "Inspect the exact job UUIDs, then run `repobox env delete {environment} --yes` to reconcile their persisted provider resources before starting over."
    ))
}

pub fn guard_run_against_unresolved_mutation(
    jobs: &JobStore,
    project_id: uuid::Uuid,
    environment: &str,
) -> Result<()> {
    let checkpoints = unreconciled_environment_checkpoint_jobs(jobs, project_id, environment)?;
    match checkpoints.as_slice() {
        [] => Ok(()),
        [job] => Err(environment_recovery_required(
            job,
            "starting the local runtime",
        )),
        _ => Err(mutation_lineage_conflict(environment, &checkpoints)),
    }
}

fn insert_deletion_target(
    targets: &mut EnvironmentDeletionTargets,
    service: &str,
    target: BranchDeletionTarget,
    checkpointed_credentials: bool,
) {
    let credentials = targets
        .entry(service.to_owned())
        .or_default()
        .entry(target)
        .or_default();
    *credentials |= checkpointed_credentials;
}

fn environment_deletion_targets(
    record: &EnvironmentRecord,
    lineages: &[JobRecord],
) -> Result<EnvironmentDeletionTargets> {
    let mut targets = EnvironmentDeletionTargets::new();
    for (service, binding) in &record.databases {
        insert_deletion_target(
            &mut targets,
            service,
            BranchDeletionTarget {
                organization: binding.organization.clone(),
                database: binding.database.clone(),
                branch: binding.branch.clone(),
            },
            true,
        );
    }
    let mut missing_identity = BTreeSet::new();
    for lineage in lineages {
        for step in &lineage.steps {
            let (service_name, pull) = if let Some(service) = step.name.strip_prefix("provision:") {
                (service, false)
            } else if let Some(service) = step.name.strip_prefix("refresh:") {
                (service, true)
            } else {
                continue;
            };
            let resource = &step.resource;
            let binding = record.databases.get(service_name);
            let organization = resource
                .get("organization")
                .and_then(serde_json::Value::as_str)
                .or_else(|| binding.map(|binding| binding.organization.as_str()));
            let database = resource
                .get("database")
                .and_then(serde_json::Value::as_str)
                .or_else(|| binding.map(|binding| binding.database.as_str()));
            let canonical = resource
                .get("canonical")
                .and_then(serde_json::Value::as_str)
                .filter(|branch| !branch.is_empty())
                .or_else(|| {
                    (!pull)
                        .then(|| {
                            resource
                                .get("branch")
                                .and_then(serde_json::Value::as_str)
                                .filter(|branch| !branch.is_empty())
                        })
                        .flatten()
                });
            let staging = pull
                .then(|| {
                    resource
                        .get("staging")
                        .and_then(serde_json::Value::as_str)
                        .filter(|branch| !branch.is_empty())
                })
                .flatten();
            let mut checkpoint_target = false;
            if let (Some(organization), Some(database), Some(canonical)) =
                (organization, database, canonical)
            {
                insert_deletion_target(
                    &mut targets,
                    service_name,
                    BranchDeletionTarget {
                        organization: organization.to_owned(),
                        database: database.to_owned(),
                        branch: canonical.to_owned(),
                    },
                    false,
                );
                checkpoint_target = true;
            }
            if let (Some(organization), Some(database), Some(staging)) =
                (organization, database, staging)
            {
                let credentials_checkpointed = resource
                    .get("phase")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|phase| {
                        matches!(
                            phase,
                            "credentialed" | "old_deleted" | "renamed" | "swapped" | "complete"
                        )
                    });
                insert_deletion_target(
                    &mut targets,
                    service_name,
                    BranchDeletionTarget {
                        organization: organization.to_owned(),
                        database: database.to_owned(),
                        branch: staging.to_owned(),
                    },
                    credentials_checkpointed,
                );
                checkpoint_target = true;
            }
            if binding.is_none() && !checkpoint_target {
                missing_identity.insert(service_name.to_owned());
            }
        }
    }
    if targets.is_empty() && lineages.is_empty() {
        return Err(RepoboxError::new(
            ErrorKind::Conflict,
            "environment_delete_identity_missing",
            format!(
                "environment `{}` has no durable provider binding or create/pull identity checkpoint",
                record.name
            ),
        )
        .with_suggestion(
            "Inspect the local state, durable jobs, and PlanetScale branches manually. Repobox will not delete a branch inferred only from current configuration.",
        ));
    }
    if !missing_identity.is_empty() {
        return Err(RepoboxError::new(
            ErrorKind::Conflict,
            "environment_delete_identity_missing",
            format!(
                "environment `{}` has no durable provider identity for services: {}",
                record.name,
                missing_identity.into_iter().collect::<Vec<_>>().join(", ")
            ),
        )
        .with_suggestion(
            "Inspect the affected durable job and PlanetScale branches manually. Repobox will not delete a branch inferred only from current configuration.",
        ));
    }
    Ok(targets)
}

fn set_step_resource(job: &mut JobRecord, step: &str, value: serde_json::Value) -> Result<()> {
    let target = job
        .steps
        .iter_mut()
        .find(|candidate| candidate.name == step)
        .ok_or_else(|| {
            RepoboxError::new(
                ErrorKind::NotFound,
                "job_step_not_found",
                format!("job step `{step}` does not exist"),
            )
        })?;
    target.resource = value;
    job.sequence += 1;
    job.updated_at = Utc::now();
    Ok(())
}

fn prepare_create_job_resources(
    job: &mut JobRecord,
    selected: &BTreeMap<String, ServiceConfig>,
    canonical: &str,
    resumed: bool,
) -> Result<()> {
    for (service_name, service) in selected {
        let step_name = format!("provision:{service_name}");
        let step = job
            .steps
            .iter()
            .find(|step| step.name == step_name)
            .ok_or_else(|| {
                RepoboxError::new(
                    ErrorKind::NotFound,
                    "job_step_not_found",
                    format!("job step `{step_name}` does not exist"),
                )
            })?;
        let RemoteServiceConfig::Planetscale {
            organization,
            database,
            base_branch,
            ..
        } = &service.remote;
        let expected = [
            ("organization", organization.as_str()),
            ("database", database.as_str()),
            ("base_branch", base_branch.as_str()),
            ("canonical", canonical),
        ];
        let has_identity = expected.iter().all(|(key, _)| {
            step.resource
                .get(key)
                .and_then(serde_json::Value::as_str)
                .is_some()
        });
        if !has_identity {
            if resumed {
                return Err(RepoboxError::new(
                    ErrorKind::Conflict,
                    "create_resume_identity_missing",
                    format!(
                        "create step `{step_name}` has no complete provider identity checkpoint"
                    ),
                )
                .with_suggestion(
                    "Do not guess a provider target. Inspect the durable job and use explicit environment deletion after identifying the owned branch.",
                ));
            }
            set_step_resource(
                job,
                &step_name,
                serde_json::json!({
                    "phase": "planned",
                    "organization": organization,
                    "database": database,
                    "base_branch": base_branch,
                    "canonical": canonical,
                }),
            )?;
            continue;
        }
        let mismatches = expected
            .iter()
            .filter(|(key, value)| {
                step.resource.get(key).and_then(serde_json::Value::as_str) != Some(*value)
            })
            .map(|(key, _)| *key)
            .collect::<Vec<_>>();
        if !mismatches.is_empty() {
            return Err(RepoboxError::new(
                ErrorKind::Conflict,
                "create_resume_identity_mismatch",
                format!(
                    "create step `{step_name}` provider identity differs from current configuration: {}",
                    mismatches.join(", ")
                ),
            )
            .with_suggestion(
                "Restore the configuration that created this job, or explicitly delete the environment using its checkpointed provider identity.",
            ));
        }
    }
    Ok(())
}

fn update_create_step_binding(
    job: &mut JobRecord,
    step: &str,
    binding: &DatabaseBinding,
) -> Result<()> {
    let mut resource = job
        .steps
        .iter()
        .find(|candidate| candidate.name == step)
        .and_then(|step| step.resource.as_object())
        .cloned()
        .ok_or_else(|| {
            RepoboxError::new(
                ErrorKind::Conflict,
                "create_step_identity_missing",
                format!("create step `{step}` has no provider identity checkpoint"),
            )
        })?;
    resource.insert(
        "phase".to_owned(),
        serde_json::Value::String("complete".to_owned()),
    );
    resource.insert(
        "binding".to_owned(),
        serde_json::to_value(binding).map_err(|error| {
            RepoboxError::new(ErrorKind::Runtime, "job_encode_failed", error.to_string())
        })?,
    );
    set_step_resource(job, step, serde_json::Value::Object(resource))
}

fn prepare_pull_job_resources(
    job: &mut JobRecord,
    selected: &BTreeMap<String, ServiceConfig>,
    canonical: &str,
    resumed: bool,
) -> Result<()> {
    let staging = staging_branch_name(canonical, job.id);
    for (service_name, service) in selected {
        let step_name = format!("refresh:{service_name}");
        let step = job
            .steps
            .iter()
            .find(|step| step.name == step_name)
            .ok_or_else(|| {
                RepoboxError::new(
                    ErrorKind::NotFound,
                    "job_step_not_found",
                    format!("job step `{step_name}` does not exist"),
                )
            })?;
        let RemoteServiceConfig::Planetscale {
            organization,
            database,
            base_branch,
            ..
        } = &service.remote;
        let expected = [
            ("organization", organization.as_str()),
            ("database", database.as_str()),
            ("base_branch", base_branch.as_str()),
            ("canonical", canonical),
            ("staging", staging.as_str()),
        ];
        let has_identity = expected.iter().all(|(key, _)| {
            step.resource
                .get(key)
                .and_then(serde_json::Value::as_str)
                .is_some()
        });
        if !has_identity {
            if step.status == StepStatus::Succeeded {
                continue;
            }
            if resumed {
                return Err(RepoboxError::new(
                    ErrorKind::Conflict,
                    "pull_resume_identity_missing",
                    format!(
                        "pull job {} step `{step_name}` predates provider-identity checkpoints and cannot be resumed safely",
                        job.id
                    ),
                )
                .with_suggestion(
                    "Inspect the staging and canonical branches in PlanetScale, then start a new pull only after reconciling any forward-only swap.",
                ));
            }
            set_step_resource(
                job,
                &step_name,
                serde_json::json!({
                    "phase": "planned",
                    "organization": organization,
                    "database": database,
                    "base_branch": base_branch,
                    "canonical": canonical,
                    "staging": staging,
                }),
            )?;
            continue;
        }
        let mismatches = expected
            .iter()
            .filter(|(key, value)| {
                step.resource.get(key).and_then(serde_json::Value::as_str) != Some(*value)
            })
            .map(|(key, _)| *key)
            .collect::<Vec<_>>();
        if !mismatches.is_empty() {
            return Err(RepoboxError::new(
                ErrorKind::Conflict,
                "pull_resume_identity_mismatch",
                format!(
                    "pull job {} step `{step_name}` no longer matches the configured provider identity ({})",
                    job.id,
                    mismatches.join(", ")
                ),
            )
            .with_suggestion(
                "Restore the original Repobox provider configuration or reconcile the recorded staging/canonical branches manually; do not resume against a different database.",
            ));
        }
    }
    Ok(())
}

fn update_pull_step_phase(
    job: &mut JobRecord,
    step: &str,
    phase: &str,
    updates: serde_json::Value,
) -> Result<()> {
    let mut resource = job
        .steps
        .iter()
        .find(|candidate| candidate.name == step)
        .and_then(|step| step.resource.as_object())
        .cloned()
        .ok_or_else(|| {
            RepoboxError::new(
                ErrorKind::Conflict,
                "pull_step_identity_missing",
                format!("pull step `{step}` has no provider identity checkpoint"),
            )
        })?;
    resource.insert(
        "phase".to_owned(),
        serde_json::Value::String(phase.to_owned()),
    );
    if let Some(updates) = updates.as_object() {
        resource.extend(updates.clone());
    }
    set_step_resource(job, step, serde_json::Value::Object(resource))
}

fn pull_step_may_require_forward_repair(job: &JobRecord, step: &str) -> bool {
    job.steps
        .iter()
        .find(|candidate| candidate.name == step)
        .and_then(|step| step.resource.get("phase"))
        .and_then(serde_json::Value::as_str)
        .is_some_and(|phase| matches!(phase, "credentialed" | "old_deleted"))
}

fn job_scope_matches(job: &JobRecord, step_prefix: &str, services: &[String]) -> bool {
    let expected = services
        .iter()
        .map(|service| format!("{step_prefix}{service}"))
        .collect::<BTreeSet<_>>();
    let actual = job
        .steps
        .iter()
        .map(|step| step.name.clone())
        .collect::<BTreeSet<_>>();
    expected == actual
}

fn provider_timeout(kind: &str, name: &str) -> RepoboxError {
    RepoboxError::new(
        ErrorKind::Runtime,
        "planetscale_operation_timeout",
        format!("timed out waiting for PlanetScale {kind} `{name}`"),
    )
    .with_suggestion("The durable job is safe to resume after checking provider status.")
}

pub fn state_for_environment<'a>(
    state: &'a ProjectState,
    environment: &str,
) -> Result<&'a EnvironmentRecord> {
    state.environments.get(environment).ok_or_else(|| {
        RepoboxError::new(
            ErrorKind::NotFound,
            "environment_not_found",
            format!("environment `{environment}` has not been provisioned"),
        )
    })
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use repobox_core::config::{
        BootstrapConfig, DatabaseEnvConfig, LocalServiceConfig, ServiceKind,
    };
    use repobox_core::provider::{Database, DatabaseRole, Organization, ProviderCapabilities};

    use super::*;
    use crate::cli::ColorChoice;

    #[derive(Default)]
    struct FakeProviderState {
        database_responses: VecDeque<Result<Vec<Database>>>,
        databases: Vec<Database>,
        branch_list_responses: VecDeque<Result<Vec<Branch>>>,
        branches: Vec<Branch>,
        branch_responses: VecDeque<Result<Branch>>,
        roles: Vec<DatabaseRole>,
        delete_branch_error: Option<RepoboxError>,
        database_list_calls: usize,
        branch_list_calls: usize,
        branch_get_calls: usize,
        create_database_calls: usize,
        create_branch_calls: usize,
        create_role_names: Vec<String>,
        rename_branch_calls: Vec<(String, String)>,
        delete_branch_calls: Vec<(String, String)>,
        delete_role_calls: usize,
        cancel_after_delete: Option<OperationCancellation>,
    }

    #[derive(Default)]
    struct FakeProvider {
        state: Mutex<FakeProviderState>,
    }

    #[async_trait]
    impl DatabaseProvider for FakeProvider {
        fn name(&self) -> &'static str {
            "fake"
        }

        async fn validate_auth(&self) -> Result<ProviderCapabilities> {
            Ok(ProviderCapabilities::default())
        }

        async fn list_organizations(&self) -> Result<Vec<Organization>> {
            Ok(vec![Organization {
                name: "test-org".to_owned(),
            }])
        }

        async fn list_databases(&self, _organization: &str) -> Result<Vec<Database>> {
            let mut state = self.state.lock().unwrap();
            state.database_list_calls += 1;
            if let Some(response) = state.database_responses.pop_front() {
                response
            } else {
                Ok(state.databases.clone())
            }
        }

        async fn create_database(&self, request: &CreateDatabaseRequest) -> Result<Database> {
            let mut state = self.state.lock().unwrap();
            state.create_database_calls += 1;
            let database = test_database(&request.name, false);
            state.databases.push(database.clone());
            Ok(database)
        }

        async fn delete_database(&self, _organization: &str, database: &str) -> Result<()> {
            self.state
                .lock()
                .unwrap()
                .databases
                .retain(|candidate| candidate.name != database);
            Ok(())
        }

        async fn list_cluster_sizes(&self, _organization: &str) -> Result<Vec<String>> {
            Ok(vec!["PS_10".to_owned()])
        }

        async fn list_branches(&self, _organization: &str, _database: &str) -> Result<Vec<Branch>> {
            let mut state = self.state.lock().unwrap();
            state.branch_list_calls += 1;
            if let Some(response) = state.branch_list_responses.pop_front() {
                response
            } else {
                Ok(state.branches.clone())
            }
        }

        async fn get_branch(
            &self,
            _organization: &str,
            _database: &str,
            branch: &str,
        ) -> Result<Branch> {
            let mut state = self.state.lock().unwrap();
            state.branch_get_calls += 1;
            if let Some(response) = state.branch_responses.pop_front() {
                return response;
            }
            state
                .branches
                .iter()
                .find(|candidate| candidate.name == branch)
                .cloned()
                .ok_or_else(|| provider_not_found("branch"))
        }

        async fn create_branch(&self, request: &CreateBranchRequest) -> Result<Branch> {
            let mut state = self.state.lock().unwrap();
            state.create_branch_calls += 1;
            let branch = test_branch(&request.name, false);
            state.branches.push(branch.clone());
            Ok(branch)
        }

        async fn rename_branch(
            &self,
            _organization: &str,
            _database: &str,
            branch: &str,
            new_name: &str,
        ) -> Result<Branch> {
            let mut state = self.state.lock().unwrap();
            state
                .rename_branch_calls
                .push((branch.to_owned(), new_name.to_owned()));
            let branch = state
                .branches
                .iter_mut()
                .find(|candidate| candidate.name == branch)
                .ok_or_else(|| provider_not_found("branch"))?;
            branch.name = new_name.to_owned();
            Ok(branch.clone())
        }

        async fn delete_branch(
            &self,
            _organization: &str,
            database: &str,
            branch: &str,
        ) -> Result<()> {
            let mut state = self.state.lock().unwrap();
            state
                .delete_branch_calls
                .push((database.to_owned(), branch.to_owned()));
            if let Some(error) = state.delete_branch_error.take() {
                return Err(error);
            }
            state.branches.retain(|candidate| candidate.name != branch);
            let cancellation = state.cancel_after_delete.take();
            drop(state);
            if let Some(cancellation) = cancellation {
                cancellation.cancel();
            }
            Ok(())
        }

        async fn list_backups(
            &self,
            _organization: &str,
            _database: &str,
            _branch: &str,
        ) -> Result<Vec<Backup>> {
            Ok(vec![test_backup()])
        }

        async fn create_backup(
            &self,
            _organization: &str,
            _database: &str,
            _branch: &str,
            _name: &str,
        ) -> Result<Backup> {
            Ok(test_backup())
        }

        async fn list_roles(
            &self,
            _organization: &str,
            _database: &str,
            _branch: &str,
        ) -> Result<Vec<DatabaseRole>> {
            Ok(self.state.lock().unwrap().roles.clone())
        }

        async fn create_role(&self, request: &CreateRoleRequest) -> Result<DatabaseRole> {
            let mut state = self.state.lock().unwrap();
            state.create_role_names.push(request.name.clone());
            let role = test_role(&request.name);
            state.roles.push(role.clone());
            Ok(role)
        }

        async fn delete_role(
            &self,
            _organization: &str,
            _database: &str,
            _branch: &str,
            role_id: &str,
            _successor: Option<&str>,
        ) -> Result<()> {
            let mut state = self.state.lock().unwrap();
            state.delete_role_calls += 1;
            state.roles.retain(|role| role.id != role_id);
            Ok(())
        }
    }

    fn provider_not_found(kind: &str) -> RepoboxError {
        RepoboxError::new(
            ErrorKind::NotFound,
            "provider_not_found",
            format!("{kind} was not found"),
        )
    }

    fn test_database(name: &str, ready: bool) -> Database {
        Database {
            id: format!("database-{name}"),
            name: name.to_owned(),
            ready,
            region: None,
        }
    }

    fn test_branch(name: &str, ready: bool) -> Branch {
        Branch {
            id: format!("branch-{name}"),
            name: name.to_owned(),
            state: if ready { "ready" } else { "pending" }.to_owned(),
            ready,
            production: false,
        }
    }

    fn test_backup() -> Backup {
        Backup {
            id: "backup-1".to_owned(),
            name: "backup-1".to_owned(),
            state: "success".to_owned(),
            size_bytes: 1,
            created_at: Utc::now(),
            completed_at: Some(Utc::now()),
        }
    }

    fn test_role(name: &str) -> DatabaseRole {
        DatabaseRole {
            id: format!("role-{name}"),
            name: name.to_owned(),
            username: "repobox_test".to_owned(),
            password: Some("test-password".to_owned()),
            database_name: "app".to_owned(),
            access_host_url: "example.test".to_owned(),
        }
    }

    fn test_service() -> ServiceConfig {
        ServiceConfig {
            kind: ServiceKind::Postgres,
            primary: true,
            local: LocalServiceConfig {
                compose_service: "postgres".to_owned(),
            },
            remote: RemoteServiceConfig::Planetscale {
                organization: "test-org".to_owned(),
                database: "app".to_owned(),
                base_branch: "main".to_owned(),
                cluster_size: "auto-smallest".to_owned(),
            },
            bootstrap: BootstrapConfig {
                mode: BootstrapMode::Empty,
            },
            env: DatabaseEnvConfig {
                pooled: "DATABASE_URL".to_owned(),
                direct: "DIRECT_DATABASE_URL".to_owned(),
            },
        }
    }

    fn test_config() -> RepoboxConfig {
        let mut config = RepoboxConfig::new_compose("test", vec![PathBuf::from("compose.yml")]);
        config.services.insert("db".to_owned(), test_service());
        config
    }

    fn two_service_config() -> RepoboxConfig {
        let mut config = RepoboxConfig::new_compose("test", vec![PathBuf::from("compose.yml")]);
        let mut first = test_service();
        let RemoteServiceConfig::Planetscale { database, .. } = &mut first.remote;
        "app-a".clone_into(database);
        first.env.pooled = "DATABASE_A_URL".to_owned();
        first.env.direct = "DIRECT_DATABASE_A_URL".to_owned();
        let mut second = test_service();
        let RemoteServiceConfig::Planetscale { database, .. } = &mut second.remote;
        "app-b".clone_into(database);
        second.env.pooled = "DATABASE_B_URL".to_owned();
        second.env.direct = "DIRECT_DATABASE_B_URL".to_owned();
        config.services.insert("a".to_owned(), first);
        config.services.insert("b".to_owned(), second);
        config
    }

    fn test_binding(service: &str, database: &str, branch: &str) -> DatabaseBinding {
        DatabaseBinding {
            service: service.to_owned(),
            provider: "planetscale".to_owned(),
            organization: "test-org".to_owned(),
            database: database.to_owned(),
            branch: branch.to_owned(),
            role_id: format!("role-{service}"),
            role_name: format!("repobox-{service}"),
            ready: true,
            updated_at: Utc::now(),
        }
    }

    fn checkpoint_pull_test_phase(
        job: &mut JobRecord,
        config: &RepoboxConfig,
        canonical: &str,
        phase: &str,
    ) {
        prepare_pull_job_resources(job, &config.services, canonical, false).unwrap();
        for service_name in config.services.keys() {
            update_pull_step_phase(
                job,
                &format!("refresh:{service_name}"),
                phase,
                serde_json::Value::Null,
            )
            .unwrap();
        }
    }

    fn fast_readiness() -> ProviderReadinessPolicy {
        ProviderReadinessPolicy {
            timeout: Duration::from_secs(1),
            poll_interval: Duration::ZERO,
        }
    }

    fn seed_database_credential_value(path: &std::path::Path, key: &str, value: &str) {
        let items = BTreeMap::from([(key.to_owned(), value.to_owned())]);
        std::fs::write(
            path,
            serde_json::to_vec(&serde_json::json!({
                "schema_version": 3,
                "items": items,
            }))
            .unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn credential_read_failure_is_neither_missing_nor_fallback_eligible() {
        let read_error = RepoboxError::new(
            ErrorKind::Runtime,
            "credential_read_failed",
            "keyring is temporarily unavailable",
        );
        let optional_error = optional_database_urls(Err(read_error.clone())).unwrap_err();
        assert_eq!(optional_error.code, "credential_read_failed");

        let fallback_called = std::cell::Cell::new(false);
        let fallback_error = database_urls_or_fallback(Err(read_error), || {
            fallback_called.set(true);
            Ok(("stale-pooled".to_owned(), "stale-direct".to_owned()))
        })
        .unwrap_err();
        assert_eq!(fallback_error.code, "credential_read_failed");
        assert!(!fallback_called.get());
    }

    #[tokio::test]
    async fn provision_read_failure_never_rotates_an_existing_role() {
        let config = test_config();
        let canonical = provider_branch_name(config.project.id, "feature").unwrap();
        let provider = FakeProvider::default();
        {
            let mut state = provider.state.lock().unwrap();
            state.databases.push(test_database("app", true));
            state.branches.push(test_branch(&canonical, true));
            state.roles.push(test_role(&role_name(&canonical, "db")));
        }
        let temp = tempfile::tempdir().unwrap();
        let credential_path = temp.path().join("credentials.json");
        let credentials = CredentialStore::new(&credential_path);
        let key = CredentialStore::database_key(config.project.id, &canonical, "db");
        seed_database_credential_value(&credential_path, &key, "not valid credential JSON");
        let output = Output::new(false, ColorChoice::Never);
        let mut manager = EnvironmentManager::new(
            &config,
            temp.path(),
            &provider,
            &credentials,
            StateStore::new(temp.path().join("state.json")),
            JobStore::new(temp.path().join("jobs.jsonl")),
            &output,
        );

        let error = manager
            .provision_service(
                &canonical,
                "db",
                config.services.get("db").unwrap(),
                &ProvisionOptions::default(),
            )
            .await
            .unwrap_err();

        assert_eq!(error.code, "credential_decode_failed");
        let state = provider.state.lock().unwrap();
        assert!(state.create_role_names.is_empty());
        assert_eq!(state.delete_role_calls, 0);
    }

    #[tokio::test]
    async fn pull_role_read_failure_never_deletes_or_creates_roles() {
        let config = test_config();
        let canonical = provider_branch_name(config.project.id, "feature").unwrap();
        let staging = format!("{canonical}-next-test");
        let provider = FakeProvider::default();
        provider
            .state
            .lock()
            .unwrap()
            .roles
            .push(test_role(&role_name(&canonical, "db")));
        let temp = tempfile::tempdir().unwrap();
        let credential_path = temp.path().join("credentials.json");
        let credentials = CredentialStore::new(&credential_path);
        let key = CredentialStore::database_key(config.project.id, &staging, "db");
        seed_database_credential_value(&credential_path, &key, "not valid credential JSON");
        let output = Output::new(false, ColorChoice::Never);
        let manager = EnvironmentManager::new(
            &config,
            temp.path(),
            &provider,
            &credentials,
            StateStore::new(temp.path().join("state.json")),
            JobStore::new(temp.path().join("jobs.jsonl")),
            &output,
        );

        let error = manager
            .ensure_pull_role("test-org", "app", &staging, &canonical, "db", &key)
            .await
            .unwrap_err();

        assert_eq!(error.code, "credential_decode_failed");
        let state = provider.state.lock().unwrap();
        assert!(state.create_role_names.is_empty());
        assert_eq!(state.delete_role_calls, 0);
    }

    #[test]
    fn stored_runtime_variables_propagate_credential_decode_errors() {
        let config = test_config();
        let canonical = provider_branch_name(config.project.id, "feature").unwrap();
        let temp = tempfile::tempdir().unwrap();
        let credential_path = temp.path().join("credentials.json");
        let credentials = CredentialStore::new(&credential_path);
        let key = CredentialStore::database_key(config.project.id, &canonical, "db");
        seed_database_credential_value(&credential_path, &key, "not valid credential JSON");
        let mut record = EnvironmentRecord::new("feature", &canonical);
        record.databases.insert(
            "db".to_owned(),
            DatabaseBinding {
                service: "db".to_owned(),
                provider: "planetscale".to_owned(),
                organization: "test-org".to_owned(),
                database: "app".to_owned(),
                branch: canonical.clone(),
                role_id: "role-1".to_owned(),
                role_name: role_name(&canonical, "db"),
                ready: true,
                updated_at: Utc::now(),
            },
        );

        let error = stored_environment_variables(&config, &record, &credentials).unwrap_err();

        assert_eq!(error.code, "credential_decode_failed");
    }

    #[test]
    fn runtime_variables_reject_ready_binding_from_previous_database_config() {
        let mut config = test_config();
        let canonical = provider_branch_name(config.project.id, "feature").unwrap();
        let temp = tempfile::tempdir().unwrap();
        let credentials = CredentialStore::new(temp.path().join("credentials.json"));
        let mut record = EnvironmentRecord::new("feature", &canonical);
        record.status = EnvironmentStatus::Ready;
        record.databases.insert(
            "db".to_owned(),
            test_binding("db", "previous-database", &canonical),
        );
        let RemoteServiceConfig::Planetscale { database, .. } =
            &mut config.services.get_mut("db").unwrap().remote;
        *database = "new-database".to_owned();

        let error = environment_variables(&config, &record, &credentials).unwrap_err();

        assert_eq!(error.code, "environment_binding_config_mismatch");
        assert!(error.message.contains("new-database"));
    }

    #[tokio::test]
    async fn ensure_rejects_config_drift_before_provider_or_state_mutation() {
        let mut config = test_config();
        let canonical = provider_branch_name(config.project.id, "feature").unwrap();
        let temp = tempfile::tempdir().unwrap();
        let state_path = temp.path().join("state.json");
        let state_store = StateStore::new(&state_path);
        let mut state = ProjectState::new(config.project.id);
        let mut environment = EnvironmentRecord::new("feature", &canonical);
        environment.status = EnvironmentStatus::Ready;
        environment
            .databases
            .insert("db".to_owned(), test_binding("db", "app", &canonical));
        state.environments.insert("feature".to_owned(), environment);
        state_store.save(&state).unwrap();
        let state_before = std::fs::read(&state_path).unwrap();
        let RemoteServiceConfig::Planetscale {
            organization,
            database,
            ..
        } = &mut config.services.get_mut("db").unwrap().remote;
        *organization = "replacement-org".to_owned();
        *database = "replacement-app".to_owned();
        let provider = FakeProvider::default();
        let credentials = CredentialStore::new(temp.path().join("credentials.json"));
        let jobs = JobStore::new(temp.path().join("jobs.jsonl"));
        let output = Output::new(false, ColorChoice::Never);
        let mut manager = EnvironmentManager::new(
            &config,
            temp.path(),
            &provider,
            &credentials,
            state_store,
            jobs.clone(),
            &output,
        );

        let error = manager
            .ensure("feature", &ProvisionOptions::default())
            .await
            .unwrap_err();

        assert_eq!(error.code, "environment_binding_config_mismatch");
        assert_eq!(std::fs::read(&state_path).unwrap(), state_before);
        assert!(jobs.list().unwrap().is_empty());
        let provider_state = provider.state.lock().unwrap();
        assert_eq!(provider_state.database_list_calls, 0);
        assert_eq!(provider_state.branch_list_calls, 0);
        assert_eq!(provider_state.create_database_calls, 0);
        assert_eq!(provider_state.create_branch_calls, 0);
        assert!(provider_state.delete_branch_calls.is_empty());
    }

    #[tokio::test]
    async fn new_pull_rejects_config_drift_before_provider_or_state_mutation() {
        let mut config = test_config();
        let canonical = provider_branch_name(config.project.id, "feature").unwrap();
        let temp = tempfile::tempdir().unwrap();
        let state_path = temp.path().join("state.json");
        let state_store = StateStore::new(&state_path);
        let mut state = ProjectState::new(config.project.id);
        let mut environment = EnvironmentRecord::new("feature", &canonical);
        environment.status = EnvironmentStatus::Ready;
        environment
            .databases
            .insert("db".to_owned(), test_binding("db", "app", &canonical));
        state.environments.insert("feature".to_owned(), environment);
        state_store.save(&state).unwrap();
        let state_before = std::fs::read(&state_path).unwrap();
        let RemoteServiceConfig::Planetscale {
            organization,
            database,
            ..
        } = &mut config.services.get_mut("db").unwrap().remote;
        *organization = "replacement-org".to_owned();
        *database = "replacement-app".to_owned();
        let provider = FakeProvider::default();
        let credentials = CredentialStore::new(temp.path().join("credentials.json"));
        let jobs = JobStore::new(temp.path().join("jobs.jsonl"));
        let output = Output::new(false, ColorChoice::Never);
        let mut manager = EnvironmentManager::new(
            &config,
            temp.path(),
            &provider,
            &credentials,
            state_store,
            jobs.clone(),
            &output,
        );

        let error = manager
            .pull("feature", &ProvisionOptions::default())
            .await
            .unwrap_err();

        assert_eq!(error.code, "environment_binding_config_mismatch");
        assert_eq!(std::fs::read(&state_path).unwrap(), state_before);
        assert!(jobs.list().unwrap().is_empty());
        let provider_state = provider.state.lock().unwrap();
        assert_eq!(provider_state.database_list_calls, 0);
        assert_eq!(provider_state.branch_list_calls, 0);
        assert_eq!(provider_state.create_branch_calls, 0);
        assert!(provider_state.delete_branch_calls.is_empty());
        assert!(provider_state.rename_branch_calls.is_empty());
    }

    #[test]
    fn create_and_pull_cannot_cross_an_unresolved_mutation_lineage() {
        let config = test_config();
        let provider = FakeProvider::default();
        let temp = tempfile::tempdir().unwrap();
        let credentials = CredentialStore::new(temp.path().join("credentials.json"));
        let output = Output::new(false, ColorChoice::Never);
        let jobs = JobStore::new(temp.path().join("jobs.jsonl"));
        let mut pull = JobRecord::new(
            JobKind::EnvironmentPull,
            config.project.id,
            "blocked-create",
            ["refresh:db"],
        );
        pull.status = JobStatus::Degraded;
        jobs.append(&pull).unwrap();
        let mut create = JobRecord::new(
            JobKind::EnvironmentCreate,
            config.project.id,
            "blocked-pull",
            ["provision:db"],
        );
        create.status = JobStatus::Degraded;
        jobs.append(&create).unwrap();
        let manager = EnvironmentManager::new(
            &config,
            temp.path(),
            &provider,
            &credentials,
            StateStore::new(temp.path().join("state.json")),
            jobs.clone(),
            &output,
        );

        let create_error = manager
            .resumable_job(
                JobKind::EnvironmentCreate,
                "blocked-create",
                "provision:",
                config.services.keys(),
            )
            .unwrap_err();
        let pull_error = manager
            .resumable_job(
                JobKind::EnvironmentPull,
                "blocked-pull",
                "refresh:",
                config.services.keys(),
            )
            .unwrap_err();

        assert_eq!(create_error.code, "environment_recovery_required");
        assert!(create_error.message.contains(&pull.id.to_string()));
        assert_eq!(pull_error.code, "environment_recovery_required");
        assert!(pull_error.message.contains(&create.id.to_string()));
        assert_eq!(jobs.list().unwrap().len(), 2);
    }

    #[test]
    fn multiple_nonterminal_jobs_are_never_silently_collapsed_into_one_lineage() {
        let config = test_config();
        let provider = FakeProvider::default();
        let temp = tempfile::tempdir().unwrap();
        let credentials = CredentialStore::new(temp.path().join("credentials.json"));
        let output = Output::new(false, ColorChoice::Never);
        let jobs = JobStore::new(temp.path().join("jobs.jsonl"));
        for _ in 0..2 {
            let mut create = JobRecord::new(
                JobKind::EnvironmentCreate,
                config.project.id,
                "feature",
                ["provision:db"],
            );
            create.status = JobStatus::Degraded;
            jobs.append(&create).unwrap();
        }
        let manager = EnvironmentManager::new(
            &config,
            temp.path(),
            &provider,
            &credentials,
            StateStore::new(temp.path().join("state.json")),
            jobs,
            &output,
        );

        let error = manager
            .resumable_job(
                JobKind::EnvironmentCreate,
                "feature",
                "provision:",
                config.services.keys(),
            )
            .unwrap_err();

        assert_eq!(error.code, "environment_mutation_lineage_conflict");
    }

    #[test]
    fn readiness_budget_covers_observed_planetscale_latency() {
        let observed_branch_readiness = Duration::from_secs(11 * 60 + 51);

        assert_eq!(PROVIDER_READINESS_TIMEOUT, Duration::from_mins(15));
        assert!(PROVIDER_READINESS_TIMEOUT > observed_branch_readiness);
    }

    #[tokio::test]
    async fn branch_wait_retries_eventual_not_found_and_pending_states() {
        let config = test_config();
        let provider = FakeProvider::default();
        {
            let mut state = provider.state.lock().unwrap();
            state
                .branch_responses
                .push_back(Err(provider_not_found("branch")));
            state
                .branch_responses
                .push_back(Ok(test_branch("rbx-feature", false)));
            state
                .branch_responses
                .push_back(Ok(test_branch("rbx-feature", true)));
        }
        let temp = tempfile::tempdir().unwrap();
        let credentials = CredentialStore::new(temp.path().join("credentials.json"));
        let output = Output::new(false, ColorChoice::Never);
        let manager = EnvironmentManager::new(
            &config,
            temp.path(),
            &provider,
            &credentials,
            StateStore::new(temp.path().join("state.json")),
            JobStore::new(temp.path().join("jobs.jsonl")),
            &output,
        )
        .with_readiness_policy(fast_readiness());

        manager
            .wait_for_branch("test-org", "app", "rbx-feature")
            .await
            .unwrap();

        assert_eq!(provider.state.lock().unwrap().branch_get_calls, 3);
    }

    #[tokio::test]
    async fn branch_wait_times_out_with_a_resumable_error() {
        let config = test_config();
        let provider = FakeProvider::default();
        provider
            .state
            .lock()
            .unwrap()
            .branch_responses
            .push_back(Ok(test_branch("rbx-feature", false)));
        let temp = tempfile::tempdir().unwrap();
        let credentials = CredentialStore::new(temp.path().join("credentials.json"));
        let output = Output::new(false, ColorChoice::Never);
        let manager = EnvironmentManager::new(
            &config,
            temp.path(),
            &provider,
            &credentials,
            StateStore::new(temp.path().join("state.json")),
            JobStore::new(temp.path().join("jobs.jsonl")),
            &output,
        )
        .with_readiness_policy(ProviderReadinessPolicy {
            timeout: Duration::ZERO,
            poll_interval: Duration::ZERO,
        });

        let error = manager
            .wait_for_branch("test-org", "app", "rbx-feature")
            .await
            .unwrap_err();

        assert_eq!(error.code, "planetscale_operation_timeout");
        assert!(error.suggestion.unwrap().contains("safe to resume"));
        assert_eq!(provider.state.lock().unwrap().branch_get_calls, 1);
    }

    #[tokio::test]
    async fn branch_wait_propagates_non_retryable_provider_errors() {
        let config = test_config();
        let provider = FakeProvider::default();
        provider
            .state
            .lock()
            .unwrap()
            .branch_responses
            .push_back(Err(RepoboxError::new(
                ErrorKind::Permission,
                "provider_permission_denied",
                "permission denied",
            )));
        let temp = tempfile::tempdir().unwrap();
        let credentials = CredentialStore::new(temp.path().join("credentials.json"));
        let output = Output::new(false, ColorChoice::Never);
        let manager = EnvironmentManager::new(
            &config,
            temp.path(),
            &provider,
            &credentials,
            StateStore::new(temp.path().join("state.json")),
            JobStore::new(temp.path().join("jobs.jsonl")),
            &output,
        )
        .with_readiness_policy(fast_readiness());

        let error = manager
            .wait_for_branch("test-org", "app", "rbx-feature")
            .await
            .unwrap_err();

        assert_eq!(error.code, "provider_permission_denied");
        assert_eq!(provider.state.lock().unwrap().branch_get_calls, 1);
    }

    #[tokio::test]
    async fn resumed_create_clears_stale_error_without_duplicate_posts() {
        let config = test_config();
        let provider_branch = provider_branch_name(config.project.id, "feature").unwrap();
        let provider = FakeProvider::default();
        {
            let mut state = provider.state.lock().unwrap();
            state
                .database_responses
                .push_back(Ok(vec![test_database("app", false)]));
            state
                .database_responses
                .push_back(Ok(vec![test_database("app", true)]));
            state.branches.push(test_branch(&provider_branch, false));
            state
                .branch_responses
                .push_back(Err(provider_not_found("branch")));
            state
                .branch_responses
                .push_back(Ok(test_branch(&provider_branch, false)));
            state
                .branch_responses
                .push_back(Ok(test_branch(&provider_branch, true)));
            state
                .roles
                .push(test_role(&role_name(&provider_branch, "db")));
        }
        let temp = tempfile::tempdir().unwrap();
        let credentials = CredentialStore::new(temp.path().join("credentials.json"));
        let credential_key =
            CredentialStore::database_key(config.project.id, &provider_branch, "db");
        credentials
            .store_database_urls(
                &credential_key,
                "postgresql://test:password@example.test:6432/app",
                "postgresql://test:password@example.test:5432/app",
            )
            .unwrap();
        let state_store = StateStore::new(temp.path().join("state.json"));
        let job_store = JobStore::new(temp.path().join("jobs.jsonl"));
        let mut previous = JobRecord::new(
            JobKind::EnvironmentCreate,
            config.project.id,
            "feature",
            ["provision:db"],
        );
        prepare_create_job_resources(&mut previous, &config.services, &provider_branch, false)
            .unwrap();
        previous.status = JobStatus::Degraded;
        previous.error_code = Some("planetscale_import_failed".to_owned());
        previous
            .update_step("provision:db", StepStatus::Running, None)
            .unwrap();
        previous
            .update_step(
                "provision:db",
                StepStatus::Failed,
                Some("readiness timed out".to_owned()),
            )
            .unwrap();
        job_store.append(&previous).unwrap();
        let mut newer = JobRecord::new(
            JobKind::EnvironmentCreate,
            config.project.id,
            "feature",
            ["provision:db"],
        );
        newer.status = JobStatus::Canceled;
        job_store.append(&newer).unwrap();
        let jobs = job_store.clone();
        let output = Output::new(false, ColorChoice::Never);
        let mut manager = EnvironmentManager::new(
            &config,
            temp.path(),
            &provider,
            &credentials,
            state_store,
            job_store,
            &output,
        )
        .with_readiness_policy(fast_readiness());

        let mutation = manager
            .resume_create(previous.id, "feature", &ProvisionOptions::default())
            .await
            .unwrap();

        assert!(mutation.resumed);
        assert_eq!(mutation.job.id, previous.id);
        assert_eq!(mutation.job.status, JobStatus::Succeeded);
        assert_eq!(mutation.job.error_code, None);
        assert_eq!(jobs.get(previous.id).unwrap().error_code, None);
        assert_eq!(jobs.get(newer.id).unwrap().status, JobStatus::Canceled);
        let state = provider.state.lock().unwrap();
        assert_eq!(state.database_list_calls, 2);
        assert_eq!(state.branch_get_calls, 3);
        assert_eq!(state.create_database_calls, 0);
        assert_eq!(state.create_branch_calls, 0);
        assert!(state.create_role_names.is_empty());
        drop(state);
        drop(manager);
        credentials.remove_database_urls(&credential_key).unwrap();
    }

    #[tokio::test]
    async fn resumed_pull_waits_for_existing_staging_branch_without_duplicate_post() {
        let config = test_config();
        let canonical = provider_branch_name(config.project.id, "feature").unwrap();
        let provider = FakeProvider::default();
        let temp = tempfile::tempdir().unwrap();
        let credentials = CredentialStore::new(temp.path().join("credentials.json"));
        let output = Output::new(false, ColorChoice::Never);
        let mut job = JobRecord::new(
            JobKind::EnvironmentPull,
            config.project.id,
            "feature",
            ["refresh:db"],
        );
        let staging = staging_branch_name(&canonical, job.id);
        checkpoint_pull_test_phase(&mut job, &config, &canonical, "credentialed");
        let desired_role_name = role_name(&canonical, "db");
        {
            let mut state = provider.state.lock().unwrap();
            state.databases.push(test_database("app", true));
            state.branches.push(test_branch(&canonical, true));
            state.branches.push(test_branch(&staging, false));
            state
                .branch_responses
                .push_back(Ok(test_branch(&staging, true)));
            state
                .branch_responses
                .push_back(Ok(test_branch(&canonical, true)));
            state.roles.push(test_role(&desired_role_name));
        }
        let staging_key = CredentialStore::database_key(config.project.id, &staging, "db");
        let canonical_key = CredentialStore::database_key(config.project.id, &canonical, "db");
        credentials
            .store_database_urls(
                &staging_key,
                "postgresql://test:password@example.test:6432/app",
                "postgresql://test:password@example.test:5432/app",
            )
            .unwrap();
        let mut manager = EnvironmentManager::new(
            &config,
            temp.path(),
            &provider,
            &credentials,
            StateStore::new(temp.path().join("state.json")),
            JobStore::new(temp.path().join("jobs.jsonl")),
            &output,
        )
        .with_readiness_policy(fast_readiness());

        let binding = manager
            .pull_service(
                &canonical,
                "db",
                config.services.get("db").unwrap(),
                &ProvisionOptions::default(),
                &mut job,
                "refresh:db",
            )
            .await
            .unwrap();

        assert_eq!(binding.branch, canonical);
        assert_eq!(binding.role_name, desired_role_name);
        let state = provider.state.lock().unwrap();
        assert_eq!(state.create_branch_calls, 0);
        assert_eq!(state.branch_get_calls, 1);
        assert!(state.branches.iter().any(|branch| branch.name == canonical));
        assert!(!state.branches.iter().any(|branch| branch.name == staging));
        drop(state);
        assert!(credentials.database_urls(&staging_key).is_err());
        assert!(credentials.database_urls(&canonical_key).is_ok());
        drop(manager);
        credentials.remove_database_urls(&canonical_key).unwrap();
    }

    #[tokio::test]
    async fn exact_pull_resume_uses_the_approved_job_and_its_staging_identity() {
        let config = test_config();
        let canonical = provider_branch_name(config.project.id, "feature").unwrap();
        let provider = FakeProvider::default();
        let temp = tempfile::tempdir().unwrap();
        let credentials = CredentialStore::new(temp.path().join("credentials.json"));
        let output = Output::new(false, ColorChoice::Never);
        let mut approved = JobRecord::new(
            JobKind::EnvironmentPull,
            config.project.id,
            "feature",
            ["refresh:db"],
        );
        approved.status = JobStatus::Degraded;
        approved.error_code = Some("planetscale_import_failed".to_owned());
        let approved_staging = staging_branch_name(&canonical, approved.id);
        checkpoint_pull_test_phase(&mut approved, &config, &canonical, "credentialed");
        let mut newer = JobRecord::new(
            JobKind::EnvironmentPull,
            config.project.id,
            "feature",
            ["refresh:db"],
        );
        newer.status = JobStatus::Succeeded;
        let newer_staging = staging_branch_name(&canonical, newer.id);

        {
            let mut state = provider.state.lock().unwrap();
            state.databases.push(test_database("app", true));
            state.branches.push(test_branch(&canonical, true));
            state.branches.push(test_branch(&approved_staging, false));
            state
                .branch_responses
                .push_back(Ok(test_branch(&approved_staging, true)));
            state
                .branch_responses
                .push_back(Ok(test_branch(&canonical, true)));
            state.roles.push(test_role(&role_name(&canonical, "db")));
        }

        let staging_key = CredentialStore::database_key(config.project.id, &approved_staging, "db");
        let canonical_key = CredentialStore::database_key(config.project.id, &canonical, "db");
        credentials
            .store_database_urls(
                &staging_key,
                "postgresql://test:password@example.test:6432/app",
                "postgresql://test:password@example.test:5432/app",
            )
            .unwrap();

        let state_store = StateStore::new(temp.path().join("state.json"));
        let mut project_state = ProjectState::new(config.project.id);
        let mut environment = EnvironmentRecord::new("feature", &canonical);
        environment.status = EnvironmentStatus::Ready;
        project_state
            .environments
            .insert("feature".to_owned(), environment);
        state_store.save(&project_state).unwrap();

        let job_store = JobStore::new(temp.path().join("jobs.jsonl"));
        job_store.append(&approved).unwrap();
        job_store.append(&newer).unwrap();
        let jobs = job_store.clone();
        let mut manager = EnvironmentManager::new(
            &config,
            temp.path(),
            &provider,
            &credentials,
            state_store,
            job_store,
            &output,
        )
        .with_readiness_policy(fast_readiness());

        let mutation = manager
            .resume_pull(approved.id, "feature", &ProvisionOptions::default())
            .await
            .unwrap();

        assert_eq!(mutation.job.id, approved.id);
        assert_eq!(mutation.job.error_code, None);
        assert_eq!(jobs.get(approved.id).unwrap().error_code, None);
        assert_eq!(jobs.get(newer.id).unwrap().status, JobStatus::Succeeded);
        let state = provider.state.lock().unwrap();
        assert_eq!(
            state.rename_branch_calls,
            vec![(approved_staging.clone(), canonical.clone())]
        );
        assert!(
            state
                .rename_branch_calls
                .iter()
                .all(|(source, _)| source != &newer_staging)
        );
        drop(state);
        drop(manager);
        assert!(credentials.database_urls(&staging_key).is_err());
        assert!(credentials.database_urls(&canonical_key).is_ok());
        credentials.remove_database_urls(&canonical_key).unwrap();
    }

    #[tokio::test]
    async fn pull_cancellation_finishes_forward_repair_and_skips_later_services() {
        let config = two_service_config();
        let canonical = provider_branch_name(config.project.id, "feature").unwrap();
        let cancellation = OperationCancellation::default();
        let provider = FakeProvider::default();
        let temp = tempfile::tempdir().unwrap();
        let credentials = CredentialStore::new(temp.path().join("credentials.json"));
        let output = Output::new(false, ColorChoice::Never);
        let mut job = JobRecord::new(
            JobKind::EnvironmentPull,
            config.project.id,
            "feature",
            ["refresh:a", "refresh:b"],
        );
        job.status = JobStatus::Degraded;
        let staging = staging_branch_name(&canonical, job.id);
        checkpoint_pull_test_phase(&mut job, &config, &canonical, "credentialed");
        {
            let mut provider_state = provider.state.lock().unwrap();
            provider_state
                .databases
                .extend([test_database("app-a", true), test_database("app-b", true)]);
            provider_state
                .branches
                .extend([test_branch(&canonical, true), test_branch(&staging, true)]);
            provider_state.cancel_after_delete = Some(cancellation.clone());
        }
        let state_store = StateStore::new(temp.path().join("state.json"));
        let mut project_state = ProjectState::new(config.project.id);
        project_state.environments.insert(
            "feature".to_owned(),
            EnvironmentRecord::new("feature", &canonical),
        );
        state_store.save(&project_state).unwrap();
        let jobs = JobStore::new(temp.path().join("jobs.jsonl"));
        jobs.append(&job).unwrap();
        let mut manager = EnvironmentManager::new(
            &config,
            temp.path(),
            &provider,
            &credentials,
            state_store,
            jobs.clone(),
            &output,
        )
        .with_cancellation(cancellation);

        let error = manager
            .resume_pull(job.id, "feature", &ProvisionOptions::default())
            .await
            .unwrap_err();

        assert_eq!(error.code, "operation_interrupted");
        let provider_state = provider.state.lock().unwrap();
        assert_eq!(
            provider_state.delete_branch_calls,
            vec![("app-a".to_owned(), canonical.clone())]
        );
        assert_eq!(
            provider_state.rename_branch_calls,
            vec![(staging, canonical)]
        );
        drop(provider_state);
        let stored = jobs.get(job.id).unwrap();
        assert_eq!(stored.status, JobStatus::Degraded);
        assert_eq!(stored.error_code.as_deref(), Some("operation_interrupted"));
        assert_eq!(stored.steps[0].status, StepStatus::Failed);
        assert_eq!(stored.steps[0].resource["phase"], "renamed");
        assert_eq!(stored.steps[1].status, StepStatus::Pending);
    }

    #[tokio::test]
    async fn pull_resume_rejects_remote_identity_changed_since_checkpoint() {
        let mut config = test_config();
        let canonical = provider_branch_name(config.project.id, "feature").unwrap();
        let mut job = JobRecord::new(
            JobKind::EnvironmentPull,
            config.project.id,
            "feature",
            ["refresh:db"],
        );
        job.status = JobStatus::Degraded;
        checkpoint_pull_test_phase(&mut job, &config, &canonical, "credentialed");
        let RemoteServiceConfig::Planetscale { database, .. } =
            &mut config.services.get_mut("db").unwrap().remote;
        *database = "replacement-app".to_owned();

        let provider = FakeProvider::default();
        let temp = tempfile::tempdir().unwrap();
        let credentials = CredentialStore::new(temp.path().join("credentials.json"));
        let state_store = StateStore::new(temp.path().join("state.json"));
        let mut project_state = ProjectState::new(config.project.id);
        let mut environment = EnvironmentRecord::new("feature", &canonical);
        environment.status = EnvironmentStatus::Degraded;
        project_state
            .environments
            .insert("feature".to_owned(), environment);
        state_store.save(&project_state).unwrap();
        let jobs = JobStore::new(temp.path().join("jobs.jsonl"));
        jobs.append(&job).unwrap();
        let output = Output::new(false, ColorChoice::Never);
        let mut manager = EnvironmentManager::new(
            &config,
            temp.path(),
            &provider,
            &credentials,
            state_store,
            jobs,
            &output,
        );

        let error = manager
            .resume_pull(job.id, "feature", &ProvisionOptions::default())
            .await
            .unwrap_err();

        assert_eq!(error.code, "pull_resume_identity_mismatch");
        let provider_state = provider.state.lock().unwrap();
        assert_eq!(provider_state.database_list_calls, 0);
        assert_eq!(provider_state.branch_list_calls, 0);
        assert_eq!(provider_state.branch_get_calls, 0);
        assert_eq!(provider_state.create_branch_calls, 0);
        assert!(provider_state.delete_branch_calls.is_empty());
        assert!(provider_state.rename_branch_calls.is_empty());
    }

    #[tokio::test]
    async fn credentialed_pull_never_deletes_canonical_when_staging_is_missing() {
        let config = test_config();
        let canonical = provider_branch_name(config.project.id, "feature").unwrap();
        let provider = FakeProvider::default();
        provider
            .state
            .lock()
            .unwrap()
            .branches
            .push(test_branch(&canonical, true));
        let temp = tempfile::tempdir().unwrap();
        let credentials = CredentialStore::new(temp.path().join("credentials.json"));
        let jobs = JobStore::new(temp.path().join("jobs.jsonl"));
        let output = Output::new(false, ColorChoice::Never);
        let mut job = JobRecord::new(
            JobKind::EnvironmentPull,
            config.project.id,
            "feature",
            ["refresh:db"],
        );
        checkpoint_pull_test_phase(&mut job, &config, &canonical, "credentialed");
        let mut manager = EnvironmentManager::new(
            &config,
            temp.path(),
            &provider,
            &credentials,
            StateStore::new(temp.path().join("state.json")),
            jobs,
            &output,
        );

        let error = manager
            .pull_service(
                &canonical,
                "db",
                config.services.get("db").unwrap(),
                &ProvisionOptions::default(),
                &mut job,
                "refresh:db",
            )
            .await
            .unwrap_err();

        assert_eq!(error.code, "pull_staging_branch_missing");
        let provider_state = provider.state.lock().unwrap();
        assert_eq!(provider_state.create_branch_calls, 0);
        assert!(provider_state.delete_branch_calls.is_empty());
        assert!(provider_state.rename_branch_calls.is_empty());
        assert!(
            provider_state
                .branches
                .iter()
                .any(|branch| branch.name == canonical)
        );
    }

    #[tokio::test]
    async fn exact_pull_resume_preserves_succeeded_service_and_repairs_failed_service() {
        let config = two_service_config();
        let canonical = provider_branch_name(config.project.id, "feature").unwrap();
        let provider = FakeProvider::default();
        let temp = tempfile::tempdir().unwrap();
        let credentials = CredentialStore::new(temp.path().join("credentials.json"));
        let mut job = JobRecord::new(
            JobKind::EnvironmentPull,
            config.project.id,
            "feature",
            ["refresh:a", "refresh:b"],
        );
        job.status = JobStatus::Degraded;
        job.error_code = Some("prior_failure".to_owned());
        checkpoint_pull_test_phase(&mut job, &config, &canonical, "credentialed");
        update_pull_step_phase(&mut job, "refresh:a", "complete", serde_json::Value::Null).unwrap();
        job.update_step(
            "refresh:a",
            StepStatus::Succeeded,
            Some("already swapped".to_owned()),
        )
        .unwrap();
        job.update_step(
            "refresh:b",
            StepStatus::Failed,
            Some("interrupted before swap".to_owned()),
        )
        .unwrap();
        let succeeded_resource = job.steps[0].resource.clone();
        let staging = staging_branch_name(&canonical, job.id);
        {
            let mut provider_state = provider.state.lock().unwrap();
            provider_state
                .branches
                .extend([test_branch(&canonical, true), test_branch(&staging, true)]);
            provider_state
                .roles
                .push(test_role(&role_name(&canonical, "b")));
        }
        let staging_key = CredentialStore::database_key(config.project.id, &staging, "b");
        let canonical_key = CredentialStore::database_key(config.project.id, &canonical, "b");
        credentials
            .store_database_urls(
                &staging_key,
                "postgresql://test:password@example.test:6432/app",
                "postgresql://test:password@example.test:5432/app",
            )
            .unwrap();
        let state_store = StateStore::new(temp.path().join("state.json"));
        let mut project_state = ProjectState::new(config.project.id);
        let mut environment = EnvironmentRecord::new("feature", &canonical);
        environment.status = EnvironmentStatus::Degraded;
        environment
            .databases
            .insert("a".to_owned(), test_binding("a", "app-a", &canonical));
        project_state
            .environments
            .insert("feature".to_owned(), environment);
        state_store.save(&project_state).unwrap();
        let jobs = JobStore::new(temp.path().join("jobs.jsonl"));
        jobs.append(&job).unwrap();
        let output = Output::new(false, ColorChoice::Never);
        let mut manager = EnvironmentManager::new(
            &config,
            temp.path(),
            &provider,
            &credentials,
            state_store,
            jobs.clone(),
            &output,
        )
        .with_readiness_policy(fast_readiness());

        let mutation = manager
            .resume_pull(job.id, "feature", &ProvisionOptions::default())
            .await
            .unwrap();

        assert_eq!(mutation.job.status, JobStatus::Succeeded);
        assert_eq!(mutation.job.error_code, None);
        assert_eq!(mutation.job.steps[0].status, StepStatus::Succeeded);
        assert_eq!(mutation.job.steps[0].resource, succeeded_resource);
        assert_eq!(mutation.job.steps[1].status, StepStatus::Succeeded);
        assert_eq!(
            provider.state.lock().unwrap().delete_branch_calls,
            vec![("app-b".to_owned(), canonical.clone())]
        );
        assert_eq!(
            provider.state.lock().unwrap().rename_branch_calls,
            vec![(staging, canonical)]
        );
        assert!(credentials.database_urls(&staging_key).is_err());
        assert!(credentials.database_urls(&canonical_key).is_ok());
        drop(manager);
        credentials.remove_database_urls(&canonical_key).unwrap();
    }

    #[tokio::test]
    async fn pull_subset_cannot_reuse_degraded_full_scope_job() {
        let config = two_service_config();
        let canonical = provider_branch_name(config.project.id, "feature").unwrap();
        let provider = FakeProvider::default();
        let temp = tempfile::tempdir().unwrap();
        let credentials = CredentialStore::new(temp.path().join("credentials.json"));
        let state_store = StateStore::new(temp.path().join("state.json"));
        let mut project_state = ProjectState::new(config.project.id);
        let mut environment = EnvironmentRecord::new("feature", &canonical);
        environment.status = EnvironmentStatus::Degraded;
        project_state
            .environments
            .insert("feature".to_owned(), environment);
        state_store.save(&project_state).unwrap();
        let mut job = JobRecord::new(
            JobKind::EnvironmentPull,
            config.project.id,
            "feature",
            ["refresh:a", "refresh:b"],
        );
        job.status = JobStatus::Degraded;
        checkpoint_pull_test_phase(&mut job, &config, &canonical, "credentialed");
        let jobs = JobStore::new(temp.path().join("jobs.jsonl"));
        jobs.append(&job).unwrap();
        let output = Output::new(false, ColorChoice::Never);
        let mut manager = EnvironmentManager::new(
            &config,
            temp.path(),
            &provider,
            &credentials,
            state_store,
            jobs,
            &output,
        );
        let options = ProvisionOptions {
            selected_services: BTreeSet::from(["a".to_owned()]),
            ..ProvisionOptions::default()
        };

        let error = manager.pull("feature", &options).await.unwrap_err();

        assert_eq!(error.code, "environment_recovery_required");
        assert!(error.suggestion.unwrap().contains(&job.id.to_string()));
        let provider_state = provider.state.lock().unwrap();
        assert_eq!(provider_state.database_list_calls, 0);
        assert_eq!(provider_state.branch_list_calls, 0);
        assert_eq!(provider_state.create_branch_calls, 0);
        assert!(provider_state.delete_branch_calls.is_empty());
        assert!(provider_state.rename_branch_calls.is_empty());
    }

    #[tokio::test]
    async fn canceled_old_deleted_pull_repairs_rename_without_database_readiness() {
        let config = test_config();
        let canonical = provider_branch_name(config.project.id, "feature").unwrap();
        let cancellation = OperationCancellation::default();
        cancellation.cancel();
        let provider = FakeProvider::default();
        let temp = tempfile::tempdir().unwrap();
        let credentials = CredentialStore::new(temp.path().join("credentials.json"));
        let jobs = JobStore::new(temp.path().join("jobs.jsonl"));
        let output = Output::new(false, ColorChoice::Never);
        let mut job = JobRecord::new(
            JobKind::EnvironmentPull,
            config.project.id,
            "feature",
            ["refresh:db"],
        );
        let staging = staging_branch_name(&canonical, job.id);
        checkpoint_pull_test_phase(&mut job, &config, &canonical, "old_deleted");
        provider
            .state
            .lock()
            .unwrap()
            .branches
            .push(test_branch(&staging, true));
        let mut manager = EnvironmentManager::new(
            &config,
            temp.path(),
            &provider,
            &credentials,
            StateStore::new(temp.path().join("state.json")),
            jobs,
            &output,
        )
        .with_cancellation(cancellation);

        let error = manager
            .pull_service(
                &canonical,
                "db",
                config.services.get("db").unwrap(),
                &ProvisionOptions::default(),
                &mut job,
                "refresh:db",
            )
            .await
            .unwrap_err();

        assert_eq!(error.code, "operation_interrupted");
        let provider_state = provider.state.lock().unwrap();
        assert_eq!(provider_state.database_list_calls, 0);
        assert_eq!(
            provider_state.rename_branch_calls,
            vec![(staging, canonical)]
        );
        drop(provider_state);
        assert_eq!(job.steps[0].resource["phase"], "renamed");
    }

    #[tokio::test]
    async fn pull_role_uses_canonical_identity_and_is_reused_by_create() {
        let config = test_config();
        let canonical = provider_branch_name(config.project.id, "feature").unwrap();
        let staging = format!("{canonical}-next-test");
        let provider = FakeProvider::default();
        let temp = tempfile::tempdir().unwrap();
        let credentials = CredentialStore::new(temp.path().join("credentials.json"));
        let staging_key = CredentialStore::database_key(config.project.id, &staging, "db");
        let canonical_key = CredentialStore::database_key(config.project.id, &canonical, "db");
        let output = Output::new(false, ColorChoice::Never);
        let mut manager = EnvironmentManager::new(
            &config,
            temp.path(),
            &provider,
            &credentials,
            StateStore::new(temp.path().join("state.json")),
            JobStore::new(temp.path().join("jobs.jsonl")),
            &output,
        )
        .with_readiness_policy(fast_readiness());

        let first = manager
            .ensure_pull_role("test-org", "app", &staging, &canonical, "db", &staging_key)
            .await
            .unwrap();
        let second = manager
            .ensure_pull_role("test-org", "app", &staging, &canonical, "db", &staging_key)
            .await
            .unwrap();

        let canonical_role_name = role_name(&canonical, "db");
        assert_eq!(first.name, canonical_role_name);
        assert_eq!(second.id, first.id);
        assert_ne!(first.name, role_name(&staging, "db"));
        assert_eq!(provider.state.lock().unwrap().create_role_names.len(), 1);

        let (pooled, direct) = credentials.database_urls(&staging_key).unwrap();
        credentials
            .store_database_urls(&canonical_key, &pooled, &direct)
            .unwrap();
        {
            let mut state = provider.state.lock().unwrap();
            state.databases.push(test_database("app", true));
            state.branches.push(test_branch(&canonical, true));
        }
        let binding = manager
            .provision_service(
                &canonical,
                "db",
                config.services.get("db").unwrap(),
                &ProvisionOptions::default(),
            )
            .await
            .unwrap();

        assert_eq!(binding.role_id, first.id);
        assert_eq!(provider.state.lock().unwrap().create_role_names.len(), 1);
        drop(manager);
        credentials.remove_database_urls(&staging_key).unwrap();
        credentials.remove_database_urls(&canonical_key).unwrap();
    }

    #[tokio::test]
    async fn pull_role_does_not_rotate_when_credentials_have_lost_the_provider_role() {
        let config = test_config();
        let canonical = provider_branch_name(config.project.id, "feature").unwrap();
        let staging = format!("{canonical}-next-test");
        let provider = FakeProvider::default();
        let temp = tempfile::tempdir().unwrap();
        let credentials = CredentialStore::new(temp.path().join("credentials.json"));
        let staging_key = CredentialStore::database_key(config.project.id, &staging, "db");
        credentials
            .store_database_urls(
                &staging_key,
                "postgresql://test:password@example.test:6432/app",
                "postgresql://test:password@example.test:5432/app",
            )
            .unwrap();
        let output = Output::new(false, ColorChoice::Never);
        let manager = EnvironmentManager::new(
            &config,
            temp.path(),
            &provider,
            &credentials,
            StateStore::new(temp.path().join("state.json")),
            JobStore::new(temp.path().join("jobs.jsonl")),
            &output,
        );

        let error = manager
            .ensure_pull_role("test-org", "app", &staging, &canonical, "db", &staging_key)
            .await
            .unwrap_err();

        assert_eq!(error.code, "staging_role_missing");
        assert!(provider.state.lock().unwrap().create_role_names.is_empty());
        drop(manager);
        credentials.remove_database_urls(&staging_key).unwrap();
    }

    #[tokio::test]
    async fn delete_removes_credentials_when_provider_branch_is_already_absent() {
        let config = test_config();
        let canonical = provider_branch_name(config.project.id, "feature").unwrap();
        let provider = FakeProvider::default();
        provider.state.lock().unwrap().delete_branch_error = Some(provider_not_found("branch"));
        let temp = tempfile::tempdir().unwrap();
        let credential_path = temp.path().join("credentials.json");
        let credentials = CredentialStore::new(&credential_path);
        let credential_key = CredentialStore::database_key(config.project.id, &canonical, "db");
        credentials
            .store_database_urls(
                &credential_key,
                "postgresql://test:password@example.test:6432/app",
                "postgresql://test:password@example.test:5432/app",
            )
            .unwrap();
        let state_path = temp.path().join("state.json");
        let state_store = StateStore::new(&state_path);
        let mut state = ProjectState::new(config.project.id);
        let mut environment = EnvironmentRecord::new("feature", &canonical);
        environment.status = EnvironmentStatus::Ready;
        environment.databases.insert(
            "db".to_owned(),
            DatabaseBinding {
                service: "db".to_owned(),
                provider: "planetscale".to_owned(),
                organization: "test-org".to_owned(),
                database: "app".to_owned(),
                branch: canonical.clone(),
                role_id: "role-1".to_owned(),
                role_name: role_name(&canonical, "db"),
                ready: true,
                updated_at: Utc::now(),
            },
        );
        state.environments.insert("feature".to_owned(), environment);
        state_store.save(&state).unwrap();
        let output = Output::new(false, ColorChoice::Never);
        let mut manager = EnvironmentManager::new(
            &config,
            temp.path(),
            &provider,
            &credentials,
            state_store,
            JobStore::new(temp.path().join("jobs.jsonl")),
            &output,
        );

        let mutation = manager.delete("feature", false).await.unwrap();

        assert_eq!(mutation.job.status, JobStatus::Succeeded);
        assert_eq!(
            mutation.job.steps[0].message.as_deref(),
            Some("provider branch was already absent")
        );
        assert!(credentials.database_urls(&credential_key).is_err());
        assert!(
            StateStore::new(state_path)
                .load(config.project.id)
                .unwrap()
                .environments
                .is_empty()
        );
    }

    #[tokio::test]
    async fn delete_cleans_checkpointed_create_branch_missing_from_degraded_state() {
        let config = test_config();
        let canonical = provider_branch_name(config.project.id, "feature").unwrap();
        let provider = FakeProvider::default();
        provider
            .state
            .lock()
            .unwrap()
            .database_responses
            .push_back(Err(RepoboxError::new(
                ErrorKind::Permission,
                "provider_permission_denied",
                "simulated create failure before a binding was recorded",
            )));
        let temp = tempfile::tempdir().unwrap();
        let credentials = CredentialStore::new(temp.path().join("credentials.json"));
        let state_store = StateStore::new(temp.path().join("state.json"));
        let jobs = JobStore::new(temp.path().join("jobs.jsonl"));
        let output = Output::new(false, ColorChoice::Never);
        let mut manager = EnvironmentManager::new(
            &config,
            temp.path(),
            &provider,
            &credentials,
            state_store.clone(),
            jobs.clone(),
            &output,
        );

        let create_error = manager
            .ensure("feature", &ProvisionOptions::default())
            .await
            .unwrap_err();

        assert_eq!(create_error.code, "environment_provision_degraded");
        let create = jobs.latest().unwrap();
        assert_eq!(create.steps[0].resource["organization"], "test-org");
        assert_eq!(create.steps[0].resource["database"], "app");
        assert_eq!(create.steps[0].resource["canonical"], canonical);
        assert!(
            state_store.load(config.project.id).unwrap().environments["feature"]
                .databases
                .is_empty()
        );
        provider
            .state
            .lock()
            .unwrap()
            .branches
            .push(test_branch(&canonical, true));

        let mutation = manager.delete("feature", false).await.unwrap();

        assert_eq!(mutation.job.status, JobStatus::Succeeded);
        assert_eq!(mutation.job.error_code, None);
        assert_eq!(
            provider.state.lock().unwrap().delete_branch_calls,
            vec![("app".to_owned(), canonical)]
        );
        assert!(
            state_store
                .load(config.project.id)
                .unwrap()
                .environments
                .is_empty()
        );
    }

    #[tokio::test]
    async fn delete_rejects_legacy_environment_without_ownership_evidence() {
        let config = test_config();
        let canonical = provider_branch_name(config.project.id, "feature").unwrap();
        let provider = FakeProvider::default();
        provider
            .state
            .lock()
            .unwrap()
            .branches
            .push(test_branch(&canonical, true));
        let temp = tempfile::tempdir().unwrap();
        let credentials = CredentialStore::new(temp.path().join("credentials.json"));
        let state_store = StateStore::new(temp.path().join("state.json"));
        let mut state = ProjectState::new(config.project.id);
        let mut environment = EnvironmentRecord::new("feature", &canonical);
        environment.status = EnvironmentStatus::Degraded;
        state.environments.insert("feature".to_owned(), environment);
        state_store.save(&state).unwrap();
        let output = Output::new(false, ColorChoice::Never);
        let mut manager = EnvironmentManager::new(
            &config,
            temp.path(),
            &provider,
            &credentials,
            state_store.clone(),
            JobStore::new(temp.path().join("jobs.jsonl")),
            &output,
        );

        let error = manager.delete("feature", false).await.unwrap_err();

        assert_eq!(error.code, "environment_delete_identity_missing");
        assert!(
            provider
                .state
                .lock()
                .unwrap()
                .delete_branch_calls
                .is_empty()
        );
        assert!(
            state_store
                .load(config.project.id)
                .unwrap()
                .environments
                .contains_key("feature")
        );
    }

    #[tokio::test]
    async fn delete_plan_and_execution_use_stored_binding_instead_of_drifted_config() {
        let mut config = test_config();
        let canonical = provider_branch_name(config.project.id, "feature").unwrap();
        let provider = FakeProvider::default();
        provider
            .state
            .lock()
            .unwrap()
            .branches
            .push(test_branch(&canonical, true));
        let temp = tempfile::tempdir().unwrap();
        let credentials = CredentialStore::new(temp.path().join("credentials.json"));
        let credential_key = CredentialStore::database_key(config.project.id, &canonical, "db");
        credentials
            .store_database_urls(
                &credential_key,
                "postgresql://test:password@example.test:6432/app",
                "postgresql://test:password@example.test:5432/app",
            )
            .unwrap();
        let state_store = StateStore::new(temp.path().join("state.json"));
        let mut state = ProjectState::new(config.project.id);
        let mut environment = EnvironmentRecord::new("feature", &canonical);
        environment.status = EnvironmentStatus::Ready;
        environment
            .databases
            .insert("db".to_owned(), test_binding("db", "app", &canonical));
        state.environments.insert("feature".to_owned(), environment);
        state_store.save(&state).unwrap();
        let RemoteServiceConfig::Planetscale {
            organization,
            database,
            ..
        } = &mut config.services.get_mut("db").unwrap().remote;
        *organization = "replacement-org".to_owned();
        *database = "replacement-app".to_owned();
        let output = Output::new(false, ColorChoice::Never);
        let mut manager = EnvironmentManager::new(
            &config,
            temp.path(),
            &provider,
            &credentials,
            state_store,
            JobStore::new(temp.path().join("jobs.jsonl")),
            &output,
        );

        let plan = manager.delete_plan("feature").unwrap();

        assert_eq!(plan.provider_calls.len(), 1);
        assert_eq!(plan.provider_calls[0].action, "delete_branch");
        assert_eq!(
            plan.provider_calls[0].resource,
            format!("test-org/app/{canonical} (db)")
        );
        assert!(!plan.provider_calls[0].resource.contains("replacement"));

        let mutation = manager.delete("feature", false).await.unwrap();

        assert_eq!(mutation.job.status, JobStatus::Succeeded);
        assert_eq!(
            provider.state.lock().unwrap().delete_branch_calls,
            vec![("app".to_owned(), canonical)]
        );
        assert!(credentials.database_urls(&credential_key).is_err());
    }

    #[tokio::test]
    async fn delete_cleans_pull_staging_cancels_lineage_and_starts_fresh_afterward() {
        let config = test_config();
        let canonical = provider_branch_name(config.project.id, "feature").unwrap();
        let provider = FakeProvider::default();
        let temp = tempfile::tempdir().unwrap();
        let credentials = CredentialStore::new(temp.path().join("credentials.json"));
        let state_store = StateStore::new(temp.path().join("state.json"));
        let mut state = ProjectState::new(config.project.id);
        let mut environment = EnvironmentRecord::new("feature", &canonical);
        environment.status = EnvironmentStatus::Degraded;
        environment
            .databases
            .insert("db".to_owned(), test_binding("db", "app", &canonical));
        state.environments.insert("feature".to_owned(), environment);
        state_store.save(&state).unwrap();

        let jobs = JobStore::new(temp.path().join("jobs.jsonl"));
        let mut pull = JobRecord::new(
            JobKind::EnvironmentPull,
            config.project.id,
            "feature",
            ["refresh:db"],
        );
        pull.status = JobStatus::Degraded;
        checkpoint_pull_test_phase(&mut pull, &config, &canonical, "old_deleted");
        let staging = pull.steps[0].resource["staging"]
            .as_str()
            .unwrap()
            .to_owned();
        jobs.append(&pull).unwrap();
        for branch in [&canonical, &staging] {
            let key = CredentialStore::database_key(config.project.id, branch, "db");
            credentials
                .store_database_urls(
                    &key,
                    "postgresql://test:password@example.test:6432/app",
                    "postgresql://test:password@example.test:5432/app",
                )
                .unwrap();
        }
        let output = Output::new(false, ColorChoice::Never);
        let mut manager = EnvironmentManager::new(
            &config,
            temp.path(),
            &provider,
            &credentials,
            state_store.clone(),
            jobs.clone(),
            &output,
        );

        manager.delete("feature", false).await.unwrap();

        assert_eq!(
            provider.state.lock().unwrap().delete_branch_calls,
            vec![
                ("app".to_owned(), canonical.clone()),
                ("app".to_owned(), staging.clone()),
            ]
        );
        for branch in [&canonical, &staging] {
            let key = CredentialStore::database_key(config.project.id, branch, "db");
            assert!(credentials.database_urls(&key).is_err());
        }
        assert_eq!(jobs.get(pull.id).unwrap().status, JobStatus::Canceled);
        assert!(
            state_store
                .load(config.project.id)
                .unwrap()
                .environments
                .is_empty()
        );

        let (fresh_create, create_resumed) = manager
            .resumable_job(
                JobKind::EnvironmentCreate,
                "feature",
                "provision:",
                config.services.keys(),
            )
            .unwrap();
        let (fresh_pull, pull_resumed) = manager
            .resumable_job(
                JobKind::EnvironmentPull,
                "feature",
                "refresh:",
                config.services.keys(),
            )
            .unwrap();
        assert!(!create_resumed);
        assert!(!pull_resumed);
        assert_ne!(fresh_create.id, pull.id);
        assert_ne!(fresh_pull.id, pull.id);
        assert_eq!(
            jobs.get(pull.id).unwrap().error_code.as_deref(),
            Some("environment_deleted")
        );
    }

    #[tokio::test]
    async fn delete_reconciles_a_directly_canceled_pull_before_environment_reuse() {
        let config = test_config();
        let canonical = provider_branch_name(config.project.id, "feature").unwrap();
        let provider = FakeProvider::default();
        let temp = tempfile::tempdir().unwrap();
        let credentials = CredentialStore::new(temp.path().join("credentials.json"));
        let state_store = StateStore::new(temp.path().join("state.json"));
        let mut state = ProjectState::new(config.project.id);
        let mut environment = EnvironmentRecord::new("feature", &canonical);
        environment.status = EnvironmentStatus::Degraded;
        environment
            .databases
            .insert("db".to_owned(), test_binding("db", "app", &canonical));
        state.environments.insert("feature".to_owned(), environment);
        state_store.save(&state).unwrap();
        let jobs = JobStore::new(temp.path().join("jobs.jsonl"));
        let mut pull = JobRecord::new(
            JobKind::EnvironmentPull,
            config.project.id,
            "feature",
            ["refresh:db"],
        );
        pull.status = JobStatus::Canceled;
        checkpoint_pull_test_phase(&mut pull, &config, &canonical, "old_deleted");
        let staging = pull.steps[0].resource["staging"]
            .as_str()
            .unwrap()
            .to_owned();
        jobs.append(&pull).unwrap();
        for branch in [&canonical, &staging] {
            let key = CredentialStore::database_key(config.project.id, branch, "db");
            credentials
                .store_database_urls(
                    &key,
                    "postgresql://test:password@example.test:6432/app",
                    "postgresql://test:password@example.test:5432/app",
                )
                .unwrap();
        }
        let output = Output::new(false, ColorChoice::Never);
        let mut manager = EnvironmentManager::new(
            &config,
            temp.path(),
            &provider,
            &credentials,
            state_store,
            jobs.clone(),
            &output,
        );

        manager.delete("feature", false).await.unwrap();

        assert_eq!(
            provider.state.lock().unwrap().delete_branch_calls,
            vec![
                ("app".to_owned(), canonical.clone()),
                ("app".to_owned(), staging.clone()),
            ]
        );
        let reconciled = jobs.get(pull.id).unwrap();
        assert_eq!(reconciled.status, JobStatus::Canceled);
        assert_eq!(
            reconciled.error_code.as_deref(),
            Some("environment_deleted")
        );
        guard_run_against_unresolved_mutation(&jobs, config.project.id, "feature").unwrap();
        let (fresh, resumed) = manager
            .resumable_job(
                JobKind::EnvironmentCreate,
                "feature",
                "provision:",
                config.services.keys(),
            )
            .unwrap();
        assert!(!resumed);
        assert_ne!(fresh.id, pull.id);
    }

    #[tokio::test]
    async fn delete_reconciles_canceled_create_checkpoint_before_environment_reuse() {
        let config = test_config();
        let canonical = provider_branch_name(config.project.id, "feature").unwrap();
        let provider = FakeProvider::default();
        provider
            .state
            .lock()
            .unwrap()
            .branches
            .push(test_branch(&canonical, true));
        let temp = tempfile::tempdir().unwrap();
        let credentials = CredentialStore::new(temp.path().join("credentials.json"));
        let state_store = StateStore::new(temp.path().join("state.json"));
        let mut state = ProjectState::new(config.project.id);
        let mut environment = EnvironmentRecord::new("feature", &canonical);
        environment.status = EnvironmentStatus::Degraded;
        state.environments.insert("feature".to_owned(), environment);
        state_store.save(&state).unwrap();
        let jobs = JobStore::new(temp.path().join("jobs.jsonl"));
        let mut create = JobRecord::new(
            JobKind::EnvironmentCreate,
            config.project.id,
            "feature",
            ["provision:db"],
        );
        prepare_create_job_resources(&mut create, &config.services, &canonical, false).unwrap();
        create.status = JobStatus::Canceled;
        jobs.append(&create).unwrap();
        let output = Output::new(false, ColorChoice::Never);
        let mut manager = EnvironmentManager::new(
            &config,
            temp.path(),
            &provider,
            &credentials,
            state_store,
            jobs.clone(),
            &output,
        );

        let run_error =
            guard_run_against_unresolved_mutation(&jobs, config.project.id, "feature").unwrap_err();
        assert_eq!(run_error.code, "environment_recovery_required");
        assert!(run_error.message.contains(&create.id.to_string()));
        let mutation_error = manager
            .resumable_job(
                JobKind::EnvironmentCreate,
                "feature",
                "provision:",
                config.services.keys(),
            )
            .unwrap_err();
        assert_eq!(mutation_error.code, "environment_recovery_required");

        manager.delete("feature", false).await.unwrap();

        assert_eq!(
            provider.state.lock().unwrap().delete_branch_calls,
            vec![("app".to_owned(), canonical)]
        );
        let reconciled = jobs.get(create.id).unwrap();
        assert_eq!(reconciled.status, JobStatus::Canceled);
        assert_eq!(
            reconciled.error_code.as_deref(),
            Some("environment_deleted")
        );
        guard_run_against_unresolved_mutation(&jobs, config.project.id, "feature").unwrap();
        let (fresh, resumed) = manager
            .resumable_job(
                JobKind::EnvironmentCreate,
                "feature",
                "provision:",
                config.services.keys(),
            )
            .unwrap();
        assert!(!resumed);
        assert_ne!(fresh.id, create.id);
    }

    #[tokio::test]
    async fn delete_cancellation_finishes_current_service_and_skips_later_services() {
        let config = two_service_config();
        let canonical = provider_branch_name(config.project.id, "feature").unwrap();
        let cancellation = OperationCancellation::default();
        let provider = FakeProvider::default();
        provider.state.lock().unwrap().cancel_after_delete = Some(cancellation.clone());
        let temp = tempfile::tempdir().unwrap();
        let credentials = CredentialStore::new(temp.path().join("credentials.json"));
        for service in ["a", "b"] {
            let key = CredentialStore::database_key(config.project.id, &canonical, service);
            credentials
                .store_database_urls(
                    &key,
                    "postgresql://test:password@example.test:6432/app",
                    "postgresql://test:password@example.test:5432/app",
                )
                .unwrap();
        }
        let state_store = StateStore::new(temp.path().join("state.json"));
        let mut state = ProjectState::new(config.project.id);
        let mut environment = EnvironmentRecord::new("feature", &canonical);
        environment
            .databases
            .insert("a".to_owned(), test_binding("a", "app-a", &canonical));
        environment
            .databases
            .insert("b".to_owned(), test_binding("b", "app-b", &canonical));
        state.environments.insert("feature".to_owned(), environment);
        state_store.save(&state).unwrap();
        let jobs = JobStore::new(temp.path().join("jobs.jsonl"));
        let output = Output::new(false, ColorChoice::Never);
        let mut manager = EnvironmentManager::new(
            &config,
            temp.path(),
            &provider,
            &credentials,
            state_store.clone(),
            jobs.clone(),
            &output,
        )
        .with_cancellation(cancellation);

        let error = manager.delete("feature", false).await.unwrap_err();

        assert_eq!(error.code, "operation_interrupted");
        assert_eq!(
            provider.state.lock().unwrap().delete_branch_calls,
            vec![("app-a".to_owned(), canonical)]
        );
        assert!(
            state_store
                .load(config.project.id)
                .unwrap()
                .environments
                .contains_key("feature")
        );
        let job = jobs.latest().unwrap();
        assert_eq!(job.status, JobStatus::Degraded);
        assert_eq!(job.error_code.as_deref(), Some("operation_interrupted"));
        assert_eq!(job.steps[0].status, StepStatus::Succeeded);
        assert_eq!(job.steps[1].status, StepStatus::Pending);
    }

    #[tokio::test]
    async fn delete_many_cancellation_skips_later_environments() {
        let config = test_config();
        let first_branch = provider_branch_name(config.project.id, "feature-a").unwrap();
        let second_branch = provider_branch_name(config.project.id, "feature-b").unwrap();
        let cancellation = OperationCancellation::default();
        let provider = FakeProvider::default();
        provider.state.lock().unwrap().cancel_after_delete = Some(cancellation.clone());
        let temp = tempfile::tempdir().unwrap();
        let credentials = CredentialStore::new(temp.path().join("credentials.json"));
        for branch in [&first_branch, &second_branch] {
            let key = CredentialStore::database_key(config.project.id, branch, "db");
            credentials
                .store_database_urls(
                    &key,
                    "postgresql://test:password@example.test:6432/app",
                    "postgresql://test:password@example.test:5432/app",
                )
                .unwrap();
        }
        let state_store = StateStore::new(temp.path().join("state.json"));
        let mut state = ProjectState::new(config.project.id);
        for (environment_name, branch) in [
            ("feature-a", first_branch.as_str()),
            ("feature-b", second_branch.as_str()),
        ] {
            let mut environment = EnvironmentRecord::new(environment_name, branch);
            environment
                .databases
                .insert("db".to_owned(), test_binding("db", "app", branch));
            state
                .environments
                .insert(environment_name.to_owned(), environment);
        }
        state_store.save(&state).unwrap();
        let output = Output::new(false, ColorChoice::Never);
        let mut manager = EnvironmentManager::new(
            &config,
            temp.path(),
            &provider,
            &credentials,
            state_store.clone(),
            JobStore::new(temp.path().join("jobs.jsonl")),
            &output,
        )
        .with_cancellation(cancellation);

        let error = manager
            .delete_many(&["feature-a".to_owned(), "feature-b".to_owned()])
            .await
            .unwrap_err();

        assert_eq!(error.code, "operation_interrupted");
        assert_eq!(
            provider.state.lock().unwrap().delete_branch_calls,
            vec![("app".to_owned(), first_branch)]
        );
        let state = state_store.load(config.project.id).unwrap();
        assert!(!state.environments.contains_key("feature-a"));
        assert!(state.environments.contains_key("feature-b"));
    }

    #[test]
    fn psql_major_version_parser_accepts_release_and_prerelease_output() {
        assert_eq!(
            parse_psql_major_version(b"psql (PostgreSQL) 16.4\n"),
            Some(16)
        );
        assert_eq!(
            parse_psql_major_version(b"psql (PostgreSQL) 18beta1\n"),
            Some(18)
        );
        assert_eq!(
            parse_psql_major_version(b"psql (PostgreSQL) 15.13 (Ubuntu 15.13-1)\n"),
            Some(15)
        );
        assert_eq!(parse_psql_major_version(b"unexpected output"), None);
        assert!(!psql_major_version_is_compatible(15, true));
        assert!(psql_major_version_is_compatible(16, true));
        assert!(psql_major_version_is_compatible(15, false));
    }

    #[tokio::test]
    async fn local_postgres_readiness_retries_until_accepting_connections() {
        let cancellation = OperationCancellation::default();
        let attempts = std::cell::Cell::new(0_u8);

        wait_for_local_postgres_readiness(
            &cancellation,
            "postgres",
            Duration::from_secs(1),
            Duration::ZERO,
            || {
                let next = attempts.get() + 1;
                attempts.set(next);
                async move { Ok(next == 3) }
            },
        )
        .await
        .unwrap();

        assert_eq!(attempts.get(), 3);
    }

    #[tokio::test]
    async fn local_postgres_readiness_is_cancellation_aware() {
        let cancellation = OperationCancellation::default();
        cancellation.cancel();
        let attempts = std::cell::Cell::new(0_u8);

        let error = wait_for_local_postgres_readiness(
            &cancellation,
            "postgres",
            Duration::from_secs(1),
            Duration::ZERO,
            || {
                attempts.set(attempts.get() + 1);
                std::future::ready(Ok(true))
            },
        )
        .await
        .unwrap_err();

        assert_eq!(error.code, "operation_interrupted");
        assert_eq!(attempts.get(), 0);
    }

    #[tokio::test]
    async fn local_postgres_readiness_timeout_is_actionable() {
        let cancellation = OperationCancellation::default();

        let error = wait_for_local_postgres_readiness(
            &cancellation,
            "postgres",
            Duration::ZERO,
            Duration::ZERO,
            || std::future::ready(Ok(false)),
        )
        .await
        .unwrap_err();

        assert_eq!(error.code, "local_postgres_readiness_timeout");
        assert!(error.message.contains("postgres"));
        assert!(error.suggestion.unwrap().contains("docker compose logs"));
    }

    #[test]
    fn interrupted_database_copy_cleanup_failure_reports_residual_processes() {
        let error = interrupted_database_copy_cleanup(
            Err(std::io::Error::other("kill denied")),
            Ok(()),
            Ok(()),
            Ok(()),
        )
        .err()
        .unwrap();

        assert_eq!(error.code, "operation_interrupted_cleanup_incomplete");
        assert!(error.message.contains("pg_dump process group"));
        assert!(error.message.contains("kill denied"));
    }

    #[test]
    fn docker_cleanup_timeout_names_the_possible_residual_container() {
        let error = docker_cleanup_attempt_result(
            "repobox-psql-timeout",
            DockerCleanupAttempt::TimedOut,
            false,
        )
        .unwrap()
        .unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
        assert!(error.to_string().contains("repobox-psql-timeout"));
        assert!(error.to_string().contains("may still be running"));
    }

    #[test]
    fn docker_cleanup_failure_retries_then_reports_the_container() {
        assert!(
            docker_cleanup_attempt_result(
                "repobox-psql-failure",
                DockerCleanupAttempt::Failed,
                false,
            )
            .is_none()
        );
        let error = docker_cleanup_attempt_result(
            "repobox-psql-failure",
            DockerCleanupAttempt::Failed,
            true,
        )
        .unwrap()
        .unwrap_err();

        assert!(error.to_string().contains("repobox-psql-failure"));
        assert!(error.to_string().contains("5 attempts"));
    }

    #[test]
    fn docker_cleanup_treats_confirmed_absence_as_success() {
        let outcome = classify_docker_cleanup_commands(
            Some(false),
            Some(false),
            "Error: No such object: repobox-psql-gone",
        );
        docker_cleanup_attempt_result("repobox-psql-gone", outcome, true)
            .unwrap()
            .unwrap();

        let mut cleanup = DockerContainerCleanup::new("must-not-run-docker".to_owned());
        cleanup.disarm();
        cleanup.run().unwrap();
    }

    #[test]
    fn docker_cleanup_existing_or_unknown_container_is_not_reported_clean() {
        for outcome in [
            classify_docker_cleanup_commands(Some(false), Some(true), ""),
            classify_docker_cleanup_commands(
                Some(false),
                Some(false),
                "cannot connect to the Docker daemon",
            ),
        ] {
            let error = docker_cleanup_attempt_result("repobox-psql-uncertain", outcome, true)
                .unwrap()
                .unwrap_err();
            assert!(error.to_string().contains("repobox-psql-uncertain"));
        }
    }

    #[cfg(unix)]
    fn streaming_test_processes(restore_script: &str) -> (ManagedChild, ManagedChild) {
        let mut dump_command = Command::new("sh");
        dump_command
            .args([
                "-c",
                "trap '' PIPE; while printf 'SELECT 1;\\n'; do :; done; exec sleep 30",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let dump = ManagedChild::spawn(dump_command, None).unwrap();
        let mut restore_command = Command::new("sh");
        restore_command
            .args(["-c", restore_script])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        let restore = ManagedChild::spawn(restore_command, None).unwrap();
        (dump, restore)
    }

    #[test]
    fn non_broken_pipe_transfer_failure_wins_when_restore_is_terminated() {
        let transfer_error =
            std::io::Error::new(std::io::ErrorKind::ConnectionReset, "transport failed");

        assert_eq!(
            failed_stream_failure_source(true, Some(&transfer_error)),
            Some(FailedStreamFailureSource::Transfer)
        );
    }

    #[test]
    fn interrupted_cleanup_failure_names_possible_residual() {
        let error = finish_with_cleanup::<()>(
            Err(operation_interrupted()),
            Err(RepoboxError::new(
                ErrorKind::Runtime,
                "cleanup_transport_failed",
                "provider unavailable",
            )),
            "temporary PlanetScale role `repobox-import-test`",
        )
        .unwrap_err();

        assert_eq!(error.code, "operation_interrupted_cleanup_incomplete");
        assert!(error.message.contains("temporary PlanetScale role"));
        assert!(error.message.contains("provider unavailable"));
    }

    #[tokio::test]
    async fn partial_compose_start_failure_stops_source_and_preserves_start_error() {
        let cleanup_attempted = std::sync::atomic::AtomicBool::new(false);
        let start_error = RepoboxError::new(
            ErrorKind::Runtime,
            "local_postgres_start_failed",
            "Docker Compose exited with exit status: 17",
        );

        let error = finish_failed_compose_start(start_error, "postgres", async {
            cleanup_attempted.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        })
        .await
        .unwrap_err();

        assert!(cleanup_attempted.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(error.code, "local_postgres_start_failed");
        assert!(error.message.contains("exit status: 17"));
    }

    #[tokio::test]
    async fn partial_compose_start_failure_reports_unconfirmed_cleanup() {
        let start_error = RepoboxError::new(
            ErrorKind::Runtime,
            "local_postgres_start_failed",
            "Docker Compose exited with exit status: 17",
        );

        let error = finish_failed_compose_start(start_error, "postgres", async {
            Err(RepoboxError::new(
                ErrorKind::Runtime,
                "local_postgres_stop_failed",
                "Docker Compose exited with exit status: 18",
            ))
        })
        .await
        .unwrap_err();

        assert_eq!(error.code, "operation_cleanup_failed");
        assert!(error.message.contains("exit status: 17"));
        assert!(error.message.contains("Compose source service `postgres`"));
        assert!(error.message.contains("exit status: 18"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn failed_restore_diagnostic_survives_broken_pipe() {
        let (dump, restore) = streaming_test_processes(
            "printf 'restore-rejected-sentinel topsecret\\n' >&2; exit 42",
        );
        let target = Url::parse("postgresql://repobox:topsecret@example.test:5432/app").unwrap();
        let cancellation = OperationCancellation::default();

        let error = tokio::time::timeout(
            Duration::from_secs(5),
            stream_database_copy(dump, restore, &target, &cancellation),
        )
        .await
        .expect("database copy should not hang")
        .unwrap_err();

        assert_eq!(error.code, "planetscale_import_failed");
        assert!(error.message.contains("restore-rejected-sentinel"));
        assert!(error.message.contains("exit status: 42"));
        assert!(!error.message.contains("Broken pipe"));
        assert!(!error.message.contains("topsecret"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn failed_restore_drains_more_than_pipe_capacity_from_stderr() {
        let (dump, restore) = streaming_test_processes(
            "i=0; while [ \"$i\" -lt 7000 ]; do \
             printf 'restore-stderr-padding-0123456789\\n' >&2; \
             i=$((i + 1)); done; \
             printf 'large-stderr-sentinel\\n' >&2; exit 43",
        );
        let target = Url::parse("postgresql://repobox:topsecret@example.test:5432/app").unwrap();
        let cancellation = OperationCancellation::default();

        let error = tokio::time::timeout(
            Duration::from_secs(10),
            stream_database_copy(dump, restore, &target, &cancellation),
        )
        .await
        .expect("database copy should drain stderr without hanging")
        .unwrap_err();

        assert_eq!(error.code, "planetscale_import_failed");
        assert!(error.message.contains("large-stderr-sentinel"));
        assert!(error.message.contains("exit status: 43"));
        assert!(error.message.contains("[stderr truncated:"));
        assert!(error.message.len() <= PROCESS_STDERR_TAIL_BYTES + 256);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn broken_pipe_does_not_wait_forever_for_restore() {
        let (dump, restore) = streaming_test_processes(
            "printf 'stalled-restore-sentinel topsecret\\n' >&2; exec 0<&-; sleep 30",
        );
        let target = Url::parse("postgresql://repobox:topsecret@example.test:5432/app").unwrap();
        let cancellation = OperationCancellation::default();

        let error = tokio::time::timeout(
            FAILED_STREAM_EXIT_GRACE_PERIOD + Duration::from_secs(2),
            stream_database_copy(dump, restore, &target, &cancellation),
        )
        .await
        .expect("database copy should terminate a stalled restore")
        .unwrap_err();

        assert_eq!(error.code, "database_stream_interrupted");
        assert!(error.message.contains("psql did not exit"));
        assert!(error.message.contains("stalled-restore-sentinel"));
        assert!(!error.message.contains("topsecret"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn controlled_command_stops_child_when_control_closes() {
        let mut command = Command::new("sh");
        command
            .args([
                "-c",
                CONTROLLED_COMMAND_SCRIPT,
                "repobox-test",
                "sh",
                "-c",
                "printf '%s\\n' \"$$\"; exec sleep 30",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = ManagedChild::spawn(command, None).unwrap();
        let control = child.child.stdin.take().unwrap();
        let mut stdout = child.child.stdout.take().unwrap();
        let mut pid_bytes = [0_u8; 32];
        let bytes_read = tokio::time::timeout(Duration::from_secs(1), stdout.read(&mut pid_bytes))
            .await
            .expect("wrapped command should report its pid")
            .unwrap();
        let child_pid = std::str::from_utf8(&pid_bytes[..bytes_read])
            .unwrap()
            .trim()
            .parse::<i32>()
            .unwrap();

        drop(control);
        tokio::time::timeout(Duration::from_secs(2), child.wait())
            .await
            .expect("control EOF should stop the wrapped command")
            .unwrap();
        assert_eq!(
            nix::sys::signal::kill(nix::unistd::Pid::from_raw(child_pid), None),
            Err(nix::errno::Errno::ESRCH)
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cancelling_database_copy_kills_child_process_groups() {
        let temp = tempfile::tempdir().unwrap();
        let dump_pid_path = temp.path().join("dump-child.pid");
        let restore_pid_path = temp.path().join("restore-child.pid");

        let mut dump_command = Command::new("sh");
        dump_command
            .args([
                "-c",
                "trap '' PIPE; sleep 30 & child=$!; printf '%s' \"$child\" > \"$1\"; \
                 while printf 'SELECT 1;\\n'; do :; done; wait \"$child\"",
                "sh",
            ])
            .arg(&dump_pid_path)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let dump = ManagedChild::spawn(dump_command, None).unwrap();
        let mut restore_command = Command::new("sh");
        restore_command
            .args([
                "-c",
                "sleep 30 & child=$!; printf '%s' \"$child\" > \"$1\"; wait \"$child\"",
                "sh",
            ])
            .arg(&restore_pid_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        let restore = ManagedChild::spawn(restore_command, None).unwrap();
        let target = Url::parse("postgresql://repobox:topsecret@example.test:5432/app").unwrap();
        let cancellation = OperationCancellation::default();
        let operation_cancellation = cancellation.clone();

        let copy = tokio::spawn(async move {
            stream_database_copy(dump, restore, &target, &operation_cancellation).await
        });
        for _ in 0..100 {
            if dump_pid_path.is_file() && restore_pid_path.is_file() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let dump_pid = std::fs::read_to_string(&dump_pid_path)
            .unwrap()
            .parse::<i32>()
            .unwrap();
        let restore_pid = std::fs::read_to_string(&restore_pid_path)
            .unwrap()
            .parse::<i32>()
            .unwrap();

        cancellation.cancel();
        let error = copy.await.unwrap().unwrap_err();
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(error.code, "operation_interrupted");
        assert_eq!(
            nix::sys::signal::kill(nix::unistd::Pid::from_raw(dump_pid), None),
            Err(nix::errno::Errno::ESRCH),
            "cancelled pg_dump descendant kept running"
        );
        assert_eq!(
            nix::sys::signal::kill(nix::unistd::Pid::from_raw(restore_pid), None),
            Err(nix::errno::Errno::ESRCH),
            "cancelled psql descendant kept running"
        );
    }

    #[test]
    fn smallest_cluster_size_uses_numeric_capacity() {
        let sizes = vec![
            "PS_80".to_owned(),
            "PS_10_v2".to_owned(),
            "PS-20".to_owned(),
        ];
        assert_eq!(select_smallest_size(&sizes).unwrap(), "PS_10_v2");
    }

    #[test]
    fn role_names_are_bounded_and_collision_resistant() {
        let branch = "rbx-feature-with-a-very-long-name-1234567890abcdef";
        let first_service = "a".repeat(100);
        let second_service = format!("{}b", "a".repeat(99));
        let first = role_name(branch, &first_service);
        let second = role_name(branch, &second_service);
        assert!(first.len() <= 63);
        assert!(second.len() <= 63);
        assert_ne!(first, second);
        assert_eq!(first, role_name(branch, &first_service));
    }

    #[test]
    fn staging_names_preserve_unique_job_suffix() {
        let first = staging_branch_name(&"x".repeat(63), uuid::Uuid::from_u128(1));
        let second = staging_branch_name(&"x".repeat(63), uuid::Uuid::from_u128(2));
        assert!(first.len() <= 63);
        assert_ne!(first, second);
    }

    #[test]
    fn bootstrap_marker_changes_with_remote_target_identity() {
        let original = test_service();
        let mut moved = original.clone();
        let RemoteServiceConfig::Planetscale {
            organization,
            database,
            base_branch,
            ..
        } = &mut moved.remote;
        *organization = "another-org".to_owned();
        *database = "another-database".to_owned();
        *base_branch = "production".to_owned();

        assert_ne!(
            bootstrap_service_marker("db", &original),
            bootstrap_service_marker("db", &moved)
        );
        assert_eq!(
            bootstrap_service_marker("db", &original),
            "db@test-org/app/main"
        );
    }
}
