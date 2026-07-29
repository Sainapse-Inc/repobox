use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{self, IsTerminal, Read, Write};
#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use chrono::Utc;
use clap::CommandFactory;
use repobox_core::config::{RepoboxConfig, RuntimeConfig};
use repobox_core::jobs::{JobKind, JobStatus};
use repobox_core::paths::RepoboxPaths;
use repobox_core::provider::DatabaseProvider;
use repobox_core::runtime::RuntimeDriver;
use repobox_core::state::EnvironmentStatus;
use repobox_core::{ErrorKind, RepoboxError, Result};
use repobox_provider_planetscale::{
    PlanetScaleClient, PlanetScaleCredentials, PlanetScaleDeviceAuth,
};
use repobox_runtime_compose::{ComposeRuntime, detect_configuration};
use serde::Serialize;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command as TokioCommand;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::cli::{
    AuthCommand, BootstrapChoice, Cli, Command, CompletionShell, ConfigCommand, EnvCommand,
    HelpArgs, InitArgs, InitRuntime, JobCommand, ServiceCommand, TelemetryCommand,
};
use crate::context::{ProjectContext, requested_repository};
use crate::credentials::CredentialStore;
use crate::environment::{
    EnvironmentManager, OperationCancellation, ProvisionOptions, environment_variables,
    guard_run_against_unresolved_mutation, job_store, state_for_environment, state_store,
    stored_environment_variables,
};
use crate::initialize;
use crate::output::Output;
use crate::tui::{DashboardEvent, DashboardOptions};

const NATIVE_RUNTIME_SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_secs(10);
const FOREGROUND_SIGNAL_PROPAGATION_GRACE_PERIOD: Duration = Duration::from_millis(50);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RuntimeChildControl {
    Immediate,
    NativeForeground,
    NativeIsolated,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InterruptDelivery {
    Forward,
    ForegroundAlreadySignaled,
}

impl RuntimeChildControl {
    fn isolates_process_group(self) -> bool {
        matches!(self, Self::NativeIsolated)
    }

    fn supports_graceful_interrupt(self) -> bool {
        cfg!(unix) && !matches!(self, Self::Immediate)
    }
}

fn native_child_control(interactive: bool, structured_output: bool) -> RuntimeChildControl {
    if interactive && !structured_output {
        RuntimeChildControl::NativeForeground
    } else {
        RuntimeChildControl::NativeIsolated
    }
}

fn native_inherits_stdin(interactive: bool, structured_output: bool) -> bool {
    interactive && !structured_output
}

#[cfg(unix)]
fn configure_native_process_group(command: &mut TokioCommand, control: RuntimeChildControl) {
    if control.isolates_process_group() {
        command.process_group(0);
    }
}

pub async fn run(cli: Cli, output: &Output, cancellation: &OperationCancellation) -> Result<()> {
    maybe_update_notice(&cli, output).await;
    let repository = requested_repository(cli.repo.as_ref())?;
    match cli.command.clone().expect("main checks for a command") {
        Command::Init(args) => init(&cli, output, &repository, &args).await,
        Command::Run(args) => run_project(&cli, output, &repository, &args, cancellation).await,
        Command::Stop(selector) => stop_project(&cli, output, &repository, &selector).await,
        Command::Pull(args) => pull(&cli, output, &repository, &args, cancellation).await,
        Command::Status(selector) => {
            status(output, &repository, selector.environment.as_deref()).await
        }
        Command::Logs(args) => {
            logs(
                output,
                &repository,
                args.environment.environment.as_deref(),
                args.service.as_deref(),
                args.follow,
                args.tail,
            )
            .await
        }
        Command::Auth(command) => auth(&cli, output, command).await,
        Command::Env(command) => {
            environment(&cli, output, &repository, command, cancellation).await
        }
        Command::Service(command) => service(&cli, output, &repository, command).await,
        Command::Job(command) => jobs(&cli, output, &repository, command, cancellation).await,
        Command::Config(command) => config(&cli, output, &repository, command).await,
        Command::Telemetry(command) => telemetry(&cli, output, command),
        Command::Update(args) => update(&cli, output, args.check).await,
        Command::Doctor(args) => doctor(output, &repository, args.online).await,
        Command::Completion(args) => completion(output, args.shell),
        Command::AgentContext(args) => agent_context(output, &repository, args.schemas).await,
        Command::Help(args) => help(output, &args),
    }
}

async fn maybe_update_notice(cli: &Cli, output: &Output) {
    if output.json()
        || cli.dry_run
        || matches!(cli.command.as_ref(), Some(Command::Update(_)))
        || std::env::var_os("REPOBOX_NO_UPDATE_CHECK").is_some()
        || std::env::var_os("CI").is_some()
    {
        return;
    }
    let Ok(paths) = RepoboxPaths::discover() else {
        return;
    };
    let cache = paths.cache_dir.join("update-check.json");
    if let Ok(contents) = fs::read_to_string(&cache)
        && let Ok(value) = serde_json::from_str::<serde_json::Value>(&contents)
        && let Some(checked_at) = value["checked_at"].as_str()
        && let Ok(checked_at) = checked_at.parse::<chrono::DateTime<chrono::Utc>>()
        && Utc::now().signed_duration_since(checked_at) < chrono::Duration::hours(24)
    {
        if let Some(latest) = value["latest"].as_str()
            && latest.trim_start_matches('v') != env!("CARGO_PKG_VERSION")
        {
            output.progress(&format!(
                "update available: {latest}; run `repobox update` for the upgrade command"
            ));
        }
        return;
    }

    let release = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .ok()
        .map(|client| {
            client
                .get("https://api.github.com/repos/Sainapse-Inc/repobox/releases/latest")
                .header(reqwest::header::USER_AGENT, "repobox-update-check")
                .send()
        });
    let latest = match release {
        Some(request) => match request.await {
            Ok(response) if response.status().is_success() => response
                .json::<serde_json::Value>()
                .await
                .ok()
                .and_then(|value| value["tag_name"].as_str().map(str::to_owned)),
            _ => None,
        },
        None => None,
    };
    let value = serde_json::json!({"checked_at": Utc::now(), "latest": latest});
    if RepoboxPaths::ensure_parent(&cache).is_ok() {
        let _ = fs::write(&cache, value.to_string());
    }
    if let Some(latest) = value["latest"].as_str()
        && latest.trim_start_matches('v') != env!("CARGO_PKG_VERSION")
    {
        output.progress(&format!(
            "update available: {latest}; run `repobox update` for the upgrade command"
        ));
    }
}

async fn stop_project(
    cli: &Cli,
    output: &Output,
    repository: &Path,
    selector: &crate::cli::EnvironmentSelector,
) -> Result<()> {
    let context = ProjectContext::load(repository)?;
    let environment = context.environment(selector.environment.as_deref()).await?;
    if !matches!(context.config.runtime, RuntimeConfig::Compose { .. }) {
        return Err(RepoboxError::new(
            ErrorKind::Usage,
            "native_runtime_stop_unsupported",
            "Repobox cannot stop a detached native process because it does not manage one",
        ));
    }
    if cli.dry_run {
        return output.data(
            "stop",
            &serde_json::json!({"environment": environment, "executed": false}),
        );
    }
    let runtime = compose_runtime(&context, &environment, &BTreeMap::new()).await?;
    if output.json() {
        runtime.stop_quiet().await?;
    } else {
        runtime.stop().await?;
    }
    output.mutation(
        "stop",
        &serde_json::json!({"environment": environment, "stopped": true}),
        &format!("stopped `{environment}`"),
        Some(format!(
            "repobox run --environment {environment} --detach --yes"
        )),
        None,
    )
}

async fn init(cli: &Cli, output: &Output, repository: &Path, args: &InitArgs) -> Result<()> {
    let organization = if let Some(organization) = &args.organization {
        organization.clone()
    } else {
        let paths = RepoboxPaths::discover()?;
        let store = CredentialStore::new(paths.credentials_file());
        let (credentials, _) = store.provider_credentials()?;
        let provider = PlanetScaleClient::new(credentials)?;
        let organizations = provider
            .list_organizations()
            .await?
            .into_iter()
            .map(|organization| organization.name)
            .collect::<Vec<_>>();
        let detection = initialize::detect(repository).await?;
        if cli.no_input {
            if organizations.len() == 1 {
                organizations[0].clone()
            } else {
                return Err(RepoboxError::new(
                    ErrorKind::Usage,
                    "organization_required",
                    "--organization is required in non-interactive mode",
                ));
            }
        } else {
            crate::tui::select_organization(&organizations, &detection).await?
        }
    };
    let result = initialize::initialize(repository, args, organization, cli.dry_run).await?;
    let message = format!(
        "configured {} with {} database service(s){}",
        result.config_path.display(),
        result.services.len(),
        if cli.dry_run { " (dry run)" } else { "" }
    );
    if cli.dry_run {
        output.human_or_data("init", &result, &message)
    } else {
        output.mutation(
            "init",
            &result,
            &message,
            None,
            Some(
                "initialization updates both project config and managed agent-guide blocks; no atomic inverse is available"
                    .to_owned(),
            ),
        )
    }
}

async fn run_project(
    cli: &Cli,
    output: &Output,
    repository: &Path,
    args: &crate::cli::RunArgs,
    cancellation: &OperationCancellation,
) -> Result<()> {
    let mut context = match ProjectContext::load(repository) {
        Ok(context) => context,
        Err(error)
            if error.code == "config_not_found"
                && !cli.no_input
                && io::stdin().is_terminal()
                && io::stderr().is_terminal() =>
        {
            let defaults = InitArgs {
                runtime: InitRuntime::Auto,
                organization: None,
                database: vec![],
                data: BootstrapChoice::Empty,
                force: false,
                command: vec![],
            };
            init(cli, output, repository, &defaults).await?;
            ProjectContext::load(repository)?
        }
        Err(error) => return Err(error),
    };
    let environment = context
        .environment(args.environment.environment.as_deref())
        .await?;
    let credentials = credential_store(&context.paths);
    let project_state_store = state_store(&context.config, &context.paths);
    let existing = project_state_store.load(context.config.project.id)?;
    guard_run_against_unresolved_mutation(
        &job_store(&context.config, &context.paths),
        context.config.project.id,
        &environment,
    )?;
    let ready = existing
        .environments
        .get(&environment)
        .is_some_and(|record| record.status == EnvironmentStatus::Ready);
    let options = ProvisionOptions {
        create_backup: args.create_backup,
        wait_for_backup: args.wait,
        selected_services: BTreeSet::new(),
    };
    if cli.dry_run {
        let provider = planning_provider()?;
        let manager = EnvironmentManager::new(
            &context.config,
            &context.repository,
            &provider,
            &credentials,
            project_state_store,
            job_store(&context.config, &context.paths),
            output,
        );
        return output.data("run", &manager.create_plan(&environment, &options)?);
    }
    if !ready {
        confirm(
            cli,
            &format!("Create billable, isolated PlanetScale data for environment `{environment}`?"),
        )?;
        let provider = provider(&credentials)?;
        let mut manager = EnvironmentManager::new(
            &context.config,
            &context.repository,
            &provider,
            &credentials,
            project_state_store,
            job_store(&context.config, &context.paths),
            output,
        )
        .with_cancellation(cancellation.clone());
        manager.ensure(&environment, &options).await?;
    }
    // Reload after provisioning so runtime receives the durable binding.
    context.config = RepoboxConfig::load(&context.config_path)?;
    let state = state_store(&context.config, &context.paths).load(context.config.project.id)?;
    let record = state_for_environment(&state, &environment)?;
    let variables = environment_variables(&context.config, record, &credentials)?;
    match &context.config.runtime {
        RuntimeConfig::Compose { .. } => {
            let runtime = compose_runtime(&context, &environment, &variables).await?;
            if args.detach {
                if output.json() {
                    runtime.start_quiet(&variables).await?;
                } else {
                    runtime.start(&variables, true).await?;
                }
                let data = serde_json::json!({
                    "environment": environment,
                    "detached": true,
                    "runtime": "compose",
                });
                let undo = Some(format!("repobox stop --environment {environment}"));
                if output.json() {
                    output.stream_mutation(&data, undo, None)
                } else {
                    output.mutation(
                        "run",
                        &data,
                        &format!("started `{environment}` in the background"),
                        undo,
                        None,
                    )
                }
            } else if output.json() {
                run_compose_json(output, &runtime, &variables, &environment, cancellation).await
            } else if args.no_tui || !io::stderr().is_terminal() {
                run_compose_plain(output, &runtime, &variables, &environment, cancellation).await
            } else {
                runtime.start(&variables, true).await?;
                let dashboard_result =
                    dashboard(&runtime, &context, &environment, &variables, cancellation).await;
                let stop_result = runtime.stop().await;
                finish_compose_shutdown(dashboard_result, stop_result, cancellation, &environment)
            }
        }
        RuntimeConfig::Native { native } => {
            if args.detach {
                return Err(RepoboxError::new(
                    ErrorKind::Usage,
                    "native_detach_unsupported",
                    "--detach is not supported for native runtimes in v0.1",
                )
                .with_suggestion(
                    "Run without --detach, or configure Docker Compose for managed background services.",
                ));
            }
            let (program, arguments) = native.command.split_first().ok_or_else(|| {
                RepoboxError::new(
                    ErrorKind::Usage,
                    "native_command_required",
                    "native runtime command is empty",
                )
            })?;
            let mut command = TokioCommand::new(program);
            command
                .args(arguments)
                .current_dir(context.repository.join(&native.working_directory))
                .envs(&variables);
            command.kill_on_drop(true);
            let child_control = native_child_control(native.interactive, output.json());
            #[cfg(unix)]
            configure_native_process_group(&mut command, child_control);
            if output.json() {
                let child = command
                    .stdin(Stdio::null())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()?;
                output.stream(
                    "runtime_started",
                    &serde_json::json!({"environment": environment, "runtime": "native"}),
                )?;
                stream_log_child(
                    output,
                    child,
                    LogStreamOptions {
                        interruptible: true,
                        stderr_service: "native",
                        compose_format: false,
                        failure_code: "native_runtime_failed",
                        process_label: "native runtime",
                        redactor: runtime_redactor(&variables),
                        cancellation: Some(cancellation.clone()),
                        child_control,
                    },
                )
                .await?;
                return output.stream(
                    "runtime_stopped",
                    &serde_json::json!({"environment": environment, "runtime": "native"}),
                );
            }
            let child = command
                .stdin(if native_inherits_stdin(native.interactive, false) {
                    Stdio::inherit()
                } else {
                    Stdio::null()
                })
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .spawn()?;
            let (status, interrupted) =
                wait_native_runtime_child(child, cancellation, child_control).await?;
            if interrupted {
                return Ok(());
            }
            if status.success() {
                Ok(())
            } else {
                Err(RepoboxError::new(
                    ErrorKind::Runtime,
                    "native_runtime_failed",
                    format!("native command exited with {status}"),
                ))
            }
        }
    }
}

async fn dashboard(
    runtime: &ComposeRuntime,
    context: &ProjectContext,
    environment: &str,
    variables: &BTreeMap<String, String>,
    cancellation: &OperationCancellation,
) -> Result<()> {
    let redactor = runtime_redactor(variables);
    let mut child = runtime.spawn_logs(None, true, 200)?;
    let stdout = child.stdout.take().expect("logs stdout is piped");
    let stderr = child.stderr.take().expect("logs stderr is piped");
    let (sender, receiver) = mpsc::channel(2_000);
    let stdout_sender = sender.clone();
    let stdout_redactor = redactor.clone();
    tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let (service, line) = parse_compose_log(&stdout_redactor.redact(&line));
            if stdout_sender
                .send(DashboardEvent::Log { service, line })
                .await
                .is_err()
            {
                break;
            }
        }
    });
    let stderr_sender = sender.clone();
    tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if stderr_sender
                .send(DashboardEvent::Log {
                    service: "docker".to_owned(),
                    line: redactor.redact(&line),
                })
                .await
                .is_err()
            {
                break;
            }
        }
    });
    let exit_sender = sender;
    tokio::spawn(async move {
        let code = child.wait().await.ok().and_then(|status| status.code());
        let _ = exit_sender
            .send(DashboardEvent::ProcessExited { code })
            .await;
    });
    let services = runtime
        .status()
        .await?
        .services
        .into_iter()
        .map(|service| service.name)
        .collect();
    let dashboard = crate::tui::run_dashboard(
        DashboardOptions {
            project: context.config.project.name.clone(),
            environment: environment.to_owned(),
            services,
        },
        receiver,
    );
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Ok(()),
        result = dashboard => result,
    }
}

async fn run_compose_json(
    output: &Output,
    runtime: &ComposeRuntime,
    variables: &BTreeMap<String, String>,
    environment: &str,
    cancellation: &OperationCancellation,
) -> Result<()> {
    runtime.start_quiet(variables).await?;
    output.stream(
        "runtime_started",
        &serde_json::json!({"environment": environment, "runtime": "compose"}),
    )?;
    let child = runtime.spawn_logs(None, true, 200)?;
    let logs_result = stream_log_child(
        output,
        child,
        LogStreamOptions {
            interruptible: true,
            stderr_service: "docker",
            compose_format: true,
            failure_code: "compose_logs_failed",
            process_label: "Docker Compose logs",
            redactor: runtime_redactor(variables),
            cancellation: Some(cancellation.clone()),
            child_control: RuntimeChildControl::Immediate,
        },
    )
    .await;
    let stop_result = runtime.stop_quiet().await;
    if stop_result.is_ok() {
        output.stream(
            "runtime_stopped",
            &serde_json::json!({"environment": environment, "runtime": "compose"}),
        )?;
    }
    finish_compose_shutdown(logs_result, stop_result, cancellation, environment)
}

async fn run_compose_plain(
    output: &Output,
    runtime: &ComposeRuntime,
    variables: &BTreeMap<String, String>,
    environment: &str,
    cancellation: &OperationCancellation,
) -> Result<()> {
    runtime.start(variables, true).await?;
    let child = runtime.spawn_logs(None, true, 200)?;
    let logs_result = stream_log_child(
        output,
        child,
        LogStreamOptions {
            interruptible: true,
            stderr_service: "docker",
            compose_format: true,
            failure_code: "compose_logs_failed",
            process_label: "Docker Compose logs",
            redactor: runtime_redactor(variables),
            cancellation: Some(cancellation.clone()),
            child_control: RuntimeChildControl::Immediate,
        },
    )
    .await;
    let stop_result = runtime.stop().await;
    finish_compose_shutdown(logs_result, stop_result, cancellation, environment)
}

fn finish_compose_shutdown(
    run_result: Result<()>,
    stop_result: Result<()>,
    cancellation: &OperationCancellation,
    environment: &str,
) -> Result<()> {
    match stop_result {
        Ok(()) => run_result,
        Err(stop_error) if cancellation.is_cancelled() => {
            let request_id = stop_error.request_id.clone();
            let mut error = RepoboxError::new(
                ErrorKind::Runtime,
                "operation_interrupted_cleanup_incomplete",
                format!(
                    "operation interrupted, but stopping Docker Compose for environment `{environment}` failed; Compose services may remain running ({}: {})",
                    stop_error.code, stop_error.message
                ),
            )
            .with_suggestion(format!(
                "Inspect `docker compose ps` and stop the services for `{environment}` before running Repobox again."
            ));
            if let Some(request_id) = request_id {
                error = error.with_request_id(request_id);
            }
            Err(error)
        }
        Err(stop_error) => Err(stop_error),
    }
}

async fn status(output: &Output, repository: &Path, explicit: Option<&str>) -> Result<()> {
    let context = ProjectContext::load(repository)?;
    let environment = context.environment(explicit).await?;
    let state = state_store(&context.config, &context.paths).load(context.config.project.id)?;
    let environment_state = state.environments.get(&environment).cloned();
    let merged = crate::git::merged_branches(
        &context.repository,
        &context.config.project.git.base_branch,
        false,
    )
    .await
    .unwrap_or_default();
    let cleanup_suggestions = merged
        .into_iter()
        .filter(|branch| state.environments.contains_key(branch))
        .collect::<Vec<_>>();
    let runtime = match &context.config.runtime {
        RuntimeConfig::Compose { .. } => {
            let values = BTreeMap::new();
            let runtime = compose_runtime(&context, &environment, &values).await?;
            Some(runtime.status().await?)
        }
        RuntimeConfig::Native { .. } => None,
    };
    let data = serde_json::json!({
        "project": context.config.project,
        "environment": environment,
        "data": environment_state,
        "runtime": runtime,
        "cleanup_suggestions": cleanup_suggestions,
    });
    output.human_or_data(
        "status",
        &data,
        &format!(
            "{}  data={}  runtime={}",
            data["environment"].as_str().unwrap_or("unknown"),
            data["data"]["status"].as_str().unwrap_or("absent"),
            data["runtime"]["running"].as_bool().unwrap_or(false)
        ),
    )
}

async fn logs(
    output: &Output,
    repository: &Path,
    environment_name: Option<&str>,
    service: Option<&str>,
    follow: bool,
    tail: usize,
) -> Result<()> {
    let context = ProjectContext::load(repository)?;
    let environment = context.environment(environment_name).await?;
    let variables = available_runtime_variables(&context, &environment)?;
    let runtime = compose_runtime(&context, &environment, &BTreeMap::new()).await?;
    let child = runtime.spawn_logs(service, follow, tail)?;
    stream_log_child(
        output,
        child,
        LogStreamOptions {
            interruptible: follow,
            stderr_service: "docker",
            compose_format: true,
            failure_code: "compose_logs_failed",
            process_label: "Docker Compose logs",
            redactor: runtime_redactor(&variables),
            cancellation: None,
            child_control: RuntimeChildControl::Immediate,
        },
    )
    .await
}

struct LogStreamOptions {
    interruptible: bool,
    stderr_service: &'static str,
    compose_format: bool,
    failure_code: &'static str,
    process_label: &'static str,
    redactor: repobox_core::redaction::SecretRedactor,
    cancellation: Option<OperationCancellation>,
    child_control: RuntimeChildControl,
}

struct ManagedRuntimeChild {
    child: tokio::process::Child,
    #[cfg(unix)]
    process_group: Option<nix::unistd::Pid>,
    control: RuntimeChildControl,
    active: bool,
}

impl ManagedRuntimeChild {
    fn new(child: tokio::process::Child, control: RuntimeChildControl) -> Self {
        #[cfg(unix)]
        let process_group = control
            .isolates_process_group()
            .then(|| child.id())
            .flatten()
            .and_then(|id| i32::try_from(id).ok())
            .map(nix::unistd::Pid::from_raw);
        Self {
            child,
            #[cfg(unix)]
            process_group,
            control,
            active: true,
        }
    }

    async fn wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
        let status = self.child.wait().await?;
        self.active = false;
        #[cfg(unix)]
        {
            self.process_group = None;
        }
        Ok(status)
    }

    #[cfg(unix)]
    fn signal_process_group(&self, signal: nix::sys::signal::Signal) -> std::io::Result<()> {
        let Some(process_group) = self.process_group else {
            return Ok(());
        };
        match nix::sys::signal::killpg(process_group, signal) {
            Ok(()) | Err(nix::errno::Errno::ESRCH) => Ok(()),
            Err(error) => Err(std::io::Error::from_raw_os_error(error as i32)),
        }
    }

    #[cfg(unix)]
    fn signal_child(&self, signal: nix::sys::signal::Signal) -> std::io::Result<()> {
        let Some(pid) = self
            .child
            .id()
            .and_then(|id| i32::try_from(id).ok())
            .map(nix::unistd::Pid::from_raw)
        else {
            return Ok(());
        };
        match nix::sys::signal::kill(pid, signal) {
            Ok(()) | Err(nix::errno::Errno::ESRCH) => Ok(()),
            Err(error) => Err(std::io::Error::from_raw_os_error(error as i32)),
        }
    }

    fn send_graceful_interrupt(&mut self) -> std::io::Result<bool> {
        if !self.control.supports_graceful_interrupt() {
            self.child.start_kill()?;
            return Ok(false);
        }
        #[cfg(unix)]
        {
            match self.control {
                RuntimeChildControl::NativeIsolated => {
                    self.signal_process_group(nix::sys::signal::Signal::SIGINT)?;
                    return Ok(true);
                }
                RuntimeChildControl::NativeForeground => {
                    self.signal_child(nix::sys::signal::Signal::SIGINT)?;
                    return Ok(true);
                }
                RuntimeChildControl::Immediate => {}
            }
        }
        debug_assert!(!self.control.supports_graceful_interrupt());
        self.child.start_kill()?;
        Ok(false)
    }

    fn start_kill(&mut self) -> std::io::Result<()> {
        #[cfg(unix)]
        if self.control.isolates_process_group() {
            self.signal_process_group(nix::sys::signal::Signal::SIGKILL)?;
        }
        match self.child.start_kill() {
            Err(error) if error.kind() == std::io::ErrorKind::InvalidInput => Ok(()),
            result => result,
        }
    }
}

impl Drop for ManagedRuntimeChild {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        #[cfg(unix)]
        if self.control.isolates_process_group() {
            let _ = self.signal_process_group(nix::sys::signal::Signal::SIGKILL);
        }
        let _ = self.child.start_kill();
    }
}

async fn stop_interrupted_runtime(
    child: &mut ManagedRuntimeChild,
    delivery: InterruptDelivery,
) -> Result<(std::process::ExitStatus, bool)> {
    let control = child.control;
    let graceful = match delivery {
        InterruptDelivery::Forward => child.send_graceful_interrupt().map_err(|error| {
            native_runtime_cleanup_io_error(control, "deliver interrupt", error)
        })?,
        InterruptDelivery::ForegroundAlreadySignaled => {
            debug_assert_eq!(child.control, RuntimeChildControl::NativeForeground);
            true
        }
    };
    if graceful {
        if let Ok(status) =
            tokio::time::timeout(NATIVE_RUNTIME_SHUTDOWN_GRACE_PERIOD, child.wait()).await
        {
            return status
                .map(|status| (status, false))
                .map_err(|error| native_runtime_cleanup_io_error(control, "reap child", error));
        }
        child
            .start_kill()
            .map_err(|error| native_runtime_cleanup_io_error(control, "force-stop child", error))?;
        let status = child.wait().await.map_err(|error| {
            native_runtime_cleanup_io_error(control, "reap forced child", error)
        })?;
        return Ok((status, true));
    }
    let status = child
        .wait()
        .await
        .map_err(|error| native_runtime_cleanup_io_error(control, "reap child", error))?;
    Ok((status, false))
}

async fn foreground_ctrl_c(control: RuntimeChildControl) -> std::io::Result<()> {
    if control == RuntimeChildControl::NativeForeground {
        tokio::signal::ctrl_c().await
    } else {
        std::future::pending::<std::io::Result<()>>().await
    }
}

async fn wait_native_runtime_child(
    child: tokio::process::Child,
    cancellation: &OperationCancellation,
    control: RuntimeChildControl,
) -> Result<(std::process::ExitStatus, bool)> {
    let mut child = ManagedRuntimeChild::new(child, control);
    let foreground_signal = foreground_ctrl_c(control);
    tokio::pin!(foreground_signal);
    tokio::select! {
        biased;
        signal = &mut foreground_signal => {
            signal.map_err(|error| {
                native_runtime_cleanup_io_error(control, "observe foreground Ctrl-C", error)
            })?;
            let (status, forced) = stop_interrupted_runtime(
                &mut child,
                InterruptDelivery::ForegroundAlreadySignaled,
            ).await?;
            if forced {
                return Err(native_runtime_forced_cleanup_incomplete(control));
            }
            Ok((status, true))
        }
        () = cancellation.cancelled() => {
            let delivery = if control == RuntimeChildControl::NativeForeground {
                match tokio::time::timeout(
                    FOREGROUND_SIGNAL_PROPAGATION_GRACE_PERIOD,
                    &mut foreground_signal,
                ).await {
                    Ok(signal) => {
                        signal.map_err(|error| {
                            native_runtime_cleanup_io_error(
                                control,
                                "observe foreground Ctrl-C",
                                error,
                            )
                        })?;
                        InterruptDelivery::ForegroundAlreadySignaled
                    }
                    Err(_) => InterruptDelivery::Forward,
                }
            } else {
                InterruptDelivery::Forward
            };
            let (status, forced) =
                stop_interrupted_runtime(&mut child, delivery).await?;
            if forced {
                return Err(native_runtime_forced_cleanup_incomplete(control));
            }
            Ok((status, true))
        }
        status = child.wait() => {
            let status = status?;
            let interrupted = native_foreground_status_was_interrupted(status, control);
            Ok((status, interrupted))
        },
    }
}

fn native_foreground_status_was_interrupted(
    status: std::process::ExitStatus,
    control: RuntimeChildControl,
) -> bool {
    if control != RuntimeChildControl::NativeForeground {
        return false;
    }
    #[cfg(unix)]
    {
        status.signal() == Some(nix::libc::SIGINT) || status.code() == Some(128 + nix::libc::SIGINT)
    }
    #[cfg(not(unix))]
    {
        false
    }
}

fn native_runtime_cleanup_io_error(
    control: RuntimeChildControl,
    action: &str,
    error: std::io::Error,
) -> RepoboxError {
    let cleanup_target = if cfg!(unix) && control.isolates_process_group() {
        "isolated process-group cleanup"
    } else {
        "child-process cleanup"
    };
    native_runtime_cleanup_incomplete(
        format!("failed to {action} during interruption: {error}"),
        format!("{cleanup_target} could not be confirmed"),
    )
}

fn native_runtime_forced_cleanup_incomplete(control: RuntimeChildControl) -> RepoboxError {
    let forced_cleanup = if cfg!(unix) && control.isolates_process_group() {
        "its process group was killed"
    } else {
        "the child process was killed"
    };
    native_runtime_cleanup_incomplete(
        format!(
            "native runtime did not exit within {} seconds",
            NATIVE_RUNTIME_SHUTDOWN_GRACE_PERIOD.as_secs()
        ),
        forced_cleanup,
    )
}

fn native_runtime_cleanup_incomplete(
    detail: impl std::fmt::Display,
    cleanup_outcome: impl std::fmt::Display,
) -> RepoboxError {
    RepoboxError::new(
        ErrorKind::Runtime,
        "native_runtime_cleanup_incomplete",
        format!(
            "{detail}; {cleanup_outcome}, and independently detached descendants may remain",
        ),
    )
    .with_suggestion(
        "Inspect the native command's descendants and listening ports before running Repobox again.",
    )
}

async fn cancellation_requested(cancellation: Option<&OperationCancellation>) {
    if let Some(cancellation) = cancellation {
        cancellation.cancelled().await;
    } else {
        std::future::pending::<()>().await;
    }
}

async fn stream_log_child(
    output: &Output,
    child: tokio::process::Child,
    options: LogStreamOptions,
) -> Result<()> {
    let mut child = ManagedRuntimeChild::new(child, options.child_control);
    let stdout = child.child.stdout.take().expect("logs stdout is piped");
    let stderr = child.child.stderr.take().expect("logs stderr is piped");
    let (sender, mut receiver) = mpsc::unbounded_channel::<(bool, String)>();
    let stdout_sender = sender.clone();
    tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if stdout_sender.send((false, line)).is_err() {
                break;
            }
        }
    });
    tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if sender.send((true, line)).is_err() {
                break;
            }
        }
    });

    let mut interrupted = false;
    loop {
        let item = if options.interruptible {
            tokio::select! {
                value = receiver.recv() => value,
                signal = tokio::signal::ctrl_c() => {
                    signal?;
                    interrupted = true;
                    None
                }
                () = cancellation_requested(options.cancellation.as_ref()) => {
                    interrupted = true;
                    None
                }
            }
        } else {
            receiver.recv().await
        };
        let Some((is_stderr, line)) = item else {
            break;
        };
        let line = options.redactor.redact(&line);
        if output.json() {
            let (service, line) = if is_stderr {
                (options.stderr_service.to_owned(), line)
            } else if options.compose_format {
                parse_compose_log(&line)
            } else {
                ("native".to_owned(), line)
            };
            output.stream(
                "log",
                &serde_json::json!({
                    "service": service,
                    "stream": if is_stderr { "stderr" } else { "stdout" },
                    "line": line,
                }),
            )?;
        } else if is_stderr {
            eprintln!("{line}");
        } else {
            let (_, line) = parse_compose_log(&line);
            println!("{line}");
        }
    }

    let (status, forced) = if interrupted {
        stop_interrupted_runtime(&mut child, InterruptDelivery::Forward).await?
    } else {
        (child.wait().await?, false)
    };
    if forced {
        return Err(native_runtime_forced_cleanup_incomplete(
            options.child_control,
        ));
    }
    if interrupted || status.success() {
        Ok(())
    } else {
        Err(RepoboxError::new(
            ErrorKind::Runtime,
            options.failure_code,
            format!("{} exited with {status}", options.process_label),
        ))
    }
}

async fn auth(cli: &Cli, output: &Output, command: AuthCommand) -> Result<()> {
    let paths = RepoboxPaths::discover()?;
    let store = CredentialStore::new(paths.credentials_file());
    match command {
        AuthCommand::Login(args) => auth_login(cli, output, &store, &args).await,
        AuthCommand::Status => {
            let status = store.status()?;
            if !status.configured {
                return Err(RepoboxError::new(
                    ErrorKind::Authentication,
                    "authentication_invalid",
                    "PlanetScale credentials are not configured",
                )
                .with_suggestion(
                    "Run `repobox auth login`, or set both PLANETSCALE_SERVICE_TOKEN_ID and PLANETSCALE_SERVICE_TOKEN.",
                ));
            }
            let (credentials, _) = store.provider_credentials()?;
            PlanetScaleClient::new(credentials)?.validate_auth().await?;
            let human = format!(
                "PlanetScale {} credentials are configured and valid",
                status.method.map_or("provider", |method| match method {
                    crate::credentials::CredentialMethod::BrowserOauth => "browser OAuth",
                    crate::credentials::CredentialMethod::ServiceToken => "service-token",
                })
            );
            output.human_or_data(
                "auth status",
                &serde_json::json!({"credentials": status, "valid": true}),
                &human,
            )
        }
        AuthCommand::Logout => {
            confirm(cli, "Remove locally stored PlanetScale credentials?")?;
            let stored = store.stored_provider_credentials()?;
            let method = stored.as_ref().map(|(credentials, _)| credentials.method());
            let revoke_required = matches!(
                stored.as_ref(),
                Some((PlanetScaleCredentials::AccessToken { .. }, _))
            );
            let mut revoked = false;
            if !cli.dry_run {
                if let Some((PlanetScaleCredentials::AccessToken { token }, _)) = &stored {
                    PlanetScaleDeviceAuth::new()?.revoke(token).await?;
                    revoked = true;
                }
                store.remove_provider()?;
            }
            let environment_override_active = std::env::var_os("PLANETSCALE_SERVICE_TOKEN_ID")
                .is_some()
                || std::env::var_os("PLANETSCALE_SERVICE_TOKEN").is_some();
            let data = serde_json::json!({
                "removed": !cli.dry_run && stored.is_some(),
                "revoke_required": revoke_required,
                "revoked": revoked,
                "method": method,
                "environment_override_active": environment_override_active,
            });
            if cli.dry_run {
                output.human_or_data(
                    "auth logout",
                    &data,
                    "would remove stored PlanetScale credentials",
                )
            } else {
                let human = if stored.is_some() {
                    "removed stored PlanetScale credentials"
                } else {
                    "no stored PlanetScale credentials were present"
                };
                output.mutation(
                    "auth logout",
                    &data,
                    human,
                    None,
                    Some("removed secrets cannot be reconstructed by Repobox".to_owned()),
                )
            }
        }
    }
}

async fn auth_login(
    cli: &Cli,
    output: &Output,
    store: &CredentialStore,
    args: &crate::cli::AuthLoginArgs,
) -> Result<()> {
    let service_token_requested = args.token_id.is_some()
        || std::env::var_os("PLANETSCALE_SERVICE_TOKEN_ID").is_some()
        || std::env::var_os("PLANETSCALE_SERVICE_TOKEN").is_some();
    if service_token_requested {
        service_token_login(cli, output, store, args).await
    } else {
        device_login(cli, output, store, args).await
    }
}

async fn service_token_login(
    cli: &Cli,
    output: &Output,
    store: &CredentialStore,
    args: &crate::cli::AuthLoginArgs,
) -> Result<()> {
    let token_id = match &args.token_id {
        Some(value) if !value.is_empty() => value.clone(),
        _ if !cli.no_input && io::stdin().is_terminal() => {
            eprint!("PlanetScale service token ID: ");
            io::stderr().flush()?;
            let mut value = String::new();
            io::stdin().read_line(&mut value)?;
            value.trim().to_owned()
        }
        _ => return Err(authentication_input_required()),
    };
    let token = match std::env::var("PLANETSCALE_SERVICE_TOKEN") {
        Ok(value) if !value.is_empty() => value,
        _ if !cli.no_input && io::stdin().is_terminal() => {
            rpassword::prompt_password("PlanetScale service token: ")?
        }
        _ => return Err(authentication_input_required()),
    };
    finish_auth_login(
        cli,
        output,
        store,
        PlanetScaleCredentials::service_token(token_id, token),
        false,
    )
    .await
}

#[derive(Serialize)]
struct AuthPending<'a> {
    status: &'static str,
    method: &'static str,
    verification_url: &'a str,
    user_code: &'a str,
    browser_opened: bool,
    expires_in_seconds: u64,
}

async fn device_login(
    cli: &Cli,
    output: &Output,
    store: &CredentialStore,
    args: &crate::cli::AuthLoginArgs,
) -> Result<()> {
    if !output.json() && (!io::stdin().is_terminal() || !io::stderr().is_terminal()) {
        return Err(RepoboxError::new(
            ErrorKind::Authentication,
            "interactive_shell_required",
            "browser login requires an interactive terminal in human output mode",
        )
        .with_suggestion(
            "Use `repobox auth login --json --no-input` for a machine-readable device flow, or set service-token environment variables.",
        ));
    }
    let authenticator = PlanetScaleDeviceAuth::new()?;
    let authorization = authenticator.start().await?;
    let browser_opened = !args.no_browser && open_browser(authorization.verification_url());
    let pending = AuthPending {
        status: "pending",
        method: "browser_oauth",
        verification_url: authorization.verification_url(),
        user_code: authorization.user_code(),
        browser_opened,
        expires_in_seconds: authorization.expires_in().as_secs(),
    };
    if output.json() {
        output.stream("auth_pending", &pending)?;
    } else {
        eprintln!(
            "\nPlanetScale confirmation code: {}",
            authorization.user_code()
        );
        if browser_opened {
            eprintln!(
                "Approve access in the browser, or open:\n{}\n",
                authorization.verification_url()
            );
        } else {
            eprintln!(
                "Open this URL to approve access:\n{}\n",
                authorization.verification_url()
            );
        }
        output.progress("Waiting for PlanetScale approval...");
    }
    let token = authenticator.wait_for_access_token(&authorization).await?;
    finish_auth_login(
        cli,
        output,
        store,
        PlanetScaleCredentials::AccessToken { token },
        true,
    )
    .await
}

async fn finish_auth_login(
    cli: &Cli,
    output: &Output,
    store: &CredentialStore,
    credentials: PlanetScaleCredentials,
    streamed: bool,
) -> Result<()> {
    PlanetScaleClient::new(credentials.clone())?
        .validate_auth()
        .await?;
    let mut revoked_after_validation = false;
    let source = if cli.dry_run {
        if let PlanetScaleCredentials::AccessToken { token } = &credentials {
            PlanetScaleDeviceAuth::new()?.revoke(token).await?;
            revoked_after_validation = true;
        }
        None
    } else {
        match store.store_provider(&credentials) {
            Ok(source) => Some(source),
            Err(storage_error) => {
                if let PlanetScaleCredentials::AccessToken { token } = &credentials {
                    return match PlanetScaleDeviceAuth::new()?.revoke(token).await {
                        Ok(()) => Err(storage_error.with_suggestion(
                            "The browser token was revoked. Fix OS keyring or config-directory permissions, then rerun `repobox auth login`.",
                        )),
                        Err(revoke_error) => Err(RepoboxError::new(
                            ErrorKind::Runtime,
                            "credential_store_and_token_revoke_failed",
                            format!(
                                "credential storage failed ({storage_error}); cleanup also failed ({revoke_error})"
                            ),
                        )
                        .with_suggestion(
                            "Revoke PlanetScale CLI authorization in PlanetScale account settings, fix local credential storage, then retry.",
                        )),
                    };
                }
                return Err(storage_error);
            }
        }
    };
    let data = serde_json::json!({
        "authenticated": true,
        "method": credentials.method(),
        "stored": !cli.dry_run,
        "source": source,
        "revoked_after_validation": revoked_after_validation,
    });
    if streamed && output.json() {
        return output.stream_mutation(
            &data,
            (!cli.dry_run).then(|| "repobox auth logout --yes".to_owned()),
            cli.dry_run
                .then(|| "dry-run authentication did not store credentials".to_owned()),
        );
    }
    if cli.dry_run {
        output.human_or_data(
            "auth login",
            &data,
            "credentials are valid; nothing stored (dry run)",
        )
    } else {
        output.mutation(
            "auth login",
            &data,
            "authenticated with PlanetScale",
            Some("repobox auth logout --yes".to_owned()),
            None,
        )
    }
}

async fn environment(
    cli: &Cli,
    output: &Output,
    repository: &Path,
    command: EnvCommand,
    cancellation: &OperationCancellation,
) -> Result<()> {
    let context = ProjectContext::load(repository)?;
    let credentials = credential_store(&context.paths);
    match command {
        EnvCommand::List => {
            let state =
                state_store(&context.config, &context.paths).load(context.config.project.id)?;
            output.data("env list", &state.environments)
        }
        EnvCommand::Create(args) => {
            let name = match args.name {
                Some(name) => name,
                None => context.environment(None).await?,
            };
            let options = ProvisionOptions {
                create_backup: args.create_backup,
                wait_for_backup: args.wait,
                selected_services: BTreeSet::new(),
            };
            if cli.dry_run {
                let provider = planning_provider()?;
                let manager = EnvironmentManager::new(
                    &context.config,
                    &context.repository,
                    &provider,
                    &credentials,
                    state_store(&context.config, &context.paths),
                    job_store(&context.config, &context.paths),
                    output,
                );
                return output.data("env create", &manager.create_plan(&name, &options)?);
            }
            let provider = provider(&credentials)?;
            let mut manager = EnvironmentManager::new(
                &context.config,
                &context.repository,
                &provider,
                &credentials,
                state_store(&context.config, &context.paths),
                job_store(&context.config, &context.paths),
                output,
            )
            .with_cancellation(cancellation.clone());
            confirm(
                cli,
                &format!("Create billable PlanetScale data for `{name}`?"),
            )?;
            let mutation = manager.ensure(&name, &options).await?;
            let undo = Some(format!("repobox env delete {name} --yes"));
            if output.json() {
                output.stream_mutation(&mutation, undo, None)
            } else {
                output.mutation(
                    "env create",
                    &mutation,
                    &format!("environment `{name}` is ready"),
                    undo,
                    None,
                )
            }
        }
        EnvCommand::Delete(args) => {
            confirm(
                cli,
                &format!(
                    "Permanently delete remote data environment `{}`?",
                    args.name
                ),
            )?;
            if cli.dry_run {
                let provider = planning_provider()?;
                let manager = EnvironmentManager::new(
                    &context.config,
                    &context.repository,
                    &provider,
                    &credentials,
                    state_store(&context.config, &context.paths),
                    job_store(&context.config, &context.paths),
                    output,
                );
                return output.data("env delete", &manager.delete_plan(&args.name)?);
            }
            let provider = provider(&credentials)?;
            let mut manager = EnvironmentManager::new(
                &context.config,
                &context.repository,
                &provider,
                &credentials,
                state_store(&context.config, &context.paths),
                job_store(&context.config, &context.paths),
                output,
            )
            .with_cancellation(cancellation.clone());
            let mutation = manager.delete(&args.name, args.keep_state).await?;
            output.mutation(
                "env delete",
                &mutation,
                &format!("deleted environment `{}`", args.name),
                None,
                Some("provider branch deletion is irreversible".to_owned()),
            )
        }
        EnvCommand::Prune(args) => {
            let merged = crate::git::merged_branches(
                &context.repository,
                &context.config.project.git.base_branch,
                args.fetch,
            )
            .await?;
            let state =
                state_store(&context.config, &context.paths).load(context.config.project.id)?;
            let targets = merged
                .into_iter()
                .filter(|branch| state.environments.contains_key(branch))
                .collect::<Vec<_>>();
            if targets.is_empty() {
                return output.human_or_data(
                    "env prune",
                    &serde_json::json!({"targets": [], "deleted": []}),
                    "no merged Repobox environments need pruning",
                );
            }
            confirm(
                cli,
                &format!(
                    "Delete remote data for merged branches: {}?",
                    targets.join(", ")
                ),
            )?;
            if cli.dry_run {
                return output.data("env prune", &serde_json::json!({"targets": targets}));
            }
            let provider = provider(&credentials)?;
            let mut manager = EnvironmentManager::new(
                &context.config,
                &context.repository,
                &provider,
                &credentials,
                state_store(&context.config, &context.paths),
                job_store(&context.config, &context.paths),
                output,
            )
            .with_cancellation(cancellation.clone());
            manager.delete_many(&targets).await?;
            output.mutation(
                "env prune",
                &serde_json::json!({"deleted": targets}),
                "pruned merged data environments",
                None,
                Some("provider branch deletion is irreversible".to_owned()),
            )
        }
    }
}

async fn service(
    cli: &Cli,
    output: &Output,
    repository: &Path,
    command: ServiceCommand,
) -> Result<()> {
    match command {
        ServiceCommand::List(selector) => {
            let context = ProjectContext::load(repository)?;
            let environment = context.environment(selector.environment.as_deref()).await?;
            let state =
                state_store(&context.config, &context.paths).load(context.config.project.id)?;
            output.data(
                "service list",
                &serde_json::json!({
                    "configured": context.config.services,
                    "environment": state.environments.get(&environment),
                }),
            )
        }
        ServiceCommand::Status(target) => {
            let context = ProjectContext::load(repository)?;
            let environment = context
                .environment(target.environment.environment.as_deref())
                .await?;
            let runtime = compose_runtime(&context, &environment, &BTreeMap::new()).await?;
            let status = runtime.status().await?;
            let service = status
                .services
                .into_iter()
                .find(|service| service.name == target.service)
                .ok_or_else(|| {
                    RepoboxError::new(
                        ErrorKind::NotFound,
                        "service_not_found",
                        format!("running service `{}` was not found", target.service),
                    )
                })?;
            output.data("service status", &service)
        }
        ServiceCommand::Restart(target) => {
            if cli.dry_run {
                return output.data(
                    "service restart",
                    &serde_json::json!({"service": target.service, "executed": false}),
                );
            }
            let context = ProjectContext::load(repository)?;
            let environment = context
                .environment(target.environment.environment.as_deref())
                .await?;
            let runtime = compose_runtime(&context, &environment, &BTreeMap::new()).await?;
            if output.json() {
                runtime.restart_quiet(Some(&target.service)).await?;
            } else {
                runtime.restart(Some(&target.service)).await?;
            }
            output.mutation(
                "service restart",
                &serde_json::json!({"service": target.service, "restarted": true}),
                &format!("restarted `{}`", target.service),
                None,
                Some("a process restart has no meaningful inverse".to_owned()),
            )
        }
        ServiceCommand::Logs(args) => {
            logs(
                output,
                repository,
                None,
                Some(&args.service),
                args.follow,
                args.tail,
            )
            .await
        }
    }
}

async fn jobs(
    cli: &Cli,
    output: &Output,
    repository: &Path,
    command: JobCommand,
    cancellation: &OperationCancellation,
) -> Result<()> {
    let context = ProjectContext::load(repository)?;
    let store = job_store(&context.config, &context.paths);
    match command {
        JobCommand::List => output.data("job list", &store.list()?),
        JobCommand::View(target) => {
            let job = resolve_job(&store, &target.id)?;
            output.data("job view", &job)?;
            if target.exit_status && job.status != JobStatus::Succeeded {
                Err(RepoboxError::new(
                    ErrorKind::Runtime,
                    "job_not_succeeded",
                    format!("job {} is {:?}", job.id, job.status),
                )
                .with_suggestion(format!("Run `repobox job view {}` for details.", job.id)))
            } else {
                Ok(())
            }
        }
        JobCommand::Cancel(target) => {
            reject_symbolic_job_mutation(&target.id)?;
            let mut job = resolve_job(&store, &target.id)?;
            if job.status.terminal() {
                return Err(RepoboxError::new(
                    ErrorKind::Conflict,
                    "job_already_terminal",
                    format!("job {} is already {:?}", job.id, job.status),
                ));
            }
            confirm(cli, &format!("Cancel durable job {}?", job.id))?;
            if !cli.dry_run {
                job.status = JobStatus::Canceled;
                job.sequence += 1;
                job.updated_at = chrono::Utc::now();
                store.append(&job)?;
            }
            if cli.dry_run {
                output.data("job cancel", &job)
            } else {
                output.mutation(
                    "job cancel",
                    &job,
                    &format!("canceled job {}", job.id),
                    None,
                    Some("a canceled durable job cannot be resumed".to_owned()),
                )
            }
        }
        JobCommand::Resume(target) => {
            reject_symbolic_job_mutation(&target.id)?;
            let job = resolve_job(&store, &target.id)?;
            if job.status.terminal() {
                return Err(RepoboxError::new(
                    ErrorKind::Conflict,
                    "job_already_terminal",
                    format!("job {} is already {:?}", job.id, job.status),
                ));
            }
            let step_prefix = match job.kind {
                JobKind::EnvironmentCreate => "provision:",
                JobKind::EnvironmentPull => "refresh:",
                _ => {
                    return Err(RepoboxError::new(
                        ErrorKind::Conflict,
                        "job_resume_unsupported",
                        "this job kind cannot yet be resumed by the current command",
                    ));
                }
            };
            let selected_services = job
                .steps
                .iter()
                .filter_map(|step| step.name.strip_prefix(step_prefix).map(str::to_owned))
                .collect();
            let options = ProvisionOptions {
                create_backup: target.create_backup,
                wait_for_backup: target.wait,
                selected_services,
            };
            confirm(
                cli,
                &format!(
                    "Resume durable job {} for environment `{}`? This may create billable resources or replace environment data.",
                    job.id, job.environment
                ),
            )?;
            let credentials = credential_store(&context.paths);
            let provider = if cli.dry_run {
                planning_provider()?
            } else {
                provider(&credentials)?
            };
            let mut manager = EnvironmentManager::new(
                &context.config,
                &context.repository,
                &provider,
                &credentials,
                state_store(&context.config, &context.paths),
                store,
                output,
            )
            .with_cancellation(cancellation.clone());
            match job.kind {
                JobKind::EnvironmentCreate => {
                    if cli.dry_run {
                        output.data(
                            "job resume",
                            &manager.create_plan(&job.environment, &options)?,
                        )
                    } else {
                        let mutation = manager
                            .resume_create(job.id, &job.environment, &options)
                            .await?;
                        let undo = Some(format!("repobox env delete {} --yes", job.environment));
                        if output.json() {
                            output.stream_mutation(&mutation, undo, None)
                        } else {
                            output.mutation(
                                "job resume",
                                &mutation,
                                &format!("resumed job {}", job.id),
                                undo,
                                None,
                            )
                        }
                    }
                }
                JobKind::EnvironmentPull => {
                    let mut restart = false;
                    if matches!(context.config.runtime, RuntimeConfig::Compose { .. })
                        && !cli.dry_run
                    {
                        let runtime =
                            compose_runtime(&context, &job.environment, &BTreeMap::new()).await?;
                        restart = runtime.status().await?.running;
                        if restart {
                            if output.json() {
                                runtime.stop_quiet().await?;
                            } else {
                                runtime.stop().await?;
                            }
                        }
                    }
                    if cli.dry_run {
                        output.data(
                            "job resume",
                            &manager.pull_plan(&job.environment, &options)?,
                        )
                    } else {
                        let mutation = manager
                            .resume_pull(job.id, &job.environment, &options)
                            .await?;
                        if restart {
                            let state = state_store(&context.config, &context.paths)
                                .load(context.config.project.id)?;
                            let variables = environment_variables(
                                &context.config,
                                state_for_environment(&state, &job.environment)?,
                                &credentials,
                            )?;
                            let runtime =
                                compose_runtime(&context, &job.environment, &variables).await?;
                            if output.json() {
                                runtime.start_quiet(&variables).await?;
                            } else {
                                runtime.start(&variables, true).await?;
                            }
                        }
                        let reason =
                            Some("a forward-only data refresh cannot be undone".to_owned());
                        if output.json() {
                            output.stream_mutation(&mutation, None, reason)
                        } else {
                            output.mutation(
                                "job resume",
                                &mutation,
                                &format!("resumed job {}", job.id),
                                None,
                                reason,
                            )
                        }
                    }
                }
                _ => unreachable!("unsupported job kinds returned before manager setup"),
            }
        }
    }
}

async fn config(
    cli: &Cli,
    output: &Output,
    repository: &Path,
    command: ConfigCommand,
) -> Result<()> {
    match command {
        ConfigCommand::Detect => {
            output.data("config detect", &initialize::detect(repository).await?)
        }
        ConfigCommand::View => {
            let context = ProjectContext::load(repository)?;
            output.data("config view", &context.config)
        }
        ConfigCommand::Schema => output.data("config schema", &RepoboxConfig::json_schema()),
        ConfigCommand::Validate(args) => {
            let path = match args.path {
                Some(path) => path,
                None => ProjectContext::load(repository)?.config_path,
            };
            let config = RepoboxConfig::load(&path)?;
            output.human_or_data(
                "config validate",
                &serde_json::json!({"valid": true, "path": path, "version": config.version}),
                &format!("{} is valid", path.display()),
            )
        }
        ConfigCommand::Update(args) => {
            let context = ProjectContext::load(repository)?;
            let patch_input = match args.patch {
                Some(patch) => patch,
                None if !io::stdin().is_terminal() => {
                    let mut patch = String::new();
                    io::stdin().read_to_string(&mut patch)?;
                    patch
                }
                None if !cli.no_input => {
                    eprint!("JSON Merge Patch: ");
                    io::stderr().flush()?;
                    let mut patch = String::new();
                    io::stdin().read_line(&mut patch)?;
                    patch
                }
                None => {
                    return Err(RepoboxError::new(
                        ErrorKind::Usage,
                        "config_patch_required",
                        "config update requires --patch JSON or a JSON object on stdin",
                    )
                    .with_suggestion(
                        "Pipe a patch to `repobox config update --no-input`, or pass --patch JSON.",
                    ));
                }
            };
            if patch_input.trim().is_empty() {
                return Err(RepoboxError::new(
                    ErrorKind::Usage,
                    "config_patch_required",
                    "config update requires --patch JSON or a JSON object on stdin",
                )
                .with_suggestion(
                    "Pipe a patch to `repobox config update --no-input`, or pass --patch JSON.",
                ));
            }
            let patch: serde_json::Value = serde_json::from_str(&patch_input).map_err(|error| {
                RepoboxError::new(
                    ErrorKind::Usage,
                    "config_patch_invalid_json",
                    error.to_string(),
                )
            })?;
            let updated = context.config.apply_merge_patch(&patch)?;
            if !cli.dry_run {
                let temporary = context.config_path.with_extension("yml.repobox.tmp");
                fs::write(&temporary, updated.to_yaml()?)?;
                fs::rename(temporary, &context.config_path)?;
                crate::agent_guides::update(&context.repository, &updated, false)?;
            }
            if cli.dry_run {
                output.data("config update", &updated)
            } else {
                output.mutation(
                    "config update",
                    &updated,
                    "updated .repobox.yml",
                    None,
                    Some(
                        "the prior configuration is not retained; use version control to revert"
                            .to_owned(),
                    ),
                )
            }
        }
    }
}

fn telemetry(cli: &Cli, output: &Output, command: TelemetryCommand) -> Result<()> {
    let paths = RepoboxPaths::discover()?;
    let path = paths.user_config();
    let current = read_telemetry(&path)?;
    match command {
        TelemetryCommand::Status => output.human_or_data(
            "telemetry status",
            &serde_json::json!({"enabled": current, "sending": false}),
            &format!(
                "telemetry preference: {}; v0.1 sends no events",
                if current { "enabled" } else { "disabled" }
            ),
        ),
        TelemetryCommand::Enable | TelemetryCommand::Disable => {
            let enabled = matches!(command, TelemetryCommand::Enable);
            if !cli.dry_run {
                RepoboxPaths::ensure_parent(&path)?;
                fs::write(&path, format!("telemetry:\n  enabled: {enabled}\n"))?;
            }
            let data = serde_json::json!({
                "enabled": enabled,
                "sending": false,
                "dry_run": cli.dry_run,
            });
            if cli.dry_run {
                output.human_or_data(
                    "telemetry update",
                    &data,
                    "would update telemetry preference; v0.1 sends no events",
                )
            } else {
                output.mutation(
                    "telemetry update",
                    &data,
                    "updated telemetry preference; v0.1 sends no events",
                    Some(if enabled {
                        "repobox telemetry disable".to_owned()
                    } else {
                        "repobox telemetry enable".to_owned()
                    }),
                    None,
                )
            }
        }
    }
}

async fn update(cli: &Cli, output: &Output, check_only: bool) -> Result<()> {
    #[derive(serde::Deserialize, Serialize)]
    struct Release {
        tag_name: String,
        html_url: String,
    }
    let response = reqwest::Client::new()
        .get("https://api.github.com/repos/Sainapse-Inc/repobox/releases/latest")
        .header(
            reqwest::header::USER_AGENT,
            concat!("repobox/", env!("CARGO_PKG_VERSION")),
        )
        .send()
        .await
        .map_err(|error| {
            RepoboxError::new(ErrorKind::Runtime, "update_check_failed", error.to_string())
        })?;
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return output.human_or_data(
            "update",
            &serde_json::json!({"current": env!("CARGO_PKG_VERSION"), "latest": null}),
            "no published Repobox release exists yet",
        );
    }
    let release: Release = response
        .error_for_status()
        .map_err(|error| {
            RepoboxError::new(ErrorKind::Runtime, "update_check_failed", error.to_string())
        })?
        .json()
        .await
        .map_err(|error| {
            RepoboxError::new(
                ErrorKind::Runtime,
                "update_response_invalid",
                error.to_string(),
            )
        })?;
    let current = env!("CARGO_PKG_VERSION");
    let available = release.tag_name.trim_start_matches('v') != current;
    if available && !check_only && !cli.dry_run {
        return Err(RepoboxError::new(
            ErrorKind::Conflict,
            "self_update_requires_installer",
            format!("{} is available", release.tag_name),
        )
        .with_suggestion("Upgrade with Homebrew or `cargo install repobox --locked`."));
    }
    output.human_or_data(
        "update",
        &serde_json::json!({
            "current": current,
            "latest": release.tag_name,
            "available": available,
            "url": release.html_url,
        }),
        if available {
            "a Repobox update is available"
        } else {
            "Repobox is current"
        },
    )
}

async fn doctor(output: &Output, repository: &Path, online: bool) -> Result<()> {
    let mut checks = vec![];
    for (name, program, args) in [
        ("git", "git", vec!["--version"]),
        ("docker", "docker", vec!["--version"]),
        ("docker_compose", "docker", vec!["compose", "version"]),
    ] {
        let result = TokioCommand::new(program).args(args).output().await;
        checks.push(serde_json::json!({
            "name": name,
            "ok": result.as_ref().is_ok_and(|output| output.status.success()),
            "detail": result.ok().map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned()),
        }));
    }
    let config = ProjectContext::load(repository);
    checks.push(serde_json::json!({
        "name": "configuration",
        "ok": config.is_ok(),
        "detail": config.as_ref().map(|context| context.config_path.display().to_string()).ok(),
    }));
    let credentials = credential_store(&RepoboxPaths::discover()?);
    let credential_status = credentials.status()?;
    checks.push(serde_json::json!({
        "name": "planetscale_credentials",
        "ok": credential_status.configured,
        "detail": credential_status,
    }));
    if online && credential_status.configured {
        let valid = provider(&credentials)?.validate_auth().await.is_ok();
        checks.push(serde_json::json!({"name": "planetscale_api", "ok": valid}));
    }
    let healthy = checks
        .iter()
        .all(|check| check["ok"].as_bool().unwrap_or(false));
    output.data(
        "doctor",
        &serde_json::json!({"healthy": healthy, "checks": checks}),
    )?;
    if healthy {
        Ok(())
    } else {
        Err(RepoboxError::new(
            ErrorKind::Runtime,
            "doctor_checks_failed",
            "one or more Repobox checks failed",
        ))
    }
}

fn completion(output: &Output, shell: CompletionShell) -> Result<()> {
    let (shell, name) = match shell {
        CompletionShell::Bash => (clap_complete::Shell::Bash, "bash"),
        CompletionShell::Elvish => (clap_complete::Shell::Elvish, "elvish"),
        CompletionShell::Fish => (clap_complete::Shell::Fish, "fish"),
        CompletionShell::PowerShell => (clap_complete::Shell::PowerShell, "powershell"),
        CompletionShell::Zsh => (clap_complete::Shell::Zsh, "zsh"),
    };
    let mut source = vec![];
    clap_complete::generate(shell, &mut Cli::command(), "repobox", &mut source);
    if output.json() {
        let source = String::from_utf8(source).map_err(|error| {
            RepoboxError::new(
                ErrorKind::Runtime,
                "completion_encode_failed",
                error.to_string(),
            )
        })?;
        output.data(
            "completion",
            &serde_json::json!({"shell": name, "source": source}),
        )
    } else {
        io::stdout().write_all(&source)?;
        Ok(())
    }
}

async fn agent_context(output: &Output, repository: &Path, schemas: bool) -> Result<()> {
    let context = ProjectContext::load(repository).ok();
    let (runtime_driver, detach_supported, recommended_run) =
        agent_runtime_guidance(context.as_ref().map(|context| &context.config));
    let project = if let Some(context) = &context {
        let environment = context.environment(None).await.ok();
        let state = state_store(&context.config, &context.paths)
            .load(context.config.project.id)
            .ok();
        serde_json::json!({
            "repository": context.repository,
            "config_path": context.config_path,
            "config": context.config,
            "current_environment": environment,
            "state": state,
        })
    } else {
        serde_json::Value::Null
    };
    let data = serde_json::json!({
        "schema_version": 1,
        "tool": {"name": "repobox", "version": env!("CARGO_PKG_VERSION")},
        "contract": {
            "json_flag": "--json",
            "dry_run_flag": "--dry-run",
            "approval_flag": "--yes",
            "non_interactive_flag": "--no-input",
            "stream_format": "jsonl",
            "secrets_in_output": false,
            "authentication": {
                "human_command": "repobox auth login",
                "agent_device_command": "repobox auth login --json --no-input",
                "device_pending_event": "auth_pending",
                "unattended_environment": [
                    "PLANETSCALE_SERVICE_TOKEN_ID",
                    "PLANETSCALE_SERVICE_TOKEN"
                ]
            },
            "exit_codes": {
                "1": "runtime",
                "2": "usage",
                "3": "not_found",
                "4": "authentication",
                "5": "conflict",
                "6": "permission"
            }
        },
        "recommended_sequence": [
            "repobox auth status --json --no-input",
            "repobox config detect --json",
            "repobox status --json",
            recommended_run
        ],
        "runtime_guidance": {
            "driver": runtime_driver,
            "detach_supported": detach_supported,
            "run_command": recommended_run,
            "interrupt": "Ctrl-C requests bounded cleanup and waits for managed runtime shutdown"
        },
        "database_connection": database_connection_guidance(),
        "commands": command_manifest(&Cli::command()),
        "output_schema_refs": {
            "success": "docs/schemas/success-v1.json",
            "error": "docs/schemas/error-v1.json",
            "stream": "docs/schemas/stream-v1.json",
            "mutation": "docs/schemas/mutation-v1.json"
        },
        "environment_variables": [
            {"name": "REPOBOX_ENV", "secret": false, "purpose": "select the data environment"},
            {"name": "REPOBOX_PLANETSCALE_ORG", "secret": false, "purpose": "default organization during init"},
            {"name": "PLANETSCALE_SERVICE_TOKEN_ID", "secret": false, "purpose": "non-interactive provider authentication"},
            {"name": "PLANETSCALE_SERVICE_TOKEN", "secret": true, "purpose": "non-interactive provider authentication"},
            {"name": "REPOBOX_BROWSER", "secret": false, "purpose": "browser executable override"},
            {"name": "BROWSER", "secret": false, "purpose": "standard browser executable override"},
            {"name": "NO_COLOR", "secret": false, "purpose": "disable ANSI styling"},
            {"name": "XDG_CONFIG_HOME", "secret": false, "purpose": "user configuration root"},
            {"name": "XDG_STATE_HOME", "secret": false, "purpose": "durable state and jobs root"},
            {"name": "XDG_CACHE_HOME", "secret": false, "purpose": "cache root"}
        ],
        "mutations": ["init", "run (first environment)", "pull", "env create", "env delete", "env prune", "service restart", "config update"],
        "project": project,
        "schemas": schemas.then(contract_schemas),
    });
    output.data("agent-context", &data)
}

fn agent_runtime_guidance(config: Option<&RepoboxConfig>) -> (&'static str, bool, &'static str) {
    match config.map(|config| &config.runtime) {
        Some(RuntimeConfig::Compose { .. }) => (
            "compose",
            true,
            "repobox run --detach --yes --json --no-input",
        ),
        Some(RuntimeConfig::Native { .. }) => (
            "native",
            false,
            "repobox run --yes --json --no-input --no-tui",
        ),
        None => (
            "unknown",
            false,
            "repobox run --yes --json --no-input --no-tui",
        ),
    }
}

fn database_connection_guidance() -> serde_json::Value {
    serde_json::json!({
        "url_profile": "libpq-16",
        "tls_mode": "verify-full",
        "trust": "system",
        "help_command": "repobox help connections --json",
        "known_adapters": {
            "asyncpg": "pass ssl=True when constructing the connection or pool"
        }
    })
}

fn contract_schemas() -> serde_json::Value {
    serde_json::json!({
        "config": RepoboxConfig::json_schema(),
        "success": schemars::schema_for!(repobox_core::output::SuccessEnvelope<serde_json::Value>),
        "error": schemars::schema_for!(repobox_core::output::ErrorEnvelope),
        "stream": schemars::schema_for!(repobox_core::output::StreamEvent),
        "mutation": schemars::schema_for!(repobox_core::output::MutationReceipt<serde_json::Value>),
        "dry_run": schemars::schema_for!(repobox_core::output::DryRunPlan),
    })
}

fn command_manifest(command: &clap::Command) -> serde_json::Value {
    let arguments = command
        .get_arguments()
        .map(|argument| {
            serde_json::json!({
                "id": argument.get_id().as_str(),
                "long": argument.get_long(),
                "short": argument.get_short().map(|value| value.to_string()),
                "required": argument.is_required_set(),
                "action": format!("{:?}", argument.get_action()).to_ascii_lowercase(),
                "value_names": argument.get_value_names().map(|values| values.iter().map(ToString::to_string).collect::<Vec<_>>()),
                "environment": argument.get_env().map(|value| value.to_string_lossy()),
                "help": argument.get_help().map(ToString::to_string),
            })
        })
        .collect::<Vec<_>>();
    let subcommands = command
        .get_subcommands()
        .map(command_manifest)
        .collect::<Vec<_>>();
    let mut usage_command = command.clone();
    serde_json::json!({
        "name": command.get_name(),
        "about": command.get_about().map(ToString::to_string),
        "usage": usage_command.render_usage().to_string(),
        "arguments": arguments,
        "subcommands": subcommands,
    })
}

fn help(output: &Output, args: &HelpArgs) -> Result<()> {
    let (topic, body) = match args.topic.as_deref().unwrap_or("overview") {
        "overview" => (
            "overview",
            "Start with `repobox auth login`, then run `repobox run` inside a repository. If no config exists, the setup TUI detects Compose and every Postgres service.",
        ),
        "setup" => (
            "setup",
            "Human: `repobox auth login && repobox run`. Agent: check `repobox auth status --json --no-input`; if approval is needed, run `repobox auth login --json --no-input` and surface its URL/code, then detect and initialize the repository.",
        ),
        "agents" => (
            "agents",
            "Call `repobox agent-context --json` first. Browser auth is agent-operable through `auth login --json --no-input`: surface the `auth_pending` URL/code and wait for `result`. Add `--json` to every command, `--yes --no-input` to approved mutations, and `--dry-run` before destructive or billable operations. Streaming commands emit JSONL.",
        ),
        "data" => (
            "data",
            "Every Git branch, including main, maps deterministically to a separate PlanetScale branch restored from the latest successful base backup. `repobox pull` replaces environment-local data.",
        ),
        "connections" => (
            "connections",
            "PlanetScale URLs use `sslmode=verify-full&sslrootcert=system`; preserve hostname and certificate verification. Local `psql` must be version 16 or newer, otherwise Repobox uses its managed PostgreSQL 18 client. `asyncpg` treats `system` as a filename unless the application passes `ssl=True` when constructing the connection or pool. Never print an injected URL or downgrade TLS to make a driver parse it.",
        ),
        "environments" => (
            "environments",
            "Use `repobox env list`, `env create`, `env delete`, and `env prune --fetch`. Cleanup is explicit; merged branches are only suggested until prune is approved.",
        ),
        "environment" => (
            "environment",
            "Resolution order is flag > environment variable > .repobox.yml > user config > default. Structured input is explicit flag > piped stdin > interactive prompt. Key variables: REPOBOX_ENV, REPOBOX_PLANETSCALE_ORG, PLANETSCALE_SERVICE_TOKEN_ID, PLANETSCALE_SERVICE_TOKEN, REPOBOX_BROWSER, BROWSER, NO_COLOR, and the XDG_*_HOME variables. Secrets are never accepted as flags.",
        ),
        "formatting" => (
            "formatting",
            "Default output is for humans. Add --json for one versioned JSON envelope; streaming commands emit one versioned JSON object per line. Stdout is data, stderr is diagnostics. NO_COLOR disables ANSI and non-TTY streams never receive ANSI.",
        ),
        "config" => (
            "config",
            "`.repobox.yml` is strict and versioned. Use `config schema`, `config validate`, and RFC 7396 `config update --patch JSON`. Secrets never belong in the file.",
        ),
        "exit-codes" => (
            "exit-codes",
            "1 runtime, 2 usage, 3 not found, 4 authentication, 5 conflict, 6 permission. JSON errors use `{schema_version,error:{kind,code,message,suggestion}}`.",
        ),
        unknown => {
            return Err(RepoboxError::new(
                ErrorKind::NotFound,
                "help_topic_not_found",
                format!("unknown help topic `{unknown}`"),
            )
            .with_suggestion(
                "Available topics: setup, agents, data, connections, environment, environments, formatting, config, exit-codes.",
            ));
        }
    };
    output.human_or_data(
        "help",
        &serde_json::json!({"topic": topic, "text": body}),
        body,
    )
}

async fn pull(
    cli: &Cli,
    output: &Output,
    repository: &Path,
    args: &crate::cli::PullArgs,
    cancellation: &OperationCancellation,
) -> Result<()> {
    let context = ProjectContext::load(repository)?;
    let environment = context
        .environment(args.environment.environment.as_deref())
        .await?;
    let options = ProvisionOptions {
        create_backup: args.create_backup,
        wait_for_backup: args.wait,
        selected_services: args.database.iter().cloned().collect(),
    };
    let credentials = credential_store(&context.paths);
    if cli.dry_run {
        let provider = planning_provider()?;
        let manager = EnvironmentManager::new(
            &context.config,
            &context.repository,
            &provider,
            &credentials,
            state_store(&context.config, &context.paths),
            job_store(&context.config, &context.paths),
            output,
        );
        return output.data("pull", &manager.pull_plan(&environment, &options)?);
    }
    confirm(
        cli,
        &format!(
            "Permanently replace data in environment `{environment}` from the latest base backup?"
        ),
    )?;

    let mut restart = false;
    if matches!(context.config.runtime, RuntimeConfig::Compose { .. }) {
        let runtime = compose_runtime(&context, &environment, &BTreeMap::new()).await?;
        restart = runtime.status().await?.running;
        if restart {
            if output.json() {
                runtime.stop_quiet().await?;
            } else {
                runtime.stop().await?;
            }
        }
    }
    let provider = provider(&credentials)?;
    let mut manager = EnvironmentManager::new(
        &context.config,
        &context.repository,
        &provider,
        &credentials,
        state_store(&context.config, &context.paths),
        job_store(&context.config, &context.paths),
        output,
    )
    .with_cancellation(cancellation.clone());
    let mutation = manager.pull(&environment, &options).await?;
    if restart {
        let state = state_store(&context.config, &context.paths).load(context.config.project.id)?;
        let variables = environment_variables(
            &context.config,
            state_for_environment(&state, &environment)?,
            &credentials,
        )?;
        let compose = compose_runtime(&context, &environment, &variables).await?;
        if output.json() {
            compose.start_quiet(&variables).await?;
        } else {
            compose.start(&variables, true).await?;
        }
    }
    let reason =
        Some("the previous environment branch is deleted during the forward-only swap".to_owned());
    if output.json() {
        output.stream_mutation(&mutation, None, reason)
    } else {
        output.mutation(
            "pull",
            &mutation,
            &format!("refreshed data for `{environment}`"),
            None,
            reason,
        )
    }
}

async fn compose_runtime(
    context: &ProjectContext,
    environment: &str,
    variables: &BTreeMap<String, String>,
) -> Result<ComposeRuntime> {
    let RuntimeConfig::Compose { compose } = &context.config.runtime else {
        return Err(RepoboxError::new(
            ErrorKind::Usage,
            "compose_runtime_required",
            "this command requires the Compose runtime",
        ));
    };
    let mut global_environment =
        compose_runtime_variables(available_runtime_variables(context, environment))?;
    global_environment.extend(variables.clone());
    let detection = detect_configuration(
        &context.repository,
        &compose.files,
        &compose.profiles,
        &global_environment,
    )
    .await?;
    let remote_services = context
        .config
        .services
        .values()
        .map(|service| service.local.compose_service.clone())
        .collect::<BTreeSet<_>>();
    let mappings = detection
        .services
        .iter()
        .filter(|service| !remote_services.contains(&service.name))
        .map(|service| {
            (
                service.name.clone(),
                global_environment
                    .keys()
                    .map(|key| (key.clone(), key.clone()))
                    .collect(),
            )
        })
        .collect();
    let branch =
        repobox_core::identity::provider_branch_name(context.config.project.id, environment)?;
    Ok(ComposeRuntime::new(
        &context.repository,
        compose.files.clone(),
        compose.profiles.clone(),
        format!("repobox-{}", branch.trim_start_matches("rbx-")),
        remote_services,
        mappings,
        global_environment,
    ))
}

fn available_runtime_variables(
    context: &ProjectContext,
    environment: &str,
) -> Result<BTreeMap<String, String>> {
    let credentials = credential_store(&context.paths);
    let state = state_store(&context.config, &context.paths).load(context.config.project.id)?;
    let Some(record) = state.environments.get(environment) else {
        return Ok(BTreeMap::new());
    };
    stored_environment_variables(&context.config, record, &credentials)
}

fn compose_runtime_variables(
    variables: Result<BTreeMap<String, String>>,
) -> Result<BTreeMap<String, String>> {
    match variables {
        Ok(variables) => Ok(variables),
        // Stop and status must remain usable when a desktop keyring is
        // temporarily unavailable. Log streaming performs a strict lookup
        // first so exact known values can still be added to its redactor.
        Err(error) if error.code == "credential_read_failed" => Ok(BTreeMap::new()),
        Err(error) => Err(error),
    }
}

fn runtime_redactor(
    variables: &BTreeMap<String, String>,
) -> repobox_core::redaction::SecretRedactor {
    let mut redactor = repobox_core::redaction::SecretRedactor::default();
    for value in variables.values() {
        redactor.add(value);
    }
    redactor
}

fn credential_store(paths: &RepoboxPaths) -> CredentialStore {
    CredentialStore::new(paths.credentials_file())
}

fn provider(store: &CredentialStore) -> Result<PlanetScaleClient> {
    let (credentials, _) = store.provider_credentials()?;
    PlanetScaleClient::new(credentials)
}

fn planning_provider() -> Result<PlanetScaleClient> {
    PlanetScaleClient::new(PlanetScaleCredentials::service_token("", ""))
}

fn confirm(cli: &Cli, message: &str) -> Result<()> {
    if cli.dry_run || cli.yes {
        return Ok(());
    }
    if cli.no_input || !io::stdin().is_terminal() {
        return Err(
            RepoboxError::new(ErrorKind::Usage, "confirmation_required", message)
                .with_suggestion("Review with `--dry-run --json`, then rerun with `--yes`."),
        );
    }
    eprint!("{message} [y/N] ");
    io::stderr().flush()?;
    let mut response = String::new();
    io::stdin().read_line(&mut response)?;
    if matches!(response.trim().to_ascii_lowercase().as_str(), "y" | "yes") {
        Ok(())
    } else {
        Err(RepoboxError::new(
            ErrorKind::Usage,
            "operation_canceled",
            "operation was not approved",
        ))
    }
}

fn reject_symbolic_job_mutation(id: &str) -> Result<()> {
    if id == "latest" {
        return Err(RepoboxError::new(
            ErrorKind::Usage,
            "exact_job_id_required",
            "mutating job commands require an exact UUID; `latest` is read-only",
        )
        .with_suggestion("Run `repobox job view latest --json`, then pass its exact job ID."));
    }
    Ok(())
}

fn resolve_job(
    store: &repobox_core::jobs::JobStore,
    id: &str,
) -> Result<repobox_core::jobs::JobRecord> {
    if id == "latest" {
        store.latest()
    } else {
        let id = Uuid::parse_str(id).map_err(|error| {
            RepoboxError::new(
                ErrorKind::Usage,
                "invalid_job_id",
                format!("`{id}` is not a job UUID: {error}"),
            )
        })?;
        store.get(id)
    }
}

fn parse_compose_log(line: &str) -> (String, String) {
    line.split_once(" | ").map_or_else(
        || ("service".to_owned(), line.to_owned()),
        |(service, line)| (service.trim().to_owned(), line.to_owned()),
    )
}

fn authentication_input_required() -> RepoboxError {
    RepoboxError::new(
        ErrorKind::Authentication,
        "authentication_input_required",
        "token ID and token are required in non-interactive mode",
    )
    .with_suggestion(
        "Set PLANETSCALE_SERVICE_TOKEN_ID and PLANETSCALE_SERVICE_TOKEN, then rerun the command.",
    )
}

fn open_browser(url: &str) -> bool {
    if let Some(browser) =
        std::env::var_os("REPOBOX_BROWSER").or_else(|| std::env::var_os("BROWSER"))
    {
        return std::process::Command::new(browser)
            .arg(url)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .is_ok();
    }
    #[cfg(target_os = "macos")]
    let command = ("open", vec![url]);
    #[cfg(not(target_os = "macos"))]
    let command = ("xdg-open", vec![url]);
    std::process::Command::new(command.0)
        .args(command.1)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .is_ok()
}

fn read_telemetry(path: &Path) -> Result<bool> {
    match fs::read_to_string(path) {
        Ok(contents) => {
            let value: serde_yaml_ng::Value =
                serde_yaml_ng::from_str(&contents).map_err(|error| {
                    RepoboxError::new(ErrorKind::Runtime, "user_config_invalid", error.to_string())
                })?;
            Ok(value
                .get("telemetry")
                .and_then(|value| value.get("enabled"))
                .and_then(serde_yaml_ng::Value::as_bool)
                .unwrap_or(false))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    #[cfg(target_os = "linux")]
    use std::process::Stdio;
    #[cfg(target_os = "linux")]
    use std::time::Duration;

    use repobox_core::config::{NativeConfig, RepoboxConfig, RuntimeConfig};
    use repobox_core::{ErrorKind, RepoboxError};
    #[cfg(target_os = "linux")]
    use tokio::process::Command as TokioCommand;

    use super::{
        AuthPending, OperationCancellation, RuntimeChildControl, agent_runtime_guidance,
        compose_runtime_variables, database_connection_guidance, finish_compose_shutdown,
        native_child_control, native_inherits_stdin, native_runtime_cleanup_io_error,
    };
    #[cfg(target_os = "linux")]
    use super::{configure_native_process_group, wait_native_runtime_child};

    #[test]
    fn auth_pending_contains_only_public_handoff_values() {
        let value = serde_json::to_value(AuthPending {
            status: "pending",
            method: "browser_oauth",
            verification_url: "https://example.test/device",
            user_code: "ABCD-EFGH",
            browser_opened: false,
            expires_in_seconds: 300,
        })
        .unwrap();
        let mut keys = value
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>();
        keys.sort_unstable();
        assert_eq!(
            keys,
            [
                "browser_opened",
                "expires_in_seconds",
                "method",
                "status",
                "user_code",
                "verification_url",
            ]
        );
    }

    #[test]
    fn compose_recovery_only_suppresses_keyring_read_failures() {
        let keyring_error = RepoboxError::new(
            ErrorKind::Runtime,
            "credential_read_failed",
            "keyring unavailable",
        );
        assert_eq!(
            compose_runtime_variables(Err(keyring_error)).unwrap(),
            BTreeMap::new()
        );

        let decode_error = RepoboxError::new(
            ErrorKind::Runtime,
            "credential_decode_failed",
            "malformed credential",
        );
        assert_eq!(
            compose_runtime_variables(Err(decode_error))
                .unwrap_err()
                .code,
            "credential_decode_failed"
        );
    }

    #[test]
    fn interactive_native_human_output_stays_in_foreground_process_group() {
        assert_eq!(
            native_child_control(true, false),
            RuntimeChildControl::NativeForeground
        );
        assert_eq!(
            native_child_control(true, true),
            RuntimeChildControl::NativeIsolated
        );
        assert_eq!(
            native_child_control(false, false),
            RuntimeChildControl::NativeIsolated
        );
        assert!(native_inherits_stdin(true, false));
        assert!(!native_inherits_stdin(false, false));
        assert!(!native_inherits_stdin(true, true));
    }

    #[test]
    fn child_control_graceful_signal_capability_is_platform_accurate() {
        assert!(!RuntimeChildControl::Immediate.supports_graceful_interrupt());
        assert_eq!(
            RuntimeChildControl::NativeForeground.supports_graceful_interrupt(),
            cfg!(unix)
        );
        assert_eq!(
            RuntimeChildControl::NativeIsolated.supports_graceful_interrupt(),
            cfg!(unix)
        );
    }

    #[test]
    fn native_interruption_io_failure_preserves_cleanup_diagnostic() {
        let error = native_runtime_cleanup_io_error(
            RuntimeChildControl::NativeIsolated,
            "reap forced child",
            std::io::Error::other("waitpid failed"),
        );

        assert_eq!(error.code, "native_runtime_cleanup_incomplete");
        assert!(error.message.contains("reap forced child"));
        assert!(error.message.contains("waitpid failed"));
        assert!(error.message.contains("cleanup could not be confirmed"));
    }

    #[test]
    fn agent_runtime_guidance_uses_detach_only_for_compose() {
        let compose = RepoboxConfig::new_compose("compose", vec![PathBuf::from("compose.yml")]);
        let (_, detach_supported, command) = agent_runtime_guidance(Some(&compose));
        assert!(detach_supported);
        assert!(command.contains("--detach"));

        let mut native = compose;
        native.runtime = RuntimeConfig::Native {
            native: NativeConfig {
                command: vec!["cargo".to_owned(), "run".to_owned()],
                interactive: true,
                working_directory: PathBuf::from("."),
            },
        };
        let (driver, detach_supported, command) = agent_runtime_guidance(Some(&native));
        assert_eq!(driver, "native");
        assert!(!detach_supported);
        assert!(command.contains("--no-tui"));
        assert!(!command.contains("--detach"));
    }

    #[test]
    fn database_connection_guidance_is_secure_and_actionable() {
        let guidance = database_connection_guidance();
        assert_eq!(guidance["url_profile"], "libpq-16");
        assert_eq!(guidance["tls_mode"], "verify-full");
        assert_eq!(guidance["trust"], "system");
        assert_eq!(
            guidance["known_adapters"]["asyncpg"],
            "pass ssl=True when constructing the connection or pool"
        );
        assert_eq!(guidance["help_command"], "repobox help connections --json");
        assert!(!guidance.to_string().contains("password"));
    }

    #[test]
    fn interrupted_compose_stop_failure_reports_residual_services() {
        let cancellation = OperationCancellation::default();
        cancellation.cancel();
        let error = finish_compose_shutdown(
            Ok(()),
            Err(RepoboxError::new(
                ErrorKind::Runtime,
                "compose_stop_failed",
                "Docker daemon stopped responding",
            )
            .with_request_id("stop-request")),
            &cancellation,
            "feature-branch",
        )
        .unwrap_err();

        assert_eq!(error.code, "operation_interrupted_cleanup_incomplete");
        assert!(error.message.contains("feature-branch"));
        assert!(
            error
                .message
                .contains("Compose services may remain running")
        );
        assert!(error.message.contains("compose_stop_failed"));
        assert!(error.message.contains("Docker daemon stopped responding"));
        assert_eq!(error.request_id.as_deref(), Some("stop-request"));
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn interactive_native_child_inherits_callers_process_group() {
        let control = native_child_control(true, false);
        let mut command = TokioCommand::new("sleep");
        command
            .arg("30")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        configure_native_process_group(&mut command, control);
        let mut child = command.spawn().unwrap();
        let child_pid = nix::unistd::Pid::from_raw(i32::try_from(child.id().unwrap()).unwrap());

        assert_eq!(
            nix::unistd::getpgid(Some(child_pid)).unwrap(),
            nix::unistd::getpgrp(),
            "interactive native child must share the caller's foreground-capable process group"
        );

        child.kill().await.unwrap();
        child.wait().await.unwrap();
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn foreground_native_sigint_exit_is_a_clean_interruption() {
        let control = RuntimeChildControl::NativeForeground;
        let mut command = TokioCommand::new("sh");
        command
            .args(["-c", "kill -INT $$"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        configure_native_process_group(&mut command, control);
        let child = command.spawn().unwrap();
        let cancellation = OperationCancellation::default();

        let (status, interrupted) = tokio::time::timeout(Duration::from_secs(2), async move {
            wait_native_runtime_child(child, &cancellation, control).await
        })
        .await
        .expect("SIGINT-exited child should be reaped promptly")
        .unwrap();

        assert!(!status.success());
        assert!(interrupted);
    }

    #[cfg(target_os = "linux")]
    async fn assert_native_interrupt_cleans_detached_descendant(control: RuntimeChildControl) {
        let temp = tempfile::tempdir().unwrap();
        let pid_path = temp.path().join("detached.pid");
        let mut command = TokioCommand::new("sh");
        command
            .args([
                "-c",
                "detached=; \
                 cleanup() { kill \"$detached\" 2>/dev/null || true; \
                 wait \"$detached\" 2>/dev/null || true; exit 0; }; \
                 trap cleanup INT TERM; \
                 setsid sleep 30 & detached=$!; \
                 printf '%s' \"$detached\" > \"$1\"; \
                 wait \"$detached\"",
                "sh",
            ])
            .arg(&pid_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        configure_native_process_group(&mut command, control);
        let child = command.spawn().unwrap();
        let cancellation = OperationCancellation::default();
        let operation_cancellation = cancellation.clone();
        let wait = tokio::spawn(async move {
            wait_native_runtime_child(child, &operation_cancellation, control).await
        });
        for _ in 0..100 {
            if pid_path.is_file() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let detached_pid = std::fs::read_to_string(&pid_path)
            .unwrap()
            .parse::<i32>()
            .unwrap();

        cancellation.cancel();
        let (_, interrupted) = tokio::time::timeout(Duration::from_secs(2), wait)
            .await
            .expect("native cleanup should finish within its grace period")
            .unwrap()
            .unwrap();

        assert!(interrupted);
        assert_eq!(
            nix::sys::signal::kill(nix::unistd::Pid::from_raw(detached_pid), None),
            Err(nix::errno::Errno::ESRCH),
            "native wrapper left its detached descendant running"
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn isolated_native_interrupt_allows_parent_to_clean_detached_descendant() {
        assert_native_interrupt_cleans_detached_descendant(RuntimeChildControl::NativeIsolated)
            .await;
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn foreground_native_interrupt_allows_parent_to_clean_detached_descendant() {
        assert_native_interrupt_cleans_detached_descendant(RuntimeChildControl::NativeForeground)
            .await;
    }
}
