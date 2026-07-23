mod agent_guides;
mod app;
mod cli;
mod context;
mod credentials;
mod environment;
mod git;
mod initialize;
mod output;
mod tui;

use std::future::Future;
use std::process::ExitCode;
use std::time::Duration;

use clap::{CommandFactory, Parser};
use cli::Cli;
use output::Output;
use repobox_core::{ErrorKind, RepoboxError};

const INTERRUPT_CLEANUP_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy)]
enum InterruptRecovery {
    ResumeExactJob,
    RerunIdempotentMutation,
    InspectCanceledJob,
    Runtime,
}

impl InterruptRecovery {
    const fn accepts_success(self) -> bool {
        matches!(self, Self::Runtime)
    }

    const fn suggestion(self) -> &'static str {
        match self {
            Self::ResumeExactJob => {
                "Run `repobox job view latest --json`, then resume the exact job UUID."
            }
            Self::RerunIdempotentMutation => {
                "Inspect `repobox job view latest --json`, then rerun the same delete or prune command; completed deletions are handled idempotently."
            }
            Self::InspectCanceledJob => {
                "Inspect the exact job with `repobox job view <UUID> --json`; canceled jobs are terminal and must not be resumed."
            }
            Self::Runtime => {
                "Inspect the runtime and listening ports before running Repobox again."
            }
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let raw = std::env::args_os().collect::<Vec<_>>();
    let json_requested = raw.iter().any(|value| value == "--json");
    let version_requested = raw
        .iter()
        .any(|value| value == "--version" || value == "-V");
    if json_requested && version_requested {
        let output = Output::new(true, cli::ColorChoice::Never);
        if let Err(error) = output.data(
            "version",
            &serde_json::json!({"version": env!("CARGO_PKG_VERSION")}),
        ) {
            output.print_error(&error);
            return ExitCode::from(error.exit_code());
        }
        return ExitCode::SUCCESS;
    }
    let cli = match Cli::try_parse_from(raw) {
        Ok(cli) => cli,
        Err(error) => {
            if json_requested {
                let output = Output::new(true, cli::ColorChoice::Never);
                let wrapped =
                    RepoboxError::new(ErrorKind::Usage, "cli_usage_error", error.to_string())
                        .with_suggestion(
                            "Run `repobox help agents` for the machine-oriented contract.",
                        );
                output.print_error(&wrapped);
                return ExitCode::from(wrapped.exit_code());
            }
            let code = u8::try_from(error.exit_code()).unwrap_or(2);
            let _ = error.print();
            return ExitCode::from(code);
        }
    };
    if cli.command.is_none() {
        let mut command = Cli::command();
        let _ = command.print_long_help();
        println!();
        return ExitCode::SUCCESS;
    }
    let output = Output::new(cli.json, cli.color);
    let interrupt_recovery = match cli.command.as_ref() {
        Some(
            cli::Command::Pull(_)
            | cli::Command::Env(cli::EnvCommand::Create(_))
            | cli::Command::Job(cli::JobCommand::Resume(_)),
        ) => Some(InterruptRecovery::ResumeExactJob),
        Some(cli::Command::Env(cli::EnvCommand::Delete(_) | cli::EnvCommand::Prune(_))) => {
            Some(InterruptRecovery::RerunIdempotentMutation)
        }
        Some(cli::Command::Job(cli::JobCommand::Cancel(_))) => {
            Some(InterruptRecovery::InspectCanceledJob)
        }
        Some(cli::Command::Run(_)) => Some(InterruptRecovery::Runtime),
        _ => None,
    };
    let authentication_on_interrupt = matches!(
        cli.command.as_ref(),
        Some(cli::Command::Auth(cli::AuthCommand::Login(_)))
    );
    let cancellation = environment::OperationCancellation::default();
    let result = if let Some(recovery) = interrupt_recovery {
        run_with_graceful_interruption(
            app::run(cli, &output, &cancellation),
            termination_signal(),
            cancellation.clone(),
            INTERRUPT_CLEANUP_TIMEOUT,
            recovery,
        )
        .await
    } else if authentication_on_interrupt {
        tokio::select! {
            result = app::run(cli, &output, &cancellation) => result,
            () = termination_signal() => Err(
                RepoboxError::new(
                    ErrorKind::Authentication,
                    "authentication_interrupted",
                    "PlanetScale authentication was interrupted before completion",
                )
                .with_suggestion("Run `repobox auth login` to request a new confirmation code."),
            ),
        }
    } else {
        app::run(cli, &output, &cancellation).await
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            output.print_error(&error);
            ExitCode::from(error.exit_code())
        }
    }
}

async fn run_with_graceful_interruption<Operation, Signal>(
    operation: Operation,
    signal: Signal,
    cancellation: environment::OperationCancellation,
    cleanup_timeout: Duration,
    recovery: InterruptRecovery,
) -> repobox_core::Result<()>
where
    Operation: Future<Output = repobox_core::Result<()>>,
    Signal: Future<Output = ()>,
{
    tokio::pin!(operation);
    tokio::pin!(signal);
    tokio::select! {
        biased;
        result = &mut operation => result,
        () = &mut signal => {
            cancellation.cancel();
            match tokio::time::timeout(cleanup_timeout, &mut operation).await {
                Ok(Ok(())) if recovery.accepts_success() => Ok(()),
                Ok(Ok(())) => Err(interrupted_after_cleanup(recovery)),
                Ok(Err(error))
                    if matches!(
                        error.code.as_str(),
                        "operation_interrupted"
                            | "operation_interrupted_cleanup_incomplete"
                            | "operation_cleanup_failed"
                            | "native_runtime_cleanup_incomplete"
                    ) =>
                {
                    Err(with_interrupt_recovery(error, recovery))
                }
                Ok(Err(error)) if matches!(recovery, InterruptRecovery::Runtime) => Err(error),
                Ok(Err(error)) => Err(interrupted_after_cleanup(recovery).with_suggestion(
                    format!("{} Cleanup completed. {}", error.message, recovery.suggestion()),
                )),
                Err(_) => Err(
                    RepoboxError::new(
                        ErrorKind::Runtime,
                        "operation_interrupted_cleanup_incomplete",
                        format!(
                            "cleanup did not finish within {} seconds; a temporary PlanetScale role, managed child process, or Compose source service may remain",
                            cleanup_timeout.as_secs()
                        ),
                    )
                    .with_suggestion(format!(
                        "Inspect the durable job, PlanetScale roles, managed Docker containers, and the configured Compose source service. {}",
                        recovery.suggestion()
                    )),
                ),
            }
        }
    }
}

fn interrupted_after_cleanup(recovery: InterruptRecovery) -> RepoboxError {
    RepoboxError::new(
        ErrorKind::Runtime,
        "operation_interrupted",
        "the operation was interrupted after its latest durable checkpoint; cleanup completed",
    )
    .with_suggestion(recovery.suggestion())
}

fn with_interrupt_recovery(mut error: RepoboxError, recovery: InterruptRecovery) -> RepoboxError {
    if error.code == "operation_interrupted" {
        error.suggestion = Some(recovery.suggestion().to_owned());
        return error;
    }
    let recovery = recovery.suggestion();
    error.suggestion = Some(match error.suggestion {
        Some(suggestion) if !suggestion.contains(recovery) => {
            format!("{suggestion} {recovery}")
        }
        Some(suggestion) => suggestion,
        None => recovery.to_owned(),
    });
    error
}

#[cfg(unix)]
async fn termination_signal() {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("SIGTERM handler can be installed");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = terminate.recv() => {}
    }
}

#[cfg(not(unix))]
async fn termination_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn interruption_waits_for_operation_cleanup() {
        let cancellation = environment::OperationCancellation::default();
        let operation_cancellation = cancellation.clone();
        let (cleanup_sender, cleanup_receiver) = tokio::sync::oneshot::channel();
        let operation = async move {
            operation_cancellation.cancelled().await;
            let _ = cleanup_sender.send(());
            Ok(())
        };

        let error = run_with_graceful_interruption(
            operation,
            std::future::ready(()),
            cancellation,
            Duration::from_secs(1),
            InterruptRecovery::ResumeExactJob,
        )
        .await
        .unwrap_err();

        cleanup_receiver
            .await
            .expect("cleanup must finish before interruption returns");
        assert_eq!(error.code, "operation_interrupted");
        assert!(error.message.contains("cleanup completed"));
    }

    #[tokio::test]
    async fn interruption_timeout_reports_possible_residual_state() {
        let cancellation = environment::OperationCancellation::default();
        let operation_cancellation = cancellation.clone();
        let operation = async move {
            operation_cancellation.cancelled().await;
            std::future::pending::<repobox_core::Result<()>>().await
        };

        let error = run_with_graceful_interruption(
            operation,
            std::future::ready(()),
            cancellation,
            Duration::from_millis(10),
            InterruptRecovery::RerunIdempotentMutation,
        )
        .await
        .unwrap_err();

        assert_eq!(error.code, "operation_interrupted_cleanup_incomplete");
        assert!(error.message.contains("PlanetScale role"));
        assert!(error.message.contains("Compose source service"));
    }

    #[tokio::test]
    async fn runtime_success_after_interrupt_remains_a_clean_exit() {
        let cancellation = environment::OperationCancellation::default();
        let operation_cancellation = cancellation.clone();
        let operation = async move {
            operation_cancellation.cancelled().await;
            Ok(())
        };

        let result = run_with_graceful_interruption(
            operation,
            std::future::ready(()),
            cancellation,
            Duration::from_secs(1),
            InterruptRecovery::Runtime,
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn delete_interruption_recommends_rerun_instead_of_resume() {
        let cancellation = environment::OperationCancellation::default();
        let operation_cancellation = cancellation.clone();
        let operation = async move {
            operation_cancellation.cancelled().await;
            Err(RepoboxError::new(
                ErrorKind::Runtime,
                "operation_interrupted",
                "delete stopped at a service boundary",
            )
            .with_suggestion("resume the exact job UUID"))
        };

        let error = run_with_graceful_interruption(
            operation,
            std::future::ready(()),
            cancellation,
            Duration::from_secs(1),
            InterruptRecovery::RerunIdempotentMutation,
        )
        .await
        .unwrap_err();

        let suggestion = error.suggestion.unwrap();
        assert!(suggestion.contains("rerun the same delete or prune command"));
        assert!(!suggestion.contains("resume the exact job UUID"));
    }
}
