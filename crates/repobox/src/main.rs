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

use std::process::ExitCode;

use clap::{CommandFactory, Parser};
use cli::Cli;
use output::Output;
use repobox_core::{ErrorKind, RepoboxError};

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
    let checkpoint_on_interrupt = matches!(
        cli.command.as_ref(),
        Some(
            cli::Command::Pull(_)
                | cli::Command::Env(
                    cli::EnvCommand::Create(_)
                        | cli::EnvCommand::Delete(_)
                        | cli::EnvCommand::Prune(_)
                )
                | cli::Command::Job(cli::JobCommand::Resume(_) | cli::JobCommand::Cancel(_))
        )
    );
    let authentication_on_interrupt = matches!(
        cli.command.as_ref(),
        Some(cli::Command::Auth(cli::AuthCommand::Login(_)))
    );
    let result = if checkpoint_on_interrupt {
        tokio::select! {
            result = app::run(cli, &output) => result,
            () = termination_signal() => Err(
                RepoboxError::new(
                    ErrorKind::Runtime,
                    "operation_interrupted",
                    "the operation was interrupted after its latest durable checkpoint",
                )
                .with_suggestion(
                    "Run `repobox job view latest --json`, then resume the exact job UUID.",
                ),
            ),
        }
    } else if authentication_on_interrupt {
        tokio::select! {
            result = app::run(cli, &output) => result,
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
        app::run(cli, &output).await
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            output.print_error(&error);
            ExitCode::from(error.exit_code())
        }
    }
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
