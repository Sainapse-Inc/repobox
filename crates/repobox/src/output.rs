use std::io::{self, IsTerminal, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::Utc;
use repobox_core::output::{ErrorEnvelope, MutationReceipt, StreamEvent, SuccessEnvelope};
use repobox_core::{RepoboxError, Result};
use serde::Serialize;

use crate::cli::ColorChoice;

#[derive(Clone, Debug)]
pub struct Output {
    json: bool,
    color: bool,
    sequence: Arc<AtomicU64>,
}

impl Output {
    pub fn new(json: bool, choice: ColorChoice) -> Self {
        let terminal_color = io::stderr().is_terminal() && std::env::var_os("NO_COLOR").is_none();
        let color = match choice {
            ColorChoice::Never => false,
            ColorChoice::Always | ColorChoice::Auto => terminal_color,
        };
        Self {
            json,
            color,
            sequence: Arc::new(AtomicU64::new(0)),
        }
    }

    pub const fn json(&self) -> bool {
        self.json
    }

    pub fn data<T: Serialize>(&self, command: &str, value: &T) -> Result<()> {
        if self.json {
            let envelope = SuccessEnvelope::new(command, value);
            serde_json::to_writer_pretty(io::stdout().lock(), &envelope).map_err(json_error)?;
            println!();
        } else {
            serde_json::to_writer_pretty(io::stdout().lock(), value).map_err(json_error)?;
            println!();
        }
        Ok(())
    }

    pub fn human_or_data<T: Serialize>(&self, command: &str, value: &T, human: &str) -> Result<()> {
        if self.json {
            self.data(command, value)
        } else {
            println!("{human}");
            Ok(())
        }
    }

    pub fn mutation<T: Serialize>(
        &self,
        command: &str,
        value: &T,
        human: &str,
        undo_command: Option<String>,
        undo_reason: Option<String>,
    ) -> Result<()> {
        if self.json {
            self.data(
                command,
                &MutationReceipt {
                    resource: value,
                    undo_command,
                    undo_reason,
                },
            )
        } else {
            println!("{human}");
            Ok(())
        }
    }

    pub fn stream_mutation<T: Serialize>(
        &self,
        value: &T,
        undo_command: Option<String>,
        undo_reason: Option<String>,
    ) -> Result<()> {
        self.stream(
            "result",
            &MutationReceipt {
                resource: value,
                undo_command,
                undo_reason,
            },
        )
    }

    pub fn progress(&self, line: &str) {
        if !self.json {
            let _ = writeln!(io::stderr(), "{}", self.accent(line));
        }
    }

    pub fn stream<T: Serialize>(&self, event: &str, value: &T) -> Result<()> {
        if self.json {
            let sequence = self.sequence.fetch_add(1, Ordering::Relaxed) + 1;
            let event = StreamEvent {
                schema_version: 1,
                sequence,
                timestamp: Utc::now(),
                event: event.to_owned(),
                data: serde_json::to_value(value).map_err(json_error)?,
            };
            serde_json::to_writer(io::stdout().lock(), &event).map_err(json_error)?;
            println!();
            io::stdout().flush()?;
        } else {
            println!("{}", serde_json::to_string(value).map_err(json_error)?);
        }
        Ok(())
    }

    pub fn print_error(&self, error: &RepoboxError) {
        if self.json {
            let _ = serde_json::to_writer(io::stderr().lock(), &ErrorEnvelope::from(error.clone()));
            let _ = writeln!(io::stderr());
            return;
        }
        let _ = writeln!(
            io::stderr(),
            "{} [{}] {}",
            self.danger("error"),
            error.code,
            error.message
        );
        if let Some(suggestion) = &error.suggestion {
            let _ = writeln!(io::stderr(), "{} {suggestion}", self.accent("hint:"));
        }
        if let Some(request_id) = &error.request_id {
            let _ = writeln!(io::stderr(), "request: {request_id}");
        }
    }

    fn accent(&self, value: &str) -> String {
        if self.color {
            format!("\x1b[38;5;81m{value}\x1b[0m")
        } else {
            value.to_owned()
        }
    }

    fn danger(&self, value: &str) -> String {
        if self.color {
            format!("\x1b[38;5;203m{value}\x1b[0m")
        } else {
            value.to_owned()
        }
    }
}

fn json_error(error: serde_json::Error) -> RepoboxError {
    RepoboxError::new(
        repobox_core::ErrorKind::Runtime,
        "output_encode_failed",
        error.to_string(),
    )
}
