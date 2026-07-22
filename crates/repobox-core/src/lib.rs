//! Core domain types and durable contracts for Repobox.

pub mod config;
pub mod error;
pub mod identity;
pub mod jobs;
pub mod output;
pub mod paths;
pub mod provider;
pub mod redaction;
pub mod runtime;
pub mod state;

pub use error::{ErrorKind, RepoboxError, Result};
