use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::RepoboxError;

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct SuccessEnvelope<T> {
    pub schema_version: u32,
    pub command: String,
    pub data: T,
}

impl<T> SuccessEnvelope<T> {
    pub fn new(command: impl Into<String>, data: T) -> Self {
        Self {
            schema_version: 1,
            command: command.into(),
            data,
        }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct ErrorEnvelope {
    pub schema_version: u32,
    pub error: RepoboxError,
}

impl From<RepoboxError> for ErrorEnvelope {
    fn from(error: RepoboxError) -> Self {
        Self {
            schema_version: 1,
            error,
        }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct StreamEvent {
    pub schema_version: u32,
    pub sequence: u64,
    pub timestamp: DateTime<Utc>,
    pub event: String,
    pub data: Value,
}

impl StreamEvent {
    pub fn new(sequence: u64, event: impl Into<String>, data: Value) -> Self {
        Self {
            schema_version: 1,
            sequence,
            timestamp: Utc::now(),
            event: event.into(),
            data,
        }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct MutationReceipt<T> {
    #[serde(flatten)]
    pub resource: T,
    pub undo_command: Option<String>,
    pub undo_reason: Option<String>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct DryRunPlan {
    pub operation: String,
    pub environment: String,
    pub provider_calls: Vec<PlannedCall>,
    pub warnings: Vec<String>,
    pub estimated_cost: Option<String>,
    pub rollback_available: bool,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct PlannedCall {
    pub provider: String,
    pub action: String,
    pub resource: String,
}
