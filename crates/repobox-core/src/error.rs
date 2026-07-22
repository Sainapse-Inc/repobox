use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type Result<T> = std::result::Result<T, RepoboxError>;

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    Runtime,
    Usage,
    NotFound,
    Authentication,
    Conflict,
    Permission,
}

impl ErrorKind {
    pub const fn exit_code(self) -> u8 {
        match self {
            Self::Runtime => 1,
            Self::Usage => 2,
            Self::NotFound => 3,
            Self::Authentication => 4,
            Self::Conflict => 5,
            Self::Permission => 6,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Error, JsonSchema, Serialize)]
#[error("{message}")]
pub struct RepoboxError {
    pub kind: ErrorKind,
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggestion: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc_url: Option<String>,
}

impl RepoboxError {
    pub fn new(kind: ErrorKind, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kind,
            code: code.into(),
            message: message.into(),
            suggestion: Some("Run `repobox help agents` for recovery guidance.".to_owned()),
            request_id: None,
            doc_url: None,
        }
    }

    pub fn with_suggestion(mut self, suggestion: impl Into<String>) -> Self {
        self.suggestion = Some(suggestion.into());
        self
    }

    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }

    pub fn with_doc_url(mut self, doc_url: impl Into<String>) -> Self {
        self.doc_url = Some(doc_url.into());
        self
    }

    pub const fn exit_code(&self) -> u8 {
        self.kind.exit_code()
    }
}

impl From<std::io::Error> for RepoboxError {
    fn from(error: std::io::Error) -> Self {
        Self::new(ErrorKind::Runtime, "io_error", error.to_string())
    }
}
