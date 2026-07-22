use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::Result;

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
pub struct RuntimeDetection {
    pub driver: String,
    pub files: Vec<PathBuf>,
    pub services: Vec<DetectedService>,
    pub warnings: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct DetectedService {
    pub name: String,
    pub image: Option<String>,
    pub kind: DetectedServiceKind,
    pub dependencies: Vec<String>,
    pub environment: BTreeMap<String, String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DetectedServiceKind {
    Postgres,
    Other,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct RuntimeServiceStatus {
    pub name: String,
    pub state: String,
    pub health: Option<String>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct RuntimeStatus {
    pub running: bool,
    pub services: Vec<RuntimeServiceStatus>,
}

#[async_trait]
pub trait RuntimeDriver: Send + Sync {
    fn name(&self) -> &'static str;
    async fn detect(&self, repository: &Path) -> Result<RuntimeDetection>;
    async fn status(&self) -> Result<RuntimeStatus>;
    async fn start(&self, environment: &BTreeMap<String, String>, detach: bool) -> Result<()>;
    async fn stop(&self) -> Result<()>;
    async fn restart(&self, service: Option<&str>) -> Result<()>;
}
