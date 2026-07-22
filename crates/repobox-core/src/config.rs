use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{ErrorKind, RepoboxError, Result};

pub const CONFIG_FILE_NAME: &str = ".repobox.yml";
pub const CONFIG_VERSION: u32 = 1;

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RepoboxConfig {
    pub version: u32,
    pub project: ProjectConfig,
    pub runtime: RuntimeConfig,
    #[serde(default)]
    pub services: BTreeMap<String, ServiceConfig>,
    #[serde(default)]
    pub data: DataConfig,
    #[serde(default)]
    pub agents: AgentConfig,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectConfig {
    pub id: Uuid,
    pub name: String,
    #[serde(default)]
    pub git: GitConfig,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GitConfig {
    #[serde(default = "default_base_branch")]
    pub base_branch: String,
}

impl Default for GitConfig {
    fn default() -> Self {
        Self {
            base_branch: default_base_branch(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "driver", rename_all = "snake_case")]
pub enum RuntimeConfig {
    Compose { compose: ComposeConfig },
    Native { native: NativeConfig },
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ComposeConfig {
    #[serde(default = "default_compose_files")]
    pub files: Vec<PathBuf>,
    #[serde(default)]
    pub profiles: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NativeConfig {
    pub command: Vec<String>,
    #[serde(default)]
    pub interactive: bool,
    #[serde(default = "default_working_directory")]
    pub working_directory: PathBuf,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceConfig {
    pub kind: ServiceKind,
    #[serde(default)]
    pub primary: bool,
    pub local: LocalServiceConfig,
    pub remote: RemoteServiceConfig,
    #[serde(default)]
    pub bootstrap: BootstrapConfig,
    pub env: DatabaseEnvConfig,
}

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceKind {
    Postgres,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LocalServiceConfig {
    pub compose_service: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "provider", rename_all = "snake_case")]
pub enum RemoteServiceConfig {
    Planetscale {
        organization: String,
        database: String,
        #[serde(default = "default_base_branch")]
        base_branch: String,
        #[serde(default = "default_cluster_size")]
        cluster_size: String,
    },
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BootstrapConfig {
    #[serde(default)]
    pub mode: BootstrapMode,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BootstrapMode {
    #[default]
    Attach,
    Empty,
    Import,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DatabaseEnvConfig {
    pub pooled: String,
    pub direct: String,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DataConfig {
    #[serde(default)]
    pub allow_copy: bool,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    #[serde(default = "default_true")]
    pub claude: bool,
    #[serde(default = "default_true")]
    pub codex: bool,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            claude: true,
            codex: true,
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_base_branch() -> String {
    "main".to_owned()
}

fn default_cluster_size() -> String {
    "auto-smallest".to_owned()
}

fn default_compose_files() -> Vec<PathBuf> {
    vec![PathBuf::from("compose.yaml")]
}

fn default_working_directory() -> PathBuf {
    PathBuf::from(".")
}

impl RepoboxConfig {
    pub fn new_compose(name: impl Into<String>, files: Vec<PathBuf>) -> Self {
        Self {
            version: CONFIG_VERSION,
            project: ProjectConfig {
                id: Uuid::new_v4(),
                name: name.into(),
                git: GitConfig::default(),
            },
            runtime: RuntimeConfig::Compose {
                compose: ComposeConfig {
                    files,
                    profiles: vec![],
                },
            },
            services: BTreeMap::new(),
            data: DataConfig::default(),
            agents: AgentConfig::default(),
        }
    }

    pub fn load(path: &Path) -> Result<Self> {
        let raw = fs::read_to_string(path).map_err(|error| {
            RepoboxError::new(
                if error.kind() == std::io::ErrorKind::NotFound {
                    ErrorKind::NotFound
                } else {
                    ErrorKind::Runtime
                },
                "config_read_failed",
                format!("could not read {}: {error}", path.display()),
            )
            .with_suggestion("Run `repobox init` to create a project configuration.")
        })?;
        Self::from_yaml(&raw)
    }

    pub fn from_yaml(raw: &str) -> Result<Self> {
        let config: Self = serde_yaml_ng::from_str(raw).map_err(|error| {
            RepoboxError::new(
                ErrorKind::Usage,
                "config_parse_failed",
                format!("invalid Repobox configuration: {error}"),
            )
            .with_suggestion("Run `repobox config validate .repobox.yml` for details.")
        })?;
        config.validate()?;
        Ok(config)
    }

    pub fn to_yaml(&self) -> Result<String> {
        serde_yaml_ng::to_string(self).map_err(|error| {
            RepoboxError::new(
                ErrorKind::Runtime,
                "config_serialize_failed",
                error.to_string(),
            )
        })
    }

    pub fn validate(&self) -> Result<()> {
        if self.version != CONFIG_VERSION {
            return Err(RepoboxError::new(
                ErrorKind::Usage,
                "unsupported_config_version",
                format!(
                    "configuration version must be {CONFIG_VERSION} (got: {})",
                    self.version
                ),
            ));
        }
        if self.project.id.is_nil() {
            return Err(RepoboxError::new(
                ErrorKind::Usage,
                "invalid_project_id",
                "project.id must be a non-nil UUID",
            ));
        }
        if self.project.name.trim().is_empty() {
            return Err(RepoboxError::new(
                ErrorKind::Usage,
                "invalid_project_name",
                "project.name cannot be empty",
            ));
        }
        match &self.runtime {
            RuntimeConfig::Compose { compose } if compose.files.is_empty() => {
                return Err(RepoboxError::new(
                    ErrorKind::Usage,
                    "missing_compose_file",
                    "runtime.compose.files must contain at least one path",
                ));
            }
            RuntimeConfig::Native { native } if native.command.is_empty() => {
                return Err(RepoboxError::new(
                    ErrorKind::Usage,
                    "missing_native_command",
                    "runtime.native.command must contain at least one argv item",
                ));
            }
            RuntimeConfig::Compose { .. } | RuntimeConfig::Native { .. } => {}
        }

        let primary_count = self
            .services
            .values()
            .filter(|service| service.primary)
            .count();
        if primary_count > 1 {
            return Err(RepoboxError::new(
                ErrorKind::Usage,
                "multiple_primary_databases",
                "at most one Postgres service may set primary: true",
            ));
        }

        let mut env_names = BTreeSet::new();
        let mut compose_services = BTreeSet::new();
        for (name, service) in &self.services {
            validate_name("service", name)?;
            if !compose_services.insert(service.local.compose_service.as_str()) {
                return Err(RepoboxError::new(
                    ErrorKind::Usage,
                    "duplicate_compose_service",
                    format!(
                        "Compose service `{}` is mapped more than once",
                        service.local.compose_service
                    ),
                ));
            }
            for env_name in [&service.env.pooled, &service.env.direct] {
                validate_env_name(env_name)?;
                if !env_names.insert(env_name.as_str()) {
                    return Err(RepoboxError::new(
                        ErrorKind::Usage,
                        "duplicate_environment_variable",
                        format!("environment variable `{env_name}` is assigned more than once"),
                    ));
                }
            }
        }
        Ok(())
    }

    pub fn apply_merge_patch(&self, patch: &Value) -> Result<Self> {
        let mut value = serde_json::to_value(self).map_err(|error| {
            RepoboxError::new(
                ErrorKind::Runtime,
                "config_encode_failed",
                error.to_string(),
            )
        })?;
        json_patch::merge(&mut value, patch);
        let updated: Self = serde_json::from_value(value).map_err(|error| {
            RepoboxError::new(
                ErrorKind::Usage,
                "config_patch_failed",
                format!("patched configuration is invalid: {error}"),
            )
        })?;
        updated.validate()?;
        Ok(updated)
    }

    pub fn json_schema() -> Value {
        serde_json::to_value(schema_for!(Self)).expect("RepoboxConfig schema is serializable")
    }
}

pub fn discover(start: &Path) -> Option<PathBuf> {
    let mut current = start;
    loop {
        let candidate = current.join(CONFIG_FILE_NAME);
        if candidate.is_file() {
            return Some(candidate);
        }
        current = current.parent()?;
    }
}

fn validate_name(kind: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || !value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-'))
    {
        return Err(RepoboxError::new(
            ErrorKind::Usage,
            format!("invalid_{kind}_name"),
            format!("{kind} name `{value}` must contain only letters, numbers, `_`, or `-`"),
        ));
    }
    Ok(())
}

fn validate_env_name(value: &str) -> Result<()> {
    let mut characters = value.chars();
    let Some(first) = characters.next() else {
        return Err(RepoboxError::new(
            ErrorKind::Usage,
            "invalid_environment_variable",
            "environment variable names cannot be empty",
        ));
    };
    if !(first.is_ascii_alphabetic() || first == '_')
        || !characters.all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        return Err(RepoboxError::new(
            ErrorKind::Usage,
            "invalid_environment_variable",
            format!("`{value}` is not a valid environment variable name"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> RepoboxConfig {
        let mut config = RepoboxConfig::new_compose("demo", vec![PathBuf::from("compose.yaml")]);
        config.services.insert(
            "db".to_owned(),
            ServiceConfig {
                kind: ServiceKind::Postgres,
                primary: true,
                local: LocalServiceConfig {
                    compose_service: "postgres".to_owned(),
                },
                remote: RemoteServiceConfig::Planetscale {
                    organization: "acme".to_owned(),
                    database: "demo".to_owned(),
                    base_branch: "main".to_owned(),
                    cluster_size: "auto-smallest".to_owned(),
                },
                bootstrap: BootstrapConfig::default(),
                env: DatabaseEnvConfig {
                    pooled: "DATABASE_URL".to_owned(),
                    direct: "DIRECT_DATABASE_URL".to_owned(),
                },
            },
        );
        config
    }

    #[test]
    fn round_trips_yaml() {
        let config = sample();
        let yaml = config.to_yaml().unwrap();
        assert_eq!(RepoboxConfig::from_yaml(&yaml).unwrap(), config);
    }

    #[test]
    fn merge_patch_is_validated() {
        let config = sample();
        let patch = serde_json::json!({"project": {"name": "renamed"}});
        let updated = config.apply_merge_patch(&patch).unwrap();
        assert_eq!(updated.project.name, "renamed");
    }

    #[test]
    fn duplicate_env_names_fail() {
        let mut config = sample();
        config.services.get_mut("db").unwrap().env.direct = "DATABASE_URL".to_owned();
        assert_eq!(
            config.validate().unwrap_err().code,
            "duplicate_environment_variable"
        );
    }
}
