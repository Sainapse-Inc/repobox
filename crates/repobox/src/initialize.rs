use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use repobox_core::config::{
    AgentConfig, BootstrapConfig, BootstrapMode, ComposeConfig, DatabaseEnvConfig, GitConfig,
    LocalServiceConfig, NativeConfig, RemoteServiceConfig, RepoboxConfig, RuntimeConfig,
    ServiceConfig, ServiceKind,
};
use repobox_core::runtime::{DetectedServiceKind, RuntimeDetection};
use repobox_core::{ErrorKind, RepoboxError, Result};
use repobox_runtime_compose::detect_repository;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::agent_guides;
use crate::cli::{BootstrapChoice, InitArgs, InitRuntime};
use crate::git;

#[derive(Clone, Debug, Serialize)]
pub struct InitResult {
    pub config_path: PathBuf,
    pub project_id: uuid::Uuid,
    pub project_name: String,
    pub runtime: String,
    pub services: Vec<InitializedService>,
    pub agent_files: Vec<PathBuf>,
    pub dry_run: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct InitializedService {
    pub name: String,
    pub local_service: String,
    pub provider: String,
    pub organization: String,
    pub database: String,
    pub primary: bool,
    pub bootstrap: String,
}

pub async fn detect(repository: &Path) -> Result<RuntimeDetection> {
    detect_repository(repository).await
}

pub async fn initialize(
    start: &Path,
    args: &InitArgs,
    organization: String,
    dry_run: bool,
) -> Result<InitResult> {
    let repository = if git::is_repository(start).await {
        git::repository_root(start).await?
    } else {
        start.to_path_buf()
    };
    let config_path = repository.join(repobox_core::config::CONFIG_FILE_NAME);
    if config_path.exists() && !args.force {
        return Err(RepoboxError::new(
            ErrorKind::Conflict,
            "config_already_exists",
            format!("{} already exists", config_path.display()),
        )
        .with_suggestion("Use `repobox config view`, or rerun `repobox init --force`."));
    }

    let detection = detect_repository(&repository).await?;
    let runtime = resolve_runtime(args, &detection)?;
    let project_name = normalize_name(
        repository
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("project"),
    );
    let mappings = parse_database_mappings(&args.database)?;
    let mut config = match runtime {
        InitRuntime::Compose | InitRuntime::Auto => {
            let mut config =
                RepoboxConfig::new_compose(project_name.clone(), detection.files.clone());
            config.runtime = RuntimeConfig::Compose {
                compose: ComposeConfig {
                    files: detection.files.clone(),
                    profiles: vec![],
                },
            };
            config
        }
        InitRuntime::Native => RepoboxConfig {
            version: repobox_core::config::CONFIG_VERSION,
            project: repobox_core::config::ProjectConfig {
                id: uuid::Uuid::new_v4(),
                name: project_name.clone(),
                git: GitConfig::default(),
            },
            runtime: RuntimeConfig::Native {
                native: NativeConfig {
                    command: args.command.clone(),
                    interactive: true,
                    working_directory: PathBuf::from("."),
                },
            },
            services: BTreeMap::new(),
            data: repobox_core::config::DataConfig {
                allow_copy: matches!(args.data, BootstrapChoice::Import),
            },
            agents: AgentConfig::default(),
        },
    };
    config.data.allow_copy = matches!(args.data, BootstrapChoice::Import);

    let detected_postgres = detection
        .services
        .iter()
        .filter(|service| service.kind == DetectedServiceKind::Postgres)
        .map(|service| service.name.clone())
        .collect::<Vec<_>>();
    let mut local_services = detected_postgres.clone();
    for service in mappings.keys() {
        if !local_services.contains(service) {
            local_services.push(service.clone());
        }
    }
    local_services.sort();
    local_services.dedup();
    let primary = local_services.first().cloned();
    let mut used_keys = BTreeSet::new();
    for local_service in local_services {
        let mut key = normalize_name(&local_service);
        let base_key = key.clone();
        let mut suffix = 2;
        while !used_keys.insert(key.clone()) {
            key = format!("{base_key}-{suffix}");
            suffix += 1;
        }
        let is_primary = primary.as_deref() == Some(local_service.as_str());
        let database = mappings
            .get(&local_service)
            .cloned()
            .unwrap_or_else(|| provider_database_name(&project_name, &key));
        let prefix = env_prefix(&key);
        config.services.insert(
            key,
            ServiceConfig {
                kind: ServiceKind::Postgres,
                primary: is_primary,
                local: LocalServiceConfig {
                    compose_service: local_service,
                },
                remote: RemoteServiceConfig::Planetscale {
                    organization: organization.clone(),
                    database,
                    base_branch: "main".to_owned(),
                    cluster_size: "auto-smallest".to_owned(),
                },
                bootstrap: BootstrapConfig {
                    mode: match args.data {
                        BootstrapChoice::Attach => BootstrapMode::Attach,
                        BootstrapChoice::Empty => BootstrapMode::Empty,
                        BootstrapChoice::Import => BootstrapMode::Import,
                    },
                },
                env: DatabaseEnvConfig {
                    pooled: if is_primary {
                        "DATABASE_URL".to_owned()
                    } else {
                        format!("{prefix}_DATABASE_URL")
                    },
                    direct: if is_primary {
                        "DIRECT_DATABASE_URL".to_owned()
                    } else {
                        format!("{prefix}_DIRECT_DATABASE_URL")
                    },
                },
            },
        );
    }
    config.validate()?;

    let agent_files = agent_guides::update(&repository, &config, dry_run)?;
    if !dry_run {
        let yaml = config.to_yaml()?;
        let temporary = config_path.with_extension("yml.repobox.tmp");
        fs::write(&temporary, yaml)?;
        fs::rename(temporary, &config_path)?;
    }
    let services = config
        .services
        .iter()
        .map(|(name, service)| {
            let RemoteServiceConfig::Planetscale {
                organization,
                database,
                ..
            } = &service.remote;
            InitializedService {
                name: name.clone(),
                local_service: service.local.compose_service.clone(),
                provider: "planetscale".to_owned(),
                organization: organization.clone(),
                database: database.clone(),
                primary: service.primary,
                bootstrap: format!("{:?}", service.bootstrap.mode).to_lowercase(),
            }
        })
        .collect();
    Ok(InitResult {
        config_path,
        project_id: config.project.id,
        project_name,
        runtime: match config.runtime {
            RuntimeConfig::Compose { .. } => "compose".to_owned(),
            RuntimeConfig::Native { .. } => "native".to_owned(),
        },
        services,
        agent_files,
        dry_run,
    })
}

fn resolve_runtime(args: &InitArgs, detection: &RuntimeDetection) -> Result<InitRuntime> {
    match args.runtime {
        InitRuntime::Auto if detection.driver == "compose" => Ok(InitRuntime::Compose),
        InitRuntime::Auto if !args.command.is_empty() => Ok(InitRuntime::Native),
        InitRuntime::Auto => Err(RepoboxError::new(
            ErrorKind::Usage,
            "runtime_not_detected",
            "no Docker Compose file or native command was provided",
        )
        .with_suggestion("Use `repobox init --runtime native -- <command> [args...]`.")),
        InitRuntime::Compose if detection.driver != "compose" => Err(RepoboxError::new(
            ErrorKind::NotFound,
            "compose_config_not_found",
            "--runtime compose was selected but no Compose file was detected",
        )),
        InitRuntime::Native if args.command.is_empty() => Err(RepoboxError::new(
            ErrorKind::Usage,
            "native_command_required",
            "native runtime requires argv after `--`",
        )),
        selected => Ok(selected),
    }
}

fn parse_database_mappings(values: &[String]) -> Result<BTreeMap<String, String>> {
    let mut output = BTreeMap::new();
    for value in values {
        let (service, database) = value.split_once('=').ok_or_else(|| {
            RepoboxError::new(
                ErrorKind::Usage,
                "invalid_database_mapping",
                format!("`{value}` must use SERVICE=DATABASE"),
            )
        })?;
        if service.is_empty() || database.is_empty() {
            return Err(RepoboxError::new(
                ErrorKind::Usage,
                "invalid_database_mapping",
                format!("`{value}` must use non-empty SERVICE=DATABASE values"),
            ));
        }
        if output
            .insert(service.to_owned(), database.to_owned())
            .is_some()
        {
            return Err(RepoboxError::new(
                ErrorKind::Usage,
                "duplicate_database_mapping",
                format!("service `{service}` was mapped more than once"),
            ));
        }
    }
    Ok(output)
}

fn normalize_name(value: &str) -> String {
    let mut output = String::new();
    let mut separator = false;
    for character in value.chars().flat_map(char::to_lowercase) {
        if character.is_ascii_alphanumeric() || character == '_' {
            output.push(character);
            separator = false;
        } else if !output.is_empty() && !separator {
            output.push('-');
            separator = true;
        }
    }
    while output.ends_with('-') {
        output.pop();
    }
    if output.is_empty() {
        "project".to_owned()
    } else {
        output
    }
}

fn provider_database_name(project: &str, service: &str) -> String {
    let name = if project == service {
        project.to_owned()
    } else {
        format!("{project}-{service}")
    };
    if name.len() <= 63 {
        return name;
    }
    let digest = hex::encode(Sha256::digest(name.as_bytes()));
    let mut prefix = name;
    prefix.truncate(63 - 1 - 12);
    format!("{prefix}-{}", &digest[..12])
}

fn env_prefix(service: &str) -> String {
    service
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_explicit_database_mappings() {
        let mappings =
            parse_database_mappings(&["db=app".to_owned(), "audit=logs".to_owned()]).unwrap();
        assert_eq!(mappings["db"], "app");
        assert_eq!(mappings["audit"], "logs");
    }

    #[test]
    fn normalizes_compose_service_names() {
        assert_eq!(normalize_name("Postgres.DB"), "postgres-db");
        assert_eq!(env_prefix("audit-db"), "AUDIT_DB");
    }

    #[test]
    fn long_provider_database_names_keep_a_unique_hash() {
        let project = "p".repeat(60);
        let first = provider_database_name(&project, "database-a");
        let second = provider_database_name(&project, "database-b");
        assert!(first.len() <= 63);
        assert!(second.len() <= 63);
        assert_ne!(first, second);
    }
}
