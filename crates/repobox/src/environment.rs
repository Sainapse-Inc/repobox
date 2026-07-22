use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use chrono::Utc;
use repobox_core::config::{
    BootstrapMode, RemoteServiceConfig, RepoboxConfig, RuntimeConfig, ServiceConfig,
};
use repobox_core::identity::provider_branch_name;
use repobox_core::jobs::{JobKind, JobRecord, JobStatus, JobStore, StepStatus};
use repobox_core::output::{DryRunPlan, PlannedCall};
use repobox_core::provider::{
    Backup, CreateBranchRequest, CreateDatabaseRequest, CreateRoleRequest, DatabaseProvider,
    connection_urls,
};
use repobox_core::state::{
    DatabaseBinding, EnvironmentRecord, EnvironmentStatus, ProjectState, StateStore,
};
use repobox_core::{ErrorKind, RepoboxError, Result};
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use url::Url;

use crate::credentials::CredentialStore;
use crate::output::Output;

const PROVIDER_WAIT_ATTEMPTS: usize = 300;
const PROVIDER_WAIT_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Clone, Debug, Default)]
pub struct ProvisionOptions {
    pub create_backup: bool,
    pub wait_for_backup: bool,
    pub selected_services: BTreeSet<String>,
}

#[derive(Clone, Copy)]
struct RemoteDatabaseRef<'a> {
    organization: &'a str,
    database: &'a str,
    branch: &'a str,
}

async fn replay_extensions(target: &Url, extensions: &[String], resource: &str) -> Result<()> {
    const MANAGED: &[&str] = &[
        "plpgsql",
        "hypopg",
        "pgextwlist",
        "pginsights",
        "pg_pscale_utils",
        "pg_strict",
    ];
    const RESTART_REQUIRED: &[&str] = &[
        "pg_cron",
        "pg_duckdb",
        "pg_hint_plan",
        "pg_partman_bgw",
        "pg_squeeze",
        "pg_stat_statements",
        "timescaledb",
    ];
    let blocked = extensions
        .iter()
        .filter(|extension| {
            RESTART_REQUIRED
                .iter()
                .any(|required| extension.eq_ignore_ascii_case(required))
        })
        .cloned()
        .collect::<Vec<_>>();
    if !blocked.is_empty() {
        return Err(RepoboxError::new(
            ErrorKind::Conflict,
            "extensions_require_dashboard",
            format!(
                "{resource} requires dashboard/restart extensions: {}",
                blocked.join(", ")
            ),
        )
        .with_suggestion(
            "Enable the listed extensions for the target branch in PlanetScale, then resume the job.",
        )
        .with_doc_url("https://planetscale.com/docs/postgres/extensions"));
    }
    let mut extensions = extensions
        .iter()
        .filter(|extension| !MANAGED.contains(&extension.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    extensions.sort_by(|left, right| {
        extension_priority(left)
            .cmp(&extension_priority(right))
            .then_with(|| left.cmp(right))
    });
    for extension in extensions {
        let identifier = extension.replace('"', "\"\"");
        run_psql(
            target,
            &format!("CREATE EXTENSION IF NOT EXISTS \"{identifier}\""),
        )
        .await
        .map_err(|error| {
            RepoboxError::new(
                error.kind,
                "extension_replay_failed",
                format!("could not enable extension `{extension}` on {resource}: {}", error.message),
            )
            .with_suggestion(
                "Verify PlanetScale supports the extension and that the Repobox role inherits `postgres`.",
            )
            .with_doc_url("https://planetscale.com/docs/postgres/extensions")
        })?;
    }
    Ok(())
}

fn extension_priority(extension: &str) -> u8 {
    match extension {
        "cube" | "postgis" | "vector" => 0,
        "earthdistance" | "address_standardizer" | "postgis_topology" | "vectorscale" => 1,
        _ => 2,
    }
}

async fn run_psql(url: &Url, sql: &str) -> Result<String> {
    let output = psql_command(url)?
        .args([
            "--no-psqlrc",
            "--set",
            "ON_ERROR_STOP=1",
            "--tuples-only",
            "--no-align",
            "--command",
            sql,
        ])
        .stdin(Stdio::null())
        .output()
        .await?;
    if output.status.success() {
        return Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned());
    }
    let mut redactor = repobox_core::redaction::SecretRedactor::default();
    if let Some(password) = url.password() {
        redactor.add(password);
    }
    Err(RepoboxError::new(
        ErrorKind::Runtime,
        "psql_failed",
        redactor.redact(String::from_utf8_lossy(&output.stderr).trim()),
    ))
}

fn psql_command(url: &Url) -> Result<Command> {
    let host = url.host_str().ok_or_else(|| {
        RepoboxError::new(
            ErrorKind::Runtime,
            "database_url_host_missing",
            "database connection URL has no host",
        )
    })?;
    let database = url.path().trim_start_matches('/');
    if database.is_empty() || url.username().is_empty() {
        return Err(RepoboxError::new(
            ErrorKind::Runtime,
            "database_url_invalid",
            "database connection URL must include username and database name",
        ));
    }
    let mut environment = BTreeMap::from([
        ("PGHOST", host.to_owned()),
        (
            "PGPORT",
            url.port_or_known_default().unwrap_or(5432).to_string(),
        ),
        ("PGUSER", url.username().to_owned()),
        ("PGDATABASE", database.to_owned()),
    ]);
    if let Some(password) = url.password() {
        environment.insert("PGPASSWORD", password.to_owned());
    }
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "sslmode" => {
                environment.insert("PGSSLMODE", value.into_owned());
            }
            "sslrootcert" => {
                environment.insert("PGSSLROOTCERT", value.into_owned());
            }
            _ => {}
        }
    }
    let local_psql = std::process::Command::new("psql")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success());
    let mut command = if local_psql {
        Command::new("psql")
    } else {
        let mut command = Command::new("docker");
        command.args(["run", "--rm", "-i"]);
        for key in environment.keys() {
            command.arg("-e").arg(key);
        }
        command.args(["postgres:18", "psql"]);
        command
    };
    command.envs(environment);
    Ok(command)
}

#[derive(Clone, Debug, Serialize)]
pub struct EnvironmentMutation {
    pub environment: EnvironmentRecord,
    pub job: JobRecord,
    pub resumed: bool,
}

pub struct EnvironmentManager<'a> {
    config: &'a RepoboxConfig,
    repository: PathBuf,
    provider: &'a dyn DatabaseProvider,
    credentials: &'a CredentialStore,
    state_store: StateStore,
    jobs: JobStore,
    output: &'a Output,
}

impl<'a> EnvironmentManager<'a> {
    pub fn new(
        config: &'a RepoboxConfig,
        repository: impl Into<PathBuf>,
        provider: &'a dyn DatabaseProvider,
        credentials: &'a CredentialStore,
        state_store: StateStore,
        jobs: JobStore,
        output: &'a Output,
    ) -> Self {
        Self {
            config,
            repository: repository.into(),
            provider,
            credentials,
            state_store,
            jobs,
            output,
        }
    }

    pub fn create_plan(&self, environment: &str, options: &ProvisionOptions) -> Result<DryRunPlan> {
        let branch = provider_branch_name(self.config.project.id, environment)?;
        let services = self.selected_services(options)?;
        let mut calls = vec![];
        for (name, service) in services {
            let RemoteServiceConfig::Planetscale {
                organization,
                database,
                base_branch,
                ..
            } = &service.remote;
            calls.extend([
                PlannedCall {
                    provider: "planetscale".to_owned(),
                    action: "ensure_database".to_owned(),
                    resource: format!("{organization}/{database}"),
                },
                PlannedCall {
                    provider: "planetscale".to_owned(),
                    action: "restore_latest_backup".to_owned(),
                    resource: format!("{organization}/{database}/{base_branch} -> {branch}"),
                },
                PlannedCall {
                    provider: "planetscale".to_owned(),
                    action: "ensure_role".to_owned(),
                    resource: format!("{organization}/{database}/{branch} ({name})"),
                },
            ]);
        }
        Ok(DryRunPlan {
            operation: "environment_create".to_owned(),
            environment: environment.to_owned(),
            provider_calls: calls,
            warnings: vec![
                "Each restored PlanetScale Postgres branch has its own billable cluster."
                    .to_owned(),
            ],
            estimated_cost: Some(
                "Provider pricing depends on the selected smallest eligible SKU.".to_owned(),
            ),
            rollback_available: true,
        })
    }

    pub async fn ensure(
        &mut self,
        environment: &str,
        options: &ProvisionOptions,
    ) -> Result<EnvironmentMutation> {
        repobox_core::identity::validate_environment_name(environment)?;
        let provider_branch = provider_branch_name(self.config.project.id, environment)?;
        let mut state = self.state_store.load(self.config.project.id)?;
        let selected = self.selected_services(options)?;
        let (mut job, resumed) =
            self.resumable_job(JobKind::EnvironmentCreate, environment, selected.keys())?;
        job.status = JobStatus::Running;
        self.jobs.append(&job)?;

        let record = state
            .environments
            .entry(environment.to_owned())
            .or_insert_with(|| EnvironmentRecord::new(environment, &provider_branch));
        record.status = EnvironmentStatus::Provisioning;
        record.failures.clear();
        record.updated_at = Utc::now();
        self.state_store.save(&state)?;

        let mut failures = vec![];
        for (name, service) in selected {
            let step = format!("provision:{name}");
            job.update_step(&step, StepStatus::Running, None)?;
            self.jobs.append(&job)?;
            self.event(
                "step_started",
                &serde_json::json!({"job_id": job.id, "step": step}),
            )?;
            let result = if service.bootstrap.mode == BootstrapMode::Import
                && !state.bootstrapped_services.contains(&name)
            {
                match self.import_local_service(&name, &service).await {
                    Ok(()) => {
                        state.bootstrapped_services.insert(name.clone());
                        self.state_store.save(&state)?;
                        self.provision_service(
                            environment,
                            &provider_branch,
                            &name,
                            &service,
                            options,
                        )
                        .await
                    }
                    Err(error) => Err(error),
                }
            } else {
                self.provision_service(environment, &provider_branch, &name, &service, options)
                    .await
            };
            match result {
                Ok(binding) => {
                    state
                        .environments
                        .get_mut(environment)
                        .expect("environment was inserted")
                        .databases
                        .insert(name.clone(), binding.clone());
                    self.state_store.save(&state)?;
                    job.update_step(
                        &step,
                        StepStatus::Succeeded,
                        Some(format!("{}/{}", binding.database, binding.branch)),
                    )?;
                    if let Some(job_step) = job
                        .steps
                        .iter_mut()
                        .find(|candidate| candidate.name == step)
                    {
                        job_step.resource = serde_json::to_value(&binding).map_err(|error| {
                            RepoboxError::new(
                                ErrorKind::Runtime,
                                "job_encode_failed",
                                error.to_string(),
                            )
                        })?;
                    }
                    self.event("step_succeeded", &binding)?;
                }
                Err(error) => {
                    failures.push(format!("{name}: {}", error.message));
                    job.error_code = Some(error.code.clone());
                    job.update_step(&step, StepStatus::Failed, Some(error.message.clone()))?;
                    self.event(
                        "step_failed",
                        &serde_json::json!({"service": name, "error": error}),
                    )?;
                }
            }
            self.jobs.append(&job)?;
        }

        let finished_record = {
            let record = state
                .environments
                .get_mut(environment)
                .expect("environment exists");
            record.updated_at = Utc::now();
            if failures.is_empty() {
                record.status = EnvironmentStatus::Ready;
                job.status = JobStatus::Succeeded;
            } else {
                record.status = EnvironmentStatus::Degraded;
                record.failures.clone_from(&failures);
                job.status = JobStatus::Degraded;
            }
            record.clone()
        };
        self.state_store.save(&state)?;
        self.jobs.append(&job)?;
        let mutation = EnvironmentMutation {
            environment: finished_record,
            job: job.clone(),
            resumed,
        };
        if failures.is_empty() {
            Ok(mutation)
        } else {
            Err(RepoboxError::new(
                ErrorKind::Runtime,
                "environment_provision_degraded",
                format!(
                    "environment `{environment}` is degraded: {}",
                    failures.join("; ")
                ),
            )
            .with_suggestion(format!(
                "Fix the reported provider issue and run `repobox job resume {}`.",
                job.id
            )))
        }
    }

    pub async fn delete(
        &mut self,
        environment: &str,
        keep_state: bool,
    ) -> Result<EnvironmentMutation> {
        let mut state = self.state_store.load(self.config.project.id)?;
        let record = state
            .environments
            .get(environment)
            .cloned()
            .ok_or_else(|| {
                RepoboxError::new(
                    ErrorKind::NotFound,
                    "environment_not_found",
                    format!("environment `{environment}` is not known locally"),
                )
            })?;
        let mut job = JobRecord::new(
            JobKind::EnvironmentDelete,
            self.config.project.id,
            environment,
            record
                .databases
                .keys()
                .map(|service| format!("delete:{service}")),
        );
        job.status = JobStatus::Running;
        self.jobs.append(&job)?;
        let mut failures = vec![];
        for (service, binding) in &record.databases {
            let step = format!("delete:{service}");
            job.update_step(&step, StepStatus::Running, None)?;
            self.jobs.append(&job)?;
            let result = self
                .provider
                .delete_branch(&binding.organization, &binding.database, &binding.branch)
                .await;
            match result {
                Ok(()) => {
                    let key = CredentialStore::database_key(
                        self.config.project.id,
                        &record.provider_branch,
                        service,
                    );
                    self.credentials.remove_database_urls(&key)?;
                    job.update_step(&step, StepStatus::Succeeded, None)?;
                }
                Err(error) if error.kind == ErrorKind::NotFound => {
                    job.update_step(
                        &step,
                        StepStatus::Succeeded,
                        Some("provider branch was already absent".to_owned()),
                    )?;
                }
                Err(error) => {
                    failures.push(error.message.clone());
                    job.update_step(&step, StepStatus::Failed, Some(error.message))?;
                }
            }
            self.jobs.append(&job)?;
        }
        if failures.is_empty() {
            job.status = JobStatus::Succeeded;
            if !keep_state {
                state.environments.remove(environment);
                self.state_store.save(&state)?;
            }
        } else {
            job.status = JobStatus::Degraded;
        }
        self.jobs.append(&job)?;
        if !failures.is_empty() {
            return Err(RepoboxError::new(
                ErrorKind::Runtime,
                "environment_delete_degraded",
                failures.join("; "),
            ));
        }
        Ok(EnvironmentMutation {
            environment: record,
            job,
            resumed: false,
        })
    }

    pub fn pull_plan(&self, environment: &str, options: &ProvisionOptions) -> Result<DryRunPlan> {
        let branch = provider_branch_name(self.config.project.id, environment)?;
        let mut calls = vec![];
        for (_, service) in self.selected_services(options)? {
            let RemoteServiceConfig::Planetscale {
                organization,
                database,
                base_branch,
                ..
            } = &service.remote;
            calls.extend([
                PlannedCall {
                    provider: "planetscale".to_owned(),
                    action: "restore_staging_branch".to_owned(),
                    resource: format!("{organization}/{database}/{base_branch} -> {branch}-next-*"),
                },
                PlannedCall {
                    provider: "planetscale".to_owned(),
                    action: "delete_environment_branch".to_owned(),
                    resource: format!("{organization}/{database}/{branch}"),
                },
                PlannedCall {
                    provider: "planetscale".to_owned(),
                    action: "rename_staging_branch".to_owned(),
                    resource: format!("{organization}/{database}/{branch}"),
                },
            ]);
        }
        Ok(DryRunPlan {
            operation: "environment_pull".to_owned(),
            environment: environment.to_owned(),
            provider_calls: calls,
            warnings: vec![
                "This permanently replaces environment-local database data.".to_owned(),
                "Local services stay stopped if any database swap is incomplete.".to_owned(),
                "No rollback backup of the old environment branch is created.".to_owned(),
            ],
            estimated_cost: Some(
                "A staging branch temporarily overlaps the existing branch during the swap."
                    .to_owned(),
            ),
            rollback_available: false,
        })
    }

    pub async fn pull(
        &mut self,
        environment: &str,
        options: &ProvisionOptions,
    ) -> Result<EnvironmentMutation> {
        let canonical = provider_branch_name(self.config.project.id, environment)?;
        let selected = self.selected_services(options)?;
        let mut state = self.state_store.load(self.config.project.id)?;
        if !state.environments.contains_key(environment) {
            return Err(RepoboxError::new(
                ErrorKind::NotFound,
                "environment_not_found",
                format!("environment `{environment}` does not exist"),
            )
            .with_suggestion("Run `repobox env create --yes` first."));
        }
        let (mut job, resumed) = self.resumable_pull_job(environment, selected.keys())?;
        job.status = JobStatus::Running;
        self.jobs.append(&job)?;
        {
            let record = state
                .environments
                .get_mut(environment)
                .expect("environment existence was checked");
            record.status = EnvironmentStatus::Provisioning;
            record.failures.clear();
            record.updated_at = Utc::now();
        }
        self.state_store.save(&state)?;

        let mut failures = vec![];
        for (name, service) in selected {
            let step = format!("refresh:{name}");
            if job
                .steps
                .iter()
                .find(|candidate| candidate.name == step)
                .is_some_and(|step| step.status == StepStatus::Succeeded)
            {
                continue;
            }
            job.update_step(&step, StepStatus::Running, None)?;
            self.jobs.append(&job)?;
            self.event(
                "step_started",
                &serde_json::json!({"job_id": job.id, "step": step}),
            )?;
            match self
                .pull_service(&canonical, &name, &service, options, &mut job, &step)
                .await
            {
                Ok(binding) => {
                    state
                        .environments
                        .get_mut(environment)
                        .expect("environment existence was checked")
                        .databases
                        .insert(name.clone(), binding.clone());
                    self.state_store.save(&state)?;
                    job.update_step(
                        &step,
                        StepStatus::Succeeded,
                        Some("provider branch swapped forward".to_owned()),
                    )?;
                    set_step_resource(
                        &mut job,
                        &step,
                        serde_json::json!({"phase": "complete", "binding": binding}),
                    )?;
                    self.event("step_succeeded", &binding)?;
                }
                Err(error) => {
                    failures.push(format!("{name}: {}", error.message));
                    job.error_code = Some(error.code.clone());
                    job.update_step(&step, StepStatus::Failed, Some(error.message.clone()))?;
                    self.event(
                        "step_failed",
                        &serde_json::json!({"service": name, "error": error}),
                    )?;
                }
            }
            self.jobs.append(&job)?;
        }

        let finished_record = {
            let record = state
                .environments
                .get_mut(environment)
                .expect("environment existence was checked");
            record.updated_at = Utc::now();
            if failures.is_empty() {
                record.status = EnvironmentStatus::Ready;
                job.status = JobStatus::Succeeded;
            } else {
                record.status = EnvironmentStatus::Degraded;
                record.failures.clone_from(&failures);
                job.status = JobStatus::Degraded;
            }
            record.clone()
        };
        self.state_store.save(&state)?;
        self.jobs.append(&job)?;
        let mutation = EnvironmentMutation {
            environment: finished_record,
            job: job.clone(),
            resumed,
        };
        if failures.is_empty() {
            Ok(mutation)
        } else {
            Err(RepoboxError::new(
                ErrorKind::Runtime,
                "environment_pull_degraded",
                format!("data refresh is incomplete: {}", failures.join("; ")),
            )
            .with_suggestion(format!(
                "Services remain stopped. Resume forward with `repobox job resume {}`.",
                job.id
            )))
        }
    }

    async fn provision_service(
        &mut self,
        _environment: &str,
        provider_branch: &str,
        service_name: &str,
        service: &ServiceConfig,
        options: &ProvisionOptions,
    ) -> Result<DatabaseBinding> {
        let RemoteServiceConfig::Planetscale {
            organization,
            database,
            base_branch,
            cluster_size,
        } = &service.remote;
        let sizes = if cluster_size == "auto-smallest" {
            self.provider.list_cluster_sizes(organization).await?
        } else {
            vec![cluster_size.clone()]
        };
        let size = select_smallest_size(&sizes)?;

        let databases = self.provider.list_databases(organization).await?;
        let database_exists = databases
            .iter()
            .any(|candidate| candidate.name == *database);
        if !database_exists {
            match service.bootstrap.mode {
                BootstrapMode::Attach => {
                    return Err(RepoboxError::new(
                        ErrorKind::NotFound,
                        "planetscale_database_not_found",
                        format!(
                            "PlanetScale database `{organization}/{database}` does not exist"
                        ),
                    )
                    .with_suggestion(
                        "Choose `bootstrap: { mode: empty }` for this service, or attach an existing database.",
                    ));
                }
                BootstrapMode::Empty | BootstrapMode::Import => {
                    self.output.progress(&format!(
                        "creating PlanetScale database {organization}/{database}"
                    ));
                    self.provider
                        .create_database(&CreateDatabaseRequest {
                            organization: organization.clone(),
                            name: database.clone(),
                            region: None,
                            cluster_size: size.clone(),
                            major_version: None,
                        })
                        .await?;
                    self.wait_for_database(organization, database).await?;
                }
            }
        }

        let branches = self.provider.list_branches(organization, database).await?;
        let branch_exists = branches
            .iter()
            .any(|candidate| candidate.name == provider_branch && candidate.ready);
        let key =
            CredentialStore::database_key(self.config.project.id, provider_branch, service_name);
        if branch_exists
            && let Ok((_, _)) = self.credentials.database_urls(&key)
            && let Some(role) = self
                .provider
                .list_roles(organization, database, provider_branch)
                .await?
                .into_iter()
                .find(|role| role.name == role_name(provider_branch, service_name))
        {
            return Ok(DatabaseBinding {
                service: service_name.to_owned(),
                provider: "planetscale".to_owned(),
                organization: organization.clone(),
                database: database.clone(),
                branch: provider_branch.to_owned(),
                role_id: role.id,
                role_name: role.name,
                ready: true,
                updated_at: Utc::now(),
            });
        }

        if !branch_exists {
            let backup = self
                .latest_backup(
                    organization,
                    database,
                    base_branch,
                    options.create_backup
                        || !database_exists
                        || service.bootstrap.mode == BootstrapMode::Import,
                    options.wait_for_backup
                        || !database_exists
                        || service.bootstrap.mode == BootstrapMode::Import,
                )
                .await?;
            self.output.progress(&format!(
                "restoring {organization}/{database}/{provider_branch} from backup {}",
                backup.name
            ));
            self.provider
                .create_branch(&CreateBranchRequest {
                    organization: organization.clone(),
                    database: database.clone(),
                    name: provider_branch.to_owned(),
                    parent_branch: base_branch.clone(),
                    backup_id: Some(backup.id),
                    cluster_size: Some(size),
                })
                .await?;
            self.wait_for_branch(organization, database, provider_branch)
                .await?;
        }

        let desired_role_name = role_name(provider_branch, service_name);
        let existing = self
            .provider
            .list_roles(organization, database, provider_branch)
            .await?
            .into_iter()
            .find(|role| role.name == desired_role_name);
        if let Some(existing) = existing {
            self.provider
                .delete_role(
                    organization,
                    database,
                    provider_branch,
                    &existing.id,
                    Some("postgres"),
                )
                .await?;
        }
        let role = self
            .provider
            .create_role(&CreateRoleRequest {
                organization: organization.clone(),
                database: database.clone(),
                branch: provider_branch.to_owned(),
                name: desired_role_name.clone(),
                inherited_roles: vec!["postgres".to_owned()],
            })
            .await?;
        if role.password.is_none() {
            return Err(RepoboxError::new(
                ErrorKind::Runtime,
                "planetscale_role_password_missing",
                "PlanetScale did not return the one-time password for a new role",
            ));
        }
        let urls = connection_urls(&role)?;
        self.replay_extensions_from_base(
            organization,
            database,
            base_branch,
            provider_branch,
            &urls.direct,
        )
        .await?;
        self.credentials
            .store_database_urls(&key, urls.pooled.as_str(), urls.direct.as_str())?;
        Ok(DatabaseBinding {
            service: service_name.to_owned(),
            provider: "planetscale".to_owned(),
            organization: organization.clone(),
            database: database.clone(),
            branch: provider_branch.to_owned(),
            role_id: role.id,
            role_name: role.name,
            ready: true,
            updated_at: Utc::now(),
        })
    }

    async fn pull_service(
        &mut self,
        canonical: &str,
        service_name: &str,
        service: &ServiceConfig,
        options: &ProvisionOptions,
        job: &mut JobRecord,
        step: &str,
    ) -> Result<DatabaseBinding> {
        let RemoteServiceConfig::Planetscale {
            organization,
            database,
            base_branch,
            cluster_size,
        } = &service.remote;
        let staging = staging_branch_name(canonical, job.id);
        let mut phase = job
            .steps
            .iter()
            .find(|candidate| candidate.name == step)
            .and_then(|step| step.resource.get("phase"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("planned")
            .to_owned();
        let size = if cluster_size == "auto-smallest" {
            select_smallest_size(&self.provider.list_cluster_sizes(organization).await?)?
        } else {
            cluster_size.clone()
        };

        if matches!(phase.as_str(), "planned" | "staged" | "credentialed") {
            let branches = self.provider.list_branches(organization, database).await?;
            let staging_exists = branches.iter().any(|branch| branch.name == staging);
            if !staging_exists {
                let backup = self
                    .latest_backup(
                        organization,
                        database,
                        base_branch,
                        options.create_backup,
                        options.wait_for_backup,
                    )
                    .await?;
                self.provider
                    .create_branch(&CreateBranchRequest {
                        organization: organization.clone(),
                        database: database.clone(),
                        name: staging.clone(),
                        parent_branch: base_branch.clone(),
                        backup_id: Some(backup.id),
                        cluster_size: Some(size),
                    })
                    .await?;
                self.wait_for_branch(organization, database, &staging)
                    .await?;
            }
            if phase == "planned" {
                phase.clear();
                phase.push_str("staged");
                set_step_resource(
                    job,
                    step,
                    serde_json::json!({"phase": phase, "staging": staging}),
                )?;
                self.jobs.append(job)?;
            }
        }

        let staging_key =
            CredentialStore::database_key(self.config.project.id, &staging, service_name);
        let canonical_key =
            CredentialStore::database_key(self.config.project.id, canonical, service_name);
        let staging_role_name = role_name(&staging, service_name);
        if phase == "staged" {
            let existing = self
                .provider
                .list_roles(organization, database, &staging)
                .await?
                .into_iter()
                .find(|role| role.name == staging_role_name);
            let role = if self.credentials.database_urls(&staging_key).is_ok() {
                existing.ok_or_else(|| {
                    RepoboxError::new(
                        ErrorKind::Conflict,
                        "staging_role_missing",
                        "staging credentials exist but the provider role is missing",
                    )
                })?
            } else {
                if let Some(existing) = existing {
                    self.provider
                        .delete_role(
                            organization,
                            database,
                            &staging,
                            &existing.id,
                            Some("postgres"),
                        )
                        .await?;
                }
                let role = self
                    .provider
                    .create_role(&CreateRoleRequest {
                        organization: organization.clone(),
                        database: database.clone(),
                        branch: staging.clone(),
                        name: staging_role_name.clone(),
                        inherited_roles: vec!["postgres".to_owned()],
                    })
                    .await?;
                let urls = connection_urls(&role)?;
                self.credentials.store_database_urls(
                    &staging_key,
                    urls.pooled.as_str(),
                    urls.direct.as_str(),
                )?;
                role
            };
            let (_, staging_direct) = self.credentials.database_urls(&staging_key)?;
            let staging_direct = Url::parse(&staging_direct).map_err(|error| {
                RepoboxError::new(
                    ErrorKind::Runtime,
                    "database_credential_invalid",
                    error.to_string(),
                )
            })?;
            self.replay_extensions_from_base(
                organization,
                database,
                base_branch,
                &staging,
                &staging_direct,
            )
            .await?;
            phase.clear();
            phase.push_str("credentialed");
            set_step_resource(
                job,
                step,
                serde_json::json!({
                    "phase": phase,
                    "staging": staging,
                    "role_id": role.id,
                    "role_name": role.name,
                }),
            )?;
            self.jobs.append(job)?;
        }

        if phase == "credentialed" {
            let branches = self.provider.list_branches(organization, database).await?;
            if branches.iter().any(|branch| branch.name == canonical) {
                self.provider
                    .delete_branch(organization, database, canonical)
                    .await?;
            }
            phase.clear();
            phase.push_str("old_deleted");
            set_step_resource(
                job,
                step,
                serde_json::json!({"phase": phase, "staging": staging}),
            )?;
            self.jobs.append(job)?;
        }

        if phase == "old_deleted" {
            let branches = self.provider.list_branches(organization, database).await?;
            if branches.iter().any(|branch| branch.name == staging) {
                self.provider
                    .rename_branch(organization, database, &staging, canonical)
                    .await?;
                self.wait_for_branch(organization, database, canonical)
                    .await?;
            } else if !branches.iter().any(|branch| branch.name == canonical) {
                return Err(RepoboxError::new(
                    ErrorKind::Conflict,
                    "pull_swap_missing_branches",
                    "neither the staging nor canonical branch exists during a forward-only swap",
                ));
            }
            phase.clear();
            phase.push_str("swapped");
            set_step_resource(
                job,
                step,
                serde_json::json!({"phase": phase, "staging": staging}),
            )?;
            self.jobs.append(job)?;
        }

        let (pooled, direct) = self
            .credentials
            .database_urls(&staging_key)
            .or_else(|_| self.credentials.database_urls(&canonical_key))?;
        self.credentials
            .store_database_urls(&canonical_key, &pooled, &direct)?;
        self.credentials.remove_database_urls(&staging_key)?;
        let roles = self
            .provider
            .list_roles(organization, database, canonical)
            .await?;
        let role = roles
            .into_iter()
            .find(|role| role.name == staging_role_name)
            .ok_or_else(|| {
                RepoboxError::new(
                    ErrorKind::NotFound,
                    "planetscale_role_not_found",
                    "the swapped branch's Repobox role was not found",
                )
            })?;
        Ok(DatabaseBinding {
            service: service_name.to_owned(),
            provider: "planetscale".to_owned(),
            organization: organization.clone(),
            database: database.clone(),
            branch: canonical.to_owned(),
            role_id: role.id,
            role_name: role.name,
            ready: true,
            updated_at: Utc::now(),
        })
    }

    async fn latest_backup(
        &self,
        organization: &str,
        database: &str,
        base_branch: &str,
        create: bool,
        wait: bool,
    ) -> Result<Backup> {
        let backups = self
            .provider
            .list_backups(organization, database, base_branch)
            .await?;
        let mut successful = backups
            .iter()
            .filter(|backup| backup.state == "success")
            .cloned()
            .collect::<Vec<_>>();
        successful.sort_by_key(|backup| backup.completed_at.unwrap_or(backup.created_at));
        if let Some(backup) = successful.pop() {
            return Ok(backup);
        }
        if let Some(pending) = backups
            .iter()
            .find(|backup| matches!(backup.state.as_str(), "pending" | "running"))
        {
            if !wait {
                return Err(RepoboxError::new(
                    ErrorKind::Conflict,
                    "planetscale_backup_in_progress",
                    format!("backup `{}` is still {}", pending.name, pending.state),
                )
                .with_suggestion("Rerun with `--wait`, or retry after the backup completes."));
            }
            return self
                .wait_for_backup(
                    organization,
                    database,
                    base_branch,
                    &pending.id,
                    &pending.name,
                )
                .await;
        }
        if !create {
            return Err(RepoboxError::new(
                ErrorKind::Conflict,
                "planetscale_backup_required",
                format!(
                    "no successful backup exists for {organization}/{database}/{base_branch}"
                ),
            )
            .with_suggestion(
                "Create one in PlanetScale, or rerun with `--create-backup` to request an immediate backup.",
            ));
        }
        let name = format!("repobox-{}", Utc::now().format("%Y%m%d%H%M%S"));
        let created = self
            .provider
            .create_backup(organization, database, base_branch, &name)
            .await?;
        self.wait_for_backup(organization, database, base_branch, &created.id, &name)
            .await
    }

    async fn import_local_service(
        &self,
        service_name: &str,
        service: &ServiceConfig,
    ) -> Result<()> {
        if !self.config.data.allow_copy {
            return Err(RepoboxError::new(
                ErrorKind::Conflict,
                "data_copy_not_approved",
                "local database import requires data.allow_copy: true",
            ));
        }
        let RuntimeConfig::Compose { compose } = &self.config.runtime else {
            return Err(RepoboxError::new(
                ErrorKind::Usage,
                "import_requires_compose",
                "automatic local Postgres import currently requires Docker Compose",
            ));
        };
        let RemoteServiceConfig::Planetscale {
            organization,
            database,
            base_branch,
            cluster_size,
        } = &service.remote;
        let size = if cluster_size == "auto-smallest" {
            select_smallest_size(&self.provider.list_cluster_sizes(organization).await?)?
        } else {
            cluster_size.clone()
        };
        if !self
            .provider
            .list_databases(organization)
            .await?
            .iter()
            .any(|candidate| candidate.name == *database)
        {
            self.provider
                .create_database(&CreateDatabaseRequest {
                    organization: organization.clone(),
                    name: database.clone(),
                    region: None,
                    cluster_size: size,
                    major_version: None,
                })
                .await?;
            self.wait_for_database(organization, database).await?;
        }

        let project_digest =
            Sha256::digest(format!("{}\0{service_name}", self.config.project.id).as_bytes());
        let backup_prefix = format!("repobox-import-{}", &hex::encode(project_digest)[..12]);
        let backups = self
            .provider
            .list_backups(organization, database, base_branch)
            .await?;
        if backups
            .iter()
            .any(|backup| backup.name.starts_with(&backup_prefix) && backup.state == "success")
        {
            self.output.progress(&format!(
                "reusing completed import backup for {organization}/{database}"
            ));
            return Ok(());
        }
        if let Some(pending) = backups.iter().find(|backup| {
            backup.name.starts_with(&backup_prefix)
                && matches!(backup.state.as_str(), "pending" | "running")
        }) {
            self.wait_for_backup(
                organization,
                database,
                base_branch,
                &pending.id,
                &pending.name,
            )
            .await?;
            self.output.progress(&format!(
                "reusing completed import backup for {organization}/{database}"
            ));
            return Ok(());
        }

        let role_name = backup_prefix.clone();
        if let Some(role) = self
            .provider
            .list_roles(organization, database, base_branch)
            .await?
            .into_iter()
            .find(|role| role.name == role_name)
        {
            self.provider
                .delete_role(
                    organization,
                    database,
                    base_branch,
                    &role.id,
                    Some("postgres"),
                )
                .await?;
        }
        let role = self
            .provider
            .create_role(&CreateRoleRequest {
                organization: organization.clone(),
                database: database.clone(),
                branch: base_branch.clone(),
                name: role_name,
                inherited_roles: vec!["postgres".to_owned()],
            })
            .await?;
        let direct = connection_urls(&role)?.direct;
        let remote = RemoteDatabaseRef {
            organization,
            database,
            branch: base_branch,
        };
        let import_result = self
            .copy_compose_database(
                &service.local.compose_service,
                &compose.files,
                &compose.profiles,
                &direct,
                remote,
            )
            .await;
        let cleanup_result = self
            .provider
            .delete_role(
                organization,
                database,
                base_branch,
                &role.id,
                Some("postgres"),
            )
            .await;
        import_result.and(cleanup_result)?;
        let backup_name = format!("{backup_prefix}-{}", Utc::now().format("%Y%m%d%H%M%S"));
        let backup = self
            .provider
            .create_backup(organization, database, base_branch, &backup_name)
            .await?;
        self.wait_for_backup(
            organization,
            database,
            base_branch,
            &backup.id,
            &backup_name,
        )
        .await?;
        self.output.progress(&format!(
            "imported local `{service_name}` into {organization}/{database} and captured backup `{backup_name}`"
        ));
        Ok(())
    }

    async fn copy_compose_database(
        &self,
        compose_service: &str,
        files: &[PathBuf],
        profiles: &[String],
        target: &Url,
        remote: RemoteDatabaseRef<'_>,
    ) -> Result<()> {
        let was_running = self
            .compose_service_running(files, profiles, compose_service)
            .await?;
        let mut compose = self.compose_command(files, profiles);
        if !was_running {
            let status = compose
                .args(["up", "--detach", compose_service])
                .status()
                .await?;
            if !status.success() {
                return Err(RepoboxError::new(
                    ErrorKind::Runtime,
                    "local_postgres_start_failed",
                    format!("Docker Compose exited with {status}"),
                ));
            }
        }

        let copy_result = self
            .copy_running_compose_database(compose_service, files, profiles, target, remote)
            .await;
        let cleanup_result = if was_running {
            Ok(())
        } else {
            self.stop_compose_service(files, profiles, compose_service)
                .await
        };
        copy_result.and(cleanup_result)
    }

    async fn copy_running_compose_database(
        &self,
        compose_service: &str,
        files: &[PathBuf],
        profiles: &[String],
        target: &Url,
        remote: RemoteDatabaseRef<'_>,
    ) -> Result<()> {
        let detection = repobox_runtime_compose::detect_configuration(
            &self.repository,
            files,
            profiles,
            &BTreeMap::new(),
        )
        .await?;
        let detected = detection
            .services
            .into_iter()
            .find(|candidate| candidate.name == compose_service)
            .ok_or_else(|| {
                RepoboxError::new(
                    ErrorKind::NotFound,
                    "local_postgres_service_not_found",
                    format!("Compose service `{compose_service}` was not detected"),
                )
            })?;
        let user = detected
            .environment
            .get("POSTGRES_USER")
            .cloned()
            .unwrap_or_else(|| "postgres".to_owned());
        let local_database = detected
            .environment
            .get("POSTGRES_DB")
            .cloned()
            .unwrap_or_else(|| user.clone());

        let extensions = self
            .local_extensions(files, profiles, compose_service, &user, &local_database)
            .await?;
        replay_extensions(
            target,
            &extensions,
            &format!(
                "{}/{}/{}",
                remote.organization, remote.database, remote.branch
            ),
        )
        .await?;

        let mut dump = self.compose_command(files, profiles);
        let mut dump = dump
            .args([
                "exec",
                "-T",
                compose_service,
                "pg_dump",
                "--clean",
                "--if-exists",
                "--no-owner",
                "--no-acl",
                "--username",
                &user,
                "--dbname",
                &local_database,
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let dump_stdout = dump.stdout.take().expect("pg_dump stdout is piped");
        let mut restore = psql_command(target)?;
        let mut restore = restore
            .args(["--no-psqlrc", "--set", "ON_ERROR_STOP=1"])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()?;
        let mut restore_stdin = restore.stdin.take().expect("psql stdin is piped");
        let mut dump_stdout = dump_stdout;
        tokio::io::copy(&mut dump_stdout, &mut restore_stdin).await?;
        restore_stdin.shutdown().await?;
        drop(restore_stdin);
        let dump_output = dump.wait_with_output().await?;
        let restore_output = restore.wait_with_output().await?;
        if !dump_output.status.success() {
            return Err(RepoboxError::new(
                ErrorKind::Runtime,
                "local_postgres_dump_failed",
                String::from_utf8_lossy(&dump_output.stderr)
                    .trim()
                    .to_owned(),
            ));
        }
        if !restore_output.status.success() {
            let mut redactor = repobox_core::redaction::SecretRedactor::default();
            if let Some(password) = target.password() {
                redactor.add(password);
            }
            return Err(RepoboxError::new(
                ErrorKind::Runtime,
                "planetscale_import_failed",
                redactor.redact(String::from_utf8_lossy(&restore_output.stderr).trim()),
            ));
        }
        Ok(())
    }

    async fn compose_service_running(
        &self,
        files: &[PathBuf],
        profiles: &[String],
        service: &str,
    ) -> Result<bool> {
        let mut command = self.compose_command(files, profiles);
        let output = command
            .args(["ps", "--status", "running", "--quiet", service])
            .output()
            .await?;
        if !output.status.success() {
            return Err(RepoboxError::new(
                ErrorKind::Runtime,
                "local_postgres_status_failed",
                String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            ));
        }
        Ok(!output.stdout.is_empty())
    }

    async fn stop_compose_service(
        &self,
        files: &[PathBuf],
        profiles: &[String],
        service: &str,
    ) -> Result<()> {
        let mut command = self.compose_command(files, profiles);
        let status = command.args(["stop", service]).status().await?;
        if status.success() {
            Ok(())
        } else {
            Err(RepoboxError::new(
                ErrorKind::Runtime,
                "local_postgres_stop_failed",
                format!("Docker Compose exited with {status}"),
            ))
        }
    }

    async fn local_extensions(
        &self,
        files: &[PathBuf],
        profiles: &[String],
        service: &str,
        user: &str,
        database: &str,
    ) -> Result<Vec<String>> {
        let mut command = self.compose_command(files, profiles);
        let output = command
            .args([
                "exec",
                "-T",
                service,
                "psql",
                "--username",
                user,
                "--dbname",
                database,
                "--tuples-only",
                "--no-align",
                "--command",
                "SELECT extname FROM pg_extension ORDER BY extname",
            ])
            .output()
            .await?;
        if !output.status.success() {
            return Err(RepoboxError::new(
                ErrorKind::Runtime,
                "postgres_extension_inspection_failed",
                String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            ));
        }
        Ok(String::from_utf8_lossy(&output.stdout)
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(str::to_owned)
            .collect())
    }

    fn compose_command(&self, files: &[PathBuf], profiles: &[String]) -> Command {
        let mut command = Command::new("docker");
        command.current_dir(&self.repository).arg("compose");
        for profile in profiles {
            command.arg("--profile").arg(profile);
        }
        for file in files {
            command.arg("-f").arg(file);
        }
        command
    }

    async fn replay_extensions_from_base(
        &self,
        organization: &str,
        database: &str,
        base_branch: &str,
        target_branch: &str,
        target_url: &Url,
    ) -> Result<()> {
        let inspect_id = uuid::Uuid::now_v7().simple().to_string();
        let name = format!("repobox-inspect-{}", &inspect_id[inspect_id.len() - 12..]);
        let source_role = self
            .provider
            .create_role(&CreateRoleRequest {
                organization: organization.to_owned(),
                database: database.to_owned(),
                branch: base_branch.to_owned(),
                name,
                inherited_roles: vec!["postgres".to_owned()],
            })
            .await?;
        let source_url = connection_urls(&source_role)?.direct;
        let replay = async {
            let output = run_psql(
                &source_url,
                "SELECT extname FROM pg_extension ORDER BY extname",
            )
            .await?;
            let extensions = output
                .lines()
                .map(str::trim)
                .filter(|line| !line.is_empty())
                .map(str::to_owned)
                .collect::<Vec<_>>();
            replay_extensions(
                target_url,
                &extensions,
                &format!("{organization}/{database}/{target_branch}"),
            )
            .await
        }
        .await;
        let cleanup = self
            .provider
            .delete_role(organization, database, base_branch, &source_role.id, None)
            .await;
        replay.and(cleanup)
    }

    async fn wait_for_backup(
        &self,
        organization: &str,
        database: &str,
        base_branch: &str,
        backup_id: &str,
        name: &str,
    ) -> Result<Backup> {
        for _ in 0..PROVIDER_WAIT_ATTEMPTS {
            let backups = self
                .provider
                .list_backups(organization, database, base_branch)
                .await?;
            if let Some(backup) = backups.into_iter().find(|backup| backup.id == backup_id) {
                match backup.state.as_str() {
                    "success" => return Ok(backup),
                    "failed" | "canceled" | "ignored" => {
                        return Err(RepoboxError::new(
                            ErrorKind::Runtime,
                            "planetscale_backup_failed",
                            format!("backup `{name}` ended in state `{}`", backup.state),
                        ));
                    }
                    _ => {}
                }
            }
            tokio::time::sleep(PROVIDER_WAIT_INTERVAL).await;
        }
        Err(provider_timeout("backup", name))
    }

    async fn wait_for_branch(
        &self,
        organization: &str,
        database: &str,
        branch: &str,
    ) -> Result<()> {
        for _ in 0..PROVIDER_WAIT_ATTEMPTS {
            let value = self
                .provider
                .get_branch(organization, database, branch)
                .await?;
            if value.ready || value.state == "ready" {
                return Ok(());
            }
            tokio::time::sleep(PROVIDER_WAIT_INTERVAL).await;
        }
        Err(provider_timeout("branch", branch))
    }

    async fn wait_for_database(&self, organization: &str, database: &str) -> Result<()> {
        for _ in 0..PROVIDER_WAIT_ATTEMPTS {
            if self
                .provider
                .list_databases(organization)
                .await?
                .into_iter()
                .any(|candidate| candidate.name == database && candidate.ready)
            {
                return Ok(());
            }
            tokio::time::sleep(PROVIDER_WAIT_INTERVAL).await;
        }
        Err(provider_timeout("database", database))
    }

    fn selected_services(
        &self,
        options: &ProvisionOptions,
    ) -> Result<BTreeMap<String, ServiceConfig>> {
        if options.selected_services.is_empty() {
            return Ok(self.config.services.clone());
        }
        let mut selected = BTreeMap::new();
        for name in &options.selected_services {
            let service = self.config.services.get(name).ok_or_else(|| {
                RepoboxError::new(
                    ErrorKind::NotFound,
                    "service_not_found",
                    format!("configured service `{name}` does not exist"),
                )
            })?;
            selected.insert(name.clone(), service.clone());
        }
        Ok(selected)
    }

    fn resumable_job<'b>(
        &self,
        kind: JobKind,
        environment: &str,
        services: impl Iterator<Item = &'b String>,
    ) -> Result<(JobRecord, bool)> {
        let services = services.cloned().collect::<Vec<_>>();
        if let Some(mut job) = self.jobs.list()?.into_iter().rev().find(|job| {
            job.kind == kind
                && job.environment == environment
                && matches!(
                    job.status,
                    JobStatus::Pending | JobStatus::Running | JobStatus::Degraded
                )
        }) {
            for service in &services {
                let name = format!("provision:{service}");
                if !job.steps.iter().any(|step| step.name == name) {
                    job.steps.push(repobox_core::jobs::JobStep {
                        name,
                        status: StepStatus::Pending,
                        attempts: 0,
                        message: None,
                        resource: serde_json::Value::Null,
                    });
                }
            }
            return Ok((job, true));
        }
        Ok((
            JobRecord::new(
                kind,
                self.config.project.id,
                environment,
                services
                    .into_iter()
                    .map(|service| format!("provision:{service}")),
            ),
            false,
        ))
    }

    fn resumable_pull_job<'b>(
        &self,
        environment: &str,
        services: impl Iterator<Item = &'b String>,
    ) -> Result<(JobRecord, bool)> {
        let services = services.cloned().collect::<Vec<_>>();
        if let Some(mut job) = self.jobs.list()?.into_iter().rev().find(|job| {
            job.kind == JobKind::EnvironmentPull
                && job.environment == environment
                && matches!(
                    job.status,
                    JobStatus::Pending | JobStatus::Running | JobStatus::Degraded
                )
        }) {
            for service in &services {
                let name = format!("refresh:{service}");
                if !job.steps.iter().any(|step| step.name == name) {
                    job.steps.push(repobox_core::jobs::JobStep {
                        name,
                        status: StepStatus::Pending,
                        attempts: 0,
                        message: None,
                        resource: serde_json::Value::Null,
                    });
                }
            }
            return Ok((job, true));
        }
        Ok((
            JobRecord::new(
                JobKind::EnvironmentPull,
                self.config.project.id,
                environment,
                services
                    .into_iter()
                    .map(|service| format!("refresh:{service}")),
            ),
            false,
        ))
    }

    fn event<T: Serialize>(&self, event: &str, data: &T) -> Result<()> {
        if self.output.json() {
            self.output.stream(event, data)
        } else {
            Ok(())
        }
    }
}

pub fn environment_variables(
    config: &RepoboxConfig,
    record: &EnvironmentRecord,
    credentials: &CredentialStore,
) -> Result<BTreeMap<String, String>> {
    if record.status != EnvironmentStatus::Ready {
        return Err(RepoboxError::new(
            ErrorKind::Conflict,
            "environment_not_ready",
            format!("environment `{}` is not ready", record.name),
        )
        .with_suggestion(
            "Run `repobox job view latest --json`, then resume the failed operation.",
        ));
    }
    let mut variables = BTreeMap::new();
    for (name, service) in &config.services {
        let binding = record.databases.get(name).ok_or_else(|| {
            RepoboxError::new(
                ErrorKind::NotFound,
                "database_binding_not_found",
                format!("environment has no binding for service `{name}`"),
            )
        })?;
        let key = CredentialStore::database_key(config.project.id, &record.provider_branch, name);
        let (pooled, direct) = credentials.database_urls(&key)?;
        variables.insert(service.env.pooled.clone(), pooled);
        variables.insert(service.env.direct.clone(), direct);
        let _ = binding;
    }
    Ok(variables)
}

pub fn stored_environment_variables(
    config: &RepoboxConfig,
    record: &EnvironmentRecord,
    credentials: &CredentialStore,
) -> BTreeMap<String, String> {
    let mut variables = BTreeMap::new();
    for (name, service) in &config.services {
        if !record.databases.contains_key(name) {
            continue;
        }
        let key = CredentialStore::database_key(config.project.id, &record.provider_branch, name);
        if let Ok((pooled, direct)) = credentials.database_urls(&key) {
            variables.insert(service.env.pooled.clone(), pooled);
            variables.insert(service.env.direct.clone(), direct);
        }
    }
    variables
}

pub fn state_store(
    config: &RepoboxConfig,
    paths: &repobox_core::paths::RepoboxPaths,
) -> StateStore {
    StateStore::new(paths.state(config.project.id))
}

pub fn job_store(config: &RepoboxConfig, paths: &repobox_core::paths::RepoboxPaths) -> JobStore {
    JobStore::new(paths.jobs(config.project.id))
}

fn select_smallest_size(sizes: &[String]) -> Result<String> {
    sizes
        .iter()
        .filter_map(|name| {
            name.trim_start_matches("PS_")
                .trim_start_matches("PS-")
                .chars()
                .take_while(char::is_ascii_digit)
                .collect::<String>()
                .parse::<u32>()
                .ok()
                .map(|size| (size, name))
        })
        .min_by_key(|(size, _)| *size)
        .map(|(_, name)| name.clone())
        .or_else(|| sizes.first().cloned())
        .ok_or_else(|| {
            RepoboxError::new(
                ErrorKind::Conflict,
                "planetscale_cluster_size_unavailable",
                "PlanetScale returned no eligible PostgreSQL cluster sizes",
            )
        })
}

fn role_name(provider_branch: &str, service: &str) -> String {
    let digest = Sha256::digest(format!("{provider_branch}\0{service}").as_bytes());
    let suffix = &hex::encode(digest)[..12];
    let mut service = service.to_owned();
    service.truncate(63 - "repobox--".len() - suffix.len());
    format!("repobox-{service}-{suffix}")
}

fn staging_branch_name(canonical: &str, job_id: uuid::Uuid) -> String {
    let job_id = job_id.simple().to_string();
    let suffix = format!("-next-{}", &job_id[job_id.len() - 8..]);
    let mut base = canonical.to_owned();
    base.truncate(63_usize.saturating_sub(suffix.len()));
    format!("{base}{suffix}")
}

fn set_step_resource(job: &mut JobRecord, step: &str, value: serde_json::Value) -> Result<()> {
    let target = job
        .steps
        .iter_mut()
        .find(|candidate| candidate.name == step)
        .ok_or_else(|| {
            RepoboxError::new(
                ErrorKind::NotFound,
                "job_step_not_found",
                format!("job step `{step}` does not exist"),
            )
        })?;
    target.resource = value;
    job.sequence += 1;
    job.updated_at = Utc::now();
    Ok(())
}

fn provider_timeout(kind: &str, name: &str) -> RepoboxError {
    RepoboxError::new(
        ErrorKind::Runtime,
        "planetscale_operation_timeout",
        format!("timed out waiting for PlanetScale {kind} `{name}`"),
    )
    .with_suggestion("The durable job is safe to resume after checking provider status.")
}

pub fn state_for_environment<'a>(
    state: &'a ProjectState,
    environment: &str,
) -> Result<&'a EnvironmentRecord> {
    state.environments.get(environment).ok_or_else(|| {
        RepoboxError::new(
            ErrorKind::NotFound,
            "environment_not_found",
            format!("environment `{environment}` has not been provisioned"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smallest_cluster_size_uses_numeric_capacity() {
        let sizes = vec![
            "PS_80".to_owned(),
            "PS_10_v2".to_owned(),
            "PS-20".to_owned(),
        ];
        assert_eq!(select_smallest_size(&sizes).unwrap(), "PS_10_v2");
    }

    #[test]
    fn role_names_are_bounded_and_collision_resistant() {
        let branch = "rbx-feature-with-a-very-long-name-1234567890abcdef";
        let first_service = "a".repeat(100);
        let second_service = format!("{}b", "a".repeat(99));
        let first = role_name(branch, &first_service);
        let second = role_name(branch, &second_service);
        assert!(first.len() <= 63);
        assert!(second.len() <= 63);
        assert_ne!(first, second);
        assert_eq!(first, role_name(branch, &first_service));
    }

    #[test]
    fn staging_names_preserve_unique_job_suffix() {
        let first = staging_branch_name(&"x".repeat(63), uuid::Uuid::from_u128(1));
        let second = staging_branch_name(&"x".repeat(63), uuid::Uuid::from_u128(2));
        assert!(first.len() <= 63);
        assert_ne!(first, second);
    }
}
