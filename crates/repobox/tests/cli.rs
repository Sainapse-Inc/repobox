#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use assert_cmd::Command;
use chrono::Utc;
use predicates::prelude::*;
use repobox_core::config::RepoboxConfig;
use repobox_core::identity::provider_branch_name;
use repobox_core::jobs::{JobKind, JobRecord, JobStatus, JobStore};
use repobox_core::paths::RepoboxPaths;
use repobox_core::state::{
    DatabaseBinding, EnvironmentRecord, EnvironmentStatus, ProjectState, StateStore,
};

fn fake_repository() -> tempfile::TempDir {
    let directory = tempfile::tempdir().unwrap();
    fs::write(
        directory.path().join("compose.yaml"),
        "services:\n  db:\n    image: postgres:18\n  audit:\n    image: postgres:17\n  app:\n    image: node:24\n    depends_on: [db, audit]\n",
    )
    .unwrap();
    let binary_directory = directory.path().join("bin");
    fs::create_dir(&binary_directory).unwrap();
    let docker = binary_directory.join("docker");
    fs::write(
        &docker,
        r#"#!/bin/sh
if [ "$1" = "compose" ]; then
  printf '%s\n' '{"name":"fixture","services":{"db":{"image":"postgres:18","environment":{"POSTGRES_DB":"app"}},"audit":{"image":"postgres:17","environment":{}},"app":{"image":"node:24","depends_on":{"db":{"condition":"service_started"},"audit":{"condition":"service_started"}},"environment":{}}}}'
  exit 0
fi
exit 1
"#,
    )
    .unwrap();
    fs::set_permissions(&docker, fs::Permissions::from_mode(0o755)).unwrap();
    directory
}

fn command(repository: &Path) -> Command {
    let mut command = Command::cargo_bin("repobox").unwrap();
    let path = format!(
        "{}:{}",
        repository.join("bin").display(),
        std::env::var("PATH").unwrap_or_default()
    );
    command.current_dir(repository).env("PATH", path);
    command
}

fn isolated_command(repository: &Path) -> Command {
    let mut command = command(repository);
    let home = repository.join(".test-home");
    command
        .env("HOME", &home)
        .env("XDG_CONFIG_HOME", repository.join(".test-xdg/config"))
        .env("XDG_STATE_HOME", repository.join(".test-xdg/state"))
        .env("XDG_CACHE_HOME", repository.join(".test-xdg/cache"))
        .env("REPOBOX_NO_UPDATE_CHECK", "1")
        .env_remove("PLANETSCALE_SERVICE_TOKEN_ID")
        .env_remove("PLANETSCALE_SERVICE_TOKEN");
    command
}

fn isolated_paths(repository: &Path) -> RepoboxPaths {
    #[cfg(target_os = "macos")]
    {
        let home = repository.join(".test-home");
        let support = home
            .join("Library/Application Support")
            .join("dev.abhirupghosh.repobox");
        return RepoboxPaths {
            config_dir: support.clone(),
            state_dir: support,
            cache_dir: home.join("Library/Caches").join("dev.abhirupghosh.repobox"),
        };
    }

    #[cfg(not(target_os = "macos"))]
    RepoboxPaths {
        config_dir: repository.join(".test-xdg/config/repobox"),
        state_dir: repository.join(".test-xdg/state/repobox"),
        cache_dir: repository.join(".test-xdg/cache/repobox"),
    }
}

fn repository_with_resumable_job(kind: JobKind) -> (tempfile::TempDir, JobRecord, PathBuf) {
    let repository = fake_repository();
    isolated_command(repository.path())
        .args([
            "init",
            "--organization",
            "acme",
            "--yes",
            "--no-input",
            "--json",
        ])
        .assert()
        .success();

    let config = RepoboxConfig::load(&repository.path().join(".repobox.yml")).unwrap();
    let step = match kind {
        JobKind::EnvironmentCreate => "provision:db",
        JobKind::EnvironmentPull => "refresh:db",
        _ => panic!("fixture only supports resumable environment jobs"),
    };
    let mut job = JobRecord::new(kind, config.project.id, "feature/resume-contract", [step]);
    job.status = JobStatus::Degraded;
    let ledger = isolated_paths(repository.path()).jobs(config.project.id);
    JobStore::new(&ledger).append(&job).unwrap();
    (repository, job, ledger)
}

fn repository_with_old_deleted_pull(
    status: JobStatus,
) -> (tempfile::TempDir, JobRecord, PathBuf, PathBuf) {
    let repository = fake_repository();
    isolated_command(repository.path())
        .args([
            "init",
            "--organization",
            "acme",
            "--yes",
            "--no-input",
            "--json",
        ])
        .assert()
        .success();
    let config = RepoboxConfig::load(&repository.path().join(".repobox.yml")).unwrap();
    let environment = "feature/run-guard";
    let canonical = provider_branch_name(config.project.id, environment).unwrap();
    let paths = isolated_paths(repository.path());
    let state_path = paths.state(config.project.id);
    let state_store = StateStore::new(&state_path);
    let mut state = ProjectState::new(config.project.id);
    let mut record = EnvironmentRecord::new(environment, &canonical);
    record.status = EnvironmentStatus::Degraded;
    state.environments.insert(environment.to_owned(), record);
    state_store.save(&state).unwrap();

    let mut pull = JobRecord::new(
        JobKind::EnvironmentPull,
        config.project.id,
        environment,
        ["refresh:db"],
    );
    pull.status = status;
    pull.steps[0].resource = serde_json::json!({
        "phase": "old_deleted",
        "organization": "acme",
        "database": "app",
        "base_branch": "main",
        "canonical": canonical,
        "staging": format!("{canonical}-next-test"),
    });
    let ledger = paths.jobs(config.project.id);
    JobStore::new(&ledger).append(&pull).unwrap();
    (repository, pull, state_path, ledger)
}

fn blocked_run_error_for_old_deleted_pull(status: JobStatus) -> (serde_json::Value, String) {
    let (repository, pull, state_path, ledger) = repository_with_old_deleted_pull(status);
    let state_before = fs::read(&state_path).unwrap();
    let ledger_before = fs::read(&ledger).unwrap();
    let marker = repository.path().join("runtime-mutated");
    let docker = repository.path().join("bin/docker");
    fs::write(
        &docker,
        "#!/bin/sh\n: > \"$REPOBOX_TEST_RUNTIME_MARKER\"\nexit 99\n",
    )
    .unwrap();
    fs::set_permissions(&docker, fs::Permissions::from_mode(0o755)).unwrap();

    let output = isolated_command(repository.path())
        .env("REPOBOX_TEST_RUNTIME_MARKER", &marker)
        .args([
            "run",
            "--environment",
            "feature/run-guard",
            "--yes",
            "--no-tui",
            "--json",
            "--no-input",
        ])
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(5));
    assert!(output.stdout.is_empty());
    assert_eq!(fs::read(&state_path).unwrap(), state_before);
    assert_eq!(fs::read(&ledger).unwrap(), ledger_before);
    assert!(!marker.exists());
    (
        serde_json::from_slice(&output.stderr).unwrap(),
        pull.id.to_string(),
    )
}

#[test]
fn top_level_help_is_clean_and_complete() {
    Command::cargo_bin("repobox")
        .unwrap()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("repobox agent-context --json"))
        .stdout(predicate::str::contains("pull"))
        .stdout(predicate::str::contains("doctor"));
}

#[test]
fn json_version_uses_the_stable_envelope() {
    let output = Command::cargo_bin("repobox")
        .unwrap()
        .args(["--json", "--version"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["schema_version"], 1);
    assert_eq!(value["command"], "version");
    assert_eq!(value["data"]["version"], env!("CARGO_PKG_VERSION"));
}

#[test]
fn service_token_secret_is_not_an_argv_flag() {
    Command::cargo_bin("repobox")
        .unwrap()
        .args(["auth", "login", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("browser"))
        .stdout(predicate::str::contains("--no-browser"))
        .stdout(predicate::str::contains("PLANETSCALE_SERVICE_TOKEN=secret"))
        .stdout(predicate::str::contains("--token ").not());
}

#[test]
fn human_browser_login_rejects_a_non_tty_before_network_access() {
    Command::cargo_bin("repobox")
        .unwrap()
        .args(["auth", "login", "--no-browser"])
        .assert()
        .code(4)
        .stderr(predicate::str::contains("interactive_shell_required"))
        .stderr(predicate::str::contains("--json --no-input"));
}

#[test]
fn usage_errors_are_structured_when_json_is_requested() {
    let output = Command::cargo_bin("repobox")
        .unwrap()
        .args(["--json", "definitely-not-a-command"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    let value: serde_json::Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(value["error"]["kind"], "usage");
    assert_eq!(value["error"]["code"], "cli_usage_error");
}

#[test]
fn detect_and_init_cover_every_postgres_service() {
    let repository = fake_repository();
    let detected = command(repository.path())
        .args(["config", "detect", "--json"])
        .output()
        .unwrap();
    assert!(detected.status.success());
    let detected: serde_json::Value = serde_json::from_slice(&detected.stdout).unwrap();
    assert_eq!(detected["data"]["driver"], "compose");
    assert_eq!(detected["data"]["services"].as_array().unwrap().len(), 3);

    let initialized = command(repository.path())
        .args([
            "init",
            "--organization",
            "acme",
            "--yes",
            "--no-input",
            "--json",
        ])
        .output()
        .unwrap();
    assert!(
        initialized.status.success(),
        "{}",
        String::from_utf8_lossy(&initialized.stderr)
    );
    let initialized: serde_json::Value = serde_json::from_slice(&initialized.stdout).unwrap();
    assert_eq!(initialized["data"]["services"].as_array().unwrap().len(), 2);
    assert!(initialized["data"]["undo_command"].is_null());
    assert!(initialized["data"]["undo_reason"].is_string());
    assert!(repository.path().join(".repobox.yml").is_file());
    let config = fs::read_to_string(repository.path().join(".repobox.yml")).unwrap();
    assert!(config.contains("DATABASE_URL"));
    assert!(config.contains("DB_DIRECT_DATABASE_URL"));
    assert!(
        fs::read_to_string(repository.path().join("CLAUDE.md"))
            .unwrap()
            .contains("<!-- repobox:start -->")
    );
    assert!(
        fs::read_to_string(repository.path().join("AGENTS.md"))
            .unwrap()
            .contains("repobox agent-context --json")
    );
}

#[test]
fn logs_environment_flag_overrides_repobox_env() {
    let repository = fake_repository();
    isolated_command(repository.path())
        .args([
            "init",
            "--organization",
            "acme",
            "--yes",
            "--no-input",
            "--json",
        ])
        .assert()
        .success();

    isolated_command(repository.path())
        .env("REPOBOX_ENV", "invalid environment")
        .args(["logs", "app", "--json", "--no-input"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("invalid_environment_name"));

    isolated_command(repository.path())
        .env("REPOBOX_ENV", "invalid environment")
        .args([
            "logs",
            "app",
            "--environment",
            "smoke/e2e",
            "--tail",
            "1",
            "--json",
            "--no-input",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"event\":\"log\""));
}

#[test]
fn environment_dry_run_needs_no_provider_credentials() {
    let repository = fake_repository();
    command(repository.path())
        .args([
            "init",
            "--organization",
            "acme",
            "--yes",
            "--no-input",
            "--json",
        ])
        .assert()
        .success();
    let output = command(repository.path())
        .env_remove("PLANETSCALE_SERVICE_TOKEN_ID")
        .env_remove("PLANETSCALE_SERVICE_TOKEN")
        .args([
            "env",
            "create",
            "feature/dry-run",
            "--dry-run",
            "--json",
            "--no-input",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["data"]["operation"], "environment_create");
}

#[test]
fn environment_delete_dry_run_lists_exact_checkpointed_targets() {
    let repository = fake_repository();
    isolated_command(repository.path())
        .args([
            "init",
            "--organization",
            "acme",
            "--yes",
            "--no-input",
            "--json",
        ])
        .assert()
        .success();
    let config = RepoboxConfig::load(&repository.path().join(".repobox.yml")).unwrap();
    let environment = "feature/delete-plan";
    let canonical = provider_branch_name(config.project.id, environment).unwrap();
    let paths = isolated_paths(repository.path());
    let state_store = StateStore::new(paths.state(config.project.id));
    let mut state = ProjectState::new(config.project.id);
    let mut record = EnvironmentRecord::new(environment, &canonical);
    record.status = EnvironmentStatus::Ready;
    record.databases.insert(
        "db".to_owned(),
        DatabaseBinding {
            service: "db".to_owned(),
            provider: "planetscale".to_owned(),
            organization: "checkpointed-org".to_owned(),
            database: "checkpointed-db".to_owned(),
            branch: canonical.clone(),
            role_id: "role-1".to_owned(),
            role_name: "repobox-db".to_owned(),
            ready: true,
            updated_at: Utc::now(),
        },
    );
    state.environments.insert(environment.to_owned(), record);
    state_store.save(&state).unwrap();

    let output = isolated_command(repository.path())
        .args([
            "env",
            "delete",
            environment,
            "--dry-run",
            "--yes",
            "--json",
            "--no-input",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["data"]["operation"], "environment_delete");
    assert_eq!(value["data"]["rollback_available"], false);
    assert_eq!(
        value["data"]["provider_calls"],
        serde_json::json!([{
            "provider": "planetscale",
            "action": "delete_branch",
            "resource": format!("checkpointed-org/checkpointed-db/{canonical} (db)"),
        }])
    );
}

#[test]
fn job_resume_requires_structured_confirmation_before_provider_access() {
    let (repository, job, ledger) = repository_with_resumable_job(JobKind::EnvironmentCreate);
    let before = fs::read(&ledger).unwrap();
    let output = isolated_command(repository.path())
        .args(["job", "resume", &job.id.to_string(), "--json", "--no-input"])
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    let error: serde_json::Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["error"]["kind"], "usage");
    assert_eq!(error["error"]["code"], "confirmation_required");
    assert!(
        error["error"]["message"]
            .as_str()
            .unwrap()
            .contains(&job.id.to_string())
    );
    assert_eq!(fs::read(&ledger).unwrap(), before);
}

#[test]
fn pull_job_resume_requires_confirmation_before_runtime_or_provider_access() {
    let (repository, job, ledger) = repository_with_resumable_job(JobKind::EnvironmentPull);
    let before = fs::read(&ledger).unwrap();
    let output = isolated_command(repository.path())
        .args(["job", "resume", &job.id.to_string(), "--json", "--no-input"])
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(2));
    let error: serde_json::Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["error"]["code"], "confirmation_required");
    assert_eq!(fs::read(&ledger).unwrap(), before);
}

#[test]
fn run_blocks_old_deleted_pull_before_provider_or_runtime_mutation() {
    let (error, job_id) = blocked_run_error_for_old_deleted_pull(JobStatus::Degraded);
    assert_eq!(error["error"]["code"], "environment_recovery_required");
    assert!(
        error["error"]["message"]
            .as_str()
            .unwrap()
            .contains(&job_id)
    );
    assert!(
        error["error"]["suggestion"]
            .as_str()
            .unwrap()
            .contains(&job_id)
    );
}

#[test]
fn run_requires_delete_after_an_old_deleted_pull_is_canceled() {
    let (error, job_id) = blocked_run_error_for_old_deleted_pull(JobStatus::Canceled);
    assert_eq!(error["error"]["code"], "environment_recovery_required");
    let suggestion = error["error"]["suggestion"].as_str().unwrap();
    assert!(suggestion.contains(&job_id));
    assert!(suggestion.contains("repobox env delete feature/run-guard --yes"));
    assert!(!suggestion.contains("job resume"));
}

#[test]
fn job_resume_yes_satisfies_the_confirmation_gate() {
    let (repository, job, ledger) = repository_with_resumable_job(JobKind::EnvironmentCreate);
    let before = fs::read(&ledger).unwrap();
    let output = isolated_command(repository.path())
        .env("PLANETSCALE_SERVICE_TOKEN_ID", "test-id")
        .args([
            "job",
            "resume",
            &job.id.to_string(),
            "--yes",
            "--json",
            "--no-input",
        ])
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(4));
    let error: serde_json::Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["error"]["code"], "incomplete_planetscale_environment");
    assert_eq!(fs::read(&ledger).unwrap(), before);
}

#[test]
fn job_resume_dry_run_needs_no_confirmation_or_provider_credentials() {
    let (repository, job, ledger) = repository_with_resumable_job(JobKind::EnvironmentCreate);
    let before = fs::read(&ledger).unwrap();
    let output = isolated_command(repository.path())
        .args([
            "job",
            "resume",
            &job.id.to_string(),
            "--dry-run",
            "--json",
            "--no-input",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["command"], "job resume");
    assert_eq!(value["data"]["operation"], "environment_create");
    assert_eq!(value["data"]["environment"], job.environment);
    assert_eq!(fs::read(&ledger).unwrap(), before);
}

#[test]
fn config_update_accepts_piped_json_without_writing_on_dry_run() {
    let repository = fake_repository();
    command(repository.path())
        .args([
            "init",
            "--organization",
            "acme",
            "--yes",
            "--no-input",
            "--json",
        ])
        .assert()
        .success();
    let before = fs::read(repository.path().join(".repobox.yml")).unwrap();
    command(repository.path())
        .args(["config", "update", "--dry-run", "--json", "--no-input"])
        .write_stdin(r#"{"data":{"allow_copy":true}}"#)
        .assert()
        .success()
        .stdout(predicate::str::contains(r#""allow_copy": true"#));
    assert_eq!(
        fs::read(repository.path().join(".repobox.yml")).unwrap(),
        before
    );
}

#[test]
fn committed_schema_snapshots_match_the_binary() {
    let repository_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let output = Command::cargo_bin("repobox")
        .unwrap()
        .current_dir(&repository_root)
        .args(["agent-context", "--schemas", "--json", "--no-input"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    for (key, file) in [
        ("config", "config-v1.json"),
        ("success", "success-v1.json"),
        ("error", "error-v1.json"),
        ("stream", "stream-v1.json"),
        ("mutation", "mutation-v1.json"),
        ("dry_run", "dry-run-v1.json"),
    ] {
        let expected: serde_json::Value = serde_json::from_slice(
            &fs::read(repository_root.join("docs/schemas").join(file)).unwrap(),
        )
        .unwrap();
        assert_eq!(value["data"]["schemas"][key], expected, "{file} drifted");
    }
}

#[test]
fn init_dry_run_writes_nothing() {
    let repository = fake_repository();
    command(repository.path())
        .args([
            "init",
            "--organization",
            "acme",
            "--dry-run",
            "--json",
            "--no-input",
        ])
        .assert()
        .success();
    assert!(!repository.path().join(".repobox.yml").exists());
    assert!(!repository.path().join("CLAUDE.md").exists());
    assert!(!repository.path().join("AGENTS.md").exists());
}
