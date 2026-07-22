#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use assert_cmd::Command;
use predicates::prelude::*;

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
        .stdout(predicate::str::contains("PLANETSCALE_SERVICE_TOKEN=secret"))
        .stdout(predicate::str::contains("--token ").not());
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
