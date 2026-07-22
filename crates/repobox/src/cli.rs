use std::path::PathBuf;

use clap::{ArgAction, Args, Parser, Subcommand, ValueEnum};

#[derive(Clone, Debug, Parser)]
#[command(
    name = "repobox",
    version,
    about = "Persistent, branch-scoped development environments",
    long_about = "Repobox keeps application processes local while moving stateful services into persistent, branch-scoped remote environments. Every command is scriptable; add --json for stable machine-readable output.",
    disable_help_subcommand = true,
    after_help = "QUICK START:\n  repobox auth login\n  cd <repository>\n  repobox run\n\nAGENT START:\n  repobox agent-context --json\n  repobox config detect --json --no-input\n  repobox run --detach --yes --json --no-input\n\nRun `repobox help <topic>` for workflows: setup, agents, data, environments, config, exit-codes."
)]
pub struct Cli {
    /// Emit stable JSON (or JSONL for streaming commands).
    #[arg(long, global = true)]
    pub json: bool,

    /// Print mutations without executing them.
    #[arg(long, global = true)]
    pub dry_run: bool,

    /// Approve required confirmations; mandatory for non-interactive mutations.
    #[arg(long, short = 'y', global = true)]
    pub yes: bool,

    /// Never prompt or open a TUI; explicitly piped stdin remains available.
    #[arg(long, global = true)]
    pub no_input: bool,

    /// Repository to operate on. Defaults to the current directory.
    #[arg(long, global = true, value_name = "PATH")]
    pub repo: Option<PathBuf>,

    /// Terminal color behavior.
    #[arg(long, global = true, default_value = "auto")]
    pub color: ColorChoice,

    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum ColorChoice {
    Auto,
    Always,
    Never,
}

#[derive(Clone, Debug, Subcommand)]
pub enum Command {
    /// Detect a repository and create .repobox.yml.
    #[command(
        after_long_help = "EXAMPLES:\n  repobox init\n  repobox init --organization acme --database db=my-app --yes\n  repobox init --runtime native --organization acme -- node server.js"
    )]
    Init(InitArgs),

    /// Provision the current data environment and start local services.
    #[command(
        after_long_help = "EXAMPLES:\n  repobox run\n  repobox run --detach\n  repobox run --environment feature/demo --detach --create-backup --wait --yes --json --no-input"
    )]
    Run(RunArgs),

    /// Stop local services started by Repobox.
    #[command(
        after_long_help = "EXAMPLES:\n  repobox stop\n  repobox stop --environment feature/demo --json"
    )]
    Stop(EnvironmentSelector),

    /// Replace an environment's databases from the latest base backups.
    #[command(
        after_long_help = "EXAMPLES:\n  repobox pull\n  repobox pull --database db --create-backup --yes\n  repobox pull --environment feature/demo --dry-run --json"
    )]
    Pull(PullArgs),

    /// Show local runtime and remote environment state.
    #[command(
        after_long_help = "EXAMPLES:\n  repobox status\n  repobox status --environment feature/demo --json"
    )]
    Status(EnvironmentSelector),

    /// Stream service logs.
    #[command(
        after_long_help = "EXAMPLES:\n  repobox logs --follow\n  repobox logs web --tail 50 --json"
    )]
    Logs(LogsArgs),

    /// Authenticate with infrastructure providers.
    #[command(
        subcommand,
        after_long_help = "EXAMPLES:\n  repobox auth login\n  repobox auth status --json"
    )]
    Auth(AuthCommand),

    /// Create, inspect, and remove data environments.
    #[command(
        subcommand,
        after_long_help = "EXAMPLES:\n  repobox env list --json\n  repobox env create feature/demo --yes"
    )]
    Env(EnvCommand),

    /// Inspect and control local services.
    #[command(
        subcommand,
        after_long_help = "EXAMPLES:\n  repobox service list --json\n  repobox service logs web --follow"
    )]
    Service(ServiceCommand),

    /// Inspect and resume durable operations.
    #[command(
        subcommand,
        after_long_help = "EXAMPLES:\n  repobox job view latest --json\n  repobox job resume 018f6f4e-7040-7000-8000-000000000001"
    )]
    Job(JobCommand),

    /// Detect, validate, and update repository configuration.
    #[command(
        subcommand,
        after_long_help = "EXAMPLES:\n  repobox config detect --json\n  repobox config validate"
    )]
    Config(ConfigCommand),

    /// Inspect local telemetry preference (no events are sent in v0.1).
    #[command(
        subcommand,
        after_long_help = "EXAMPLES:\n  repobox telemetry status --json\n  repobox telemetry disable"
    )]
    Telemetry(TelemetryCommand),

    /// Check for a newer Repobox release and print the exact upgrade command.
    #[command(after_long_help = "EXAMPLES:\n  repobox update --check --json\n  repobox update")]
    Update(UpdateArgs),

    /// Check dependencies, authentication, configuration, and connectivity.
    #[command(
        after_long_help = "EXAMPLES:\n  repobox doctor --json\n  repobox doctor --online --json"
    )]
    Doctor(DoctorArgs),

    /// Generate shell completion source.
    #[command(
        after_long_help = "EXAMPLES:\n  repobox completion zsh > ~/.zfunc/_repobox\n  repobox completion fish > ~/.config/fish/completions/repobox.fish"
    )]
    Completion(CompletionArgs),

    /// Describe commands and project state for an LLM caller.
    #[command(
        after_long_help = "EXAMPLES:\n  repobox agent-context --json\n  repobox agent-context --schemas --json"
    )]
    AgentContext(AgentContextArgs),

    /// Explain a Repobox workflow or contract.
    #[command(
        after_long_help = "EXAMPLES:\n  repobox help agents\n  repobox help exit-codes --json"
    )]
    Help(HelpArgs),
}

#[derive(Clone, Debug, Args)]
pub struct InitArgs {
    /// Runtime to configure.
    #[arg(long, default_value = "auto")]
    pub runtime: InitRuntime,

    /// `PlanetScale` organization name.
    #[arg(long, env = "REPOBOX_PLANETSCALE_ORG")]
    pub organization: Option<String>,

    /// Map a detected service to a `PlanetScale` database (SERVICE=DATABASE).
    #[arg(long, value_name = "SERVICE=DATABASE", action = ArgAction::Append)]
    pub database: Vec<String>,

    /// How a new remote database should be initialized.
    #[arg(long, default_value = "empty")]
    pub data: BootstrapChoice,

    /// Replace an existing .repobox.yml.
    #[arg(long)]
    pub force: bool,

    /// Native command argv. Place after `--`.
    #[arg(last = true)]
    pub command: Vec<String>,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum InitRuntime {
    Auto,
    Compose,
    Native,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum BootstrapChoice {
    Attach,
    Empty,
    Import,
}

#[derive(Clone, Debug, Args)]
pub struct RunArgs {
    #[command(flatten)]
    pub environment: EnvironmentSelector,

    /// Leave local services running after the command exits.
    #[arg(long)]
    pub detach: bool,

    /// Disable the full-screen interface and stream plain text.
    #[arg(long)]
    pub no_tui: bool,

    /// Create an immediate base backup if provisioning needs one.
    #[arg(long)]
    pub create_backup: bool,

    /// Wait for an in-progress base backup instead of failing fast.
    #[arg(long)]
    pub wait: bool,
}

#[derive(Clone, Debug, Args)]
pub struct PullArgs {
    #[command(flatten)]
    pub environment: EnvironmentSelector,

    /// Refresh only this configured database service. Repeatable.
    #[arg(long, value_name = "SERVICE", action = ArgAction::Append)]
    pub database: Vec<String>,

    /// Create an immediate base backup when no successful backup exists.
    #[arg(long)]
    pub create_backup: bool,

    /// Wait for an in-progress backup instead of failing fast.
    #[arg(long)]
    pub wait: bool,
}

#[derive(Clone, Debug, Default, Args)]
pub struct EnvironmentSelector {
    /// Environment name. Defaults to `REPOBOX_ENV`, then the current Git branch.
    #[arg(long, short = 'e', env = "REPOBOX_ENV")]
    pub environment: Option<String>,
}

#[derive(Clone, Debug, Args)]
pub struct LogsArgs {
    /// Service name. Omit to show all services.
    pub service: Option<String>,

    #[command(flatten)]
    pub environment: EnvironmentSelector,

    /// Continue streaming new log lines.
    #[arg(long, short = 'f')]
    pub follow: bool,

    /// Number of historical lines per service.
    #[arg(long, default_value_t = 200)]
    pub tail: usize,
}

#[derive(Clone, Debug, Subcommand)]
pub enum AuthCommand {
    /// Authenticate through a browser or service-token environment variables.
    #[command(
        after_long_help = "EXAMPLES:\n  repobox auth login\n  repobox auth login --no-browser\n  repobox auth login --json --no-input\n  export PLANETSCALE_SERVICE_TOKEN_ID=id\n  export PLANETSCALE_SERVICE_TOKEN=secret\n  repobox auth login --no-input --json"
    )]
    Login(AuthLoginArgs),
    /// Show where credentials resolve from and validate them.
    #[command(after_long_help = "EXAMPLES:\n  repobox auth status\n  repobox auth status --json")]
    Status,
    /// Revoke browser auth and remove locally stored credentials.
    #[command(
        after_long_help = "EXAMPLES:\n  repobox auth logout\n  repobox auth logout --yes --no-input --json"
    )]
    Logout,
}

#[derive(Clone, Debug, Args)]
pub struct AuthLoginArgs {
    /// Use service-token auth with this ID instead of browser auth.
    #[arg(long, env = "PLANETSCALE_SERVICE_TOKEN_ID")]
    pub token_id: Option<String>,

    /// Print the device-approval URL without opening a browser.
    #[arg(long)]
    pub no_browser: bool,
}

#[derive(Clone, Debug, Subcommand)]
pub enum EnvCommand {
    /// List known environments.
    #[command(after_long_help = "EXAMPLES:\n  repobox env list\n  repobox env list --json")]
    List,
    /// Provision an environment without starting services.
    #[command(
        after_long_help = "EXAMPLES:\n  repobox env create feature/demo --yes\n  repobox env create main --create-backup --wait --dry-run --json"
    )]
    Create(EnvCreateArgs),
    /// Delete an environment's remote database branches.
    #[command(
        after_long_help = "EXAMPLES:\n  repobox env delete feature/demo --dry-run --json\n  repobox env delete feature/demo --yes"
    )]
    Delete(EnvDeleteArgs),
    /// Delete Repobox environments whose Git branches are merged.
    #[command(
        after_long_help = "EXAMPLES:\n  repobox env prune --dry-run --json\n  repobox env prune --fetch --yes"
    )]
    Prune(EnvPruneArgs),
}

#[derive(Clone, Debug, Args)]
pub struct EnvCreateArgs {
    /// Environment name. Defaults to the current Git branch.
    pub name: Option<String>,

    /// Create an immediate base backup if necessary.
    #[arg(long)]
    pub create_backup: bool,

    /// Wait for an in-progress base backup instead of failing fast.
    #[arg(long)]
    pub wait: bool,
}

#[derive(Clone, Debug, Args)]
pub struct EnvDeleteArgs {
    pub name: String,

    /// Keep local metadata after removing provider resources.
    #[arg(long)]
    pub keep_state: bool,
}

#[derive(Clone, Debug, Args)]
pub struct EnvPruneArgs {
    /// Fetch and prune remote Git refs before finding merged branches.
    #[arg(long)]
    pub fetch: bool,
}

#[derive(Clone, Debug, Subcommand)]
pub enum ServiceCommand {
    /// List local and remote service bindings.
    #[command(
        after_long_help = "EXAMPLES:\n  repobox service list\n  repobox service list --environment feature/demo --json"
    )]
    List(EnvironmentSelector),
    /// Show one service's state.
    #[command(
        after_long_help = "EXAMPLES:\n  repobox service status web\n  repobox service status web --json"
    )]
    Status(ServiceTarget),
    /// Restart a local service.
    #[command(
        after_long_help = "EXAMPLES:\n  repobox service restart web\n  repobox service restart worker --json"
    )]
    Restart(ServiceTarget),
    /// Stream one service's logs.
    #[command(
        after_long_help = "EXAMPLES:\n  repobox service logs web --follow\n  repobox service logs worker --tail 50 --json"
    )]
    Logs(ServiceLogsArgs),
}

#[derive(Clone, Debug, Args)]
pub struct ServiceTarget {
    pub service: String,

    #[command(flatten)]
    pub environment: EnvironmentSelector,
}

#[derive(Clone, Debug, Args)]
pub struct ServiceLogsArgs {
    pub service: String,

    #[arg(long, short = 'f')]
    pub follow: bool,

    #[arg(long, default_value_t = 200)]
    pub tail: usize,
}

#[derive(Clone, Debug, Subcommand)]
pub enum JobCommand {
    /// List durable jobs for this project.
    #[command(after_long_help = "EXAMPLES:\n  repobox job list\n  repobox job list --json")]
    List,
    /// Show a job by UUID or `latest`.
    #[command(
        after_long_help = "EXAMPLES:\n  repobox job view latest --json\n  repobox job view 018f6f4e-7040-7000-8000-000000000001 --exit-status"
    )]
    View(JobViewArgs),
    /// Resume an interrupted or degraded job.
    #[command(
        after_long_help = "EXAMPLES:\n  repobox job resume 018f6f4e-7040-7000-8000-000000000001 --wait\n  repobox job resume 018f6f4e-7040-7000-8000-000000000001 --create-backup --dry-run --json"
    )]
    Resume(JobResumeArgs),
    /// Mark a non-terminal job canceled.
    #[command(
        after_long_help = "EXAMPLES:\n  repobox job cancel 018f6f4e-7040-7000-8000-000000000001\n  repobox job cancel 018f6f4e-7040-7000-8000-000000000001 --yes --json"
    )]
    Cancel(JobTarget),
}

#[derive(Clone, Debug, Args)]
pub struct JobViewArgs {
    pub id: String,

    /// Exit non-zero unless the job succeeded.
    #[arg(long)]
    pub exit_status: bool,
}

#[derive(Clone, Debug, Args)]
pub struct JobTarget {
    pub id: String,
}

#[derive(Clone, Debug, Args)]
pub struct JobResumeArgs {
    pub id: String,

    /// Create an immediate base backup if the resumed operation needs one.
    #[arg(long)]
    pub create_backup: bool,

    /// Wait for an in-progress base backup instead of failing fast.
    #[arg(long)]
    pub wait: bool,
}

#[derive(Clone, Debug, Subcommand)]
pub enum ConfigCommand {
    /// Detect runtime and database services without writing files.
    #[command(
        after_long_help = "EXAMPLES:\n  repobox config detect\n  repobox config detect --json"
    )]
    Detect,
    /// Print the resolved repository configuration.
    #[command(after_long_help = "EXAMPLES:\n  repobox config view\n  repobox config view --json")]
    View,
    /// Print the versioned JSON Schema.
    #[command(
        after_long_help = "EXAMPLES:\n  repobox config schema --json\n  repobox config schema --json | jq '.data'"
    )]
    Schema,
    /// Validate a config file (defaults to discovered .repobox.yml).
    #[command(
        after_long_help = "EXAMPLES:\n  repobox config validate\n  repobox config validate ./fixtures/repobox.yml --json"
    )]
    Validate(ConfigValidateArgs),
    /// Apply an RFC 7396 JSON Merge Patch atomically.
    #[command(
        after_long_help = "EXAMPLES:\n  repobox config update --patch '{\"agents\":{\"claude\":true}}' --dry-run --json\n  repobox config update --patch '{\"data\":{\"allow_copy\":true}}'"
    )]
    Update(ConfigUpdateArgs),
}

#[derive(Clone, Debug, Args)]
pub struct ConfigValidateArgs {
    pub path: Option<PathBuf>,
}

#[derive(Clone, Debug, Args)]
pub struct ConfigUpdateArgs {
    /// JSON Merge Patch object.
    #[arg(long, value_name = "JSON")]
    pub patch: Option<String>,
}

#[derive(Clone, Copy, Debug, Subcommand)]
pub enum TelemetryCommand {
    #[command(
        after_long_help = "EXAMPLES:\n  repobox telemetry status\n  repobox telemetry status --json"
    )]
    Status,
    #[command(
        after_long_help = "EXAMPLES:\n  repobox telemetry enable\n  repobox telemetry enable --json"
    )]
    Enable,
    #[command(
        after_long_help = "EXAMPLES:\n  repobox telemetry disable\n  repobox telemetry disable --json"
    )]
    Disable,
}

#[derive(Clone, Debug, Args)]
pub struct UpdateArgs {
    /// Only report whether an update exists.
    #[arg(long)]
    pub check: bool,
}

#[derive(Clone, Debug, Args)]
pub struct DoctorArgs {
    /// Include provider network checks.
    #[arg(long)]
    pub online: bool,
}

#[derive(Clone, Debug, Args)]
pub struct CompletionArgs {
    pub shell: CompletionShell,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum CompletionShell {
    Bash,
    Elvish,
    Fish,
    PowerShell,
    Zsh,
}

#[derive(Clone, Debug, Default, Args)]
pub struct AgentContextArgs {
    /// Include the versioned config and output schemas.
    #[arg(long)]
    pub schemas: bool,
}

#[derive(Clone, Debug, Args)]
pub struct HelpArgs {
    /// setup, agents, data, environment, environments, formatting, config, or exit-codes.
    pub topic: Option<String>,
}
