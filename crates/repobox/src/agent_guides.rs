use std::fs;
use std::path::{Path, PathBuf};

use repobox_core::config::RepoboxConfig;
use repobox_core::{ErrorKind, RepoboxError, Result};

const START: &str = "<!-- repobox:start -->";
const END: &str = "<!-- repobox:end -->";

pub fn update(repository: &Path, config: &RepoboxConfig, dry_run: bool) -> Result<Vec<PathBuf>> {
    let block = managed_block(config);
    let mut changed = vec![];
    for (enabled, name) in [
        (config.agents.claude, "CLAUDE.md"),
        (config.agents.codex, "AGENTS.md"),
    ] {
        if !enabled {
            continue;
        }
        let path = repository.join(name);
        let current = match fs::read_to_string(&path) {
            Ok(current) => current,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(error) => return Err(error.into()),
        };
        let updated = replace_block(&current, &block)?;
        if updated == current {
            continue;
        }
        changed.push(path.clone());
        if !dry_run {
            let temporary = path.with_extension("md.repobox.tmp");
            fs::write(&temporary, updated)?;
            fs::rename(temporary, path)?;
        }
    }
    Ok(changed)
}

fn managed_block(config: &RepoboxConfig) -> String {
    let services = config
        .services
        .keys()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "{START}\n## Repobox development environment\n\nThis repository uses Repobox for persistent, branch-scoped development data ({services}).\n\n- Inspect the machine contract first: `repobox agent-context --json`.\n- Start or reconnect without blocking: `repobox run --detach --yes --json --no-input`.\n- Refresh data only when explicitly requested: `repobox pull --yes --json --no-input` is destructive for the current environment.\n- Use `repobox status --json` and `repobox job view latest --json` for diagnosis.\n- Never print, persist, or commit database URLs or PlanetScale credentials; Repobox injects them into child processes.\n- Every Git branch, including `{}`, maps to an isolated provider branch.\n{END}\n",
        config.project.git.base_branch
    )
}

fn replace_block(current: &str, block: &str) -> Result<String> {
    match (current.find(START), current.find(END)) {
        (Some(start), Some(end)) if end >= start => {
            let end = end + END.len();
            let mut output = String::with_capacity(current.len() + block.len());
            output.push_str(&current[..start]);
            output.push_str(block.trim_end());
            output.push_str(&current[end..]);
            if !output.ends_with('\n') {
                output.push('\n');
            }
            Ok(output)
        }
        (None, None) => {
            let mut output = current.trim_end().to_owned();
            if !output.is_empty() {
                output.push_str("\n\n");
            }
            output.push_str(block);
            Ok(output)
        }
        _ => Err(RepoboxError::new(
            ErrorKind::Conflict,
            "agent_guide_block_malformed",
            "found only one Repobox managed-block marker",
        )
        .with_suggestion(format!(
            "Repair the `{START}` / `{END}` markers, then rerun `repobox init`."
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn managed_block_is_idempotent() {
        let first = replace_block(
            "# Guide\n",
            "<!-- repobox:start -->\nx\n<!-- repobox:end -->\n",
        )
        .unwrap();
        let second =
            replace_block(&first, "<!-- repobox:start -->\nx\n<!-- repobox:end -->\n").unwrap();
        assert_eq!(first, second);
    }

    #[test]
    fn preserves_surrounding_content() {
        let current = "before\n<!-- repobox:start -->\nold\n<!-- repobox:end -->\nafter\n";
        let updated = replace_block(
            current,
            "<!-- repobox:start -->\nnew\n<!-- repobox:end -->\n",
        )
        .unwrap();
        assert!(updated.starts_with("before\n"));
        assert!(updated.contains("new"));
        assert!(updated.ends_with("\nafter\n"));
    }
}
