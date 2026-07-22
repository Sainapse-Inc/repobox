use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{ErrorKind, RepoboxError, Result};

const MAX_PROVIDER_BRANCH_LEN: usize = 63;

pub fn validate_environment_name(value: &str) -> Result<()> {
    let valid_characters = value.chars().all(|character| {
        character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '/' | '@' | '+' | '-')
    });
    let valid_boundaries = value
        .chars()
        .next()
        .is_some_and(|character| character.is_ascii_alphanumeric())
        && value
            .chars()
            .last()
            .is_some_and(|character| character.is_ascii_alphanumeric());
    if value.len() > 256
        || !valid_characters
        || !valid_boundaries
        || value.contains("..")
        || value.contains("//")
        || value.contains("@{")
        || std::path::Path::new(value)
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("lock"))
    {
        return Err(RepoboxError::new(
            ErrorKind::Usage,
            "invalid_environment_name",
            "environment names must be 1-256 shell-safe Git-style characters and start/end with a letter or number",
        ));
    }
    Ok(())
}

pub fn provider_branch_name(project_id: Uuid, environment: &str) -> Result<String> {
    validate_environment_name(environment)?;
    let digest = Sha256::digest(format!("{project_id}:{environment}").as_bytes());
    let hash = hex::encode(&digest[..4]);
    let project = &project_id.simple().to_string()[..8];
    let slug = slugify(environment);
    let fixed_len = "rbx-- -".len() + project.len() + hash.len();
    let slug_len = MAX_PROVIDER_BRANCH_LEN.saturating_sub(fixed_len);
    let slug = slug.chars().take(slug_len).collect::<String>();
    Ok(format!("rbx-{project}-{slug}-{hash}"))
}

fn slugify(value: &str) -> String {
    let mut output = String::new();
    let mut previous_dash = false;
    for character in value.chars().flat_map(char::to_lowercase) {
        if character.is_ascii_alphanumeric() {
            output.push(character);
            previous_dash = false;
        } else if !previous_dash && !output.is_empty() {
            output.push('-');
            previous_dash = true;
        }
    }
    while output.ends_with('-') {
        output.pop();
    }
    if output.is_empty() {
        "env".to_owned()
    } else {
        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_names_are_stable_and_bounded() {
        let id = Uuid::parse_str("3f5430e0-ee14-48cf-aa6c-633343533e5f").unwrap();
        let first = provider_branch_name(id, "feature/My-Fine-Branch").unwrap();
        let second = provider_branch_name(id, "feature/My-Fine-Branch").unwrap();
        assert_eq!(first, second);
        assert!(first.starts_with("rbx-3f5430e0-feature-my-fine-branch-"));
        assert!(first.len() <= MAX_PROVIDER_BRANCH_LEN);
    }

    #[test]
    fn hashes_avoid_slug_collisions() {
        let id = Uuid::new_v4();
        assert_ne!(
            provider_branch_name(id, "a/b").unwrap(),
            provider_branch_name(id, "a-b").unwrap()
        );
    }

    #[test]
    fn rejects_environment_names_that_are_unsafe_in_recovery_commands() {
        for value in ["feature name", "feature;rm", "-flag", "feature/", "a..b"] {
            assert!(
                validate_environment_name(value).is_err(),
                "accepted {value}"
            );
        }
        validate_environment_name("feature/demo-1.2+agent@local").unwrap();
    }
}
