use regex::{Captures, Regex};

#[derive(Clone, Debug)]
pub struct SecretRedactor {
    secrets: Vec<String>,
    password: Regex,
    pscale_password: Regex,
}

impl Default for SecretRedactor {
    fn default() -> Self {
        Self {
            secrets: vec![],
            password: Regex::new(r"(?i)(postgres(?:ql)?://[^:\s/]+:)([^@\s]+)(@)")
                .expect("static URL regex compiles"),
            pscale_password: Regex::new(r"pscale_pw_[A-Za-z0-9_-]+")
                .expect("static PlanetScale password regex compiles"),
        }
    }
}

impl SecretRedactor {
    pub fn add(&mut self, secret: impl Into<String>) {
        let secret = secret.into();
        if !secret.is_empty() && !self.secrets.contains(&secret) {
            self.secrets.push(secret);
            self.secrets
                .sort_by_key(|secret| std::cmp::Reverse(secret.len()));
        }
    }

    pub fn redact(&self, input: &str) -> String {
        let mut output = input.to_owned();
        for secret in &self.secrets {
            output = output.replace(secret, "[REDACTED]");
        }
        output = self
            .pscale_password
            .replace_all(&output, "[REDACTED]")
            .into_owned();
        self.password
            .replace_all(&output, |captures: &Captures<'_>| {
                format!("{}[REDACTED]{}", &captures[1], &captures[3])
            })
            .into_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_known_and_postgres_secrets() {
        let mut redactor = SecretRedactor::default();
        redactor.add("token-value");
        let output =
            redactor.redact("token-value postgres://user:password@example.com/db pscale_pw_abc123");
        assert!(!output.contains("token-value"));
        assert!(!output.contains("password"));
        assert!(!output.contains("pscale_pw_abc123"));
    }
}
