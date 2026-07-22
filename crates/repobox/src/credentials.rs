use std::collections::BTreeMap;
use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use repobox_core::{ErrorKind, RepoboxError, Result};
use repobox_provider_planetscale::PlanetScaleCredentials;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const KEYRING_SERVICE: &str = "repobox";
const PROVIDER_KEY: &str = "provider.planetscale";

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialSource {
    Environment,
    Keyring,
    PermissionLockedFile,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CredentialStatus {
    pub configured: bool,
    pub source: Option<CredentialSource>,
    pub fallback_path: PathBuf,
    pub warning: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ProviderSecret {
    token_id: String,
    token: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct SecretFile {
    #[serde(default = "schema_version")]
    schema_version: u32,
    #[serde(default)]
    items: BTreeMap<String, String>,
}

const fn schema_version() -> u32 {
    1
}

#[derive(Clone, Debug)]
pub struct CredentialStore {
    fallback_path: PathBuf,
}

impl CredentialStore {
    pub fn new(fallback_path: impl Into<PathBuf>) -> Self {
        Self {
            fallback_path: fallback_path.into(),
        }
    }

    pub fn provider_credentials(&self) -> Result<(PlanetScaleCredentials, CredentialSource)> {
        let token_id = env::var("PLANETSCALE_SERVICE_TOKEN_ID").ok();
        let token = env::var("PLANETSCALE_SERVICE_TOKEN").ok();
        match (token_id, token) {
            (Some(token_id), Some(token)) if !token_id.is_empty() && !token.is_empty() => {
                return Ok((
                    PlanetScaleCredentials {
                        token_id: SecretString::from(token_id),
                        token: SecretString::from(token),
                    },
                    CredentialSource::Environment,
                ));
            }
            (Some(_), None) | (None, Some(_)) => {
                return Err(RepoboxError::new(
                    ErrorKind::Authentication,
                    "incomplete_planetscale_environment",
                    "both PLANETSCALE_SERVICE_TOKEN_ID and PLANETSCALE_SERVICE_TOKEN must be set",
                ));
            }
            _ => {}
        }

        if let Ok(value) = keyring_get(PROVIDER_KEY) {
            let secret = decode_provider_secret(&value)?;
            return Ok((secret, CredentialSource::Keyring));
        }
        if let Some(value) = self.read_file()?.items.get(PROVIDER_KEY) {
            let secret = decode_provider_secret(value)?;
            return Ok((secret, CredentialSource::PermissionLockedFile));
        }
        Err(RepoboxError::new(
            ErrorKind::Authentication,
            "planetscale_authentication_required",
            "PlanetScale credentials are not configured",
        )
        .with_suggestion(
            "Run `repobox auth login`, or set PLANETSCALE_SERVICE_TOKEN_ID and PLANETSCALE_SERVICE_TOKEN.",
        ))
    }

    pub fn store_provider(&self, credentials: &PlanetScaleCredentials) -> Result<CredentialSource> {
        let value = serde_json::to_string(&ProviderSecret {
            token_id: credentials.token_id.expose_secret().to_string(),
            token: credentials.token.expose_secret().to_string(),
        })
        .map_err(|error| {
            RepoboxError::new(
                ErrorKind::Runtime,
                "credential_encode_failed",
                error.to_string(),
            )
        })?;
        if keyring_set(PROVIDER_KEY, &value).is_ok() {
            self.remove_file_item(PROVIDER_KEY)?;
            return Ok(CredentialSource::Keyring);
        }
        self.set_file_item(PROVIDER_KEY, value)?;
        Ok(CredentialSource::PermissionLockedFile)
    }

    pub fn remove_provider(&self) -> Result<()> {
        let _ = keyring_delete(PROVIDER_KEY);
        self.remove_file_item(PROVIDER_KEY)
    }

    pub fn status(&self) -> Result<CredentialStatus> {
        match self.provider_credentials() {
            Ok((_, source)) => Ok(CredentialStatus {
                configured: true,
                source: Some(source),
                fallback_path: self.fallback_path.clone(),
                warning: matches!(source, CredentialSource::PermissionLockedFile).then(|| {
                    "provider tokens are stored in a local 0600 file because the OS keyring was unavailable"
                        .to_owned()
                }),
            }),
            Err(error) if error.kind == ErrorKind::Authentication => Ok(CredentialStatus {
                configured: false,
                source: None,
                fallback_path: self.fallback_path.clone(),
                warning: None,
            }),
            Err(error) => Err(error),
        }
    }

    pub fn database_key(project_id: Uuid, provider_branch: &str, service: &str) -> String {
        format!("database.{project_id}.{provider_branch}.{service}")
    }

    pub fn store_database_urls(
        &self,
        key: &str,
        pooled: &str,
        direct: &str,
    ) -> Result<CredentialSource> {
        let value = serde_json::to_string(&serde_json::json!({
            "pooled": pooled,
            "direct": direct,
        }))
        .map_err(|error| {
            RepoboxError::new(
                ErrorKind::Runtime,
                "credential_encode_failed",
                error.to_string(),
            )
        })?;
        if keyring_set(key, &value).is_ok() {
            self.remove_file_item(key)?;
            return Ok(CredentialSource::Keyring);
        }
        self.set_file_item(key, value)?;
        Ok(CredentialSource::PermissionLockedFile)
    }

    pub fn database_urls(&self, key: &str) -> Result<(String, String)> {
        let value = keyring_get(key)
            .ok()
            .or_else(|| self.read_file().ok()?.items.get(key).cloned())
            .ok_or_else(|| {
                RepoboxError::new(
                    ErrorKind::NotFound,
                    "database_credentials_not_found",
                    "database credentials are missing from local secure storage",
                )
                .with_suggestion("Run `repobox env create --yes` to rotate credentials safely.")
            })?;
        let value: serde_json::Value = serde_json::from_str(&value).map_err(|error| {
            RepoboxError::new(
                ErrorKind::Runtime,
                "credential_decode_failed",
                error.to_string(),
            )
        })?;
        let pooled = value
            .get("pooled")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(invalid_database_credential)?;
        let direct = value
            .get("direct")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(invalid_database_credential)?;
        Ok((pooled.to_owned(), direct.to_owned()))
    }

    pub fn remove_database_urls(&self, key: &str) -> Result<()> {
        let _ = keyring_delete(key);
        self.remove_file_item(key)
    }

    fn read_file(&self) -> Result<SecretFile> {
        match fs::read(&self.fallback_path) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|error| {
                RepoboxError::new(
                    ErrorKind::Runtime,
                    "credential_file_invalid",
                    format!("could not decode {}: {error}", self.fallback_path.display()),
                )
            }),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(SecretFile::default()),
            Err(error) => Err(error.into()),
        }
    }

    fn set_file_item(&self, key: &str, value: String) -> Result<()> {
        let mut file = self.read_file()?;
        file.schema_version = schema_version();
        file.items.insert(key.to_owned(), value);
        self.write_file(&file)
    }

    fn remove_file_item(&self, key: &str) -> Result<()> {
        let mut file = self.read_file()?;
        if file.items.remove(key).is_some() {
            self.write_file(&file)?;
        }
        Ok(())
    }

    fn write_file(&self, contents: &SecretFile) -> Result<()> {
        if let Some(parent) = self.fallback_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let temporary = self.fallback_path.with_extension("json.tmp");
        write_private(
            &temporary,
            &serde_json::to_vec_pretty(contents).map_err(|error| {
                RepoboxError::new(
                    ErrorKind::Runtime,
                    "credential_encode_failed",
                    error.to_string(),
                )
            })?,
        )?;
        fs::rename(temporary, &self.fallback_path)?;
        set_private_permissions(&self.fallback_path)?;
        Ok(())
    }
}

fn decode_provider_secret(value: &str) -> Result<PlanetScaleCredentials> {
    let stored: ProviderSecret = serde_json::from_str(value).map_err(|error| {
        RepoboxError::new(
            ErrorKind::Runtime,
            "credential_decode_failed",
            format!("stored provider credential is invalid: {error}"),
        )
    })?;
    Ok(PlanetScaleCredentials {
        token_id: SecretString::from(stored.token_id),
        token: SecretString::from(stored.token),
    })
}

fn keyring_get(key: &str) -> std::result::Result<String, keyring::Error> {
    keyring::Entry::new(KEYRING_SERVICE, key)?.get_password()
}

fn keyring_set(key: &str, value: &str) -> std::result::Result<(), keyring::Error> {
    keyring::Entry::new(KEYRING_SERVICE, key)?.set_password(value)
}

fn keyring_delete(key: &str) -> std::result::Result<(), keyring::Error> {
    keyring::Entry::new(KEYRING_SERVICE, key)?.delete_credential()
}

fn invalid_database_credential() -> RepoboxError {
    RepoboxError::new(
        ErrorKind::Runtime,
        "database_credential_invalid",
        "stored database credential is invalid",
    )
}

fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut options = OpenOptions::new();
    options.create(true).truncate(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

#[cfg(unix)]
fn set_private_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_private_permissions(_path: &Path) -> Result<()> {
    Ok(())
}
