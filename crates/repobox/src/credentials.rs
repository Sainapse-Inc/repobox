use std::collections::{BTreeMap, BTreeSet};
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

trait KeyringBackend {
    fn get(&self, key: &str) -> std::result::Result<String, keyring::Error>;
    fn set(&self, key: &str, value: &str) -> std::result::Result<(), keyring::Error>;
    fn delete(&self, key: &str) -> std::result::Result<(), keyring::Error>;
}

struct SystemKeyring;

impl KeyringBackend for SystemKeyring {
    fn get(&self, key: &str) -> std::result::Result<String, keyring::Error> {
        keyring_get(key)
    }

    fn set(&self, key: &str, value: &str) -> std::result::Result<(), keyring::Error> {
        keyring_set(key, value)
    }

    fn delete(&self, key: &str) -> std::result::Result<(), keyring::Error> {
        keyring_delete(key)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialSource {
    Environment,
    Keyring,
    PermissionLockedFile,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialMethod {
    BrowserOauth,
    ServiceToken,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CredentialStatus {
    pub configured: bool,
    pub method: Option<CredentialMethod>,
    pub source: Option<CredentialSource>,
    pub fallback_path: PathBuf,
    pub warning: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ProviderSecret {
    BrowserOauth { access_token: String },
    ServiceToken { token_id: String, token: String },
}

#[derive(Clone, Debug, Deserialize)]
struct LegacyProviderSecret {
    token_id: String,
    token: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
enum StoredProviderSecret {
    Current(ProviderSecret),
    Legacy(LegacyProviderSecret),
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct SecretFile {
    #[serde(default = "schema_version")]
    schema_version: u32,
    #[serde(default)]
    items: BTreeMap<String, String>,
    /// Non-secret evidence that an item is expected to exist in the OS keyring.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    keyring_items: BTreeSet<String>,
    /// Non-secret evidence that an item was already removed.
    ///
    /// This lets cleanup remain idempotent when a later retry runs without
    /// access to the OS keyring, and prevents legacy duplicates from
    /// resurfacing on later reads.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    removed_items: BTreeSet<String>,
}

const fn schema_version() -> u32 {
    3
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
        let environment_present = env::var_os("PLANETSCALE_SERVICE_TOKEN_ID").is_some()
            || env::var_os("PLANETSCALE_SERVICE_TOKEN").is_some();
        let token_id = env::var("PLANETSCALE_SERVICE_TOKEN_ID")
            .ok()
            .filter(|value| !value.is_empty());
        let token = env::var("PLANETSCALE_SERVICE_TOKEN")
            .ok()
            .filter(|value| !value.is_empty());
        match (token_id, token) {
            (Some(token_id), Some(token)) if !token_id.is_empty() && !token.is_empty() => {
                return Ok((
                    PlanetScaleCredentials::service_token(token_id, token),
                    CredentialSource::Environment,
                ));
            }
            _ if environment_present => {
                return Err(RepoboxError::new(
                    ErrorKind::Authentication,
                    "incomplete_planetscale_environment",
                    "both PLANETSCALE_SERVICE_TOKEN_ID and PLANETSCALE_SERVICE_TOKEN must be set",
                ));
            }
            _ => {}
        }

        self.stored_provider_credentials()?.ok_or_else(|| {
            RepoboxError::new(
                ErrorKind::Authentication,
                "planetscale_authentication_required",
                "PlanetScale credentials are not configured",
            )
            .with_suggestion(
                "Run `repobox auth login`, or set PLANETSCALE_SERVICE_TOKEN_ID and PLANETSCALE_SERVICE_TOKEN.",
            )
        })
    }

    pub fn stored_provider_credentials(
        &self,
    ) -> Result<Option<(PlanetScaleCredentials, CredentialSource)>> {
        self.stored_provider_credentials_with(&SystemKeyring)
    }

    pub fn store_provider(&self, credentials: &PlanetScaleCredentials) -> Result<CredentialSource> {
        let value = encode_provider_secret(credentials)?;
        self.store_value_with(PROVIDER_KEY, value, &SystemKeyring)
    }

    pub fn remove_provider(&self) -> Result<()> {
        self.remove_stored_value(PROVIDER_KEY)
    }

    pub fn status(&self) -> Result<CredentialStatus> {
        match self.provider_credentials() {
            Ok((credentials, source)) => Ok(CredentialStatus {
                configured: true,
                method: Some(credential_method(&credentials)),
                source: Some(source),
                fallback_path: self.fallback_path.clone(),
                warning: matches!(source, CredentialSource::PermissionLockedFile).then(|| {
                    "provider tokens are stored in a local 0600 file because the OS keyring was unavailable"
                        .to_owned()
                }),
            }),
            Err(error) if error.kind == ErrorKind::Authentication => Ok(CredentialStatus {
                configured: false,
                method: None,
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
        self.store_value_with(key, value, &SystemKeyring)
    }

    pub fn database_urls(&self, key: &str) -> Result<(String, String)> {
        let value = self
            .stored_value_with(key, &SystemKeyring)?
            .map(|(value, _)| value)
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
        self.remove_stored_value(key)
    }

    fn remove_stored_value(&self, key: &str) -> Result<()> {
        self.remove_stored_value_with(key, &SystemKeyring)
    }

    fn stored_provider_credentials_with<B: KeyringBackend>(
        &self,
        keyring: &B,
    ) -> Result<Option<(PlanetScaleCredentials, CredentialSource)>> {
        let Some((value, source)) = self.stored_value_with(PROVIDER_KEY, keyring)? else {
            return Ok(None);
        };
        Ok(Some((decode_provider_secret(&value)?, source)))
    }

    fn stored_value_with<B: KeyringBackend>(
        &self,
        key: &str,
        keyring: &B,
    ) -> Result<Option<(String, CredentialSource)>> {
        let file = self.read_file()?;
        if let Some(value) = file.items.get(key) {
            return Ok(Some((
                value.clone(),
                CredentialSource::PermissionLockedFile,
            )));
        }
        if file.removed_items.contains(key) {
            return Ok(None);
        }
        match keyring.get(key) {
            Ok(value) => Ok(Some((value, CredentialSource::Keyring))),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(error) if file.keyring_items.contains(key) => Err(keyring_read_error(error)),
            Err(_) => Ok(None),
        }
    }

    fn store_value_with<B: KeyringBackend>(
        &self,
        key: &str,
        value: String,
        keyring: &B,
    ) -> Result<CredentialSource> {
        let mut file = self.read_file()?;
        let fallback_present = file.items.contains_key(key);
        let keyring_known = file.keyring_items.contains(key);

        if fallback_present {
            reconcile_keyring_for_fallback(key, keyring_known, keyring)?;
            return self.store_fallback_value(&mut file, key, value);
        }

        if keyring_known {
            keyring.set(key, &value).map_err(keyring_store_error)?;
            if file.removed_items.remove(key) {
                file.schema_version = schema_version();
                self.write_file(&file)?;
            }
            return Ok(CredentialSource::Keyring);
        }

        let previous_keyring_value = match keyring.get(key) {
            Ok(previous) => Some(previous),
            Err(keyring::Error::NoEntry) => None,
            Err(_) => {
                return self.store_fallback_value(&mut file, key, value);
            }
        };

        match keyring.set(key, &value) {
            Ok(()) => {
                file.schema_version = schema_version();
                file.items.remove(key);
                file.keyring_items.insert(key.to_owned());
                file.removed_items.remove(key);
                if let Err(metadata_error) = self.write_file(&file) {
                    return match restore_keyring_value(
                        key,
                        previous_keyring_value.as_deref(),
                        keyring,
                    ) {
                        Ok(()) => Err(metadata_error),
                        Err(rollback_error) => {
                            Err(keyring_rollback_error(&metadata_error, rollback_error))
                        }
                    };
                }
                Ok(CredentialSource::Keyring)
            }
            Err(error) if previous_keyring_value.is_some() => Err(keyring_store_error(error)),
            Err(_) => self.store_fallback_value(&mut file, key, value),
        }
    }

    fn store_fallback_value(
        &self,
        file: &mut SecretFile,
        key: &str,
        value: String,
    ) -> Result<CredentialSource> {
        file.schema_version = schema_version();
        file.items.insert(key.to_owned(), value);
        file.keyring_items.remove(key);
        file.removed_items.remove(key);
        self.write_file(file)?;
        Ok(CredentialSource::PermissionLockedFile)
    }

    fn remove_stored_value_with<B: KeyringBackend>(&self, key: &str, keyring: &B) -> Result<()> {
        let mut file = self.read_file()?;
        let fallback_present = file.items.contains_key(key);
        let keyring_known = file.keyring_items.contains(key);
        let already_removed = file.removed_items.contains(key);
        remove_keyring_for_delete(
            key,
            keyring_known,
            fallback_present || already_removed,
            keyring,
        )?;

        let fallback_removed = file.items.remove(key).is_some();
        let keyring_metadata_removed = file.keyring_items.remove(key);
        let mut changed = fallback_removed || keyring_metadata_removed;
        changed |= file.removed_items.insert(key.to_owned());
        if changed {
            file.schema_version = schema_version();
            self.write_file(&file)?;
        }
        Ok(())
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

    #[cfg(test)]
    fn set_file_item(&self, key: &str, value: String) -> Result<()> {
        let mut file = self.read_file()?;
        file.schema_version = schema_version();
        file.items.insert(key.to_owned(), value);
        file.keyring_items.remove(key);
        file.removed_items.remove(key);
        self.write_file(&file)
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

fn encode_provider_secret(credentials: &PlanetScaleCredentials) -> Result<String> {
    let stored = match credentials {
        PlanetScaleCredentials::AccessToken { token } => ProviderSecret::BrowserOauth {
            access_token: token.expose_secret().to_owned(),
        },
        PlanetScaleCredentials::ServiceToken { token_id, token } => ProviderSecret::ServiceToken {
            token_id: token_id.expose_secret().to_owned(),
            token: token.expose_secret().to_owned(),
        },
    };
    serde_json::to_string(&stored).map_err(|error| {
        RepoboxError::new(
            ErrorKind::Runtime,
            "credential_encode_failed",
            error.to_string(),
        )
    })
}

fn decode_provider_secret(value: &str) -> Result<PlanetScaleCredentials> {
    let stored: StoredProviderSecret = serde_json::from_str(value).map_err(|error| {
        RepoboxError::new(
            ErrorKind::Runtime,
            "credential_decode_failed",
            format!("stored provider credential is invalid: {error}"),
        )
    })?;
    Ok(match stored {
        StoredProviderSecret::Current(ProviderSecret::BrowserOauth { access_token }) => {
            PlanetScaleCredentials::AccessToken {
                token: SecretString::from(access_token),
            }
        }
        StoredProviderSecret::Current(ProviderSecret::ServiceToken { token_id, token })
        | StoredProviderSecret::Legacy(LegacyProviderSecret { token_id, token }) => {
            PlanetScaleCredentials::ServiceToken {
                token_id: SecretString::from(token_id),
                token: SecretString::from(token),
            }
        }
    })
}

const fn credential_method(credentials: &PlanetScaleCredentials) -> CredentialMethod {
    match credentials {
        PlanetScaleCredentials::AccessToken { .. } => CredentialMethod::BrowserOauth,
        PlanetScaleCredentials::ServiceToken { .. } => CredentialMethod::ServiceToken,
    }
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

fn reconcile_keyring_for_fallback<B: KeyringBackend>(
    key: &str,
    keyring_known: bool,
    keyring: &B,
) -> Result<()> {
    if keyring_known {
        return delete_keyring_entry(key, keyring);
    }
    match keyring.get(key) {
        Ok(_) => delete_keyring_entry(key, keyring),
        Err(keyring::Error::NoEntry) => Ok(()),
        Err(error) if keyring_unavailable(&error) => Ok(()),
        Err(error) => Err(keyring_delete_error(error)),
    }
}

fn remove_keyring_for_delete<B: KeyringBackend>(
    key: &str,
    keyring_known: bool,
    fallback_present: bool,
    keyring: &B,
) -> Result<()> {
    if keyring_known {
        return delete_keyring_entry(key, keyring);
    }
    match keyring.get(key) {
        Ok(_) => delete_keyring_entry(key, keyring),
        Err(keyring::Error::NoEntry) => Ok(()),
        Err(error) if fallback_present && keyring_unavailable(&error) => Ok(()),
        Err(error) => Err(keyring_delete_error(error)),
    }
}

fn delete_keyring_entry<B: KeyringBackend>(key: &str, keyring: &B) -> Result<()> {
    match keyring.delete(key) {
        Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
        Err(error) => Err(keyring_delete_error(error)),
    }
}

fn restore_keyring_value<B: KeyringBackend>(
    key: &str,
    previous: Option<&str>,
    keyring: &B,
) -> std::result::Result<(), keyring::Error> {
    match previous {
        Some(value) => keyring.set(key, value),
        None => match keyring.delete(key) {
            Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
            Err(error) => Err(error),
        },
    }
}

fn keyring_unavailable(error: &keyring::Error) -> bool {
    matches!(
        error,
        keyring::Error::PlatformFailure(_) | keyring::Error::NoStorageAccess(_)
    )
}

fn keyring_read_error(error: keyring::Error) -> RepoboxError {
    RepoboxError::new(
        ErrorKind::Runtime,
        "credential_read_failed",
        format!("could not read credentials from the OS keyring: {error}"),
    )
    .with_suggestion("Restore OS keyring access, then retry the operation.")
}

fn keyring_store_error(error: keyring::Error) -> RepoboxError {
    RepoboxError::new(
        ErrorKind::Runtime,
        "credential_store_failed",
        format!("could not update credentials in the OS keyring: {error}"),
    )
    .with_suggestion("Restore OS keyring access, then retry the operation.")
}

fn keyring_rollback_error(
    metadata_error: &RepoboxError,
    rollback_error: keyring::Error,
) -> RepoboxError {
    RepoboxError::new(
        ErrorKind::Runtime,
        "credential_store_rollback_failed",
        format!(
            "credential source metadata could not be persisted ({}), and the OS keyring write could not be rolled back: {rollback_error}",
            metadata_error.message
        ),
    )
    .with_suggestion(
        "Repair local credential-file and OS keyring access, then run the cleanup operation again.",
    )
}

fn keyring_delete_error(error: keyring::Error) -> RepoboxError {
    RepoboxError::new(
        ErrorKind::Runtime,
        "credential_delete_failed",
        format!("could not remove credentials from the OS keyring: {error}"),
    )
    .with_suggestion("Fix OS keyring access, then retry the cleanup operation.")
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

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};

    use super::*;

    #[derive(Clone, Copy)]
    enum FakeFailure {
        Unavailable,
        Platform,
    }

    #[derive(Default)]
    struct FakeKeyring {
        value: RefCell<Option<String>>,
        get_failure: Cell<Option<FakeFailure>>,
        set_failure: Cell<Option<FakeFailure>>,
        delete_failure: Cell<Option<FakeFailure>>,
        get_calls: Cell<usize>,
        set_calls: Cell<usize>,
        delete_calls: Cell<usize>,
    }

    impl FakeKeyring {
        fn with_value(value: &str) -> Self {
            Self {
                value: RefCell::new(Some(value.to_owned())),
                ..Self::default()
            }
        }
    }

    impl KeyringBackend for FakeKeyring {
        fn get(&self, _key: &str) -> std::result::Result<String, keyring::Error> {
            self.get_calls.set(self.get_calls.get() + 1);
            if let Some(failure) = self.get_failure.get() {
                return Err(fake_keyring_error(failure));
            }
            self.value.borrow().clone().ok_or(keyring::Error::NoEntry)
        }

        fn set(&self, _key: &str, value: &str) -> std::result::Result<(), keyring::Error> {
            self.set_calls.set(self.set_calls.get() + 1);
            if let Some(failure) = self.set_failure.get() {
                return Err(fake_keyring_error(failure));
            }
            *self.value.borrow_mut() = Some(value.to_owned());
            Ok(())
        }

        fn delete(&self, _key: &str) -> std::result::Result<(), keyring::Error> {
            self.delete_calls.set(self.delete_calls.get() + 1);
            if let Some(failure) = self.delete_failure.get() {
                return Err(fake_keyring_error(failure));
            }
            if self.value.borrow_mut().take().is_some() {
                Ok(())
            } else {
                Err(keyring::Error::NoEntry)
            }
        }
    }

    fn fake_keyring_error(failure: FakeFailure) -> keyring::Error {
        let error = || Box::new(std::io::Error::other("fake keyring failure"));
        match failure {
            FakeFailure::Unavailable => keyring::Error::NoStorageAccess(error()),
            FakeFailure::Platform => keyring::Error::PlatformFailure(error()),
        }
    }

    fn seed_secret_file(
        store: &CredentialStore,
        key: &str,
        fallback_value: Option<&str>,
        keyring_known: bool,
    ) {
        let mut file = SecretFile {
            schema_version: schema_version(),
            ..SecretFile::default()
        };
        if let Some(value) = fallback_value {
            file.items.insert(key.to_owned(), value.to_owned());
        }
        if keyring_known {
            file.keyring_items.insert(key.to_owned());
        }
        store.write_file(&file).unwrap();
    }

    #[test]
    fn browser_oauth_credentials_round_trip() {
        let encoded =
            encode_provider_secret(&PlanetScaleCredentials::access_token("oauth-token")).unwrap();
        assert!(encoded.contains(r#""kind":"browser_oauth""#));
        let decoded = decode_provider_secret(&encoded).unwrap();
        assert_eq!(credential_method(&decoded), CredentialMethod::BrowserOauth);
        let PlanetScaleCredentials::AccessToken { token } = decoded else {
            panic!("expected browser OAuth credentials");
        };
        assert_eq!(token.expose_secret(), "oauth-token");
    }

    #[test]
    fn legacy_service_token_credentials_are_migrated_on_read() {
        let decoded =
            decode_provider_secret(r#"{"token_id":"legacy-id","token":"legacy-token"}"#).unwrap();
        assert_eq!(credential_method(&decoded), CredentialMethod::ServiceToken);
        let PlanetScaleCredentials::ServiceToken { token_id, token } = decoded else {
            panic!("expected service-token credentials");
        };
        assert_eq!(token_id.expose_secret(), "legacy-id");
        assert_eq!(token.expose_secret(), "legacy-token");
    }

    #[test]
    fn legacy_secret_file_schema_remains_readable() {
        let file: SecretFile =
            serde_json::from_str(r#"{"schema_version":2,"items":{"database.test":"fallback"}}"#)
                .unwrap();

        assert_eq!(file.items.get("database.test").unwrap(), "fallback");
        assert!(file.keyring_items.is_empty());
        assert!(file.removed_items.is_empty());
    }

    #[test]
    fn dual_source_prefers_fallback_and_delete_removes_both_copies() {
        let temp = tempfile::tempdir().unwrap();
        let store = CredentialStore::new(temp.path().join("credentials.json"));
        let key = "database.test";
        seed_secret_file(&store, key, Some("fallback-secret"), true);
        let keyring = FakeKeyring::with_value("keyring-secret");

        let (value, source) = store.stored_value_with(key, &keyring).unwrap().unwrap();
        assert_eq!(value, "fallback-secret");
        assert!(matches!(source, CredentialSource::PermissionLockedFile));

        store.remove_stored_value_with(key, &keyring).unwrap();

        assert!(keyring.value.borrow().is_none());
        assert_eq!(keyring.delete_calls.get(), 1);
        let file = store.read_file().unwrap();
        assert!(!file.items.contains_key(key));
        assert!(!file.keyring_items.contains(key));
    }

    #[test]
    fn known_keyring_delete_failure_preserves_dual_source_evidence() {
        let temp = tempfile::tempdir().unwrap();
        let store = CredentialStore::new(temp.path().join("credentials.json"));
        let key = "database.test";
        seed_secret_file(&store, key, Some("fallback-secret"), true);
        let keyring = FakeKeyring::with_value("keyring-secret");
        keyring.delete_failure.set(Some(FakeFailure::Platform));

        let error = store.remove_stored_value_with(key, &keyring).unwrap_err();

        assert_eq!(error.code, "credential_delete_failed");
        assert!(keyring.value.borrow().is_some());
        let file = store.read_file().unwrap();
        assert!(file.items.contains_key(key));
        assert!(file.keyring_items.contains(key));
    }

    #[test]
    fn legacy_fallback_removal_tolerates_an_unavailable_keyring() {
        let temp = tempfile::tempdir().unwrap();
        let store = CredentialStore::new(temp.path().join("credentials.json"));
        let key = "database.test";
        seed_secret_file(&store, key, Some("fallback-secret"), false);
        let keyring = FakeKeyring::default();
        keyring.get_failure.set(Some(FakeFailure::Unavailable));

        store.remove_stored_value_with(key, &keyring).unwrap();

        assert_eq!(keyring.delete_calls.get(), 0);
        assert!(!store.read_file().unwrap().items.contains_key(key));
    }

    #[test]
    fn fallback_deletion_retry_is_idempotent_without_keyring_access() {
        let temp = tempfile::tempdir().unwrap();
        let store = CredentialStore::new(temp.path().join("credentials.json"));
        let first_key = "database.test.first";
        let second_key = "database.test.second";
        store
            .set_file_item(first_key, "first-secret".to_owned())
            .unwrap();
        store
            .set_file_item(second_key, "second-secret".to_owned())
            .unwrap();
        let keyring = FakeKeyring::default();
        keyring.get_failure.set(Some(FakeFailure::Unavailable));

        store.remove_stored_value_with(first_key, &keyring).unwrap();

        let partial = store.read_file().unwrap();
        assert!(!partial.items.contains_key(first_key));
        assert!(partial.items.contains_key(second_key));
        assert!(partial.removed_items.contains(first_key));
        assert_eq!(keyring.get_calls.get(), 1);

        store.remove_stored_value_with(first_key, &keyring).unwrap();

        assert_eq!(keyring.get_calls.get(), 2);
        store
            .remove_stored_value_with(second_key, &keyring)
            .unwrap();
        assert_eq!(keyring.get_calls.get(), 3);
    }

    #[test]
    fn deletion_tombstone_hides_then_reconciles_a_legacy_keyring_duplicate() {
        let temp = tempfile::tempdir().unwrap();
        let store = CredentialStore::new(temp.path().join("credentials.json"));
        let key = "database.test";
        store
            .set_file_item(key, "fallback-secret".to_owned())
            .unwrap();
        let keyring = FakeKeyring::default();
        keyring.get_failure.set(Some(FakeFailure::Unavailable));
        store.remove_stored_value_with(key, &keyring).unwrap();

        keyring.get_failure.set(None);
        *keyring.value.borrow_mut() = Some("legacy-duplicate".to_owned());
        assert!(store.stored_value_with(key, &keyring).unwrap().is_none());
        assert_eq!(keyring.get_calls.get(), 1);

        store.remove_stored_value_with(key, &keyring).unwrap();

        assert!(keyring.value.borrow().is_none());
        assert_eq!(keyring.get_calls.get(), 2);
        assert_eq!(keyring.delete_calls.get(), 1);
        assert!(store.read_file().unwrap().removed_items.contains(key));

        let source = store
            .store_value_with(key, "replacement".to_owned(), &keyring)
            .unwrap();
        assert!(matches!(source, CredentialSource::Keyring));
        assert!(!store.read_file().unwrap().removed_items.contains(key));
    }

    #[test]
    fn keyring_deletion_retry_is_idempotent_when_access_later_disappears() {
        let temp = tempfile::tempdir().unwrap();
        let store = CredentialStore::new(temp.path().join("credentials.json"));
        let key = "database.test";
        seed_secret_file(&store, key, None, true);
        let keyring = FakeKeyring::with_value("keyring-secret");

        store.remove_stored_value_with(key, &keyring).unwrap();
        assert!(store.read_file().unwrap().removed_items.contains(key));

        keyring.get_failure.set(Some(FakeFailure::Unavailable));
        store.remove_stored_value_with(key, &keyring).unwrap();
        assert!(store.read_file().unwrap().removed_items.contains(key));
    }

    #[test]
    fn keyring_access_failure_without_fallback_evidence_is_reported() {
        let temp = tempfile::tempdir().unwrap();
        let store = CredentialStore::new(temp.path().join("credentials.json"));
        let keyring = FakeKeyring::default();
        keyring.get_failure.set(Some(FakeFailure::Unavailable));

        let error = store
            .remove_stored_value_with("database.test", &keyring)
            .unwrap_err();

        assert_eq!(error.code, "credential_delete_failed");
    }

    #[test]
    fn successful_keyring_write_records_non_secret_source_metadata() {
        let temp = tempfile::tempdir().unwrap();
        let store = CredentialStore::new(temp.path().join("credentials.json"));
        let key = "database.test";
        let keyring = FakeKeyring::default();

        let source = store
            .store_value_with(key, "stored-secret".to_owned(), &keyring)
            .unwrap();

        assert!(matches!(source, CredentialSource::Keyring));
        assert_eq!(keyring.value.borrow().as_deref(), Some("stored-secret"));
        let file = store.read_file().unwrap();
        assert!(!file.items.contains_key(key));
        assert!(file.keyring_items.contains(key));
        assert!(
            !fs::read_to_string(&store.fallback_path)
                .unwrap()
                .contains("stored-secret")
        );
    }

    #[test]
    fn known_keyring_update_failure_does_not_create_a_fallback_copy() {
        let temp = tempfile::tempdir().unwrap();
        let store = CredentialStore::new(temp.path().join("credentials.json"));
        let key = "database.test";
        seed_secret_file(&store, key, None, true);
        let keyring = FakeKeyring::with_value("old-secret");
        keyring.set_failure.set(Some(FakeFailure::Unavailable));

        let error = store
            .store_value_with(key, "new-secret".to_owned(), &keyring)
            .unwrap_err();

        assert_eq!(error.code, "credential_store_failed");
        assert_eq!(keyring.value.borrow().as_deref(), Some("old-secret"));
        let file = store.read_file().unwrap();
        assert!(!file.items.contains_key(key));
        assert!(file.keyring_items.contains(key));
    }

    #[cfg(unix)]
    #[test]
    fn keyring_write_is_rolled_back_when_metadata_cannot_be_persisted() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let store = CredentialStore::new(temp.path().join("credentials.json"));
        let keyring = FakeKeyring::default();
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o500)).unwrap();

        let error = store
            .store_value_with("database.test", "stored-secret".to_owned(), &keyring)
            .unwrap_err();

        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
        assert_eq!(error.code, "io_error");
        assert!(keyring.value.borrow().is_none());
        assert_eq!(keyring.delete_calls.get(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn legacy_keyring_value_is_restored_when_metadata_cannot_be_persisted() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let store = CredentialStore::new(temp.path().join("credentials.json"));
        let keyring = FakeKeyring::with_value("legacy-secret");
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o500)).unwrap();

        let error = store
            .store_value_with("database.test", "new-secret".to_owned(), &keyring)
            .unwrap_err();

        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
        assert_eq!(error.code, "io_error");
        assert_eq!(keyring.value.borrow().as_deref(), Some("legacy-secret"));
        assert_eq!(keyring.set_calls.get(), 2);
        assert_eq!(keyring.delete_calls.get(), 0);
    }

    #[cfg(unix)]
    #[test]
    fn failed_keyring_write_rollback_is_reported_explicitly() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let store = CredentialStore::new(temp.path().join("credentials.json"));
        let keyring = FakeKeyring::default();
        keyring.delete_failure.set(Some(FakeFailure::Platform));
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o500)).unwrap();

        let error = store
            .store_value_with("database.test", "stored-secret".to_owned(), &keyring)
            .unwrap_err();

        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
        assert_eq!(error.code, "credential_store_rollback_failed");
        assert_eq!(keyring.value.borrow().as_deref(), Some("stored-secret"));
        assert_eq!(keyring.delete_calls.get(), 1);
    }

    #[test]
    fn fallback_store_reconciles_an_accessible_legacy_keyring_copy() {
        let temp = tempfile::tempdir().unwrap();
        let store = CredentialStore::new(temp.path().join("credentials.json"));
        let key = "database.test";
        seed_secret_file(&store, key, Some("old-fallback"), false);
        let keyring = FakeKeyring::with_value("legacy-keyring-copy");

        let source = store
            .store_value_with(key, "new-fallback".to_owned(), &keyring)
            .unwrap();

        assert!(matches!(source, CredentialSource::PermissionLockedFile));
        assert!(keyring.value.borrow().is_none());
        let file = store.read_file().unwrap();
        assert_eq!(file.items.get(key).unwrap(), "new-fallback");
        assert!(!file.keyring_items.contains(key));
    }

    #[test]
    fn fallback_store_remains_available_when_legacy_keyring_is_inaccessible() {
        let temp = tempfile::tempdir().unwrap();
        let store = CredentialStore::new(temp.path().join("credentials.json"));
        let key = "database.test";
        seed_secret_file(&store, key, Some("old-fallback"), false);
        let keyring = FakeKeyring::default();
        keyring.get_failure.set(Some(FakeFailure::Unavailable));

        let source = store
            .store_value_with(key, "new-fallback".to_owned(), &keyring)
            .unwrap();

        assert!(matches!(source, CredentialSource::PermissionLockedFile));
        assert_eq!(
            store.read_file().unwrap().items.get(key).unwrap(),
            "new-fallback"
        );
    }

    #[test]
    fn production_fallback_removal_still_works_without_a_desktop_keyring() {
        let temp = tempfile::tempdir().unwrap();
        let store = CredentialStore::new(temp.path().join("credentials.json"));
        let key = "database.test";
        store
            .set_file_item(key, "fallback-secret".to_owned())
            .unwrap();

        store.remove_database_urls(key).unwrap();

        assert!(!store.read_file().unwrap().items.contains_key(key));
    }
}
