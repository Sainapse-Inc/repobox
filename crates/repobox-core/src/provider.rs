use std::collections::BTreeSet;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::Result;

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
pub struct ProviderCapabilities {
    pub accesses: BTreeSet<String>,
}

impl ProviderCapabilities {
    pub fn missing<'a>(&self, required: impl IntoIterator<Item = &'a str>) -> Vec<String> {
        required
            .into_iter()
            .filter(|access| !self.accesses.contains(*access))
            .map(str::to_owned)
            .collect()
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct Organization {
    pub name: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct Database {
    pub id: String,
    pub name: String,
    pub ready: bool,
    pub region: Option<String>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct Branch {
    pub id: String,
    pub name: String,
    pub state: String,
    pub ready: bool,
    pub production: bool,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct Backup {
    pub id: String,
    pub name: String,
    pub state: String,
    pub size_bytes: u64,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct DatabaseRole {
    pub id: String,
    pub name: String,
    pub username: String,
    pub password: Option<String>,
    pub database_name: String,
    pub access_host_url: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct ConnectionUrls {
    pub pooled: Url,
    pub direct: Url,
}

#[derive(Clone, Debug)]
pub struct CreateDatabaseRequest {
    pub organization: String,
    pub name: String,
    pub region: Option<String>,
    pub cluster_size: String,
    pub major_version: Option<String>,
}

#[derive(Clone, Debug)]
pub struct CreateBranchRequest {
    pub organization: String,
    pub database: String,
    pub name: String,
    pub parent_branch: String,
    pub backup_id: Option<String>,
    pub cluster_size: Option<String>,
}

#[derive(Clone, Debug)]
pub struct CreateRoleRequest {
    pub organization: String,
    pub database: String,
    pub branch: String,
    pub name: String,
    pub inherited_roles: Vec<String>,
}

#[async_trait]
pub trait DatabaseProvider: Send + Sync {
    fn name(&self) -> &'static str;

    async fn validate_auth(&self) -> Result<ProviderCapabilities>;
    async fn list_organizations(&self) -> Result<Vec<Organization>>;
    async fn list_databases(&self, organization: &str) -> Result<Vec<Database>>;
    async fn create_database(&self, request: &CreateDatabaseRequest) -> Result<Database>;
    async fn delete_database(&self, organization: &str, database: &str) -> Result<()>;
    async fn list_cluster_sizes(&self, organization: &str) -> Result<Vec<String>>;

    async fn list_branches(&self, organization: &str, database: &str) -> Result<Vec<Branch>>;
    async fn get_branch(&self, organization: &str, database: &str, branch: &str) -> Result<Branch>;
    async fn create_branch(&self, request: &CreateBranchRequest) -> Result<Branch>;
    async fn rename_branch(
        &self,
        organization: &str,
        database: &str,
        branch: &str,
        new_name: &str,
    ) -> Result<Branch>;
    async fn delete_branch(&self, organization: &str, database: &str, branch: &str) -> Result<()>;

    async fn list_backups(
        &self,
        organization: &str,
        database: &str,
        branch: &str,
    ) -> Result<Vec<Backup>>;
    async fn create_backup(
        &self,
        organization: &str,
        database: &str,
        branch: &str,
        name: &str,
    ) -> Result<Backup>;

    async fn list_roles(
        &self,
        organization: &str,
        database: &str,
        branch: &str,
    ) -> Result<Vec<DatabaseRole>>;
    async fn create_role(&self, request: &CreateRoleRequest) -> Result<DatabaseRole>;
    async fn delete_role(
        &self,
        organization: &str,
        database: &str,
        branch: &str,
        role_id: &str,
        successor: Option<&str>,
    ) -> Result<()>;
}

pub fn connection_urls(role: &DatabaseRole) -> Result<ConnectionUrls> {
    let password = role.password.as_deref().ok_or_else(|| {
        crate::RepoboxError::new(
            crate::ErrorKind::Runtime,
            "planetscale_role_password_missing",
            "PlanetScale did not return the one-time password for a newly created role",
        )
    })?;
    let mut direct = Url::parse("postgresql://localhost").expect("static PostgreSQL URL is valid");
    direct.set_host(Some(&role.access_host_url)).map_err(|_| {
        crate::RepoboxError::new(
            crate::ErrorKind::Runtime,
            "invalid_connection_host",
            "PlanetScale returned an invalid Postgres hostname",
        )
    })?;
    direct.set_username(&role.username).map_err(|()| {
        crate::RepoboxError::new(
            crate::ErrorKind::Runtime,
            "invalid_connection_username",
            "PlanetScale returned a username that cannot be encoded in a Postgres URL",
        )
    })?;
    direct.set_password(Some(password)).map_err(|()| {
        crate::RepoboxError::new(
            crate::ErrorKind::Runtime,
            "invalid_connection_password",
            "PlanetScale returned a password that cannot be encoded in a Postgres URL",
        )
    })?;
    direct.set_path(&format!("/{}", role.database_name));
    direct.set_port(Some(5432)).map_err(|()| {
        crate::RepoboxError::new(
            crate::ErrorKind::Runtime,
            "invalid_connection_url",
            "PlanetScale returned a URL whose port cannot be changed",
        )
    })?;
    set_tls_query(&mut direct);

    let mut pooled = direct.clone();
    pooled.set_port(Some(6432)).map_err(|()| {
        crate::RepoboxError::new(
            crate::ErrorKind::Runtime,
            "invalid_connection_url",
            "PlanetScale returned a URL whose port cannot be changed",
        )
    })?;
    Ok(ConnectionUrls { pooled, direct })
}

fn set_tls_query(url: &mut Url) {
    let existing: Vec<(String, String)> = url
        .query_pairs()
        .filter(|(key, _)| key != "sslmode" && key != "sslrootcert")
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();
    url.query_pairs_mut()
        .clear()
        .extend_pairs(existing)
        .append_pair("sslmode", "verify-full")
        .append_pair("sslrootcert", "system");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_direct_and_pooled_urls_from_planetscale_hostname() {
        let mut role = DatabaseRole {
            id: "role-1".to_owned(),
            name: "app".to_owned(),
            username: "app.branch".to_owned(),
            password: Some("p@ss:/word".to_owned()),
            database_name: "app db".to_owned(),
            access_host_url: "example.horizon.psdb.cloud".to_owned(),
        };
        let urls = connection_urls(&role).unwrap();
        assert_eq!(urls.direct.scheme(), "postgresql");
        assert_eq!(urls.direct.host_str(), Some("example.horizon.psdb.cloud"));
        assert_eq!(urls.direct.port(), Some(5432));
        assert_eq!(urls.pooled.port(), Some(6432));
        assert_eq!(urls.direct.username(), "app.branch");
        assert_eq!(urls.direct.password(), Some("p%40ss%3A%2Fword"));
        assert_eq!(urls.direct.path(), "/app%20db");
        assert!(urls.direct.query().unwrap().contains("sslmode=verify-full"));
        assert!(urls.direct.query().unwrap().contains("sslrootcert=system"));
        role.password = None;
        assert_eq!(
            connection_urls(&role).unwrap_err().code,
            "planetscale_role_password_missing"
        );
    }
}
