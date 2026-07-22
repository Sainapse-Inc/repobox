use chrono::{DateTime, Utc};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub(crate) struct Page<T> {
    pub data: Vec<T>,
    pub next_page: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct OrganizationResponse {
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct DatabaseResponse {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub ready: bool,
    pub region: Option<RegionResponse>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct RegionResponse {
    pub slug: Option<String>,
    pub id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct BranchResponse {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub ready: bool,
    #[serde(default)]
    pub production: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct BackupResponse {
    pub id: String,
    pub name: String,
    pub state: String,
    #[serde(default, rename = "size")]
    pub size_bytes: u64,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct RoleResponse {
    pub id: String,
    pub name: String,
    pub username: String,
    pub password: Option<String>,
    pub database_name: String,
    pub access_host_url: String,
}
