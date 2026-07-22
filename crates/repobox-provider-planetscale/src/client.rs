use std::collections::BTreeSet;
use std::time::Duration;

use async_trait::async_trait;
use reqwest::{Method, StatusCode};
use secrecy::{ExposeSecret, SecretString};
use serde::Serialize;
use serde::de::DeserializeOwned;

use repobox_core::provider::{
    Backup, Branch, CreateBranchRequest, CreateDatabaseRequest, CreateRoleRequest, Database,
    DatabaseProvider, DatabaseRole, Organization, ProviderCapabilities,
};
use repobox_core::{ErrorKind, RepoboxError, Result};

use crate::models::{
    BackupResponse, BranchResponse, DatabaseResponse, OrganizationResponse, Page, RoleResponse,
};

const DEFAULT_API_URL: &str = "https://api.planetscale.com/v1";
const MAX_RETRIES: usize = 4;

#[derive(Clone, Debug)]
pub enum PlanetScaleCredentials {
    AccessToken {
        token: SecretString,
    },
    ServiceToken {
        token_id: SecretString,
        token: SecretString,
    },
}

impl PlanetScaleCredentials {
    pub fn access_token(token: impl Into<String>) -> Self {
        Self::AccessToken {
            token: SecretString::from(token.into()),
        }
    }

    pub fn service_token(token_id: impl Into<String>, token: impl Into<String>) -> Self {
        Self::ServiceToken {
            token_id: SecretString::from(token_id.into()),
            token: SecretString::from(token.into()),
        }
    }

    pub const fn method(&self) -> &'static str {
        match self {
            Self::AccessToken { .. } => "browser_oauth",
            Self::ServiceToken { .. } => "service_token",
        }
    }

    fn authenticate(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match self {
            Self::AccessToken { token } => request.bearer_auth(token.expose_secret()),
            Self::ServiceToken { token_id, token } => request.header(
                reqwest::header::AUTHORIZATION,
                format!("{}:{}", token_id.expose_secret(), token.expose_secret()),
            ),
        }
    }
}

#[derive(Clone)]
pub struct PlanetScaleClient {
    http: reqwest::Client,
    base_url: String,
    credentials: PlanetScaleCredentials,
}

impl PlanetScaleClient {
    pub fn new(credentials: PlanetScaleCredentials) -> Result<Self> {
        Self::with_base_url(credentials, DEFAULT_API_URL)
    }

    pub fn with_base_url(
        credentials: PlanetScaleCredentials,
        base_url: impl Into<String>,
    ) -> Result<Self> {
        let http = reqwest::Client::builder()
            .user_agent(concat!("repobox/", env!("CARGO_PKG_VERSION")))
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|error| {
                RepoboxError::new(ErrorKind::Runtime, "http_client_failed", error.to_string())
            })?;
        Ok(Self {
            http,
            base_url: base_url.into().trim_end_matches('/').to_owned(),
            credentials,
        })
    }

    async fn request<T, B>(&self, method: Method, path: &str, body: Option<&B>) -> Result<T>
    where
        T: DeserializeOwned,
        B: Serialize + ?Sized,
    {
        let url = format!("{}{}", self.base_url, path);
        let retryable = method == Method::GET;
        for attempt in 0..=MAX_RETRIES {
            let mut request = self.credentials.authenticate(
                self.http
                    .request(method.clone(), &url)
                    .header(reqwest::header::ACCEPT, "application/json"),
            );
            if let Some(body) = body {
                request = request.json(body);
            }
            let response = match request.send().await {
                Ok(response) => response,
                Err(error)
                    if retryable
                        && attempt < MAX_RETRIES
                        && (error.is_connect() || error.is_timeout()) =>
                {
                    tokio::time::sleep(Duration::from_secs(1_u64 << attempt.min(4))).await;
                    continue;
                }
                Err(error) => {
                    return Err(RepoboxError::new(
                        ErrorKind::Runtime,
                        "planetscale_network_error",
                        format!("PlanetScale request failed: {error}"),
                    )
                    .with_suggestion("Check network connectivity and retry the command."));
                }
            };
            let status = response.status();
            let request_id = response
                .headers()
                .get("x-request-id")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            if matches!(
                status,
                StatusCode::TOO_MANY_REQUESTS
                    | StatusCode::BAD_GATEWAY
                    | StatusCode::SERVICE_UNAVAILABLE
                    | StatusCode::GATEWAY_TIMEOUT
            ) && retryable
                && attempt < MAX_RETRIES
            {
                let retry = response
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse::<u64>().ok())
                    .unwrap_or(1_u64 << attempt.min(4));
                tokio::time::sleep(Duration::from_secs(retry.min(10))).await;
                continue;
            }
            if status.is_success() {
                return response.json().await.map_err(|error| {
                    RepoboxError::new(
                        ErrorKind::Runtime,
                        "planetscale_response_invalid",
                        format!("PlanetScale returned invalid JSON: {error}"),
                    )
                });
            }
            let body = response.text().await.unwrap_or_default();
            return Err(api_error(status, &body, request_id));
        }
        unreachable!("retry loop returns on final attempt")
    }

    async fn delete(&self, path: &str, body: Option<&serde_json::Value>) -> Result<()> {
        let url = format!("{}{}", self.base_url, path);
        for attempt in 0..=MAX_RETRIES {
            let mut request = self.credentials.authenticate(self.http.delete(&url));
            if let Some(body) = body {
                request = request.json(body);
            }
            let response = match request.send().await {
                Ok(response) => response,
                Err(error)
                    if attempt < MAX_RETRIES && (error.is_connect() || error.is_timeout()) =>
                {
                    tokio::time::sleep(Duration::from_secs(1_u64 << attempt.min(4))).await;
                    continue;
                }
                Err(error) => {
                    return Err(RepoboxError::new(
                        ErrorKind::Runtime,
                        "planetscale_network_error",
                        error.to_string(),
                    ));
                }
            };
            let status = response.status();
            if status.is_success() || (status == StatusCode::NOT_FOUND && attempt > 0) {
                return Ok(());
            }
            let request_id = response
                .headers()
                .get("x-request-id")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            if matches!(
                status,
                StatusCode::TOO_MANY_REQUESTS
                    | StatusCode::BAD_GATEWAY
                    | StatusCode::SERVICE_UNAVAILABLE
                    | StatusCode::GATEWAY_TIMEOUT
            ) && attempt < MAX_RETRIES
            {
                let retry = response
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse::<u64>().ok())
                    .unwrap_or(1_u64 << attempt.min(4));
                tokio::time::sleep(Duration::from_secs(retry.min(10))).await;
                continue;
            }
            let response_body = response.text().await.unwrap_or_default();
            return Err(api_error(status, &response_body, request_id));
        }
        unreachable!("retry loop returns on final attempt")
    }

    async fn paginated<T: DeserializeOwned>(&self, path: &str) -> Result<Vec<T>> {
        let mut page_number = 1;
        let mut items = vec![];
        loop {
            let separator = if path.contains('?') { '&' } else { '?' };
            let page: Page<T> = self
                .request::<_, serde_json::Value>(
                    Method::GET,
                    &format!("{path}{separator}page={page_number}&per_page=100"),
                    None,
                )
                .await?;
            items.extend(page.data);
            let Some(next) = page.next_page.filter(|next| *next > page_number) else {
                break;
            };
            page_number = next;
        }
        Ok(items)
    }
}

#[async_trait]
impl DatabaseProvider for PlanetScaleClient {
    fn name(&self) -> &'static str {
        "planetscale"
    }

    async fn validate_auth(&self) -> Result<ProviderCapabilities> {
        let _: Page<OrganizationResponse> = self
            .request::<_, serde_json::Value>(Method::GET, "/organizations?page=1&per_page=1", None)
            .await?;
        // PlanetScale does not expose a portable endpoint for introspecting all
        // effective token accesses. Commands still turn 403 responses into exact
        // permission errors; this set documents capabilities validated by use.
        Ok(ProviderCapabilities {
            accesses: BTreeSet::new(),
        })
    }

    async fn list_organizations(&self) -> Result<Vec<Organization>> {
        let response: Vec<OrganizationResponse> = self.paginated("/organizations").await?;
        Ok(response
            .into_iter()
            .map(|organization| Organization {
                name: organization.name,
            })
            .collect())
    }

    async fn list_databases(&self, organization: &str) -> Result<Vec<Database>> {
        let response: Vec<DatabaseResponse> = self
            .paginated(&format!("/organizations/{organization}/databases"))
            .await?;
        Ok(response.into_iter().map(map_database).collect())
    }

    async fn create_database(&self, request: &CreateDatabaseRequest) -> Result<Database> {
        #[derive(Serialize)]
        struct Body<'a> {
            name: &'a str,
            kind: &'static str,
            cluster_size: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            region: Option<&'a str>,
            replicas: u8,
            #[serde(skip_serializing_if = "Option::is_none")]
            major_version: Option<&'a str>,
        }
        let body = Body {
            name: &request.name,
            kind: "postgresql",
            cluster_size: &request.cluster_size,
            region: request.region.as_deref(),
            replicas: 0,
            major_version: request.major_version.as_deref(),
        };
        let response: DatabaseResponse = self
            .request(
                Method::POST,
                &format!("/organizations/{}/databases", request.organization),
                Some(&body),
            )
            .await?;
        Ok(map_database(response))
    }

    async fn delete_database(&self, organization: &str, database: &str) -> Result<()> {
        self.delete(
            &format!("/organizations/{organization}/databases/{database}"),
            None,
        )
        .await
    }

    async fn list_cluster_sizes(&self, organization: &str) -> Result<Vec<String>> {
        #[derive(serde::Deserialize)]
        struct Sku {
            name: String,
            enabled: bool,
        }
        let response: Vec<Sku> = self
            .request::<_, serde_json::Value>(
                Method::GET,
                &format!("/organizations/{organization}/cluster-size-skus?engine=postgresql"),
                None,
            )
            .await?;
        Ok(response
            .into_iter()
            .filter(|sku| sku.enabled)
            .map(|sku| sku.name)
            .collect())
    }

    async fn list_branches(&self, organization: &str, database: &str) -> Result<Vec<Branch>> {
        let response: Vec<BranchResponse> = self
            .paginated(&format!(
                "/organizations/{organization}/databases/{database}/branches"
            ))
            .await?;
        Ok(response.into_iter().map(map_branch).collect())
    }

    async fn get_branch(&self, organization: &str, database: &str, branch: &str) -> Result<Branch> {
        let response: BranchResponse = self
            .request::<_, serde_json::Value>(
                Method::GET,
                &format!("/organizations/{organization}/databases/{database}/branches/{branch}"),
                None,
            )
            .await?;
        Ok(map_branch(response))
    }

    async fn create_branch(&self, request: &CreateBranchRequest) -> Result<Branch> {
        #[derive(Serialize)]
        struct Body<'a> {
            name: &'a str,
            parent_branch: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            backup_id: Option<&'a str>,
            #[serde(skip_serializing_if = "Option::is_none")]
            cluster_size: Option<&'a str>,
        }
        let body = Body {
            name: &request.name,
            parent_branch: &request.parent_branch,
            backup_id: request.backup_id.as_deref(),
            cluster_size: request.cluster_size.as_deref(),
        };
        let response: BranchResponse = self
            .request(
                Method::POST,
                &format!(
                    "/organizations/{}/databases/{}/branches",
                    request.organization, request.database
                ),
                Some(&body),
            )
            .await?;
        Ok(map_branch(response))
    }

    async fn rename_branch(
        &self,
        organization: &str,
        database: &str,
        branch: &str,
        new_name: &str,
    ) -> Result<Branch> {
        let body = serde_json::json!({"new_name": new_name});
        let response: BranchResponse = self
            .request(
                Method::PATCH,
                &format!("/organizations/{organization}/databases/{database}/branches/{branch}"),
                Some(&body),
            )
            .await?;
        Ok(map_branch(response))
    }

    async fn delete_branch(&self, organization: &str, database: &str, branch: &str) -> Result<()> {
        self.delete(
            &format!("/organizations/{organization}/databases/{database}/branches/{branch}"),
            None,
        )
        .await
    }

    async fn list_backups(
        &self,
        organization: &str,
        database: &str,
        branch: &str,
    ) -> Result<Vec<Backup>> {
        let response: Vec<BackupResponse> = self
            .paginated(&format!(
                "/organizations/{organization}/databases/{database}/branches/{branch}/backups"
            ))
            .await?;
        Ok(response.into_iter().map(map_backup).collect())
    }

    async fn create_backup(
        &self,
        organization: &str,
        database: &str,
        branch: &str,
        name: &str,
    ) -> Result<Backup> {
        let body = serde_json::json!({
            "name": name,
            "retention_unit": "day",
            "retention_value": 2,
            "emergency": false,
        });
        let response: BackupResponse = self
            .request(
                Method::POST,
                &format!(
                    "/organizations/{organization}/databases/{database}/branches/{branch}/backups"
                ),
                Some(&body),
            )
            .await?;
        Ok(map_backup(response))
    }

    async fn list_roles(
        &self,
        organization: &str,
        database: &str,
        branch: &str,
    ) -> Result<Vec<DatabaseRole>> {
        let response: Vec<RoleResponse> = self
            .paginated(&format!(
                "/organizations/{organization}/databases/{database}/branches/{branch}/roles"
            ))
            .await?;
        Ok(response.into_iter().map(map_role).collect())
    }

    async fn create_role(&self, request: &CreateRoleRequest) -> Result<DatabaseRole> {
        let body = serde_json::json!({
            "name": request.name,
            "inherited_roles": request.inherited_roles,
            "require_where_on_delete": "off",
            "require_where_on_update": "off",
        });
        let response: RoleResponse = self
            .request(
                Method::POST,
                &format!(
                    "/organizations/{}/databases/{}/branches/{}/roles",
                    request.organization, request.database, request.branch
                ),
                Some(&body),
            )
            .await?;
        Ok(map_role(response))
    }

    async fn delete_role(
        &self,
        organization: &str,
        database: &str,
        branch: &str,
        role_id: &str,
        successor: Option<&str>,
    ) -> Result<()> {
        let body = successor.map(|successor| serde_json::json!({"successor": successor}));
        self.delete(
            &format!(
                "/organizations/{organization}/databases/{database}/branches/{branch}/roles/{role_id}"
            ),
            body.as_ref(),
        )
        .await
    }
}

fn map_database(response: DatabaseResponse) -> Database {
    Database {
        id: response.id,
        name: response.name,
        ready: response.ready,
        region: response.region.and_then(|region| region.slug.or(region.id)),
    }
}

fn map_branch(response: BranchResponse) -> Branch {
    Branch {
        id: response.id,
        name: response.name,
        state: response.state,
        ready: response.ready,
        production: response.production,
    }
}

fn map_backup(response: BackupResponse) -> Backup {
    Backup {
        id: response.id,
        name: response.name,
        state: response.state,
        size_bytes: response.size_bytes,
        created_at: response.created_at,
        completed_at: response.completed_at,
    }
}

fn map_role(response: RoleResponse) -> DatabaseRole {
    DatabaseRole {
        id: response.id,
        name: response.name,
        username: response.username,
        password: response.password,
        database_name: response.database_name,
        access_host_url: response.access_host_url,
    }
}

fn api_error(status: StatusCode, body: &str, request_id: Option<String>) -> RepoboxError {
    let (kind, code, suggestion) = match status {
        StatusCode::UNAUTHORIZED => (
            ErrorKind::Authentication,
            "planetscale_authentication_required",
            "Run `repobox auth login` or set PLANETSCALE_SERVICE_TOKEN_ID and PLANETSCALE_SERVICE_TOKEN.",
        ),
        StatusCode::FORBIDDEN => (
            ErrorKind::Permission,
            "planetscale_permission_denied",
            "Update the PlanetScale service-token accesses shown by `repobox doctor`.",
        ),
        StatusCode::NOT_FOUND => (
            ErrorKind::NotFound,
            "planetscale_resource_not_found",
            "Run `repobox status --json` to refresh provider state.",
        ),
        StatusCode::CONFLICT | StatusCode::UNPROCESSABLE_ENTITY => (
            ErrorKind::Conflict,
            "planetscale_conflict",
            "Inspect the existing resource with `repobox status --json` and resume the job.",
        ),
        _ => (
            ErrorKind::Runtime,
            "planetscale_api_error",
            "Retry the command; use `repobox job view latest --json` if it remains interrupted.",
        ),
    };
    let message = serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|value| {
            value
                .get("error")
                .or_else(|| value.get("message"))
                .and_then(|value| value.as_str())
                .map(str::to_owned)
        })
        .unwrap_or_else(|| format!("PlanetScale returned HTTP {status}"));
    let mut error = RepoboxError::new(kind, code, message)
        .with_suggestion(suggestion)
        .with_doc_url("https://planetscale.com/docs/api");
    if let Some(request_id) = request_id {
        error = error.with_request_id(request_id);
    }
    error
}

#[cfg(test)]
mod tests {
    use super::*;
    use repobox_core::provider::DatabaseProvider;
    use wiremock::matchers::{body_json, header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn credentials() -> PlanetScaleCredentials {
        PlanetScaleCredentials::service_token("token-id", "token-secret")
    }

    #[tokio::test]
    async fn service_token_auth_is_id_colon_secret() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/organizations"))
            .and(query_param("page", "1"))
            .and(query_param("per_page", "1"))
            .and(header("authorization", "token-id:token-secret"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": [],
                "next_page": null
            })))
            .mount(&server)
            .await;
        let client = PlanetScaleClient::with_base_url(credentials(), server.uri()).unwrap();
        client.validate_auth().await.unwrap();
    }

    #[tokio::test]
    async fn browser_auth_uses_bearer_access_token() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/organizations"))
            .and(query_param("page", "1"))
            .and(query_param("per_page", "1"))
            .and(header("authorization", "Bearer access-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": [],
                "next_page": null
            })))
            .mount(&server)
            .await;
        let client = PlanetScaleClient::with_base_url(
            PlanetScaleCredentials::access_token("access-token"),
            server.uri(),
        )
        .unwrap();
        client.validate_auth().await.unwrap();
    }

    #[tokio::test]
    async fn organizations_are_paginated() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/organizations"))
            .and(query_param("page", "1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": [{"name": "first"}],
                "next_page": 2
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/organizations"))
            .and(query_param("page", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": [{"name": "second"}],
                "next_page": null
            })))
            .mount(&server)
            .await;
        let client = PlanetScaleClient::with_base_url(credentials(), server.uri()).unwrap();
        let organizations = client.list_organizations().await.unwrap();
        assert_eq!(
            organizations
                .into_iter()
                .map(|organization| organization.name)
                .collect::<Vec<_>>(),
            ["first", "second"]
        );
    }

    #[tokio::test]
    async fn cluster_sizes_are_postgres_only_and_enabled() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/organizations/acme/cluster-size-skus"))
            .and(query_param("engine", "postgresql"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                {"name": "PS_10", "enabled": true},
                {"name": "PS_5_PRIVATE", "enabled": false}
            ])))
            .mount(&server)
            .await;
        let client = PlanetScaleClient::with_base_url(credentials(), server.uri()).unwrap();
        assert_eq!(
            client.list_cluster_sizes("acme").await.unwrap(),
            vec!["PS_10".to_owned()]
        );
    }

    #[tokio::test]
    async fn database_create_omits_unspecified_provider_defaults() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/organizations/acme/databases"))
            .and(body_json(serde_json::json!({
                "name": "app",
                "kind": "postgresql",
                "cluster_size": "PS_10",
                "replicas": 0
            })))
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
                "id": "database-123",
                "name": "app",
                "ready": false,
                "region": null
            })))
            .mount(&server)
            .await;
        let client = PlanetScaleClient::with_base_url(credentials(), server.uri()).unwrap();
        let database = client
            .create_database(&CreateDatabaseRequest {
                organization: "acme".to_owned(),
                name: "app".to_owned(),
                region: None,
                cluster_size: "PS_10".to_owned(),
                major_version: None,
            })
            .await
            .unwrap();
        assert_eq!(database.name, "app");
        assert!(!database.ready);
    }

    #[tokio::test]
    async fn permission_errors_have_stable_classification() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/organizations"))
            .respond_with(
                ResponseTemplate::new(403)
                    .insert_header("x-request-id", "request-123")
                    .set_body_json(serde_json::json!({"error": "missing access"})),
            )
            .mount(&server)
            .await;
        let client = PlanetScaleClient::with_base_url(credentials(), server.uri()).unwrap();
        let error = client.list_organizations().await.unwrap_err();
        assert_eq!(error.kind, ErrorKind::Permission);
        assert_eq!(error.code, "planetscale_permission_denied");
        assert_eq!(error.request_id.as_deref(), Some("request-123"));
    }

    #[tokio::test]
    async fn created_role_accepts_planetscale_hostname_response() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(
                "/organizations/acme/databases/app/branches/main/roles",
            ))
            .and(body_json(serde_json::json!({
                "name": "repobox-app",
                "inherited_roles": ["postgres"],
                "require_where_on_delete": "off",
                "require_where_on_update": "off"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "role-123",
                "name": "repobox-app",
                "username": "repobox-app.branch-id",
                "password": "pscale_pw_fake_for_testing",
                "database_name": "app",
                "access_host_url": "example.horizon.psdb.cloud"
            })))
            .mount(&server)
            .await;
        let client = PlanetScaleClient::with_base_url(credentials(), server.uri()).unwrap();
        let role = client
            .create_role(&CreateRoleRequest {
                organization: "acme".to_owned(),
                database: "app".to_owned(),
                branch: "main".to_owned(),
                name: "repobox-app".to_owned(),
                inherited_roles: vec!["postgres".to_owned()],
            })
            .await
            .unwrap();
        let urls = repobox_core::provider::connection_urls(&role).unwrap();
        assert_eq!(urls.direct.host_str(), Some("example.horizon.psdb.cloud"));
        assert_eq!(urls.direct.port(), Some(5432));
        assert_eq!(urls.pooled.port(), Some(6432));
    }

    #[tokio::test]
    async fn role_delete_sends_successor_in_json_body() {
        let server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path(
                "/organizations/acme/databases/app/branches/main/roles/role-123",
            ))
            .and(body_json(serde_json::json!({"successor": "postgres"})))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;
        let client = PlanetScaleClient::with_base_url(credentials(), server.uri()).unwrap();
        client
            .delete_role("acme", "app", "main", "role-123", Some("postgres"))
            .await
            .unwrap();
    }
}
