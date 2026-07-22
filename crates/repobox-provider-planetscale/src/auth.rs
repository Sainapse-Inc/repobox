use std::time::{Duration, Instant};

use reqwest::{StatusCode, header};
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use url::form_urlencoded;

use repobox_core::{ErrorKind, RepoboxError, Result};

/// Public OAuth client used by `PlanetScale`'s official CLI device flow.
///
/// `PlanetScale` documents this client as non-confidential in the
/// [CLI source](https://github.com/planetscale/cli/blob/18c5d476ea58c886e455f427dab70849d6cde49e/internal/auth/authenticator.go).
const OAUTH_CLIENT_ID: &str = "wzzkYKOfRcxFAiMgDgfbhO9yIikNIlt9-yhosmvPBQA";
const OAUTH_CLIENT_SECRET: &str = "eIDdgw21BYsovcrpC4iKZQ0o7ol9cN1LsSr8fuNyg5o";
const DEFAULT_AUTH_URL: &str = "https://auth.planetscale.com";
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(5);
const SLOW_DOWN_INCREMENT: Duration = Duration::from_secs(5);
const SCOPES: &str = "read_databases write_databases read_user read_organization";

#[derive(Clone, Debug)]
pub struct DeviceAuthorization {
    device_code: SecretString,
    user_code: String,
    verification_url: String,
    verification_url_complete: String,
    interval: Duration,
    deadline: Instant,
    expires_in: Duration,
}

impl DeviceAuthorization {
    pub fn user_code(&self) -> &str {
        &self.user_code
    }

    pub fn verification_url(&self) -> &str {
        if self.verification_url_complete.is_empty() {
            &self.verification_url
        } else {
            &self.verification_url_complete
        }
    }

    pub const fn expires_in(&self) -> Duration {
        self.expires_in
    }
}

#[derive(Clone)]
pub struct PlanetScaleDeviceAuth {
    http: reqwest::Client,
    base_url: String,
    client_id: String,
    client_secret: SecretString,
}

impl PlanetScaleDeviceAuth {
    pub fn new() -> Result<Self> {
        Self::with_base_url(DEFAULT_AUTH_URL, OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET)
    }

    pub fn with_base_url(
        base_url: impl Into<String>,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
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
            client_id: client_id.into(),
            client_secret: SecretString::from(client_secret.into()),
        })
    }

    pub async fn start(&self) -> Result<DeviceAuthorization> {
        let body = form_body(&[("client_id", &self.client_id), ("scope", SCOPES)]);
        let response = self.send_form("/oauth/authorize_device", body).await?;
        let status = response.status();
        let request_id = request_id(&response);
        let body = response.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(oauth_error(
                status,
                &body,
                request_id,
                "planetscale_device_authorization_failed",
            ));
        }
        let response: DeviceCodeResponse = serde_json::from_str(&body).map_err(|error| {
            RepoboxError::new(
                ErrorKind::Runtime,
                "planetscale_oauth_response_invalid",
                format!("PlanetScale returned an invalid device authorization response: {error}"),
            )
        })?;
        if response.device_code.is_empty()
            || response.user_code.is_empty()
            || (response.verification_uri.is_empty()
                && response.verification_uri_complete.is_empty())
            || response.expires_in == 0
        {
            return Err(RepoboxError::new(
                ErrorKind::Runtime,
                "planetscale_oauth_response_invalid",
                "PlanetScale returned an incomplete device authorization response",
            ));
        }
        let expires_in = Duration::from_secs(response.expires_in);
        let deadline = Instant::now().checked_add(expires_in).ok_or_else(|| {
            RepoboxError::new(
                ErrorKind::Runtime,
                "planetscale_oauth_response_invalid",
                "PlanetScale returned an invalid device authorization expiry",
            )
        })?;
        Ok(DeviceAuthorization {
            device_code: SecretString::from(response.device_code),
            user_code: response.user_code,
            verification_url: response.verification_uri,
            verification_url_complete: response.verification_uri_complete,
            interval: if response.interval == 0 {
                DEFAULT_POLL_INTERVAL
            } else {
                Duration::from_secs(response.interval)
            },
            deadline,
            expires_in,
        })
    }

    pub async fn wait_for_access_token(
        &self,
        authorization: &DeviceAuthorization,
    ) -> Result<SecretString> {
        let mut interval = authorization.interval;
        loop {
            let remaining = authorization
                .deadline
                .checked_duration_since(Instant::now())
                .ok_or_else(device_authorization_expired)?;
            tokio::time::sleep(interval.min(remaining)).await;
            if Instant::now() >= authorization.deadline {
                return Err(device_authorization_expired());
            }
            match self.request_access_token(authorization).await? {
                TokenPoll::AccessToken(token) => return Ok(token),
                TokenPoll::Pending => {}
                TokenPoll::SlowDown => {
                    interval = interval.saturating_add(SLOW_DOWN_INCREMENT);
                }
            }
        }
    }

    pub async fn revoke(&self, access_token: &SecretString) -> Result<()> {
        let body = form_body(&[
            ("client_id", &self.client_id),
            ("client_secret", self.client_secret.expose_secret()),
            ("token", access_token.expose_secret()),
        ]);
        let response = self.send_form("/oauth/revoke", body).await?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        let request_id = request_id(&response);
        let body = response.text().await.unwrap_or_default();
        Err(oauth_error(
            status,
            &body,
            request_id,
            "planetscale_token_revoke_failed",
        ))
    }

    async fn request_access_token(&self, authorization: &DeviceAuthorization) -> Result<TokenPoll> {
        let body = form_body(&[
            ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
            ("device_code", authorization.device_code.expose_secret()),
            ("client_id", &self.client_id),
        ]);
        let response = self.send_form("/oauth/token", body).await?;
        let status = response.status();
        let request_id = request_id(&response);
        let body = response.text().await.unwrap_or_default();
        classify_token_response(status, &body, request_id)
    }

    async fn send_form(&self, path: &str, body: String) -> Result<reqwest::Response> {
        self.http
            .post(format!("{}{}", self.base_url, path))
            .header(header::ACCEPT, "application/json")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(body)
            .send()
            .await
            .map_err(|error| {
                RepoboxError::new(
                    ErrorKind::Runtime,
                    "planetscale_oauth_network_error",
                    format!("PlanetScale authentication failed: {error}"),
                )
                .with_suggestion("Check network connectivity and retry `repobox auth login`.")
            })
    }
}

#[derive(Debug, Deserialize)]
struct DeviceCodeResponse {
    device_code: String,
    user_code: String,
    verification_uri: String,
    #[serde(default)]
    verification_uri_complete: String,
    expires_in: u64,
    #[serde(default)]
    interval: u64,
}

#[derive(Debug, Deserialize)]
struct OAuthTokenResponse {
    access_token: String,
}

#[derive(Debug, Deserialize)]
struct OAuthErrorResponse {
    error: String,
    #[serde(default)]
    error_description: String,
}

enum TokenPoll {
    AccessToken(SecretString),
    Pending,
    SlowDown,
}

fn classify_token_response(
    status: StatusCode,
    body: &str,
    request_id: Option<String>,
) -> Result<TokenPoll> {
    if status.is_success() {
        let token: OAuthTokenResponse = serde_json::from_str(body).map_err(|error| {
            RepoboxError::new(
                ErrorKind::Runtime,
                "planetscale_oauth_response_invalid",
                format!("PlanetScale returned an invalid access-token response: {error}"),
            )
        })?;
        if token.access_token.is_empty() {
            return Err(RepoboxError::new(
                ErrorKind::Runtime,
                "planetscale_oauth_response_invalid",
                "PlanetScale returned an empty access token",
            ));
        }
        return Ok(TokenPoll::AccessToken(SecretString::from(
            token.access_token,
        )));
    }

    let response = serde_json::from_str::<OAuthErrorResponse>(body).ok();
    match response.as_ref().map(|response| response.error.as_str()) {
        Some("authorization_pending") => Ok(TokenPoll::Pending),
        Some("slow_down") => Ok(TokenPoll::SlowDown),
        Some("access_denied") => Err(RepoboxError::new(
            ErrorKind::Authentication,
            "planetscale_device_access_denied",
            "PlanetScale device authorization was denied",
        )
        .with_suggestion("Run `repobox auth login` to start a new authorization.")),
        Some("expired_token") => Err(device_authorization_expired()),
        _ => Err(oauth_error(
            status,
            body,
            request_id,
            "planetscale_device_authorization_failed",
        )),
    }
}

fn oauth_error(
    status: StatusCode,
    body: &str,
    request_id: Option<String>,
    code: &str,
) -> RepoboxError {
    let response = serde_json::from_str::<OAuthErrorResponse>(body).ok();
    let message = response
        .as_ref()
        .and_then(|response| {
            if !response.error_description.is_empty() {
                Some(response.error_description.clone())
            } else if !response.error.is_empty() {
                Some(format!(
                    "PlanetScale authentication failed: {}",
                    response.error
                ))
            } else {
                None
            }
        })
        .unwrap_or_else(|| format!("PlanetScale authentication returned HTTP {status}"));
    let kind = if status.is_client_error() {
        ErrorKind::Authentication
    } else {
        ErrorKind::Runtime
    };
    let mut error = RepoboxError::new(kind, code, message)
        .with_suggestion("Retry `repobox auth login`; if this persists, check PlanetScale status.")
        .with_doc_url("https://github.com/planetscale/cli/tree/main/internal/auth");
    if let Some(request_id) = request_id {
        error = error.with_request_id(request_id);
    }
    error
}

fn device_authorization_expired() -> RepoboxError {
    RepoboxError::new(
        ErrorKind::Authentication,
        "planetscale_device_authorization_expired",
        "PlanetScale device authorization expired before it was approved",
    )
    .with_suggestion("Run `repobox auth login` to request a new confirmation code.")
}

fn request_id(response: &reqwest::Response) -> Option<String> {
    response
        .headers()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

fn form_body(values: &[(&str, &str)]) -> String {
    let mut serializer = form_urlencoded::Serializer::new(String::new());
    serializer.extend_pairs(values.iter().copied());
    serializer.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_string, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn test_client(server: &MockServer) -> PlanetScaleDeviceAuth {
        PlanetScaleDeviceAuth::with_base_url(server.uri(), "test-client", "test-secret").unwrap()
    }

    #[tokio::test]
    async fn starts_device_authorization_with_official_scopes() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/oauth/authorize_device"))
            .and(header("content-type", "application/x-www-form-urlencoded"))
            .and(body_string(
                "client_id=test-client&scope=read_databases+write_databases+read_user+read_organization",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "device_code": "private-device-code",
                "user_code": "ABCD-EFGH",
                "verification_uri": "https://example.test/device",
                "verification_uri_complete": "https://example.test/device?user_code=ABCD-EFGH",
                "expires_in": 900,
                "interval": 1
            })))
            .mount(&server)
            .await;

        let authorization = test_client(&server).start().await.unwrap();
        assert_eq!(authorization.user_code(), "ABCD-EFGH");
        assert_eq!(
            authorization.verification_url(),
            "https://example.test/device?user_code=ABCD-EFGH"
        );
        assert_eq!(authorization.expires_in(), Duration::from_mins(15));
    }

    #[tokio::test]
    async fn exchanges_device_code_for_access_token() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/oauth/token"))
            .and(body_string("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Adevice_code&device_code=device-code&client_id=test-client"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "access-token"
            })))
            .mount(&server)
            .await;
        let authorization = DeviceAuthorization {
            device_code: SecretString::from("device-code".to_owned()),
            user_code: "ABCD-EFGH".to_owned(),
            verification_url: "https://example.test/device".to_owned(),
            verification_url_complete: String::new(),
            interval: Duration::from_millis(1),
            deadline: Instant::now() + Duration::from_secs(1),
            expires_in: Duration::from_secs(1),
        };

        let token = test_client(&server)
            .wait_for_access_token(&authorization)
            .await
            .unwrap();
        assert_eq!(token.expose_secret(), "access-token");
    }

    #[test]
    fn classifies_pending_slowdown_and_denial() {
        assert!(matches!(
            classify_token_response(
                StatusCode::BAD_REQUEST,
                r#"{"error":"authorization_pending"}"#,
                None
            )
            .unwrap(),
            TokenPoll::Pending
        ));
        assert!(matches!(
            classify_token_response(StatusCode::BAD_REQUEST, r#"{"error":"slow_down"}"#, None)
                .unwrap(),
            TokenPoll::SlowDown
        ));
        let error = classify_token_response(
            StatusCode::BAD_REQUEST,
            r#"{"error":"access_denied"}"#,
            None,
        )
        .err()
        .unwrap();
        assert_eq!(error.code, "planetscale_device_access_denied");
    }

    #[tokio::test]
    async fn revokes_access_token() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/oauth/revoke"))
            .and(body_string(
                "client_id=test-client&client_secret=test-secret&token=access-token",
            ))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        test_client(&server)
            .revoke(&SecretString::from("access-token".to_owned()))
            .await
            .unwrap();
    }
}
