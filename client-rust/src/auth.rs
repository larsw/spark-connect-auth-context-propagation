//! Token acquisition.
//!
//! Deliberately pluggable, because how a user proves who they are is orthogonal to the thing this
//! PoC is about. Four implementations, mirroring the other two clients:
//!
//! * [`DeviceCodeTokenProvider`] -- the real CLI experience (RFC 8628).
//! * [`ClientCredentialsTokenProvider`] -- for a workload with no user behind it (RFC 6749 §4.4).
//! * [`PasswordGrantTokenProvider`] -- used by the tests, so they stay headless.
//! * [`StaticTokenProvider`] -- a fixed string, for tests that want a specific or a bad token.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

/// Refresh this long before the server thinks the token expires.
const EXPIRY_MARGIN: Duration = Duration::from_secs(30);

#[derive(Debug, thiserror::Error)]
pub enum PropagationError {
    #[error("{error}{}", .description.as_deref().map(|d| format!(": {d}")).unwrap_or_default())]
    OAuth {
        error: String,
        description: Option<String>,
        status: u16,
    },
    #[error("HTTP error talking to the authorization server: {0}")]
    Http(#[from] reqwest::Error),
    #[error("device authorization expired before sign-in completed")]
    DeviceFlowTimedOut,
    #[error("{0}")]
    Spark(String),
}

impl PropagationError {
    /// The OAuth error code, when this was one -- e.g. `invalid_grant`, `authorization_pending`.
    pub fn oauth_error(&self) -> Option<&str> {
        match self {
            PropagationError::OAuth { error, .. } => Some(error),
            _ => None,
        }
    }
}

/// The OIDC endpoints of one realm.
#[derive(Debug, Clone)]
pub struct Endpoints {
    issuer: String,
}

impl Endpoints {
    pub fn new(issuer: &str) -> Self {
        Self {
            issuer: issuer.trim_end_matches('/').to_string(),
        }
    }

    pub fn token(&self) -> String {
        format!("{}/protocol/openid-connect/token", self.issuer)
    }

    pub fn device_authorization(&self) -> String {
        format!("{}/protocol/openid-connect/auth/device", self.issuer)
    }
}

/// Anything that can supply a currently-valid access token.
///
/// Called once per [`connect`](crate::connect), not once per RPC: see the crate docs for why this
/// client cannot refresh mid-session.
#[allow(async_fn_in_trait)]
pub trait TokenProvider {
    async fn token(&self) -> Result<String, PropagationError>;
}

/// A fixed token.
#[derive(Debug, Clone)]
pub struct StaticTokenProvider {
    token: String,
}

impl StaticTokenProvider {
    pub fn new(token: &str) -> Self {
        Self {
            token: token.to_string(),
        }
    }
}

impl TokenProvider for StaticTokenProvider {
    async fn token(&self) -> Result<String, PropagationError> {
        Ok(self.token.clone())
    }
}

/// One access token and what is known about its life.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Tokens {
    access_token: String,
    #[serde(default)]
    refresh_token: Option<String>,
    /// Epoch SECONDS, so the file is byte-compatible with the Python and JVM clients' cache.
    expires_at: f64,
}

impl Tokens {
    fn from_response(payload: &serde_json::Value) -> Result<Self, PropagationError> {
        let access_token = payload
            .get("access_token")
            .and_then(|v| v.as_str())
            .ok_or_else(|| PropagationError::Spark("token response carried no access_token".into()))?
            .to_string();
        let expires_in = payload.get("expires_in").and_then(|v| v.as_f64()).unwrap_or(60.0);
        Ok(Self {
            access_token,
            refresh_token: payload
                .get("refresh_token")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            expires_at: now_seconds() + expires_in,
        })
    }

    fn fresh(&self) -> bool {
        now_seconds() < self.expires_at - EXPIRY_MARGIN.as_secs_f64()
    }
}

fn now_seconds() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

/// Form POST to a token endpoint, parsing the JSON reply.
async fn post_form(
    url: &str,
    form: &[(&str, &str)],
) -> Result<serde_json::Value, PropagationError> {
    post_form_as(url, form, None).await
}

/// As [`post_form`], authenticating the client with an `Authorization` header when given one.
async fn post_form_as(
    url: &str,
    form: &[(&str, &str)],
    authorization: Option<&str>,
) -> Result<serde_json::Value, PropagationError> {
    let mut request = reqwest::Client::new().post(url).form(form);
    if let Some(value) = authorization {
        request = request.header(reqwest::header::AUTHORIZATION, value);
    }
    let response = request.send().await?;
    let status = response.status();
    let body = response.text().await?;

    let parsed: serde_json::Value = serde_json::from_str(&body).map_err(|_| {
        PropagationError::Spark(format!(
            "{url} returned HTTP {} with a non-JSON body: {}",
            status.as_u16(),
            body.chars().take(300).collect::<String>()
        ))
    })?;

    if status.is_client_error() || status.is_server_error() {
        return Err(PropagationError::OAuth {
            error: parsed
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string(),
            description: parsed
                .get("error_description")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            status: status.as_u16(),
        });
    }
    Ok(parsed)
}

/// The `Authorization` value for `client_secret_basic` (RFC 6749 §2.3.1).
///
/// Both halves are form-urlencoded *before* the pair is base64'd, which is why this is not
/// reqwest's `basic_auth`. Skipping that step works until a client id holds a `:` or a secret holds
/// a `%` or a `+`, and then fails as `invalid_client` with nothing to say why.
fn client_secret_basic(client_id: &str, client_secret: &str) -> String {
    let encode = |raw: &str| form_urlencoded::byte_serialize(raw.as_bytes()).collect::<String>();
    let pair = format!("{}:{}", encode(client_id), encode(client_secret));
    format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(pair)
    )
}

/// OAuth 2.0 client credentials grant (RFC 6749 §4.4) -- for a job with no user behind it.
///
/// The token is the client's own: Keycloak issues it to the client's service account, so that
/// account is what the server propagates and what Polaris authorises, by its own grants. Nothing is
/// cached on disk and no refresh token is used (§4.4.3 advises servers not to issue one): a stale
/// token is replaced by running the grant again, which needs only the secret in hand.
pub struct ClientCredentialsTokenProvider {
    endpoints: Endpoints,
    client_id: String,
    client_secret: String,
    scope: Option<String>,
    cached: Mutex<Option<Tokens>>,
}

impl ClientCredentialsTokenProvider {
    pub fn new(endpoints: Endpoints, client_id: &str, client_secret: &str) -> Self {
        Self {
            endpoints,
            client_id: client_id.to_string(),
            client_secret: client_secret.to_string(),
            scope: None,
            cached: Mutex::new(None),
        }
    }

    /// Ask for this scope. Some issuers insist on one; Keycloak does not.
    pub fn with_scope(mut self, scope: &str) -> Self {
        self.scope = Some(scope.to_string());
        self
    }
}

impl TokenProvider for ClientCredentialsTokenProvider {
    async fn token(&self) -> Result<String, PropagationError> {
        let mut cached = self.cached.lock().await;
        if let Some(tokens) = cached.as_ref().filter(|tokens| tokens.fresh()) {
            return Ok(tokens.access_token.clone());
        }

        let mut form = vec![("grant_type", "client_credentials")];
        if let Some(scope) = self.scope.as_deref() {
            form.push(("scope", scope));
        }
        let authorization = client_secret_basic(&self.client_id, &self.client_secret);
        let payload = post_form_as(&self.endpoints.token(), &form, Some(&authorization)).await?;
        let tokens = Tokens::from_response(&payload)?;
        let access = tokens.access_token.clone();
        *cached = Some(tokens);
        Ok(access)
    }
}

/// Direct access grant. Used by the tests, so they stay headless.
pub struct PasswordGrantTokenProvider {
    endpoints: Endpoints,
    client_id: String,
    username: String,
    password: String,
    cached: Mutex<Option<Tokens>>,
}

impl PasswordGrantTokenProvider {
    pub fn new(endpoints: Endpoints, client_id: &str, username: &str, password: &str) -> Self {
        Self {
            endpoints,
            client_id: client_id.to_string(),
            username: username.to_string(),
            password: password.to_string(),
            cached: Mutex::new(None),
        }
    }
}

impl TokenProvider for PasswordGrantTokenProvider {
    async fn token(&self) -> Result<String, PropagationError> {
        let mut cached = self.cached.lock().await;
        if let Some(tokens) = cached.as_ref() {
            if tokens.fresh() {
                return Ok(tokens.access_token.clone());
            }
            if let Some(refresh) = tokens.refresh_token.as_deref() {
                match post_form(
                    &self.endpoints.token(),
                    &[
                        ("grant_type", "refresh_token"),
                        ("client_id", &self.client_id),
                        ("refresh_token", refresh),
                    ],
                )
                .await
                {
                    Ok(payload) => {
                        let tokens = Tokens::from_response(&payload)?;
                        let access = tokens.access_token.clone();
                        *cached = Some(tokens);
                        return Ok(access);
                    }
                    // Refresh token spent, or the network blinked. Either way a full grant is the
                    // honest next move; if the IdP is really down, that fails loudly on its own.
                    Err(PropagationError::OAuth { .. }) | Err(PropagationError::Http(_)) => {}
                    Err(other) => return Err(other),
                }
            }
        }

        let payload = post_form(
            &self.endpoints.token(),
            &[
                ("grant_type", "password"),
                ("client_id", &self.client_id),
                ("username", &self.username),
                ("password", &self.password),
            ],
        )
        .await?;
        let tokens = Tokens::from_response(&payload)?;
        let access = tokens.access_token.clone();
        *cached = Some(tokens);
        Ok(access)
    }
}

/// OAuth 2.0 Device Authorization Grant (RFC 8628) -- the CLI login flow.
///
/// Caches to the same file, in the same directory, with the same field names as the Python and
/// JVM clients, so signing in with any one of the three signs you in for all of them.
pub struct DeviceCodeTokenProvider {
    endpoints: Endpoints,
    client_id: String,
    label: String,
    cache_dir: PathBuf,
    prompt: Box<dyn Fn(&str) + Send + Sync>,
    cached: Mutex<Option<Tokens>>,
}

impl DeviceCodeTokenProvider {
    pub fn new(endpoints: Endpoints, client_id: &str, label: &str) -> Self {
        Self::with_cache_dir(endpoints, client_id, label, default_cache_dir())
    }

    pub fn with_cache_dir(
        endpoints: Endpoints,
        client_id: &str,
        label: &str,
        cache_dir: PathBuf,
    ) -> Self {
        Self {
            endpoints,
            client_id: client_id.to_string(),
            label: label.to_string(),
            cache_dir,
            prompt: Box::new(|line| println!("{line}")),
            cached: Mutex::new(None),
        }
    }

    /// Where the "open this URL" instructions go. A library should not assume stdout.
    pub fn prompt_with(mut self, prompt: impl Fn(&str) + Send + Sync + 'static) -> Self {
        self.prompt = Box::new(prompt);
        self
    }

    pub fn cache_file(&self) -> PathBuf {
        let safe: String = format!("{}-{}", self.client_id, self.label)
            .chars()
            .map(|c| if c.is_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
            .collect();
        self.cache_dir.join(format!("{safe}.json"))
    }

    fn load_cache(&self) -> Option<Tokens> {
        let raw = std::fs::read_to_string(self.cache_file()).ok()?;
        serde_json::from_str(&raw).ok()
    }

    fn save_cache(&self, tokens: &Tokens) {
        // A cold login next time is a nuisance, not an error, so nothing here is fatal.
        if std::fs::create_dir_all(&self.cache_dir).is_err() {
            return;
        }
        let Ok(json) = serde_json::to_string(tokens) else {
            return;
        };
        let file = self.cache_file();
        if std::fs::write(&file, json).is_ok() {
            restrict_permissions(&file);
        }
    }

    async fn try_refresh(&self, refresh_token: &str) -> Option<Tokens> {
        // Either the refresh token is spent -- a Keycloak restart wipes every SSO session while
        // the cache on disk still looks usable -- or the network blinked. Both mean "go back to a
        // device login", not "fail".
        let payload = post_form(
            &self.endpoints.token(),
            &[
                ("grant_type", "refresh_token"),
                ("client_id", &self.client_id),
                ("refresh_token", refresh_token),
            ],
        )
        .await
        .ok()?;
        Tokens::from_response(&payload).ok()
    }

    async fn device_flow(&self) -> Result<Tokens, PropagationError> {
        let start = post_form(
            &self.endpoints.device_authorization(),
            &[("client_id", &self.client_id), ("scope", "openid profile")],
        )
        .await?;

        let complete = start.get("verification_uri_complete").and_then(|v| v.as_str());
        let verification = complete
            .or_else(|| start.get("verification_uri").and_then(|v| v.as_str()))
            .unwrap_or("");

        (self.prompt)("");
        (self.prompt)("  To sign in, open this URL in a browser:");
        (self.prompt)("");
        (self.prompt)(&format!("      {verification}"));
        if complete.is_none() {
            let code = start.get("user_code").and_then(|v| v.as_str()).unwrap_or("");
            (self.prompt)("");
            (self.prompt)(&format!("  and enter the code:  {code}"));
        }
        (self.prompt)("");
        (self.prompt)("  Waiting for you to finish ...");

        let device_code = start
            .get("device_code")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let mut interval = Duration::from_secs_f64(
            start.get("interval").and_then(|v| v.as_f64()).unwrap_or(5.0),
        );
        let deadline = SystemTime::now()
            + Duration::from_secs_f64(
                start.get("expires_in").and_then(|v| v.as_f64()).unwrap_or(600.0),
            );

        while SystemTime::now() < deadline {
            tokio::time::sleep(interval).await;
            match post_form(
                &self.endpoints.token(),
                &[
                    ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
                    ("client_id", &self.client_id),
                    ("device_code", &device_code),
                ],
            )
            .await
            {
                Ok(payload) => {
                    (self.prompt)("  Signed in.");
                    (self.prompt)("");
                    return Tokens::from_response(&payload);
                }
                Err(e) => match e.oauth_error() {
                    Some("authorization_pending") => {}
                    Some("slow_down") => interval += Duration::from_secs(5),
                    _ => return Err(e),
                },
            }
        }
        Err(PropagationError::DeviceFlowTimedOut)
    }
}

impl TokenProvider for DeviceCodeTokenProvider {
    async fn token(&self) -> Result<String, PropagationError> {
        let mut cached = self.cached.lock().await;
        if cached.is_none() {
            *cached = self.load_cache();
        }
        if let Some(tokens) = cached.as_ref() {
            if tokens.fresh() {
                return Ok(tokens.access_token.clone());
            }
            if let Some(refresh) = tokens.refresh_token.clone() {
                if let Some(refreshed) = self.try_refresh(&refresh).await {
                    self.save_cache(&refreshed);
                    let access = refreshed.access_token.clone();
                    *cached = Some(refreshed);
                    return Ok(access);
                }
            }
        }
        let tokens = self.device_flow().await?;
        self.save_cache(&tokens);
        let access = tokens.access_token.clone();
        *cached = Some(tokens);
        Ok(access)
    }
}

pub(crate) fn default_cache_dir() -> PathBuf {
    let base = std::env::var("XDG_CACHE_HOME")
        .ok()
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .or_else(|| std::env::var("HOME").ok().map(|h| Path::new(&h).join(".cache")))
        .unwrap_or_else(|| PathBuf::from(".cache"));
    base.join("spark-connect-poc")
}

#[cfg(unix)]
fn restrict_permissions(file: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let _ = std::fs::set_permissions(file, std::fs::Permissions::from_mode(0o600));
}

#[cfg(not(unix))]
fn restrict_permissions(_file: &Path) {}

/// Headers every RPC of a session will carry.
pub(crate) fn propagation_headers(
    token: &str,
    correlation_id: &str,
    shared_secret: Option<&str>,
) -> HashMap<String, String> {
    let mut headers = HashMap::new();
    headers.insert(crate::DEFAULT_TOKEN_HEADER.to_string(), token.to_string());
    headers.insert(
        crate::DEFAULT_CORRELATION_HEADER.to_string(),
        correlation_id.to_string(),
    );
    if let Some(secret) = shared_secret.filter(|s| !s.is_empty()) {
        // Spark's own PreSharedKeyAuthenticationInterceptor owns this header and compares it to a
        // single shared secret. It answers "is this a trusted client"; the user token above
        // answers "which user is it".
        headers.insert("authorization".to_string(), format!("Bearer {secret}"));
    }
    headers
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoints_are_derived_from_the_issuer() {
        let plain = Endpoints::new("http://keycloak:8080/realms/spark");
        let slashed = Endpoints::new("http://keycloak:8080/realms/spark/");

        assert_eq!(
            plain.token(),
            "http://keycloak:8080/realms/spark/protocol/openid-connect/token"
        );
        assert_eq!(plain.token(), slashed.token());
        assert_eq!(
            plain.device_authorization(),
            "http://keycloak:8080/realms/spark/protocol/openid-connect/auth/device"
        );
    }

    #[test]
    fn headers_carry_the_token_and_the_correlation_id() {
        let headers = propagation_headers("jwt-value", "cid-1", Some("pre-shared"));

        assert_eq!(headers[crate::DEFAULT_TOKEN_HEADER], "jwt-value");
        assert_eq!(headers[crate::DEFAULT_CORRELATION_HEADER], "cid-1");
        assert_eq!(headers["authorization"], "Bearer pre-shared");
    }

    #[test]
    fn the_shared_secret_is_omitted_when_unset() {
        for secret in [None, Some("")] {
            let headers = propagation_headers("jwt", "cid", secret);
            assert!(!headers.contains_key("authorization"), "{secret:?}");
            assert_eq!(headers[crate::DEFAULT_TOKEN_HEADER], "jwt");
        }
    }

    #[test]
    fn the_cache_file_is_named_the_way_the_other_clients_name_it() {
        let provider = DeviceCodeTokenProvider::with_cache_dir(
            Endpoints::new("http://keycloak:8080/realms/spark"),
            "spark-cli",
            "alice",
            PathBuf::from("/tmp/does-not-matter"),
        );

        assert_eq!(
            provider.cache_file().file_name().unwrap().to_str().unwrap(),
            "spark-cli-alice.json"
        );
    }

    #[tokio::test]
    async fn the_python_clients_cache_file_is_read_as_is() {
        let dir = tempfile::tempdir().unwrap();
        let provider = DeviceCodeTokenProvider::with_cache_dir(
            Endpoints::new("http://keycloak:8080/realms/spark"),
            "spark-cli",
            "alice",
            dir.path().to_path_buf(),
        );
        // Exactly what the Python and JVM clients write: expires_at in epoch SECONDS.
        let cached = serde_json::json!({
            "access_token": "cached-access-token",
            "refresh_token": "cached-refresh-token",
            "expires_at": now_seconds() + 300.0,
        });
        std::fs::write(provider.cache_file(), cached.to_string()).unwrap();

        // Fresh in the cache, so this must not touch the network at all.
        assert_eq!(provider.token().await.unwrap(), "cached-access-token");
    }

    #[tokio::test]
    async fn a_corrupt_cache_file_is_ignored_rather_than_fatal() {
        let dir = tempfile::tempdir().unwrap();
        let provider = DeviceCodeTokenProvider::with_cache_dir(
            Endpoints::new("http://keycloak:8080/realms/spark"),
            "spark-cli",
            "bob",
            dir.path().to_path_buf(),
        );
        std::fs::write(provider.cache_file(), "{not json").unwrap();

        assert!(provider.load_cache().is_none());
    }

    #[test]
    fn a_written_cache_file_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let provider = DeviceCodeTokenProvider::with_cache_dir(
            Endpoints::new("http://keycloak:8080/realms/spark"),
            "spark-cli",
            "alice",
            dir.path().to_path_buf(),
        );
        let tokens = Tokens {
            access_token: "a".into(),
            refresh_token: Some("r".into()),
            expires_at: now_seconds() + 300.0,
        };
        provider.save_cache(&tokens);

        let raw = std::fs::read_to_string(provider.cache_file()).unwrap();
        let json: serde_json::Value = serde_json::from_str(&raw).unwrap();
        // Field names the other two clients expect.
        assert_eq!(json["access_token"], "a");
        assert_eq!(json["refresh_token"], "r");
        assert!(json["expires_at"].as_f64().unwrap() > 0.0);
        assert!(provider.load_cache().unwrap().fresh());
    }

    #[tokio::test]
    async fn a_static_token_is_handed_back_unchanged() {
        assert_eq!(
            StaticTokenProvider::new("not-a-jwt").token().await.unwrap(),
            "not-a-jwt"
        );
    }

    #[test]
    fn ids_are_uuid4() {
        let id = crate::new_correlation_id();
        assert_eq!(uuid::Uuid::parse_str(&id).unwrap().get_version_num(), 4);
        assert_ne!(crate::new_operation_id(), crate::new_operation_id());
    }

    // ------------------------------------------------------------- client credentials --

    /// What one request to the fake token endpoint carried.
    #[derive(Debug)]
    struct Seen {
        authorization: Option<String>,
        form: HashMap<String, String>,
    }

    /// A loopback token endpoint that records each request, so the wire itself is checked. Every
    /// reply lives `expires_in` seconds; 0 makes each token stale the moment it arrives.
    async fn fake_token_endpoint(
        expires_in: u64,
    ) -> (Endpoints, std::sync::Arc<std::sync::Mutex<Vec<Seen>>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorder = seen.clone();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut raw = Vec::new();
                let mut chunk = [0u8; 4096];
                // Read the head, then exactly Content-Length bytes of body.
                let (head, body) = loop {
                    let n = socket.read(&mut chunk).await.unwrap();
                    raw.extend_from_slice(&chunk[..n]);
                    let text = String::from_utf8_lossy(&raw).to_string();
                    if let Some((head, body)) = text.split_once("\r\n\r\n") {
                        let length = head
                            .lines()
                            .find_map(|l| {
                                l.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .map(|v| v.trim().parse::<usize>().unwrap())
                            })
                            .unwrap_or(0);
                        if body.len() >= length {
                            break (head.to_string(), body.to_string());
                        }
                    }
                };
                let authorization = head.lines().find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("authorization")
                        .then(|| value.trim().to_string())
                });
                let form = form_urlencoded::parse(body.as_bytes())
                    .into_owned()
                    .collect();
                let count = {
                    let mut seen = recorder.lock().unwrap();
                    seen.push(Seen {
                        authorization,
                        form,
                    });
                    seen.len()
                };
                let reply = format!(r#"{{"access_token":"cc-{count}","expires_in":{expires_in}}}"#);
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply}",
                    reply.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        // Endpoints appends the token path, which this endpoint does not care about.
        (
            Endpoints::new(&format!("http://127.0.0.1:{port}/realms/spark")),
            seen,
        )
    }

    fn decode_basic(header: &str) -> String {
        let encoded = header.strip_prefix("Basic ").expect("a Basic credential");
        String::from_utf8(
            base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .unwrap(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn client_credentials_authenticate_with_basic_and_keep_the_secret_out_of_the_body() {
        let (endpoints, seen) = fake_token_endpoint(300).await;
        let provider =
            ClientCredentialsTokenProvider::new(endpoints, "spark-job", "spark-job-secret");

        assert_eq!(provider.token().await.unwrap(), "cc-1");

        let seen = seen.lock().unwrap();
        assert_eq!(
            seen[0].form,
            HashMap::from([("grant_type".to_string(), "client_credentials".to_string())])
        );
        assert_eq!(
            decode_basic(seen[0].authorization.as_deref().unwrap()),
            "spark-job:spark-job-secret"
        );
    }

    #[tokio::test]
    async fn client_credentials_send_a_scope_only_when_given_one() {
        let (endpoints, seen) = fake_token_endpoint(300).await;
        ClientCredentialsTokenProvider::new(endpoints, "svc", "s")
            .with_scope("api://spark/.default")
            .token()
            .await
            .unwrap();

        assert_eq!(
            seen.lock().unwrap()[0].form["scope"],
            "api://spark/.default"
        );
    }

    #[tokio::test]
    async fn client_credentials_reuse_a_fresh_token() {
        let (endpoints, seen) = fake_token_endpoint(300).await;
        let provider =
            ClientCredentialsTokenProvider::new(endpoints, "spark-job", "spark-job-secret");

        assert_eq!(provider.token().await.unwrap(), "cc-1");
        assert_eq!(provider.token().await.unwrap(), "cc-1");
        assert_eq!(seen.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn client_credentials_re_run_the_grant_for_a_stale_token() {
        let (endpoints, seen) = fake_token_endpoint(0).await;
        let provider =
            ClientCredentialsTokenProvider::new(endpoints, "spark-job", "spark-job-secret");

        assert_eq!(provider.token().await.unwrap(), "cc-1");
        assert_eq!(provider.token().await.unwrap(), "cc-2");
        let grants: Vec<_> = seen
            .lock()
            .unwrap()
            .iter()
            .map(|s| s.form["grant_type"].clone())
            .collect();
        assert_eq!(grants, ["client_credentials", "client_credentials"]);
    }

    #[test]
    fn client_secret_basic_encodes_each_half_before_base64() {
        assert_eq!(
            decode_basic(&client_secret_basic("spark-job", "spark-job-secret")),
            "spark-job:spark-job-secret"
        );
        // RFC 6749 §2.3.1: form-urlencode each half first, so the server can split and decode.
        assert_eq!(
            decode_basic(&client_secret_basic("svc:a", "p%ss w+rd")),
            "svc%3Aa:p%25ss+w%2Brd"
        );
    }
}
