//! Convenience wiring from a token provider to a `SparkSession`.

use base64::Engine;
use spark_connect_rs::client::Config;
use spark_connect_rs::{SparkSession, SparkSessionBuilder};

use crate::auth::{propagation_headers, PropagationError, TokenProvider};
use crate::correlation::new_correlation_id;

/// Options for opening a session.
pub struct ConnectOptions {
    /// Spark's own channel-level pre-shared key, unrelated to the user's token. Defaults to the
    /// `CONNECT_SHARED_SECRET` environment variable. Without it a server that has
    /// `SPARK_CONNECT_AUTHENTICATE_TOKEN` set answers every RPC with UNAUTHENTICATED
    /// "No authentication token provided", before ever looking at the user token.
    pub shared_secret: Option<String>,

    /// The correlation ID every RPC of this session will carry. A fresh UUID4 by default.
    ///
    /// This is the scoping knob: because the crate fixes headers per session, grouping work under
    /// its own ID means opening a session for it rather than opening a block.
    pub correlation_id: Option<String>,

    /// Overrides the `user_id` sent to Spark Connect. Leave unset -- the default is the `sub`
    /// claim of the token, which is what the server's subject-to-session binding requires. The
    /// crate would otherwise default it to `$USER`, which the server refuses.
    pub user_id: Option<String>,

    /// The Spark Connect session id, if you need to pin it.
    pub session_id: Option<uuid::Uuid>,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            shared_secret: std::env::var("CONNECT_SHARED_SECRET").ok().filter(|s| !s.is_empty()),
            correlation_id: None,
            user_id: None,
            session_id: None,
        }
    }
}

/// The `sub` claim of a JWT, without verifying it.
///
/// The client has no business validating its own token -- the server does that. This only tells
/// Spark Connect which `user_id` the session belongs to, which the server then checks against the
/// authenticated subject.
pub fn subject_of(jwt: &str) -> Option<String> {
    let payload = jwt.split('.').nth(1)?;
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .ok()?;
    let json: serde_json::Value = serde_json::from_slice(&decoded).ok()?;
    json.get("sub")?.as_str().map(|s| s.to_string())
}

/// Opens a Spark Connect session that carries the user's token and a correlation ID on every RPC.
pub async fn connect(
    remote: &str,
    token_provider: &impl TokenProvider,
) -> Result<SparkSession, PropagationError> {
    connect_with(remote, token_provider, ConnectOptions::default()).await
}

/// [`connect`], with the knobs.
///
/// Each call opens a *new* session rather than reusing a cached one: two users in one process must
/// not share a Connect session, because the server binds a session to the first subject that uses
/// it and refuses the second.
pub async fn connect_with(
    remote: &str,
    token_provider: &impl TokenProvider,
    options: ConnectOptions,
) -> Result<SparkSession, PropagationError> {
    let token = token_provider.token().await?;
    let correlation_id = options.correlation_id.unwrap_or_else(new_correlation_id);
    let (host, port) = split_remote(remote)?;

    let user_id = options
        .user_id
        .or_else(|| subject_of(&token))
        .ok_or_else(|| {
            PropagationError::Spark(
                "could not read a `sub` claim from the token, and no user_id was supplied; \
                 the server refuses a user_id that is not the authenticated subject"
                    .into(),
            )
        })?;

    let mut config = Config::new()
        .host(&host)
        .port(port)
        .user_id(&user_id)
        .headers(propagation_headers(
            &token,
            &correlation_id,
            options.shared_secret.as_deref(),
        ));
    if let Some(session_id) = options.session_id {
        config = config.session_id(session_id);
    }

    SparkSessionBuilder::from_config(config)
        .build()
        .await
        .map_err(|e| PropagationError::Spark(e.to_string()))
}

/// Splits `sc://host:port` into its parts.
///
/// The crate parses a connection string itself, but only through `ChannelBuilder`, and taking the
/// `Config` route instead is what lets us set headers and `user_id` without stringly-typed
/// options. So the little bit of parsing lands here.
pub(crate) fn split_remote(remote: &str) -> Result<(String, u16), PropagationError> {
    let rest = remote.strip_prefix("sc://").ok_or_else(|| {
        PropagationError::Spark(format!("remote must start with sc://, got {remote:?}"))
    })?;
    let rest = rest.split(['/', ';']).next().unwrap_or(rest);
    let (host, port) = rest.rsplit_once(':').ok_or_else(|| {
        PropagationError::Spark(format!("remote must include a port, got {remote:?}"))
    })?;
    if host.is_empty() {
        return Err(PropagationError::Spark(format!(
            "remote must include a host, got {remote:?}"
        )));
    }
    let port: u16 = port
        .parse()
        .map_err(|_| PropagationError::Spark(format!("remote port is not a number: {remote:?}")))?;
    Ok((host.to_string(), port))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn jwt_with(claims: &str) -> String {
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(claims);
        format!("header.{payload}.signature")
    }

    #[test]
    fn subject_is_read_from_the_token() {
        let jwt = jwt_with(r#"{"sub":"abc-123","preferred_username":"alice"}"#);
        assert_eq!(subject_of(&jwt).as_deref(), Some("abc-123"));
    }

    #[test]
    fn subject_extraction_never_panics() {
        for value in ["", "garbage", "a.b", "a.!!!.c", "a..c"] {
            assert!(subject_of(value).is_none(), "{value:?} should yield no subject");
        }
    }

    #[test]
    fn remote_is_split_into_host_and_port() {
        assert_eq!(
            split_remote("sc://spark-connect:15002").unwrap(),
            ("spark-connect".to_string(), 15002)
        );
        // Connection strings may carry options; host and port stop at the first / or ;.
        assert_eq!(
            split_remote("sc://spark-connect:15002/;user_id=alice").unwrap(),
            ("spark-connect".to_string(), 15002)
        );
    }

    #[test]
    fn a_bad_remote_is_refused_with_a_useful_message() {
        for bad in ["spark-connect:15002", "sc://spark-connect", "sc://:15002", "sc://host:nope"] {
            let error = split_remote(bad).unwrap_err().to_string();
            assert!(error.contains(bad), "{bad:?} -> {error}");
        }
    }

    #[test]
    fn the_shared_secret_defaults_to_the_environment() {
        // Asserts the wiring, not a value: the test process may or may not have it set.
        let options = ConnectOptions::default();
        assert_eq!(
            options.shared_secret,
            std::env::var("CONNECT_SHARED_SECRET").ok().filter(|s| !s.is_empty())
        );
        assert!(options.correlation_id.is_none());
        assert!(options.user_id.is_none());
    }
}
