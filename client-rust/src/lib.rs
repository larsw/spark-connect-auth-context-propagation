//! Thread a user token and a correlation ID through Spark Connect, from Rust.
//!
//! The third client in this PoC, after the Python package and the JVM library. It talks to the
//! same unchanged server, presents the same headers, and pins `user_id` to the authenticated
//! subject so the server's subject-to-session binding accepts it.
//!
//! ```no_run
//! use spark_connect_propagation_client::{Endpoints, PasswordGrantTokenProvider, connect};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let provider = PasswordGrantTokenProvider::new(
//!     Endpoints::new("http://keycloak:8080/realms/spark"),
//!     "spark-cli",
//!     "alice",
//!     "alice",
//! );
//! let spark = connect("sc://spark-connect:15002", &provider).await?;
//! spark.sql("SELECT * FROM polaris.shared.events").await?.show(Some(5), None, None).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # How this one differs from the Python and JVM clients
//!
//! Two differences, both forced by [`spark-connect-rs`], and both worth knowing before you reach
//! for a correlation scope that is not there.
//!
//! **The good one: `operation_id` is already handled.** The crate mints a fresh UUID4 per
//! ExecutePlan in `execute_plan_request_with_metadata`, which is exactly the behaviour the Python
//! client needs a patched private method for and the JVM client implements with an interceptor.
//! Nothing to do here, and the server's per-operation keying works out of the box.
//!
//! **The awkward one: headers are fixed for the life of a session.** The crate's session type is
//! `SparkSession { client: SparkClient }`, and
//!
//! ```text
//! pub type SparkClient = SparkConnectClient<HeadersMiddleware<Channel>>;
//! ```
//!
//! closes the generic: the only service a [`SparkSession`] can hold is the crate's own
//! `HeadersMiddleware`, whose headers are a `HashMap<String, String>` captured when it is built.
//! A tonic interceptor or a tower layer of our own would be a different type and would not fit.
//! So, unlike the other two clients, this one cannot:
//!
//! * re-read the token per RPC, so a session lasts as long as the token it opened with, and
//! * narrow the correlation ID to a block, because there is nowhere to vary it per call.
//!
//! What it does instead is take both at [`connect`] time. To group work under its own correlation
//! ID, open a session for it with [`connect_with`] — a Connect session is the unit of scoping
//! here. See [`ConnectOptions::correlation_id`].
//!
//! [`spark-connect-rs`]: https://crates.io/crates/spark-connect-rs
//! [`SparkSession`]: spark_connect_rs::SparkSession

mod auth;
mod correlation;
mod session;

pub use auth::{
    DeviceCodeTokenProvider, Endpoints, PasswordGrantTokenProvider, PropagationError,
    StaticTokenProvider, TokenProvider,
};
pub use correlation::{new_correlation_id, new_operation_id};
pub use session::{connect, connect_with, subject_of, ConnectOptions};

/// The header the user's JWT rides on, kept off `authorization` because Spark's own
/// `PreSharedKeyAuthenticationInterceptor` owns that one.
pub const DEFAULT_TOKEN_HEADER: &str = "x-user-token";

/// The header carrying the correlation ID, which the server puts in its MDC and passes to Polaris
/// as `X-Request-ID`.
pub const DEFAULT_CORRELATION_HEADER: &str = "x-correlation-id";
