//! Correlation and operation identifiers.
//!
//! There is deliberately no scope type here, unlike the Python client's `correlation_id()` context
//! manager and the JVM client's `CorrelationId.Scope`. A scope would be a lie: the crate fixes a
//! session's headers when the session is built (see the crate docs), so nothing set afterwards can
//! reach the wire. The unit of scoping in this client is a session, opened with
//! [`connect_with`](crate::connect_with).

use uuid::Uuid;

/// A fresh correlation ID.
///
/// Deliberately a UUID4: Spark validates `operation_id` as a UUID4, so a correlation ID stays
/// usable anywhere an operation id is.
pub fn new_correlation_id() -> String {
    Uuid::new_v4().to_string()
}

/// A fresh operation id.
///
/// Exposed for symmetry with the other two clients, but nothing here needs to call it:
/// `spark-connect-rs` already mints one per ExecutePlan in
/// `execute_plan_request_with_metadata`, which is the behaviour PySpark leaves to the server.
pub fn new_operation_id() -> String {
    Uuid::new_v4().to_string()
}
