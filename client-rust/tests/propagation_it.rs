//! The same claims the Python and JVM suites make, asserted from the Rust client.
//!
//! Needs the compose stack up and the /etc/hosts aliases in place, so every test is `#[ignore]`d
//! and runs under `cargo test -- --ignored` (or `make test-rust-it`).

use std::process::Command;

use spark_connect_propagation_client::{
    connect, connect_with, new_correlation_id, ConnectOptions, Endpoints,
    PasswordGrantTokenProvider, StaticTokenProvider,
};
use spark_connect_rs::SparkSession;
use tokio::sync::OnceCell;

fn remote() -> String {
    std::env::var("SPARK_REMOTE").unwrap_or_else(|_| "sc://spark-connect:15002".to_string())
}

fn issuer() -> String {
    std::env::var("KEYCLOAK_ISSUER")
        .unwrap_or_else(|_| "http://keycloak:8080/realms/spark".to_string())
}

fn provider(user: &str) -> PasswordGrantTokenProvider {
    PasswordGrantTokenProvider::new(Endpoints::new(&issuer()), "spark-cli", user, user)
}

/// Spark's channel-level pre-shared key, which is separate from the user's token.
///
/// compose sets this for containers, so only host runs can hit a missing value -- where the
/// server's reply ("No authentication token provided") points at the wrong credential entirely.
fn require_shared_secret() {
    if std::env::var("CONNECT_SHARED_SECRET").map(|v| v.is_empty()).unwrap_or(true) {
        panic!(
            "CONNECT_SHARED_SECRET is not set, so the client cannot satisfy Spark Connect's \
             pre-shared-key check and every RPC would fail as UNAUTHENTICATED. Run this through \
             `make test-rust-it`, which exports it, or set it to match compose.yaml."
        );
    }
}

static SEEDED: OnceCell<()> = OnceCell::const_new();

/// alice seeds the tables through Spark Connect, exercising the write path from Rust too.
async fn seed() {
    SEEDED
        .get_or_init(|| async {
            require_shared_secret();
            let spark = connect(&remote(), &provider("alice")).await.expect("alice connects");
            for statement in [
                "DROP TABLE IF EXISTS polaris.shared.events",
                "CREATE TABLE polaris.shared.events (id BIGINT, kind STRING) USING iceberg",
                "INSERT INTO polaris.shared.events VALUES (1,'login'),(2,'logout'),(3,'purchase')",
                "DROP TABLE IF EXISTS polaris.restricted.salaries",
                "CREATE TABLE polaris.restricted.salaries (person STRING, amount BIGINT) USING iceberg",
                "INSERT INTO polaris.restricted.salaries VALUES ('alice',100),('bob',90)",
            ] {
                spark
                    .sql(statement)
                    .await
                    .unwrap_or_else(|e| panic!("seeding failed on {statement:?}: {e}"))
                    .collect()
                    .await
                    .unwrap_or_else(|e| panic!("seeding failed on {statement:?}: {e}"));
            }
        })
        .await;
}

async fn rows(spark: &SparkSession, sql: &str) -> Result<usize, String> {
    let df = spark.sql(sql).await.map_err(|e| e.to_string())?;
    let batch = df.collect().await.map_err(|e| e.to_string())?;
    Ok(batch.num_rows())
}

/// Logs of one compose service, read through the docker CLI from the repository root.
fn compose_logs(service: &str) -> String {
    let repo_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("client-rust has a parent")
        .to_path_buf();
    let output = Command::new("docker")
        .args(["compose", "logs", "--no-log-prefix", service])
        .current_dir(repo_root)
        .output()
        .expect("docker compose logs");
    String::from_utf8_lossy(&output.stdout).into_owned()
}

#[tokio::test]
#[ignore]
async fn both_users_read_the_shared_table() {
    seed().await;
    let alice = connect(&remote(), &provider("alice")).await.unwrap();
    let bob = connect(&remote(), &provider("bob")).await.unwrap();

    assert_eq!(rows(&alice, "SELECT * FROM polaris.shared.events").await.unwrap(), 3);
    assert_eq!(rows(&bob, "SELECT * FROM polaris.shared.events").await.unwrap(), 3);
}

#[tokio::test]
#[ignore]
async fn only_alice_reads_the_restricted_table() {
    seed().await;
    let alice = connect(&remote(), &provider("alice")).await.unwrap();
    let bob = connect(&remote(), &provider("bob")).await.unwrap();

    assert_eq!(
        rows(&alice, "SELECT * FROM polaris.restricted.salaries").await.unwrap(),
        2
    );

    let refused = rows(&bob, "SELECT * FROM polaris.restricted.salaries")
        .await
        .expect_err("bob must be refused by Polaris");
    assert!(
        refused.contains("bob") || refused.contains("orbidden") || refused.contains("not authorized"),
        "expected a Polaris authorisation failure, got: {refused}"
    );
}

#[tokio::test]
#[ignore]
async fn a_client_with_a_bad_token_is_refused_at_the_edge() {
    seed().await;
    // user_id is supplied explicitly because the token carries no `sub` to read one from; the
    // point here is the server's verdict on the token, not our own preflight.
    let options = ConnectOptions {
        user_id: Some("does-not-matter".to_string()),
        ..Default::default()
    };
    let session = connect_with(&remote(), &StaticTokenProvider::new("not-a-jwt"), options).await;

    let error = match session {
        Ok(spark) => rows(&spark, "SELECT 1").await.expect_err("expected a refusal"),
        Err(e) => e.to_string(),
    };

    // Asserted on our server's own words rather than the crate's rendering of the status: 0.0.2
    // prints the gRPC code as "Unauthenicated" (sic, fixed upstream but not released), so matching
    // the spelling would be matching someone else's typo.
    assert!(
        error.to_lowercase().contains("unauthen"),
        "expected an unauthenticated status, got: {error}"
    );
    assert!(
        error.contains("Rejected user token"),
        "expected the server's own explanation, got: {error}"
    );
}

#[tokio::test]
#[ignore]
async fn one_correlation_id_reaches_spark_connect_and_polaris() {
    seed().await;
    let marker = new_correlation_id();
    let options = ConnectOptions {
        correlation_id: Some(marker.clone()),
        ..Default::default()
    };
    let spark = connect_with(&remote(), &provider("alice"), options).await.unwrap();

    // SHOW NAMESPACES always performs a REST call to the catalog.
    rows(&spark, "SHOW NAMESPACES IN polaris").await.unwrap();
    rows(&spark, "SELECT count(*) FROM polaris.shared.events").await.unwrap();

    assert!(
        compose_logs("spark-connect").contains(&marker),
        "correlation ID missing from the Spark Connect logs"
    );
    assert!(
        compose_logs("polaris").contains(&marker),
        "correlation ID never reached Polaris as X-Request-ID"
    );
}

#[tokio::test]
#[ignore]
async fn the_crate_supplies_spark_its_operation_id() {
    // spark-connect-rs fills in ExecutePlanRequest.operation_id itself, which PySpark leaves to
    // the server. The server logs "operation <uuid>" when the client supplied one and
    // "operation <server-generated>" when it did not, so the distinction is visible from here.
    seed().await;
    let marker = new_correlation_id();
    let options = ConnectOptions {
        correlation_id: Some(marker.clone()),
        ..Default::default()
    };
    let spark = connect_with(&remote(), &provider("alice"), options).await.unwrap();
    rows(&spark, "SELECT 1").await.unwrap();

    let logs = compose_logs("spark-connect");
    let ours: Vec<&str> = logs
        .lines()
        .filter(|line| line.contains(&marker) && line.contains("/ExecutePlan"))
        .collect();

    assert!(!ours.is_empty(), "no ExecutePlan reached the server under {marker}");
    for line in &ours {
        assert!(
            !line.contains("operation <server-generated>"),
            "the crate should have supplied an operation id, but the server generated one:\n{line}"
        );
    }
}
