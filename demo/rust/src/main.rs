//! Interactive walkthrough: two users, one Spark Connect server, different data.
//!
//! The Rust twin of `demo/demo.py`, printing the same thing from the same server. Run it from the
//! host after `./install.sh` has added the /etc/hosts aliases:
//!
//! ```text
//! make demo-rust
//! ```
//!
//! Each user signs in with the OAuth 2.0 device flow -- the same shape as `gh auth login`. Tokens
//! cache under ~/.cache/spark-connect-poc in the file the Python and JVM clients use, so whichever
//! demo you ran last, the other two start already signed in.

use std::process::ExitCode;

use arrow::util::pretty::pretty_format_batches;
use spark_connect_propagation_client::{
    connect_with, new_correlation_id, ConnectOptions, DeviceCodeTokenProvider, Endpoints,
};
use spark_connect_rs::SparkSession;

const BOLD: &str = "\x1b[1m";
const DIM: &str = "\x1b[2m";
const GREEN: &str = "\x1b[32m";
const RED: &str = "\x1b[31m";
const RESET: &str = "\x1b[0m";

fn remote() -> String {
    std::env::var("SPARK_REMOTE").unwrap_or_else(|_| "sc://spark-connect:15002".to_string())
}

fn issuer() -> String {
    std::env::var("KEYCLOAK_ISSUER")
        .unwrap_or_else(|_| "http://keycloak:8080/realms/spark".to_string())
}

fn heading(text: &str) {
    println!();
    println!("{BOLD}{text}{RESET}");
    println!("{DIM}{}{RESET}", "-".repeat(text.chars().count()));
}

/// Checked up front so a misconfigured run fails before asking anyone to log in.
fn require_shared_secret() -> bool {
    if std::env::var("CONNECT_SHARED_SECRET").map(|v| !v.is_empty()).unwrap_or(false) {
        return true;
    }
    eprintln!(
        "{RED}CONNECT_SHARED_SECRET is not set.{RESET}\n\
         \x20 Spark Connect's pre-shared-key check will reject every RPC, and its error talks\n\
         \x20 about an authentication token, which is a different credential from your login.\n\
         \x20 Run this through {BOLD}make demo-rust{RESET}, which exports it.\n"
    );
    false
}

/// Signs a user in and opens their session.
///
/// Unlike the other two demos, the correlation ID is handed over here rather than opened as a
/// block around the queries: this client fixes a session's headers when the session is built, so
/// the session *is* the scope. See FINDINGS #13.
async fn sign_in(user: &str, correlation_id: &str) -> Result<SparkSession, Box<dyn std::error::Error>> {
    heading(&format!("Sign in as {user}"));
    println!("{DIM}  (password is '{user}' in this sandbox realm){RESET}");

    let provider = DeviceCodeTokenProvider::new(Endpoints::new(&issuer()), "spark-cli", user);
    let options = ConnectOptions {
        correlation_id: Some(correlation_id.to_string()),
        ..Default::default()
    };
    Ok(connect_with(&remote(), &provider, options).await?)
}

async fn run(spark: &SparkSession, sql: &str) -> Result<arrow::record_batch::RecordBatch, String> {
    let df = spark.sql(sql).await.map_err(|e| e.to_string())?;
    df.collect().await.map_err(|e| e.to_string())
}

async fn attempt(spark: &SparkSession, user: &str, sql: &str) {
    match run(spark, sql).await {
        Ok(batch) => {
            println!("  {GREEN}OK{RESET}      {user}: {sql}");
            let rendered = pretty_format_batches(std::slice::from_ref(&batch))
                .map(|t| t.to_string())
                .unwrap_or_else(|e| e.to_string());
            for line in rendered.lines().take(8) {
                println!("            {line}");
            }
        }
        Err(error) => {
            let first_line = error.lines().next().unwrap_or("").trim().to_string();
            println!("  {RED}DENIED{RESET}  {user}: {sql}");
            for line in wrap(&first_line, 92) {
                println!("            {line}");
            }
        }
    }
}

/// textwrap.fill, near enough: the denial from Polaris is one long sentence.
fn wrap(text: &str, width: usize) -> Vec<String> {
    let mut lines = Vec::new();
    let mut line = String::new();
    for word in text.split_whitespace() {
        if !line.is_empty() && line.chars().count() + 1 + word.chars().count() > width {
            lines.push(std::mem::take(&mut line));
        }
        if !line.is_empty() {
            line.push(' ');
        }
        line.push_str(word);
    }
    if !line.is_empty() {
        lines.push(line);
    }
    lines
}

async fn walkthrough() -> Result<String, Box<dyn std::error::Error>> {
    // Minted before the sessions, because this client takes the correlation ID at connect time.
    let cid = new_correlation_id();

    let alice = sign_in("alice", &cid).await?;
    let bob = sign_in("bob", &cid).await?;

    heading("alice seeds the tables (she has CATALOG_MANAGE_CONTENT)");
    for statement in [
        "DROP TABLE IF EXISTS polaris.shared.events",
        "CREATE TABLE polaris.shared.events (id BIGINT, kind STRING) USING iceberg",
        "INSERT INTO polaris.shared.events VALUES (1,'login'),(2,'logout')",
        "DROP TABLE IF EXISTS polaris.restricted.salaries",
        "CREATE TABLE polaris.restricted.salaries (person STRING, amount BIGINT) USING iceberg",
        "INSERT INTO polaris.restricted.salaries VALUES ('alice',100),('bob',90)",
    ] {
        run(&alice, statement)
            .await
            .map_err(|e| format!("seeding failed on {statement:?}: {e}"))?;
    }
    println!("  done");

    heading("Both users read the shared namespace");
    attempt(&alice, "alice", "SELECT * FROM polaris.shared.events ORDER BY id").await;
    attempt(&bob, "bob  ", "SELECT * FROM polaris.shared.events ORDER BY id").await;

    heading("Only alice may read the restricted namespace");
    attempt(&alice, "alice", "SELECT * FROM polaris.restricted.salaries").await;
    attempt(&bob, "bob  ", "SELECT * FROM polaris.restricted.salaries").await;

    Ok(cid)
}

#[tokio::main]
async fn main() -> ExitCode {
    println!();
    println!("{BOLD}Spark Connect token + correlation-ID propagation{RESET}");
    println!();
    println!("One Spark Connect server, one Polaris catalog, two users. Nothing about the");
    println!("server changes between them -- only the token each client presents.");
    println!();

    if !require_shared_secret() {
        return ExitCode::from(2);
    }

    let cid = match walkthrough().await {
        Ok(cid) => cid,
        Err(error) => {
            eprintln!("{RED}{error}{RESET}");
            return ExitCode::FAILURE;
        }
    };

    heading("Follow the whole thing through the logs");
    println!("  Every RPC above carried correlation id {BOLD}{cid}{RESET}");
    println!("  {DIM}make cid CID={cid}{RESET}");
    println!();
    println!("  {DIM}Spark holds no S3 credentials at all; the only ones on the data path{RESET}");
    println!("  {DIM}were vended by Polaris for whichever user made the request.{RESET}");
    println!();

    ExitCode::SUCCESS
}
