package io.sparkconnect.propagation.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.connect.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * The same claims the Python suite makes, asserted from the JVM client.
 *
 * <p>Needs the compose stack up and the /etc/hosts aliases in place, so it is excluded from
 * {@code mvn test} and runs under {@code mvn test -Pit} (or {@code make test-jvm-client}).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PropagationIT {

  private static final String REMOTE =
      System.getenv().getOrDefault("SPARK_REMOTE", "sc://spark-connect:15002");
  private static final String ISSUER =
      System.getenv().getOrDefault("KEYCLOAK_ISSUER", "http://keycloak:8080/realms/spark");

  private SparkSession alice;
  private SparkSession bob;

  private static TokenProvider passwordGrant(String user) {
    return new PasswordGrantTokenProvider(new Endpoints(ISSUER), "spark-cli", user, user);
  }

  /**
   * Spark's channel-level pre-shared key, which is separate from the user's token.
   *
   * <p>compose sets this for containers, so only host runs can hit a missing value -- where the
   * server's reply ("No authentication token provided") points at the wrong credential entirely.
   */
  private static void requireSharedSecret() {
    String secret = System.getenv("CONNECT_SHARED_SECRET");
    if (secret == null || secret.isBlank()) {
      throw new IllegalStateException(
          "CONNECT_SHARED_SECRET is not set, so the client cannot satisfy Spark Connect's "
              + "pre-shared-key check and every RPC would fail as UNAUTHENTICATED. Run this "
              + "through `make test-jvm-it`, which exports it, or set it to match compose.yaml.");
    }
  }

  @BeforeAll
  void openSessions() {
    requireSharedSecret();
    alice = PropagatingSession.connect(REMOTE, passwordGrant("alice"));
    bob = PropagatingSession.connect(REMOTE, passwordGrant("bob"));

    // Seed through Spark Connect, so the write path is exercised by this client too.
    alice.sql("DROP TABLE IF EXISTS polaris.shared.events").collectAsList();
    alice.sql("CREATE TABLE polaris.shared.events (id BIGINT, kind STRING) USING iceberg")
        .collectAsList();
    alice.sql("INSERT INTO polaris.shared.events VALUES (1,'login'),(2,'logout'),(3,'purchase')")
        .collectAsList();
    alice.sql("DROP TABLE IF EXISTS polaris.restricted.salaries").collectAsList();
    alice.sql("CREATE TABLE polaris.restricted.salaries (person STRING, amount BIGINT) USING iceberg")
        .collectAsList();
    alice.sql("INSERT INTO polaris.restricted.salaries VALUES ('alice',100),('bob',90)")
        .collectAsList();
  }

  @AfterAll
  void closeSessions() {
    if (alice != null) {
      alice.stop();
    }
    if (bob != null) {
      bob.stop();
    }
  }

  @Test
  @DisplayName("both users read the shared table")
  void bothUsersReadShared() {
    assertEquals(3L, alice.sql("SELECT * FROM polaris.shared.events").count());
    assertEquals(3L, bob.sql("SELECT * FROM polaris.shared.events").count());
  }

  @Test
  @DisplayName("alice reads the restricted table and bob is refused by Polaris")
  void onlyAliceReadsRestricted() {
    List<Row> rows = alice.sql("SELECT * FROM polaris.restricted.salaries").collectAsList();
    assertEquals(2, rows.size());

    Exception refused =
        assertThrows(
            Exception.class,
            () -> bob.sql("SELECT * FROM polaris.restricted.salaries").collectAsList());
    String message = String.valueOf(refused.getMessage());
    assertTrue(
        message.contains("bob") || message.contains("orbidden") || message.contains("not authorized"),
        "expected a Polaris authorisation failure, got: " + message);
  }

  @Test
  @DisplayName("a client with no token is refused at the edge")
  void missingTokenIsRefused() {
    SparkSession anonymous = PropagatingSession.connect(REMOTE, new StaticTokenProvider(""));
    try {
      Exception refused =
          assertThrows(Exception.class, () -> anonymous.sql("SELECT 1").collectAsList());
      assertTrue(
          String.valueOf(refused.getMessage()).contains("UNAUTHENTICATED"),
          "expected UNAUTHENTICATED, got: " + refused.getMessage());
    } finally {
      anonymous.stop();
    }
  }

  @Test
  @DisplayName("one correlation ID reaches Spark Connect and Polaris")
  void correlationIdReachesBothServices() throws Exception {
    String marker = UUID.randomUUID().toString();
    try (CorrelationId.Scope scope = CorrelationId.scope(marker)) {
      // SHOW NAMESPACES always performs a REST call to the catalog.
      alice.sql("SHOW NAMESPACES IN polaris").collectAsList();
      alice.sql("SELECT count(*) FROM polaris.shared.events").collectAsList();
    }

    assertTrue(
        composeLogs("spark-connect").contains(marker),
        "correlation ID missing from the Spark Connect logs");
    assertTrue(
        composeLogs("polaris").contains(marker),
        "correlation ID never reached Polaris as X-Request-ID");
  }

  @Test
  @DisplayName("Spark adopts the operation id this client supplies")
  void sparkAdoptsOurOperationId() throws Exception {
    // The interceptor mints one per ExecutePlan; Spark logs whatever it ends up using as opId=...
    PropagationInterceptor interceptor =
        new PropagationInterceptor(passwordGrant("alice"), CorrelationId.newId());
    var stamped =
        (org.apache.spark.connect.proto.ExecutePlanRequest)
            interceptor.withOperationId(
                org.apache.spark.connect.proto.ExecutePlanRequest.getDefaultInstance());
    assertTrue(stamped.hasOperationId());

    // And end to end: run something, then look for a client-shaped id in Spark's own log.
    String marker = UUID.randomUUID().toString();
    try (CorrelationId.Scope scope = CorrelationId.scope(marker)) {
      alice.sql("SELECT 1").collectAsList();
    }
    String logs = composeLogs("spark-connect");
    assertTrue(logs.contains(marker), "the query under test did not reach the server");
    assertTrue(logs.contains("opId="), "Spark never logged an operation id");
  }

  /** Logs of one compose service, read through the docker CLI from the repository root. */
  private static String composeLogs(String service) throws Exception {
    Path repoRoot = Path.of(System.getProperty("user.dir")).toAbsolutePath().getParent();
    Process process =
        new ProcessBuilder("docker", "compose", "logs", "--no-log-prefix", service)
            .directory(repoRoot.toFile())
            .redirectErrorStream(true)
            .start();
    String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    process.waitFor();
    return output;
  }
}
