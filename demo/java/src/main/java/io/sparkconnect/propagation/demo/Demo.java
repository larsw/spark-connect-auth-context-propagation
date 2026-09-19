package io.sparkconnect.propagation.demo;

import io.sparkconnect.propagation.client.CorrelationId;
import io.sparkconnect.propagation.client.DeviceCodeTokenProvider;
import io.sparkconnect.propagation.client.Endpoints;
import io.sparkconnect.propagation.client.PropagatingSession;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.connect.SparkSession;

/**
 * Interactive walkthrough: two users, one Spark Connect server, different data.
 *
 * <p>The JVM twin of {@code demo/demo.py}, printing the same thing from the same server. Run it
 * from the host after {@code ./install.sh} has added the /etc/hosts aliases:
 *
 * <pre>make demo-jvm</pre>
 *
 * <p>Each user signs in with the OAuth 2.0 device flow -- the same shape as {@code gh auth login}.
 * Tokens cache under ~/.cache/spark-connect-poc in the file the Python client uses, so whichever
 * demo you ran last, the other one starts already signed in.
 */
public final class Demo {

  private static final String REMOTE =
      System.getenv().getOrDefault("SPARK_REMOTE", "sc://spark-connect:15002");
  private static final String ISSUER =
      System.getenv().getOrDefault("KEYCLOAK_ISSUER", "http://keycloak:8080/realms/spark");

  private static final String BOLD = "\033[1m";
  private static final String DIM = "\033[2m";
  private static final String GREEN = "\033[32m";
  private static final String RED = "\033[31m";
  private static final String RESET = "\033[0m";

  private Demo() {}

  private static void heading(String text) {
    System.out.println();
    System.out.println(BOLD + text + RESET);
    System.out.println(DIM + "-".repeat(text.length()) + RESET);
  }

  /** Checked up front so a misconfigured run fails before asking anyone to log in. */
  private static void requireSharedSecret() {
    String secret = System.getenv("CONNECT_SHARED_SECRET");
    if (secret != null && !secret.isBlank()) {
      return;
    }
    System.err.println(
        RED + "CONNECT_SHARED_SECRET is not set." + RESET + "\n"
            + "  Spark Connect's pre-shared-key check will reject every RPC, and its error talks\n"
            + "  about an authentication token, which is a different credential from your login.\n"
            + "  Run this through " + BOLD + "make demo-jvm" + RESET + ", which exports it.\n");
    System.exit(2);
  }

  private static SparkSession signIn(String user) {
    heading("Sign in as " + user);
    System.out.println(DIM + "  (password is '" + user + "' in this sandbox realm)" + RESET);
    DeviceCodeTokenProvider provider =
        new DeviceCodeTokenProvider(new Endpoints(ISSUER), "spark-cli", user);
    provider.token(); // trigger the flow now, so the prompts are not interleaved later
    return PropagatingSession.connect(REMOTE, provider);
  }

  private static void attempt(SparkSession session, String user, String sql) {
    try {
      List<Row> rows = session.sql(sql).collectAsList();
      System.out.println("  " + GREEN + "OK" + RESET + "      " + user + ": " + sql);
      rows.stream().limit(4).forEach(row -> System.out.println("            " + row));
    } catch (Exception error) {
      String firstLine = String.valueOf(error.getMessage()).strip().lines().findFirst().orElse("");
      System.out.println("  " + RED + "DENIED" + RESET + "  " + user + ": " + sql);
      wrap(firstLine, 92).forEach(line -> System.out.println("            " + line));
    }
  }

  /** textwrap.fill, near enough: the denial from Polaris is one long sentence. */
  private static List<String> wrap(String text, int width) {
    List<String> lines = new java.util.ArrayList<>();
    StringBuilder line = new StringBuilder();
    for (String word : text.split("\\s+")) {
      if (line.length() > 0 && line.length() + 1 + word.length() > width) {
        lines.add(line.toString());
        line.setLength(0);
      }
      line.append(line.length() > 0 ? " " : "").append(word);
    }
    if (line.length() > 0) {
      lines.add(line.toString());
    }
    return lines;
  }

  public static void main(String[] args) {
    System.out.println();
    System.out.println(BOLD + "Spark Connect token + correlation-ID propagation" + RESET);
    System.out.println();
    System.out.println("One Spark Connect server, one Polaris catalog, two users. Nothing about the");
    System.out.println("server changes between them -- only the token each client presents.");
    System.out.println();

    requireSharedSecret();

    SparkSession alice = signIn("alice");
    SparkSession bob = signIn("bob");

    String cid;
    try (CorrelationId.Scope scope = CorrelationId.scope()) {
      cid = scope.id();

      heading("alice seeds the tables (she has CATALOG_MANAGE_CONTENT)");
      alice.sql("DROP TABLE IF EXISTS polaris.shared.events").collectAsList();
      alice.sql("CREATE TABLE polaris.shared.events (id BIGINT, kind STRING) USING iceberg")
          .collectAsList();
      alice.sql("INSERT INTO polaris.shared.events VALUES (1,'login'),(2,'logout')").collectAsList();
      alice.sql("DROP TABLE IF EXISTS polaris.restricted.salaries").collectAsList();
      alice.sql("CREATE TABLE polaris.restricted.salaries (person STRING, amount BIGINT) USING iceberg")
          .collectAsList();
      alice.sql("INSERT INTO polaris.restricted.salaries VALUES ('alice',100),('bob',90)")
          .collectAsList();
      System.out.println("  done");

      heading("Both users read the shared namespace");
      attempt(alice, "alice", "SELECT * FROM polaris.shared.events ORDER BY id");
      attempt(bob, "bob  ", "SELECT * FROM polaris.shared.events ORDER BY id");

      heading("Only alice may read the restricted namespace");
      attempt(alice, "alice", "SELECT * FROM polaris.restricted.salaries");
      attempt(bob, "bob  ", "SELECT * FROM polaris.restricted.salaries");
    }

    heading("Follow the whole thing through the logs");
    System.out.println("  Every RPC above carried correlation id " + BOLD + cid + RESET);
    System.out.println("  " + DIM + "make cid CID=" + cid + RESET);
    System.out.println();
    System.out.println("  " + DIM + "Spark holds no S3 credentials at all; the only ones on the data path" + RESET);
    System.out.println("  " + DIM + "were vended by Polaris for whichever user made the request." + RESET);
    System.out.println();

    alice.stop();
    bob.stop();
    // Spark Connect leaves non-daemon gRPC threads behind, so say when we are done rather than
    // letting the JVM look like it hung after the last line of output.
    System.exit(0);
  }
}
