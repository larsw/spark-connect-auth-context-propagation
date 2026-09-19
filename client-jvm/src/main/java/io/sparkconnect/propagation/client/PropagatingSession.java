package io.sparkconnect.propagation.client;

import com.nimbusds.jwt.JWTParser;
import java.util.Objects;
import java.util.Optional;
import org.apache.spark.sql.connect.SparkSession;
import org.apache.spark.sql.connect.client.SparkConnectClient;

/**
 * Convenience wiring from a token provider to a Spark Connect {@link SparkSession}.
 *
 * <pre>{@code
 * SparkSession spark = PropagatingSession.connect(
 *     "sc://spark-connect:15002",
 *     new DeviceCodeTokenProvider(new Endpoints(issuer), "spark-cli"));
 *
 * try (CorrelationId.Scope scope = CorrelationId.scope()) {
 *     spark.sql("SELECT * FROM polaris.shared.events").show();
 *     System.out.println("trace it: " + scope.id());
 * }
 * }</pre>
 *
 * <p>Every seam used here is public API: {@code SparkConnectClient.builder().interceptor(...)} and
 * {@code SparkSession.builder().client(...)}. Nothing is forked or patched, which is more than the
 * Python twin can say.
 *
 * <p>Each call opens a <em>new</em> session rather than reusing a cached one. Two users in one
 * process must not share a Connect session -- the server binds a session to the first subject that
 * uses it and refuses the second.
 */
public final class PropagatingSession {

  private PropagatingSession() {}

  /** Opens a session that propagates identity and correlation on every RPC. */
  public static SparkSession connect(String remote, TokenProvider tokenProvider) {
    return builder(remote, tokenProvider).open();
  }

  public static Builder builder(String remote, TokenProvider tokenProvider) {
    return new Builder(remote, tokenProvider);
  }

  /**
   * The {@code sub} claim of a JWT, without verifying it.
   *
   * <p>The client has no business validating its own token -- the server does that. This only tells
   * Spark Connect which {@code user_id} the session belongs to, which the server then checks
   * against the authenticated subject. Getting it wrong, or leaving it at the JVM's OS user name,
   * is refused with PERMISSION_DENIED.
   */
  public static Optional<String> subjectOf(String jwt) {
    try {
      return Optional.ofNullable(JWTParser.parse(jwt).getJWTClaimsSet().getSubject());
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  /** Options for opening a session. */
  public static final class Builder {

    private final String remote;
    private final TokenProvider tokenProvider;
    private String sharedSecret = System.getenv("CONNECT_SHARED_SECRET");
    private String sessionCorrelationId;
    private boolean perOperationIds = true;
    private String tokenHeader = PropagationInterceptor.DEFAULT_TOKEN_HEADER;
    private String correlationHeader = PropagationInterceptor.DEFAULT_CORRELATION_HEADER;

    private Builder(String remote, TokenProvider tokenProvider) {
      this.remote = Objects.requireNonNull(remote, "remote");
      this.tokenProvider = Objects.requireNonNull(tokenProvider, "tokenProvider");
    }

    /**
     * Spark's own channel-level pre-shared key, unrelated to the user's token. Defaults to the
     * {@code CONNECT_SHARED_SECRET} environment variable; without it a server that has
     * {@code SPARK_CONNECT_AUTHENTICATE_TOKEN} set answers every RPC with UNAUTHENTICATED "No
     * authentication token provided" before ever looking at the user token.
     *
     * <p>It goes on the wire as per-RPC metadata rather than through
     * {@code SparkConnectClient.Builder.token()}, so the channel stays plaintext and the
     * interceptor path is the only thing supplying credentials.
     */
    public Builder sharedSecret(String sharedSecret) {
      this.sharedSecret = sharedSecret;
      return this;
    }

    /** The correlation ID sent when no {@link CorrelationId#scope()} is open. */
    public Builder sessionCorrelationId(String sessionCorrelationId) {
      this.sessionCorrelationId = sessionCorrelationId;
      return this;
    }

    /**
     * Whether to fill in {@code ExecutePlanRequest.operation_id}, so the server can key what it
     * learns per operation instead of per session. Turn it off to put a stock Spark Connect client
     * on the wire.
     */
    public Builder perOperationIds(boolean perOperationIds) {
      this.perOperationIds = perOperationIds;
      return this;
    }

    public Builder tokenHeader(String tokenHeader) {
      this.tokenHeader = Objects.requireNonNull(tokenHeader, "tokenHeader");
      return this;
    }

    public Builder correlationHeader(String correlationHeader) {
      this.correlationHeader = Objects.requireNonNull(correlationHeader, "correlationHeader");
      return this;
    }

    /** The correlation ID this session will send outside any scope. */
    public String correlationId() {
      if (sessionCorrelationId == null) {
        sessionCorrelationId = CorrelationId.newId();
      }
      return sessionCorrelationId;
    }

    public PropagationInterceptor interceptor() {
      return new PropagationInterceptor(
          tokenProvider, correlationId(), sharedSecret, perOperationIds,
          tokenHeader, correlationHeader);
    }

    public SparkSession open() {
      SparkConnectClient.Builder clientBuilder =
          SparkConnectClient.builder().connectionString(remote).interceptor(interceptor());

      // Tell Spark Connect which user this session belongs to. The server rejects the request if
      // this does not match the authenticated subject, which is what stops one client attaching to
      // another user's SessionHolder -- and the default here would be the JVM's OS user name.
      String subject = subjectOf(tokenProvider.token()).orElse(null);
      if (subject != null) {
        clientBuilder = clientBuilder.userId(subject);
      }

      return SparkSession.builder().client(clientBuilder.build()).create();
    }
  }
}
