package io.sparkconnect.propagation;

import com.nimbusds.jwt.JWTClaimsSet;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.spark.connect.proto.AddArtifactsRequest;
import org.apache.spark.connect.proto.AnalyzePlanRequest;
import org.apache.spark.connect.proto.ArtifactStatusesRequest;
import org.apache.spark.connect.proto.ConfigRequest;
import org.apache.spark.connect.proto.ExecutePlanRequest;
import org.apache.spark.connect.proto.FetchErrorDetailsRequest;
import org.apache.spark.connect.proto.InterruptRequest;
import org.apache.spark.connect.proto.ReattachExecuteRequest;
import org.apache.spark.connect.proto.ReleaseExecuteRequest;
import org.apache.spark.connect.proto.ReleaseSessionRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sparkproject.connect.grpc.ForwardingServerCallListener;
import org.sparkproject.connect.grpc.Metadata;
import org.sparkproject.connect.grpc.ServerCall;
import org.sparkproject.connect.grpc.ServerCallHandler;
import org.sparkproject.connect.grpc.ServerInterceptor;
import org.sparkproject.connect.grpc.Status;

/**
 * Extracts the caller's token and correlation ID from gRPC metadata, authenticates them, exchanges
 * the token for a downstream credential, and parks the result where the Iceberg AuthManager can
 * find it from the ExecutionThread.
 *
 * <p><strong>Two things about this class are dictated by Spark, not by taste.</strong>
 *
 * <ol>
 *   <li>It implements {@code org.sparkproject.connect.grpc.ServerInterceptor}, not
 *       {@code io.grpc.ServerInterceptor}. Spark Connect relocates gRPC when it shades, and the
 *       published jar contains zero {@code io/grpc/} entries. The docstring on
 *       {@code spark.connect.grpc.interceptor.classes} still says {@code io.grpc}; it is stale.
 *   <li>It must have a zero-argument constructor, because
 *       {@code SparkConnectInterceptorRegistry.createInstance} looks for exactly that and throws
 *       {@code CONNECT.INTERCEPTOR_CTOR_MISSING} otherwise. Hence configuration is read here
 *       rather than injected.
 * </ol>
 *
 * <p>The user JWT deliberately rides a dedicated header rather than {@code Authorization}, because
 * Spark's own {@code PreSharedKeyAuthenticationInterceptor} owns that header and compares it byte
 * for byte against a single shared secret. The two mechanisms are complementary: one answers "is
 * this a trusted client", this one answers "which user is it".
 */
public final class UserTokenServerInterceptor implements ServerInterceptor {

  private static final Logger LOG = LoggerFactory.getLogger(UserTokenServerInterceptor.class);

  public static final String MDC_CORRELATION_ID = "correlationId";
  public static final String MDC_PRINCIPAL = "principal";

  /**
   * Lifecycle RPCs are exempt from strict authentication. If a token expires mid-session, the
   * client must still be able to release server-side state rather than strand it.
   */
  private static final List<String> EXEMPT_METHOD_SUFFIXES =
      List.of("/ReleaseSession", "/ReleaseExecute");

  private final PluginConfig config;
  private final TokenValidator validator;
  private final TokenExchangeService exchange;
  private final Metadata.Key<String> tokenKey;
  private final Metadata.Key<String> correlationKey;

  /** Required by Spark's interceptor registry. Do not add parameters. */
  public UserTokenServerInterceptor() {
    this.config = PluginConfig.fromSparkEnv();
    this.validator = new TokenValidator(config);
    this.exchange = new TokenExchangeService(config);
    this.tokenKey = Metadata.Key.of(config.tokenHeader, Metadata.ASCII_STRING_MARSHALLER);
    this.correlationKey =
        Metadata.Key.of(config.correlationHeader, Metadata.ASCII_STRING_MARSHALLER);
    LOG.info(
        "propagation interceptor active: token header '{}', correlation header '{}', "
            + "issuer {}, exchange audience {}",
        config.tokenHeader,
        config.correlationHeader,
        config.issuer,
        config.exchangeAudience);
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

    String method = call.getMethodDescriptor().getFullMethodName();
    String correlationId = Optional.ofNullable(headers.get(correlationKey))
        .filter(s -> !s.isBlank())
        .orElseGet(() -> "srv-" + UUID.randomUUID());

    if (isExempt(method)) {
      return next.startCall(call, headers);
    }

    String userToken = headers.get(tokenKey);
    if (userToken == null || userToken.isBlank()) {
      return reject(
          call,
          Status.UNAUTHENTICATED,
          "No user token supplied in the '" + config.tokenHeader + "' header",
          correlationId);
    }

    JWTClaimsSet claims;
    try {
      claims = validator.validate(userToken);
    } catch (TokenValidator.InvalidTokenException e) {
      return reject(call, Status.UNAUTHENTICATED, "Rejected user token: " + e.getMessage(),
          correlationId);
    }

    String subject = claims.getSubject();
    String principalName = claimAsString(claims, "principal_name");
    if (principalName == null) {
      principalName = claimAsString(claims, "preferred_username");
    }

    TokenExchangeService.CachedToken downstream;
    try {
      downstream = exchange.exchange(userToken);
    } catch (TokenExchangeService.ExchangeFailedException e) {
      return reject(call, Status.PERMISSION_DENIED,
          "Could not obtain a downstream credential for subject " + subject + ": " + e.getMessage(),
          correlationId);
    }

    PropagatedIdentity identity =
        new PropagatedIdentity(
            subject, principalName, downstream.token(), correlationId,
            downstream.expiresAtMillis());

    return new IdentityBindingListener<>(
        next.startCall(call, headers), call, identity, correlationId, method);
  }

  /**
   * Waits for the request message, because the Connect session and user ids live in the body rather
   * than in metadata, then enforces the binding between the authenticated subject and the session
   * the client claims, and finally publishes the identity for the ExecutionThread to pick up.
   */
  private final class IdentityBindingListener<ReqT>
      extends ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT> {

    private final ServerCall<ReqT, ?> call;
    private final PropagatedIdentity identity;
    private final String correlationId;
    private final String method;
    private boolean closed;

    IdentityBindingListener(
        ServerCall.Listener<ReqT> delegate,
        ServerCall<ReqT, ?> call,
        PropagatedIdentity identity,
        String correlationId,
        String method) {
      super(delegate);
      this.call = call;
      this.identity = identity;
      this.correlationId = correlationId;
      this.method = method;
    }

    @Override
    public void onMessage(ReqT message) {
      if (closed) {
        return;
      }
      ConnectCoordinates coordinates = coordinatesOf(message);
      if (coordinates == null) {
        super.onMessage(message);
        return;
      }

      // Spark Connect trusts whatever (user_id, session_id) the client puts in the request, with
      // no link to who the client actually is. Without these checks a caller could attach to
      // another user's live SessionHolder -- their temp views, cached frames and, here, their
      // credentials.
      //
      // First: the claimed user_id must be the authenticated subject. Spark keys SessionHolder on
      // (userId, sessionId), so pinning userId partitions the whole session namespace by
      // authenticated identity, and makes our binding below agree with Spark's own keying.
      String claimedUserId = coordinates.userId();
      if (claimedUserId != null
          && !claimedUserId.isBlank()
          && !claimedUserId.equals(identity.subject())) {
        closed = true;
        LOG.warn(
            "rejected {}: subject {} claimed user_id '{}' [cid={}]",
            method, identity.subject(), claimedUserId, correlationId);
        call.close(
            Status.PERMISSION_DENIED.withDescription(
                "The user_id in the request does not match the authenticated subject "
                    + "(correlation-id=" + correlationId + ")"),
            new Metadata());
        return;
      }

      // Second: a Connect session belongs to whoever first used it.
      Optional<String> owner =
          PropagatedIdentityHolder.claimSession(coordinates.sessionId(), identity.subject());
      if (owner.isPresent()) {
        closed = true;
        LOG.warn(
            "rejected {}: subject {} tried to use session {} owned by subject {} [cid={}]",
            method, identity.subject(), coordinates.sessionId(), owner.get(), correlationId);
        call.close(
            Status.PERMISSION_DENIED.withDescription(
                "Session " + coordinates.sessionId()
                    + " belongs to a different authenticated user (correlation-id="
                    + correlationId + ")"),
            new Metadata());
        return;
      }

      PropagatedIdentityHolder.put(coordinates.userId(), coordinates.sessionId(), identity);

      org.slf4j.MDC.put(MDC_CORRELATION_ID, correlationId);
      if (identity.principalName() != null) {
        org.slf4j.MDC.put(MDC_PRINCIPAL, identity.principalName());
      }
      try {
        LOG.debug(
            "{} authenticated as {} (session {})",
            method, identity.principalName(), coordinates.sessionId());
        super.onMessage(message);
      } finally {
        org.slf4j.MDC.remove(MDC_CORRELATION_ID);
        org.slf4j.MDC.remove(MDC_PRINCIPAL);
      }
    }
  }

  private <ReqT, RespT> ServerCall.Listener<ReqT> reject(
      ServerCall<ReqT, RespT> call, Status status, String message, String correlationId) {
    LOG.warn("{} [cid={}]", message, correlationId);
    call.close(
        status.withDescription(message + " (correlation-id=" + correlationId + ")"),
        new Metadata());
    return new ServerCall.Listener<ReqT>() {};
  }

  private static boolean isExempt(String fullMethodName) {
    return EXEMPT_METHOD_SUFFIXES.stream().anyMatch(fullMethodName::endsWith);
  }

  private static String claimAsString(JWTClaimsSet claims, String name) {
    Object value = claims.getClaim(name);
    return value instanceof String s && !s.isBlank() ? s : null;
  }

  /**
   * Each Connect request is a distinct generated class with no shared interface, so the session and
   * user ids have to be pulled out per message type.
   */
  private static ConnectCoordinates coordinatesOf(Object message) {
    if (message instanceof ExecutePlanRequest r) {
      return new ConnectCoordinates(r.getUserContext().getUserId(), r.getSessionId());
    } else if (message instanceof AnalyzePlanRequest r) {
      return new ConnectCoordinates(r.getUserContext().getUserId(), r.getSessionId());
    } else if (message instanceof ConfigRequest r) {
      return new ConnectCoordinates(r.getUserContext().getUserId(), r.getSessionId());
    } else if (message instanceof AddArtifactsRequest r) {
      return new ConnectCoordinates(r.getUserContext().getUserId(), r.getSessionId());
    } else if (message instanceof ArtifactStatusesRequest r) {
      return new ConnectCoordinates(r.getUserContext().getUserId(), r.getSessionId());
    } else if (message instanceof InterruptRequest r) {
      return new ConnectCoordinates(r.getUserContext().getUserId(), r.getSessionId());
    } else if (message instanceof ReattachExecuteRequest r) {
      return new ConnectCoordinates(r.getUserContext().getUserId(), r.getSessionId());
    } else if (message instanceof ReleaseExecuteRequest r) {
      return new ConnectCoordinates(r.getUserContext().getUserId(), r.getSessionId());
    } else if (message instanceof ReleaseSessionRequest r) {
      return new ConnectCoordinates(r.getUserContext().getUserId(), r.getSessionId());
    } else if (message instanceof FetchErrorDetailsRequest r) {
      return new ConnectCoordinates(r.getUserContext().getUserId(), r.getSessionId());
    }
    return null;
  }

  private record ConnectCoordinates(String userId, String sessionId) {}
}
