package io.sparkconnect.propagation.client;

import java.util.Objects;
import java.util.UUID;
import org.apache.spark.connect.proto.ExecutePlanRequest;
import org.sparkproject.io.grpc.CallOptions;
import org.sparkproject.io.grpc.Channel;
import org.sparkproject.io.grpc.ClientCall;
import org.sparkproject.io.grpc.ClientInterceptor;
import org.sparkproject.io.grpc.ForwardingClientCall;
import org.sparkproject.io.grpc.Metadata;
import org.sparkproject.io.grpc.MethodDescriptor;

/**
 * Stamps the auth and correlation headers onto every outgoing RPC, and fills in the operation id
 * Spark would otherwise generate for itself.
 *
 * <p>Implements {@code org.sparkproject.io.grpc.ClientInterceptor}, not {@code io.grpc}: the
 * published {@code spark-connect-client-jvm} jar is already shaded and contains zero
 * {@code io/grpc/} entries. Note the prefix is <em>not</em> the server's
 * {@code org.sparkproject.connect.grpc} -- the two Spark jars relocate the same gRPC to two
 * different packages, so server and client interceptors cannot share a superinterface.
 *
 * <p>One interceptor covers every RPC shape here. gRPC-Java dispatches all method types through
 * {@link #interceptCall}, unlike the Python client, which needs a separate implementation for
 * unary-unary and unary-stream and silently leaves half the traffic unauthenticated if you forget
 * one.
 *
 * <p>Why headers rather than the request body: Spark Connect stringifies the whole request proto
 * into the job description and callSite, so a JWT carried in {@code UserContext.extensions} would
 * surface in the Spark UI. Metadata does not.
 */
public final class PropagationInterceptor implements ClientInterceptor {

  public static final String DEFAULT_TOKEN_HEADER = "x-user-token";
  public static final String DEFAULT_CORRELATION_HEADER = "x-correlation-id";

  private final TokenProvider tokenProvider;
  private final String sessionCorrelationId;
  private final String sharedSecret;
  private final boolean perOperationIds;
  private final Metadata.Key<String> tokenKey;
  private final Metadata.Key<String> correlationKey;
  private final Metadata.Key<String> authorizationKey;

  public PropagationInterceptor(TokenProvider tokenProvider, String sessionCorrelationId) {
    this(tokenProvider, sessionCorrelationId, null, true,
        DEFAULT_TOKEN_HEADER, DEFAULT_CORRELATION_HEADER);
  }

  public PropagationInterceptor(
      TokenProvider tokenProvider,
      String sessionCorrelationId,
      String sharedSecret,
      boolean perOperationIds,
      String tokenHeader,
      String correlationHeader) {
    this.tokenProvider = Objects.requireNonNull(tokenProvider, "tokenProvider");
    this.sessionCorrelationId = Objects.requireNonNull(sessionCorrelationId, "sessionCorrelationId");
    this.sharedSecret = sharedSecret;
    this.perOperationIds = perOperationIds;
    this.tokenKey = Metadata.Key.of(tokenHeader, Metadata.ASCII_STRING_MARSHALLER);
    this.correlationKey = Metadata.Key.of(correlationHeader, Metadata.ASCII_STRING_MARSHALLER);
    this.authorizationKey = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {

    return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {

      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        // Fetched per call rather than captured once, so a provider that refreshes keeps a
        // long-lived session alive instead of failing when the original token expires.
        headers.put(tokenKey, tokenProvider.token());
        headers.put(correlationKey, CorrelationId.current(sessionCorrelationId));
        if (sharedSecret != null && !sharedSecret.isBlank()) {
          // Spark's own PreSharedKeyAuthenticationInterceptor owns this header and compares it to
          // a single shared secret. It answers "is this a trusted client"; the user token above
          // answers "which user is it".
          headers.put(authorizationKey, "Bearer " + sharedSecret);
        }
        super.start(responseListener, headers);
      }

      @Override
      @SuppressWarnings("unchecked")
      public void sendMessage(ReqT message) {
        super.sendMessage((ReqT) withOperationId(message));
      }
    };
  }

  /**
   * Gives an ExecutePlan its operation id, which neither Spark client fills in on its own.
   *
   * <p>Without it the server generates one, never tells the client, and a server-side interceptor
   * can only file what it learns under the session -- so two operations running concurrently in one
   * session under different correlation IDs overwrite each other's. Supplying it here means Spark
   * adopts our id, and it turns up in {@code ExecuteHolder}, in the Spark UI and in the operation
   * job tag that reaches the ExecutionThread.
   *
   * <p>A fresh id per request, never the correlation ID: one correlation scope deliberately spans
   * several operations, while Spark keys {@code ExecuteHolder} by
   * {@code (userId, sessionId, operationId)} and rejects a repeat with
   * {@code INVALID_HANDLE.OPERATION_ALREADY_EXISTS}.
   *
   * <p>This is the one place the JVM client has it easier than the Python one: rewriting the
   * outgoing message is ordinary gRPC interceptor work, where PySpark offers no public seam at all.
   */
  Object withOperationId(Object message) {
    if (!perOperationIds || !(message instanceof ExecutePlanRequest request)) {
      return message;
    }
    if (request.hasOperationId() && !request.getOperationId().isBlank()) {
      return message;
    }
    return request.toBuilder().setOperationId(UUID.randomUUID().toString()).build();
  }
}
