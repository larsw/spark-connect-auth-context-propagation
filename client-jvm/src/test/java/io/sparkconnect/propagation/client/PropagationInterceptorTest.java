package io.sparkconnect.propagation.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.spark.connect.proto.AnalyzePlanRequest;
import org.apache.spark.connect.proto.ExecutePlanRequest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.sparkproject.io.grpc.CallOptions;
import org.sparkproject.io.grpc.Channel;
import org.sparkproject.io.grpc.ClientCall;
import org.sparkproject.io.grpc.Metadata;
import org.sparkproject.io.grpc.MethodDescriptor;

/** What the interceptor actually puts on the wire, checked through a fake channel. */
class PropagationInterceptorTest {

  private static final Metadata.Key<String> TOKEN =
      Metadata.Key.of(PropagationInterceptor.DEFAULT_TOKEN_HEADER, Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> CORRELATION =
      Metadata.Key.of(
          PropagationInterceptor.DEFAULT_CORRELATION_HEADER, Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> AUTHORIZATION =
      Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

  /** Records what a call was started and fed with. */
  private static final class CapturingCall extends ClientCall<Object, Object> {
    Metadata headers;
    final List<Object> messages = new ArrayList<>();

    @Override
    public void start(Listener<Object> responseListener, Metadata headers) {
      this.headers = headers;
    }

    @Override
    public void request(int numMessages) {}

    @Override
    public void cancel(String message, Throwable cause) {}

    @Override
    public void halfClose() {}

    @Override
    public void sendMessage(Object message) {
      messages.add(message);
    }
  }

  private static final class FakeChannel extends Channel {
    final CapturingCall call = new CapturingCall();

    @Override
    @SuppressWarnings("unchecked")
    public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(
        MethodDescriptor<ReqT, RespT> method, CallOptions callOptions) {
      return (ClientCall<ReqT, RespT>) call;
    }

    @Override
    public String authority() {
      return "test";
    }
  }

  private static final MethodDescriptor.Marshaller<Object> MARSHALLER =
      new MethodDescriptor.Marshaller<>() {
        @Override
        public InputStream stream(Object value) {
          return new ByteArrayInputStream(new byte[0]);
        }

        @Override
        public Object parse(InputStream stream) {
          return new Object();
        }
      };

  private static final MethodDescriptor<Object, Object> EXECUTE_PLAN =
      MethodDescriptor.newBuilder(MARSHALLER, MARSHALLER)
          .setType(MethodDescriptor.MethodType.SERVER_STREAMING)
          .setFullMethodName("spark.connect.SparkConnectService/ExecutePlan")
          .build();

  private static class FixedProvider implements TokenProvider {
    private final String value;
    int calls;

    FixedProvider(String value) {
      this.value = value;
    }

    @Override
    public String token() {
      calls++;
      return value;
    }
  }

  private static Metadata start(PropagationInterceptor interceptor, FakeChannel channel) {
    ClientCall<Object, Object> call =
        interceptor.interceptCall(EXECUTE_PLAN, CallOptions.DEFAULT, channel);
    call.start(null, new Metadata());
    return channel.call.headers;
  }

  @Test
  @DisplayName("every RPC carries the token and the correlation ID")
  void sendsTokenAndCorrelationId() {
    FakeChannel channel = new FakeChannel();
    PropagationInterceptor interceptor =
        new PropagationInterceptor(new FixedProvider("jwt-value"), "session-cid");

    Metadata headers = start(interceptor, channel);

    assertEquals("jwt-value", headers.get(TOKEN));
    assertEquals("session-cid", headers.get(CORRELATION));
    assertNull(headers.get(AUTHORIZATION), "no shared secret was configured");
  }

  @Test
  @DisplayName("an open correlation scope wins over the session default")
  void usesTheOpenScope() {
    FakeChannel channel = new FakeChannel();
    PropagationInterceptor interceptor =
        new PropagationInterceptor(new FixedProvider("jwt"), "session-cid");

    try (CorrelationId.Scope scope = CorrelationId.scope("block-cid")) {
      assertEquals("block-cid", start(interceptor, channel).get(CORRELATION));
    }
  }

  @Test
  @DisplayName("the shared secret rides Authorization, not the token header")
  void sharedSecretGoesOnAuthorization() {
    FakeChannel channel = new FakeChannel();
    PropagationInterceptor interceptor =
        new PropagationInterceptor(
            new FixedProvider("jwt"), "cid", "pre-shared", true,
            PropagationInterceptor.DEFAULT_TOKEN_HEADER,
            PropagationInterceptor.DEFAULT_CORRELATION_HEADER);

    Metadata headers = start(interceptor, channel);

    assertEquals("Bearer pre-shared", headers.get(AUTHORIZATION));
    assertEquals("jwt", headers.get(TOKEN), "the user token must not be replaced by the secret");
  }

  @Test
  @DisplayName("the token is re-read on every call, so a refresh takes effect")
  void tokenIsReReadPerCall() {
    FakeChannel channel = new FakeChannel();
    FixedProvider provider = new FixedProvider("jwt");
    PropagationInterceptor interceptor = new PropagationInterceptor(provider, "cid");

    start(interceptor, channel);
    start(interceptor, channel);

    assertEquals(2, provider.calls);
  }

  @Test
  @DisplayName("existing metadata is preserved")
  void existingMetadataIsPreserved() {
    FakeChannel channel = new FakeChannel();
    PropagationInterceptor interceptor = new PropagationInterceptor(new FixedProvider("jwt"), "cid");
    Metadata.Key<String> theirs = Metadata.Key.of("x-theirs", Metadata.ASCII_STRING_MARSHALLER);

    ClientCall<Object, Object> call =
        interceptor.interceptCall(EXECUTE_PLAN, CallOptions.DEFAULT, channel);
    Metadata headers = new Metadata();
    headers.put(theirs, "kept");
    call.start(null, headers);

    assertEquals("kept", channel.call.headers.get(theirs));
    assertEquals("jwt", channel.call.headers.get(TOKEN));
  }

  // --------------------------------------------------------------- operation ids --

  @Test
  @DisplayName("an ExecutePlan with no operation id gets a fresh UUID4")
  void executePlanGetsAnOperationId() {
    PropagationInterceptor interceptor = new PropagationInterceptor(new FixedProvider("jwt"), "cid");

    ExecutePlanRequest stamped =
        (ExecutePlanRequest) interceptor.withOperationId(ExecutePlanRequest.getDefaultInstance());

    assertTrue(stamped.hasOperationId());
    assertEquals(4, UUID.fromString(stamped.getOperationId()).version());
  }

  @Test
  @DisplayName("every request gets its own id, because one statement is several operations")
  void everyRequestGetsItsOwnId() {
    PropagationInterceptor interceptor = new PropagationInterceptor(new FixedProvider("jwt"), "cid");
    ExecutePlanRequest blank = ExecutePlanRequest.getDefaultInstance();

    String first = ((ExecutePlanRequest) interceptor.withOperationId(blank)).getOperationId();
    String second = ((ExecutePlanRequest) interceptor.withOperationId(blank)).getOperationId();

    assertNotEquals(first, second, "reusing one id is INVALID_HANDLE.OPERATION_ALREADY_EXISTS");
  }

  @Test
  @DisplayName("an operation id Spark already set is left alone")
  void existingOperationIdIsKept() {
    PropagationInterceptor interceptor = new PropagationInterceptor(new FixedProvider("jwt"), "cid");
    ExecutePlanRequest already =
        ExecutePlanRequest.newBuilder().setOperationId("11111111-2222-4333-8444-555555555555").build();

    assertSame(already, interceptor.withOperationId(already));
  }

  @Test
  @DisplayName("other request types are passed through untouched")
  void otherRequestsAreUntouched() {
    PropagationInterceptor interceptor = new PropagationInterceptor(new FixedProvider("jwt"), "cid");
    AnalyzePlanRequest analyze = AnalyzePlanRequest.getDefaultInstance();

    assertSame(analyze, interceptor.withOperationId(analyze));
  }

  @Test
  @DisplayName("opting out puts a stock client on the wire")
  void perOperationIdsCanBeDisabled() {
    PropagationInterceptor interceptor =
        new PropagationInterceptor(
            new FixedProvider("jwt"), "cid", null, false,
            PropagationInterceptor.DEFAULT_TOKEN_HEADER,
            PropagationInterceptor.DEFAULT_CORRELATION_HEADER);

    ExecutePlanRequest blank = ExecutePlanRequest.getDefaultInstance();

    assertSame(blank, interceptor.withOperationId(blank));
    assertFalse(((ExecutePlanRequest) interceptor.withOperationId(blank)).hasOperationId());
  }

  @Test
  @DisplayName("the stamped request actually reaches the channel")
  void stampedRequestIsSent() {
    FakeChannel channel = new FakeChannel();
    PropagationInterceptor interceptor = new PropagationInterceptor(new FixedProvider("jwt"), "cid");

    ClientCall<Object, Object> call =
        interceptor.interceptCall(EXECUTE_PLAN, CallOptions.DEFAULT, channel);
    call.start(null, new Metadata());
    call.sendMessage(ExecutePlanRequest.getDefaultInstance());

    assertEquals(1, channel.call.messages.size());
    ExecutePlanRequest sent = (ExecutePlanRequest) channel.call.messages.get(0);
    assertTrue(sent.hasOperationId(), "the message on the wire must carry the id, not just a copy");
  }
}
