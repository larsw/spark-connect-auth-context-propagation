package io.sparkconnect.propagation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jose.util.JSONObjectUtils;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The AuthZEN client against a fake PDP: what it sends, and that anything but a clear yes is a no. */
class AuthZenClientTest {

  private HttpServer server;
  private final AtomicReference<String> lastBody = new AtomicReference<>();
  private final AtomicReference<String> lastRequestId = new AtomicReference<>();
  private volatile int status = 200;
  private volatile String answer = "{\"decision\":true}";
  private volatile long delayMillis = 0;

  @BeforeEach
  void startPdp() throws Exception {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        AuthZenClient.EVALUATION_PATH,
        exchange -> {
          lastBody.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
          lastRequestId.set(exchange.getRequestHeaders().getFirst("X-Request-ID"));
          try {
            Thread.sleep(delayMillis);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          byte[] bytes = answer.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(status, bytes.length);
          exchange.getResponseBody().write(bytes);
          exchange.close();
        });
    server.start();
  }

  @AfterEach
  void stopPdp() {
    server.stop(0);
  }

  private AuthZenClient client() {
    return AuthZenClient.forBaseUrl(
        "http://127.0.0.1:" + server.getAddress().getPort() + "/", Duration.ofMillis(500));
  }

  private static final AuthZenClient.Subject BOB =
      new AuthZenClient.Subject("user", "bob", List.of("analyst"), Map.of("sub", "b0b"));

  private static final AuthZenClient.Resource SESSION =
      new AuthZenClient.Resource("spark_connect", "s-1", Map.of("user_id", "b0b"));

  @Test
  void sendsOneAccessEvaluationWithTheCorrelationId() throws Exception {
    AuthZenClient.Decision decision = client().evaluate(BOB, "ExecutePlan", SESSION, "cid-7");

    assertTrue(decision.allowed());
    assertEquals("cid-7", lastRequestId.get());
    Map<String, Object> body = JSONObjectUtils.parse(lastBody.get());
    assertEquals(
        Map.of("type", "user", "id", "bob", "properties", Map.of("sub", "b0b", "roles", List.of("analyst"))),
        body.get("subject"));
    assertEquals(Map.of("name", "ExecutePlan"), body.get("action"));
    assertEquals(
        Map.of("type", "spark_connect", "id", "s-1", "properties", Map.of("user_id", "b0b")),
        body.get("resource"));
    assertEquals(Map.of("request_id", "cid-7"), body.get("context"));
  }

  @Test
  void aSubjectWithNoRolesSendsNone() throws Exception {
    client()
        .evaluate(
            new AuthZenClient.Subject("user", "carol", List.of(), Map.of()),
            "ExecutePlan", SESSION, null);

    Map<String, Object> body = JSONObjectUtils.parse(lastBody.get());
    assertEquals(Map.of("type", "user", "id", "carol"), body.get("subject"));
    assertFalse(body.containsKey("context"));
  }

  @Test
  void aFalseDecisionIsADenyByPolicy() {
    answer = "{\"decision\":false}";
    AuthZenClient.Decision decision = client().evaluate(BOB, "AddArtifacts", SESSION, "cid");

    assertFalse(decision.allowed());
    assertTrue(decision.answered(), "the PDP did answer; it said no");
  }

  @Test
  void anErrorStatusIsADenyWithoutAnAnswer() {
    status = 500;
    AuthZenClient.Decision decision = client().evaluate(BOB, "ExecutePlan", SESSION, "cid");

    assertFalse(decision.allowed());
    assertFalse(decision.answered());
    assertTrue(decision.reason().contains("500"), decision.reason());
  }

  @Test
  void aMissingOrNonBooleanDecisionIsADeny() {
    for (String body : List.of("{}", "{\"decision\":\"true\"}", "{\"decision\":1}", "not json")) {
      answer = body;
      AuthZenClient.Decision decision = client().evaluate(BOB, "ExecutePlan", SESSION, "cid");
      assertFalse(decision.allowed(), "allowed on " + body);
      assertFalse(decision.answered(), "counted as an answer: " + body);
    }
  }

  @Test
  void aPdpThatTakesTooLongIsADeny() {
    delayMillis = 1500;
    AuthZenClient.Decision decision = client().evaluate(BOB, "ExecutePlan", SESSION, "cid");

    assertFalse(decision.allowed());
    assertFalse(decision.answered());
  }

  @Test
  void aPdpThatIsNotThereIsADeny() {
    AuthZenClient nowhere =
        new AuthZenClient(URI.create("http://127.0.0.1:1/access/v1/evaluation"), Duration.ofMillis(300));
    AuthZenClient.Decision decision = nowhere.evaluate(BOB, "ExecutePlan", SESSION, "cid");

    assertFalse(decision.allowed());
    assertFalse(decision.answered());
  }

  @Test
  void theSubjectComesFromTheExchangedToken() throws Exception {
    JWTClaimsSet claims =
        new JWTClaimsSet.Builder()
            .subject("9682af88")
            .claim("principal_name", "alice")
            .claim("principal_roles", List.of("data_engineer"))
            .build();
    SignedJWT token = new SignedJWT(new JWSHeader(JWSAlgorithm.HS256), claims);
    token.sign(new MACSigner(new byte[32]));

    AuthZenClient.Subject subject =
        UserTokenServerInterceptor.pdpSubject("9682af88", "alice-from-inbound", token.serialize());

    assertEquals("alice", subject.id());
    assertEquals(List.of("data_engineer"), subject.roles());
    assertEquals(Map.of("sub", "9682af88"), subject.properties());
  }

  @Test
  void anUnreadableExchangedTokenFallsBackToTheInboundNameAndNoRoles() {
    AuthZenClient.Subject subject =
        UserTokenServerInterceptor.pdpSubject("9682af88", "alice", "not-a-jwt");

    assertEquals("alice", subject.id());
    assertEquals(List.of(), subject.roles());
  }

  @Test
  void theActionIsTheRpcName() {
    assertEquals(
        "ExecutePlan",
        UserTokenServerInterceptor.actionOf("spark.connect.SparkConnectService/ExecutePlan"));
    assertEquals("AddArtifacts", UserTokenServerInterceptor.actionOf("AddArtifacts"));
  }
}
