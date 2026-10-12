package io.sparkconnect.propagation.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nimbusds.jose.util.JSONObjectUtils;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.PlainJWT;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class TokenProviderTest {

  @Test
  @DisplayName("endpoints are derived from the issuer, trailing slash or not")
  void endpointsAreDerived() {
    Endpoints plain = new Endpoints("http://keycloak:8080/realms/spark");
    Endpoints slashed = new Endpoints("http://keycloak:8080/realms/spark/");

    assertEquals("http://keycloak:8080/realms/spark/protocol/openid-connect/token", plain.token());
    assertEquals(plain.token(), slashed.token());
    assertEquals(
        "http://keycloak:8080/realms/spark/protocol/openid-connect/auth/device",
        plain.deviceAuthorization());
  }

  @Test
  @DisplayName("the subject is read off the token without verifying it")
  void subjectIsReadFromTheToken() throws Exception {
    String jwt =
        new PlainJWT(new JWTClaimsSet.Builder().subject("abc-123").build()).serialize();

    assertEquals("abc-123", PropagatingSession.subjectOf(jwt).orElseThrow());
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "garbage", "a.b", "a.!!!.c"})
  @DisplayName("subject extraction never throws, whatever it is handed")
  void subjectExtractionNeverThrows(String value) {
    assertTrue(PropagatingSession.subjectOf(value).isEmpty());
  }

  @Test
  @DisplayName("the device-flow cache file matches the Python client's, byte for byte")
  void readsThePythonCacheFile(@TempDir Path cacheDir) throws Exception {
    // Exactly what the Python DeviceCodeTokenProvider writes: expires_at in epoch SECONDS.
    Map<String, Object> cached = new LinkedHashMap<>();
    cached.put("access_token", "cached-access-token");
    cached.put("refresh_token", "cached-refresh-token");
    cached.put("expires_at", System.currentTimeMillis() / 1000.0 + 300);

    DeviceCodeTokenProvider provider =
        new DeviceCodeTokenProvider(
            new Endpoints("http://keycloak:8080/realms/spark"),
            "spark-cli",
            "alice",
            cacheDir,
            line -> {});

    Files.writeString(provider.cacheFile(), JSONObjectUtils.toJSONString(cached));

    // Fresh in the cache, so this must not touch the network at all.
    assertEquals("cached-access-token", provider.token());
  }

  @Test
  @DisplayName("the cache file is named the way the Python client names it")
  void cacheFileNameMatchesPython(@TempDir Path cacheDir) {
    DeviceCodeTokenProvider provider =
        new DeviceCodeTokenProvider(
            new Endpoints("http://keycloak:8080/realms/spark"),
            "spark-cli",
            "alice",
            cacheDir,
            line -> {});

    assertEquals("spark-cli-alice.json", provider.cacheFile().getFileName().toString());
  }

  @Test
  @DisplayName("a corrupt cache file is ignored rather than fatal")
  void corruptCacheIsIgnored(@TempDir Path cacheDir) throws Exception {
    DeviceCodeTokenProvider provider =
        new DeviceCodeTokenProvider(
            new Endpoints("http://keycloak:8080/realms/spark"),
            "spark-cli",
            "bob",
            cacheDir,
            line -> {});
    Files.writeString(provider.cacheFile(), "{not json");

    // Falls through to a device flow, which fails here because nothing is listening -- the point
    // is that reading the cache did not throw first.
    assertFalse(Files.readString(provider.cacheFile()).isEmpty());
  }

  @Test
  @DisplayName("a static token is handed back unchanged")
  void staticTokenIsVerbatim() {
    assertEquals("not-a-jwt", new StaticTokenProvider("not-a-jwt").token());
  }

  // ------------------------------------------------------------------ client credentials --

  /** What one request to the fake token endpoint carried. */
  private record Seen(String authorization, Map<String, String> form) {}

  /**
   * A loopback token endpoint that records each request, so the wire itself is checked. Every
   * reply lives {@code expiresIn} seconds; 0 makes each token stale the moment it arrives.
   */
  private static final class FakeTokenEndpoint implements AutoCloseable {
    final List<Seen> seen = new CopyOnWriteArrayList<>();
    private final HttpServer server;

    FakeTokenEndpoint(int expiresIn) throws IOException {
      server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      server.createContext("/", exchange -> {
        String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        Map<String, String> form = new LinkedHashMap<>();
        for (String pair : body.split("&")) {
          String[] kv = pair.split("=", 2);
          form.put(decode(kv[0]), decode(kv[1]));
        }
        seen.add(new Seen(exchange.getRequestHeaders().getFirst("Authorization"), form));
        byte[] reply =
            ("{\"access_token\":\"cc-" + seen.size() + "\",\"expires_in\":" + expiresIn + "}")
                .getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, reply.length);
        exchange.getResponseBody().write(reply);
        exchange.close();
      });
      server.start();
    }

    /** {@link Endpoints} appends the token path, which the handler does not care about. */
    Endpoints endpoints() {
      return new Endpoints("http://127.0.0.1:" + server.getAddress().getPort() + "/realms/spark");
    }

    private static String decode(String raw) {
      return URLDecoder.decode(raw, StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
      server.stop(0);
    }
  }

  @Test
  @DisplayName("client credentials authenticate with Basic and keep the secret out of the body")
  void clientCredentialsUseBasicAuth() throws Exception {
    try (FakeTokenEndpoint endpoint = new FakeTokenEndpoint(300)) {
      TokenProvider provider =
          new ClientCredentialsTokenProvider(endpoint.endpoints(), "spark-job", "spark-job-secret");

      assertEquals("cc-1", provider.token());

      Seen request = endpoint.seen.get(0);
      assertEquals(Map.of("grant_type", "client_credentials"), request.form());
      assertTrue(request.authorization().startsWith("Basic "));
      assertEquals(
          "spark-job:spark-job-secret",
          new String(
              Base64.getDecoder().decode(request.authorization().substring("Basic ".length())),
              StandardCharsets.UTF_8));
    }
  }

  @Test
  @DisplayName("client credentials send a scope only when given one")
  void clientCredentialsScope() throws Exception {
    try (FakeTokenEndpoint endpoint = new FakeTokenEndpoint(300)) {
      new ClientCredentialsTokenProvider(endpoint.endpoints(), "svc", "s", "api://spark/.default")
          .token();

      assertEquals(
          Map.of("grant_type", "client_credentials", "scope", "api://spark/.default"),
          endpoint.seen.get(0).form());
    }
  }

  @Test
  @DisplayName("client credentials reuse a fresh token")
  void clientCredentialsReuseAFreshToken() throws Exception {
    try (FakeTokenEndpoint endpoint = new FakeTokenEndpoint(300)) {
      TokenProvider provider =
          new ClientCredentialsTokenProvider(endpoint.endpoints(), "spark-job", "spark-job-secret");

      assertEquals("cc-1", provider.token());
      assertEquals("cc-1", provider.token());
      assertEquals(1, endpoint.seen.size());
    }
  }

  @Test
  @DisplayName("client credentials re-run the grant for a stale token, with no refresh token")
  void clientCredentialsReGrantAStaleToken() throws Exception {
    try (FakeTokenEndpoint endpoint = new FakeTokenEndpoint(0)) {
      TokenProvider provider =
          new ClientCredentialsTokenProvider(endpoint.endpoints(), "spark-job", "spark-job-secret");

      assertEquals("cc-1", provider.token());
      assertEquals("cc-2", provider.token());
      assertEquals(
          List.of("client_credentials", "client_credentials"),
          endpoint.seen.stream().map(seen -> seen.form().get("grant_type")).toList());
    }
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "spark-job | spark-job-secret | spark-job:spark-job-secret",
        // RFC 6749 §2.3.1: form-urlencode each half first, so the server can split and decode.
        "svc:a     | p%ss w+rd        | svc%3Aa:p%25ss+w%2Brd",
      })
  @DisplayName("client_secret_basic encodes each half before base64")
  void clientSecretBasicEncoding(String clientId, String secret, String expected) {
    String header = OAuthHttp.clientSecretBasic(clientId, secret);

    assertEquals(
        expected,
        new String(
            Base64.getDecoder().decode(header.substring("Basic ".length())),
            StandardCharsets.UTF_8));
  }
}
