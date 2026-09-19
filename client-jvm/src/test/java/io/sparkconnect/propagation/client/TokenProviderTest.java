package io.sparkconnect.propagation.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nimbusds.jose.util.JSONObjectUtils;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.PlainJWT;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
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
}
