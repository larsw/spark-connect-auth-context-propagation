package io.sparkconnect.propagation.client;

import com.nimbusds.jose.util.JSONObjectUtils;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * OAuth 2.0 Device Authorization Grant (RFC 8628) -- the CLI login flow.
 *
 * <p>Prints a URL, polls until the user finishes in a browser, then caches the refresh token so
 * later runs are silent. This is how {@code gh auth login} and {@code az login --use-device-code}
 * behave, and it is the right shape for a client with no redirect URI.
 *
 * <p>The cache file is byte-compatible with the Python client's, in the same directory and under
 * the same name, so signing in with either one signs you in for both.
 */
public final class DeviceCodeTokenProvider implements TokenProvider {

  private final Endpoints endpoints;
  private final String clientId;
  private final String label;
  private final Path cacheDir;
  private final Consumer<String> prompt;

  private Tokens tokens;
  private boolean cacheRead;

  public DeviceCodeTokenProvider(Endpoints endpoints, String clientId) {
    this(endpoints, clientId, "default", defaultCacheDir(), System.out::println);
  }

  public DeviceCodeTokenProvider(Endpoints endpoints, String clientId, String label) {
    this(endpoints, clientId, label, defaultCacheDir(), System.out::println);
  }

  /**
   * @param label distinguishes cache files when one process signs in as several users
   * @param prompt where the "open this URL" instructions go; a library should not assume stdout
   */
  public DeviceCodeTokenProvider(
      Endpoints endpoints, String clientId, String label, Path cacheDir, Consumer<String> prompt) {
    this.endpoints = Objects.requireNonNull(endpoints, "endpoints");
    this.clientId = Objects.requireNonNull(clientId, "clientId");
    this.label = Objects.requireNonNull(label, "label");
    this.cacheDir = Objects.requireNonNull(cacheDir, "cacheDir");
    this.prompt = Objects.requireNonNull(prompt, "prompt");
  }

  static Path defaultCacheDir() {
    String xdg = System.getenv("XDG_CACHE_HOME");
    Path base = xdg != null && !xdg.isBlank() ? Path.of(xdg) : Path.of(System.getProperty("user.home"), ".cache");
    return base.resolve("spark-connect-poc");
  }

  Path cacheFile() {
    StringBuilder safe = new StringBuilder();
    for (char c : (clientId + "-" + label).toCharArray()) {
      safe.append(Character.isLetterOrDigit(c) || c == '-' || c == '_' ? c : '_');
    }
    return cacheDir.resolve(safe + ".json");
  }

  @Override
  public synchronized String token() {
    if (tokens == null && !cacheRead) {
      cacheRead = true;
      tokens = loadCache();
    }
    if (tokens != null && tokens.fresh()) {
      return tokens.accessToken();
    }
    if (tokens != null && tokens.refreshToken() != null) {
      Tokens refreshed = tryRefresh(tokens.refreshToken());
      if (refreshed != null) {
        tokens = refreshed;
        saveCache();
        return tokens.accessToken();
      }
    }
    tokens = deviceFlow();
    saveCache();
    return tokens.accessToken();
  }

  private Tokens tryRefresh(String refreshToken) {
    try {
      return Tokens.fromResponse(
          OAuthHttp.postForm(
              endpoints.token(),
              OAuthHttp.form(
                  "grant_type", "refresh_token",
                  "client_id", clientId,
                  "refresh_token", refreshToken)));
    } catch (OAuthException | UncheckedIOException e) {
      // Either the refresh token is spent -- a Keycloak restart wipes every SSO session while the
      // cache on disk still looks perfectly usable -- or the network blinked. Both mean "go back to
      // a device login", not "throw".
      return null;
    }
  }

  private Tokens deviceFlow() {
    Map<String, Object> start =
        OAuthHttp.postForm(
            endpoints.deviceAuthorization(),
            OAuthHttp.form("client_id", clientId, "scope", "openid profile"));

    String complete = OAuthHttp.string(start, "verification_uri_complete", null);
    String verification = complete != null ? complete : OAuthHttp.string(start, "verification_uri", null);
    String userCode = OAuthHttp.string(start, "user_code", "");

    prompt.accept("");
    prompt.accept("  To sign in, open this URL in a browser:");
    prompt.accept("");
    prompt.accept("      " + verification);
    if (complete == null) {
      prompt.accept("");
      prompt.accept("  and enter the code:  " + userCode);
    }
    prompt.accept("");
    prompt.accept("  Waiting for you to finish ...");

    long intervalMillis = (long) (OAuthHttp.number(start, "interval", 5) * 1000L);
    long deadline = System.currentTimeMillis() + (long) (OAuthHttp.number(start, "expires_in", 600) * 1000L);
    String deviceCode = OAuthHttp.string(start, "device_code", "");

    while (System.currentTimeMillis() < deadline) {
      try {
        Thread.sleep(intervalMillis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("interrupted while waiting for device authorization", e);
      }
      try {
        Map<String, Object> payload =
            OAuthHttp.postForm(
                endpoints.token(),
                OAuthHttp.form(
                    "grant_type", "urn:ietf:params:oauth:grant-type:device_code",
                    "client_id", clientId,
                    "device_code", deviceCode));
        prompt.accept("  Signed in.");
        prompt.accept("");
        return Tokens.fromResponse(payload);
      } catch (OAuthException e) {
        switch (e.error()) {
          case "authorization_pending" -> { /* keep polling */ }
          case "slow_down" -> intervalMillis += 5_000L;
          default -> throw e;
        }
      }
    }
    throw new IllegalStateException("device authorization expired before sign-in completed");
  }

  private Tokens loadCache() {
    try {
      Map<String, Object> raw = JSONObjectUtils.parse(Files.readString(cacheFile()));
      String accessToken = OAuthHttp.string(raw, "access_token", null);
      if (accessToken == null) {
        return null;
      }
      // Python writes expires_at as epoch SECONDS; keep the two files interchangeable.
      long expiresAt = (long) (OAuthHttp.number(raw, "expires_at", 0) * 1000L);
      return new Tokens(accessToken, OAuthHttp.string(raw, "refresh_token", null), expiresAt);
    } catch (IOException | java.text.ParseException | RuntimeException e) {
      return null;
    }
  }

  private void saveCache() {
    if (tokens == null) {
      return;
    }
    try {
      Files.createDirectories(cacheDir);
      Map<String, Object> json = new LinkedHashMap<>();
      json.put("access_token", tokens.accessToken());
      json.put("refresh_token", tokens.refreshToken());
      json.put("expires_at", tokens.expiresAtEpochMillis() / 1000.0);
      Path file = cacheFile();
      Files.writeString(file, JSONObjectUtils.toJSONString(json));
      try {
        Files.setPosixFilePermissions(
            file, EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
      } catch (UnsupportedOperationException | IOException ignored) {
        // Not a POSIX filesystem. The token is still only as exposed as the user's home directory.
      }
    } catch (IOException e) {
      // A cold login next time is a nuisance, not an error.
    }
  }
}
