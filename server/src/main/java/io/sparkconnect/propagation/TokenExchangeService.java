package io.sparkconnect.propagation;

import com.nimbusds.jose.util.JSONObjectUtils;
import com.nimbusds.jwt.JWTParser;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Base64;
import java.util.Date;
import java.util.HexFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exchanges a user's inbound token for one addressed to the downstream catalog, using RFC 8693
 * standard token exchange.
 *
 * <p>Cached by a SHA-256 of the inbound token rather than by session: when a client refreshes its
 * JWT mid-session the hash changes and a fresh exchange happens automatically, with no session
 * state to invalidate. Entries live until the exchanged token's own expiry, minus a skew margin.
 *
 * <p>Note the audience parameter of RFC 8693 only <em>filters</em> audiences, it never adds one --
 * so the identity provider must already be configured to put the downstream audience on this
 * client's tokens (an audience protocol mapper), or the exchange silently returns a token the
 * catalog will reject.
 */
public final class TokenExchangeService {

  private static final Logger LOG = LoggerFactory.getLogger(TokenExchangeService.class);

  private static final String GRANT_TYPE = "urn:ietf:params:oauth:grant-type:token-exchange";
  private static final String TOKEN_TYPE_ACCESS = "urn:ietf:params:oauth:token-type:access_token";

  private final PluginConfig config;
  private final HttpClient http;
  private final ConcurrentMap<String, CachedToken> cache = new ConcurrentHashMap<>();

  public TokenExchangeService(PluginConfig config) {
    this.config = config;
    this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
  }

  /** Returns a downstream token for the given user token, exchanging only when necessary. */
  public CachedToken exchange(String userToken) throws ExchangeFailedException {
    String key = sha256(userToken);
    long now = System.currentTimeMillis();

    CachedToken cached = cache.get(key);
    if (cached != null && now < cached.expiresAtMillis()) {
      return cached;
    }
    cache.entrySet().removeIf(e -> now >= e.getValue().expiresAtMillis());

    CachedToken fresh = performExchange(userToken);
    cache.put(key, fresh);
    return fresh;
  }

  private CachedToken performExchange(String userToken) throws ExchangeFailedException {
    String form =
        "grant_type=" + urlEncode(GRANT_TYPE)
            + "&subject_token=" + urlEncode(userToken)
            + "&subject_token_type=" + urlEncode(TOKEN_TYPE_ACCESS)
            + "&requested_token_type=" + urlEncode(TOKEN_TYPE_ACCESS)
            + "&audience=" + urlEncode(config.exchangeAudience);

    String basic =
        Base64.getEncoder()
            .encodeToString(
                (config.exchangeClientId + ":" + config.exchangeClientSecret)
                    .getBytes(StandardCharsets.UTF_8));

    HttpRequest request =
        HttpRequest.newBuilder(URI.create(config.tokenEndpoint))
            .timeout(Duration.ofSeconds(15))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header("Authorization", "Basic " + basic)
            .header("Accept", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(form, StandardCharsets.UTF_8))
            .build();

    HttpResponse<String> response;
    try {
      response = http.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    } catch (Exception e) {
      throw new ExchangeFailedException("token exchange request failed: " + e.getMessage(), e);
    }

    if (response.statusCode() != 200) {
      throw new ExchangeFailedException(
          "token exchange rejected with HTTP " + response.statusCode() + ": "
              + describeError(response.body()),
          null);
    }

    try {
      Map<String, Object> body = JSONObjectUtils.parse(response.body());
      String accessToken = (String) body.get("access_token");
      if (accessToken == null || accessToken.isBlank()) {
        throw new ExchangeFailedException("token exchange returned no access_token", null);
      }
      // Trust the token's own exp rather than expires_in, then back off by the skew margin.
      Date expiry = JWTParser.parse(accessToken).getJWTClaimsSet().getExpirationTime();
      long expiresAt =
          (expiry != null ? expiry.getTime() : System.currentTimeMillis() + 60_000L)
              - config.clockSkewSeconds * 1000L;
      LOG.debug("exchanged a user token for audience {}", config.exchangeAudience);
      return new CachedToken(accessToken, expiresAt);
    } catch (ExchangeFailedException e) {
      throw e;
    } catch (Exception e) {
      throw new ExchangeFailedException("could not parse the token exchange response", e);
    }
  }

  private static String describeError(String body) {
    try {
      Map<String, Object> parsed = JSONObjectUtils.parse(body);
      Object error = parsed.get("error");
      Object description = parsed.get("error_description");
      return description != null ? error + " (" + description + ")" : String.valueOf(error);
    } catch (Exception e) {
      return body == null ? "<empty>" : body.substring(0, Math.min(body.length(), 200));
    }
  }

  private static String urlEncode(String value) {
    return java.net.URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  private static String sha256(String value) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      return HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
    } catch (Exception e) {
      throw new IllegalStateException("SHA-256 unavailable", e);
    }
  }

  /** An exchanged token and the instant after which it must not be reused. */
  public record CachedToken(String token, long expiresAtMillis) {}

  /** Signals an exchange that failed; the caller maps this to PERMISSION_DENIED. */
  public static final class ExchangeFailedException extends Exception {
    public ExchangeFailedException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
