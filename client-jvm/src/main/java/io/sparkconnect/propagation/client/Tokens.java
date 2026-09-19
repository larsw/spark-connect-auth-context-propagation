package io.sparkconnect.propagation.client;

import java.util.Map;

/**
 * One access token and what is known about its life.
 *
 * @param expiresAtEpochMillis when the server thinks the access token stops being valid
 */
record Tokens(String accessToken, String refreshToken, long expiresAtEpochMillis) {

  /** Refresh this long before the server thinks the token expires. */
  private static final long EXPIRY_MARGIN_MILLIS = 30_000L;

  static Tokens fromResponse(Map<String, Object> payload) {
    String accessToken = OAuthHttp.string(payload, "access_token", null);
    if (accessToken == null) {
      throw new IllegalStateException("token response carried no access_token: " + payload.keySet());
    }
    long expiresIn = (long) (OAuthHttp.number(payload, "expires_in", 60) * 1000L);
    return new Tokens(
        accessToken,
        OAuthHttp.string(payload, "refresh_token", null),
        System.currentTimeMillis() + expiresIn);
  }

  boolean fresh() {
    return System.currentTimeMillis() < expiresAtEpochMillis - EXPIRY_MARGIN_MILLIS;
  }
}
