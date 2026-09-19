package io.sparkconnect.propagation.client;

import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Objects;

/**
 * Direct access grant. Used by automated tests, so they stay headless; never by an interactive
 * application, where {@link DeviceCodeTokenProvider} is the right shape.
 */
public final class PasswordGrantTokenProvider implements TokenProvider {

  private final Endpoints endpoints;
  private final String clientId;
  private final String username;
  private final String password;

  private Tokens tokens;

  public PasswordGrantTokenProvider(
      Endpoints endpoints, String clientId, String username, String password) {
    this.endpoints = Objects.requireNonNull(endpoints, "endpoints");
    this.clientId = Objects.requireNonNull(clientId, "clientId");
    this.username = Objects.requireNonNull(username, "username");
    this.password = Objects.requireNonNull(password, "password");
  }

  @Override
  public synchronized String token() {
    if (tokens != null && tokens.fresh()) {
      return tokens.accessToken();
    }
    if (tokens != null && tokens.refreshToken() != null) {
      try {
        tokens =
            Tokens.fromResponse(
                OAuthHttp.postForm(
                    endpoints.token(),
                    OAuthHttp.form(
                        "grant_type", "refresh_token",
                        "client_id", clientId,
                        "refresh_token", tokens.refreshToken())));
        return tokens.accessToken();
      } catch (OAuthException | UncheckedIOException e) {
        // Refresh token spent, or the network blinked. Either way a full grant is the honest next
        // move; if the IdP is really down, that fails loudly on its own.
      }
    }
    Map<String, Object> payload =
        OAuthHttp.postForm(
            endpoints.token(),
            OAuthHttp.form(
                "grant_type", "password",
                "client_id", clientId,
                "username", username,
                "password", password));
    tokens = Tokens.fromResponse(payload);
    return tokens.accessToken();
  }
}
