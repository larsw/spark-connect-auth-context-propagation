package io.sparkconnect.propagation.client;

import java.util.Map;
import java.util.Objects;

/**
 * OAuth 2.0 client credentials grant (RFC 6749 §4.4) -- for a job with no user behind it.
 *
 * <p>The token is the client's own: Keycloak issues it to the client's service account, so that
 * account is what the server propagates and what Polaris authorises, by its own grants. Nothing is
 * cached on disk and no refresh token is used (§4.4.3 advises servers not to issue one): a stale
 * token is replaced by running the grant again, which needs only the secret in hand.
 */
public final class ClientCredentialsTokenProvider implements TokenProvider {

  private final Endpoints endpoints;
  private final String clientId;
  private final String clientSecret;
  private final String scope;

  private Tokens tokens;

  public ClientCredentialsTokenProvider(Endpoints endpoints, String clientId, String clientSecret) {
    this(endpoints, clientId, clientSecret, null);
  }

  /** @param scope sent only when non-null; some issuers insist on one, Keycloak does not */
  public ClientCredentialsTokenProvider(
      Endpoints endpoints, String clientId, String clientSecret, String scope) {
    this.endpoints = Objects.requireNonNull(endpoints, "endpoints");
    this.clientId = Objects.requireNonNull(clientId, "clientId");
    this.clientSecret = Objects.requireNonNull(clientSecret, "clientSecret");
    this.scope = scope;
  }

  @Override
  public synchronized String token() {
    if (tokens != null && tokens.fresh()) {
      return tokens.accessToken();
    }
    Map<String, String> form = OAuthHttp.form("grant_type", "client_credentials");
    if (scope != null && !scope.isBlank()) {
      form.put("scope", scope);
    }
    tokens =
        Tokens.fromResponse(
            OAuthHttp.postForm(
                endpoints.token(), form, OAuthHttp.clientSecretBasic(clientId, clientSecret)));
    return tokens.accessToken();
  }
}
