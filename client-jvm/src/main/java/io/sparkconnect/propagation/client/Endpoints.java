package io.sparkconnect.propagation.client;

import java.util.Objects;

/** The OIDC endpoints of one realm. */
public record Endpoints(String issuer) {

  public Endpoints {
    Objects.requireNonNull(issuer, "issuer");
    if (issuer.endsWith("/")) {
      issuer = issuer.substring(0, issuer.length() - 1);
    }
  }

  public String token() {
    return issuer + "/protocol/openid-connect/token";
  }

  public String deviceAuthorization() {
    return issuer + "/protocol/openid-connect/auth/device";
  }
}
