package io.sparkconnect.propagation;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.jwk.source.JWKSourceBuilder;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import java.net.URI;
import java.util.Set;

/**
 * Validates inbound user JWTs: signature against the identity provider's JWKS, plus issuer,
 * audience and expiry.
 *
 * <p>Runs on the gRPC thread for every RPC, so it must not touch the network on the hot path.
 * The JWKS is fetched once and cached by nimbus, with background refresh and rate limiting, so
 * steady-state validation is a local signature check.
 */
public final class TokenValidator {

  private final DefaultJWTProcessor<SecurityContext> processor;

  public TokenValidator(PluginConfig config) {
    try {
      JWKSource<SecurityContext> keySource =
          JWKSourceBuilder.create(URI.create(config.jwksUri).toURL()).retrying(true).build();

      DefaultJWTProcessor<SecurityContext> jwtProcessor = new DefaultJWTProcessor<>();
      jwtProcessor.setJWSKeySelector(
          new JWSVerificationKeySelector<>(JWSAlgorithm.RS256, keySource));
      jwtProcessor.setJWTClaimsSetVerifier(
          new DefaultJWTClaimsVerifier<>(
              config.expectedAudience,
              new JWTClaimsSet.Builder().issuer(config.issuer).build(),
              Set.of("sub", "exp")));
      this.processor = jwtProcessor;
    } catch (Exception e) {
      throw new IllegalStateException("Could not initialise the JWT validator", e);
    }
  }

  /**
   * @throws InvalidTokenException when the token is unparseable, unsigned by a known key, expired,
   *     issued by the wrong issuer, or not addressed to this service
   */
  public JWTClaimsSet validate(String serialisedToken) throws InvalidTokenException {
    try {
      return processor.process(serialisedToken, null);
    } catch (Exception e) {
      throw new InvalidTokenException(e.getMessage(), e);
    }
  }

  /** Signals a token that must be rejected with UNAUTHENTICATED. */
  public static final class InvalidTokenException extends Exception {
    public InvalidTokenException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
