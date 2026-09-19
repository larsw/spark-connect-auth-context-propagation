package io.sparkconnect.propagation;

/**
 * What the gRPC interceptor learned about a caller, for the Iceberg AuthManager to pick up
 * later on a different thread.
 *
 * @param subject      the JWT {@code sub} of the authenticated user
 * @param principalName the Polaris principal name (from {@code principal_name})
 * @param polarisToken the token obtained by RFC 8693 exchange, audience {@code polaris}
 * @param correlationId the caller-supplied correlation ID for this session
 * @param polarisTokenExpiresAtMillis expiry of {@code polarisToken}, already reduced by skew
 */
public record PropagatedIdentity(
    String subject,
    String principalName,
    String polarisToken,
    String correlationId,
    long polarisTokenExpiresAtMillis) {

  public PropagatedIdentity withCorrelationId(String newCorrelationId) {
    return new PropagatedIdentity(
        subject, principalName, polarisToken, newCorrelationId, polarisTokenExpiresAtMillis);
  }

  @Override
  public String toString() {
    // Never let a bearer token reach a log line.
    return "PropagatedIdentity[subject=%s, principal=%s, correlationId=%s, polarisToken=<redacted>]"
        .formatted(subject, principalName, correlationId);
  }
}
