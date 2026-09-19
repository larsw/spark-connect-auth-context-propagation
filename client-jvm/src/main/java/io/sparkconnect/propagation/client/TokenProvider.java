package io.sparkconnect.propagation.client;

/**
 * Anything that can supply a currently-valid access token.
 *
 * <p>Deliberately pluggable, because how a user proves who they are is orthogonal to the thing this
 * PoC is about -- threading that identity through Spark Connect.
 *
 * <p>Implementations hand back a token per call rather than once, so a provider that refreshes
 * keeps a long-lived Spark session alive instead of failing when the original token expires. They
 * are called from whichever thread issues an RPC, so they must be thread-safe.
 */
@FunctionalInterface
public interface TokenProvider {

  /** A token that is valid now, refreshing or re-authenticating if it is not. */
  String token();
}
