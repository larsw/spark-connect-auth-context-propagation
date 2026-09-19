package io.sparkconnect.propagation.client;

/** A fixed token. Useful for tests that want to present a specific, or deliberately bad, token. */
public final class StaticTokenProvider implements TokenProvider {

  private final String token;

  public StaticTokenProvider(String token) {
    this.token = token;
  }

  @Override
  public String token() {
    return token;
  }
}
