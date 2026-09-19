package io.sparkconnect.propagation.client;

/** An {@code error}/{@code error_description} pair the authorization server returned. */
public class OAuthException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  private final String error;
  private final String description;
  private final int status;

  public OAuthException(String error, String description, int status) {
    super(description == null || description.isBlank() ? error : error + ": " + description);
    this.error = error;
    this.description = description;
    this.status = status;
  }

  /** The OAuth error code, e.g. {@code invalid_grant} or {@code authorization_pending}. */
  public String error() {
    return error;
  }

  public String description() {
    return description;
  }

  public int status() {
    return status;
  }
}
