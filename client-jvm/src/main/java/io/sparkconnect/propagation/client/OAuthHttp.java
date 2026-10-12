package io.sparkconnect.propagation.client;

import com.nimbusds.jose.util.JSONObjectUtils;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringJoiner;

/** Form POSTs to an OIDC token endpoint, using only the JDK's HTTP client. */
final class OAuthHttp {

  private static final HttpClient CLIENT =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

  private OAuthHttp() {}

  /**
   * POSTs a form and parses the JSON reply.
   *
   * @throws OAuthException when the server answered with a JSON {@code error} object
   * @throws UncheckedIOException when the exchange failed at the transport level
   */
  static Map<String, Object> postForm(String url, Map<String, String> form) {
    return postForm(url, form, null);
  }

  /**
   * As {@link #postForm(String, Map)}, authenticating the client as well.
   *
   * @param authorization an {@code Authorization} header value, or {@code null} for none
   */
  static Map<String, Object> postForm(String url, Map<String, String> form, String authorization) {
    StringJoiner body = new StringJoiner("&");
    form.forEach((key, value) -> body.add(encode(key) + "=" + encode(value)));

    HttpRequest.Builder builder =
        HttpRequest.newBuilder(URI.create(url))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header("Accept", "application/json")
            .timeout(Duration.ofSeconds(30))
            .POST(HttpRequest.BodyPublishers.ofString(body.toString(), StandardCharsets.UTF_8));
    if (authorization != null) {
      builder.header("Authorization", authorization);
    }
    HttpRequest request = builder.build();

    HttpResponse<String> response;
    try {
      response = CLIENT.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new UncheckedIOException("POST " + url + " failed", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted while calling " + url, e);
    }

    Map<String, Object> parsed;
    try {
      parsed = JSONObjectUtils.parse(response.body());
    } catch (java.text.ParseException e) {
      throw new IllegalStateException(
          url + " returned HTTP " + response.statusCode() + " with a non-JSON body: "
              + abbreviate(response.body()));
    }

    if (response.statusCode() >= 400) {
      throw new OAuthException(
          string(parsed, "error", "unknown"),
          string(parsed, "error_description", ""),
          response.statusCode());
    }
    return parsed;
  }

  /**
   * The {@code Authorization} value for {@code client_secret_basic} (RFC 6749 §2.3.1).
   *
   * <p>Both halves are form-urlencoded <em>before</em> the pair is base64'd. Skipping that step
   * works until a client id holds a {@code :} or a secret holds a {@code %} or a {@code +}, and then
   * fails as {@code invalid_client} with nothing to say why.
   */
  static String clientSecretBasic(String clientId, String clientSecret) {
    String pair = encode(clientId) + ":" + encode(clientSecret);
    return "Basic " + Base64.getEncoder().encodeToString(pair.getBytes(StandardCharsets.UTF_8));
  }

  static String string(Map<String, Object> json, String key, String fallback) {
    Object value = json.get(key);
    return value instanceof String s && !s.isBlank() ? s : fallback;
  }

  static double number(Map<String, Object> json, String key, double fallback) {
    Object value = json.get(key);
    return value instanceof Number n ? n.doubleValue() : fallback;
  }

  /** A mutable, insertion-ordered form builder, so call sites read like the Python dicts. */
  static Map<String, String> form(String... keysAndValues) {
    if (keysAndValues.length % 2 != 0) {
      throw new IllegalArgumentException("form() takes alternating keys and values");
    }
    Map<String, String> form = new LinkedHashMap<>();
    for (int i = 0; i < keysAndValues.length; i += 2) {
      form.put(keysAndValues[i], keysAndValues[i + 1]);
    }
    return form;
  }

  private static String encode(String raw) {
    return java.net.URLEncoder.encode(raw, StandardCharsets.UTF_8);
  }

  private static String abbreviate(String body) {
    if (body == null) {
      return "<empty>";
    }
    return body.length() <= 300 ? body : body.substring(0, 300) + "...";
  }
}
