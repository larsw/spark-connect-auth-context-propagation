package io.sparkconnect.propagation;

import com.nimbusds.jose.util.JSONObjectUtils;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Asks a Policy Decision Point for one decision over the OpenID AuthZEN Authorization API 1.0.
 *
 * <p>It posts one Access Evaluation request and reads back {@code {"decision": true|false}}. Anything
 * else counts as a deny: a status other than 200, a body that is not JSON, a missing or non-boolean
 * {@code decision}, a timeout, or a PDP that cannot be reached. Every deny carries a reason, so a log
 * line can say whether the policy refused or the PDP could not answer.
 *
 * <p>The correlation ID goes in two places: the {@code X-Request-ID} header, which is where PDPs that
 * log requests usually look, and {@code context.request_id}, which is where AuthZEN puts it. Polaris's
 * AuthZEN authorizer does the same, so a decision log carries one ID for both callers.
 *
 * <p>Modelled on svipul-authzen's {@code AuthZenAuthorizer}, cut down to the single-evaluation call the
 * interceptor needs.
 */
public final class AuthZenClient {

  /** Where the AuthZEN spec puts the Access Evaluation API, relative to the PDP's base URL. */
  public static final String EVALUATION_PATH = "/access/v1/evaluation";

  private final URI evaluationEndpoint;
  private final Duration timeout;
  private final HttpClient http;

  public AuthZenClient(URI evaluationEndpoint, Duration timeout) {
    this(evaluationEndpoint, timeout, HttpClient.newBuilder().connectTimeout(timeout).build());
  }

  AuthZenClient(URI evaluationEndpoint, Duration timeout, HttpClient http) {
    this.evaluationEndpoint = evaluationEndpoint;
    this.timeout = timeout;
    this.http = http;
  }

  /** The client for a PDP at {@code baseUrl}, such as {@code http://cerbos:3592}. */
  public static AuthZenClient forBaseUrl(String baseUrl, Duration timeout) {
    String trimmed = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
    return new AuthZenClient(URI.create(trimmed + EVALUATION_PATH), timeout);
  }

  public URI evaluationEndpoint() {
    return evaluationEndpoint;
  }

  /** Who is asking. {@code roles} goes in the subject's properties, beside any others given. */
  public record Subject(String type, String id, List<String> roles, Map<String, Object> properties) {}

  /** What they are asking about. */
  public record Resource(String type, String id, Map<String, Object> properties) {}

  /**
   * The PDP's answer, or the reason there was none. {@code answered} tells a refusal by policy from a
   * PDP that could not be asked: both are denies, but only the first is the caller's to fix.
   */
  public record Decision(boolean allowed, boolean answered, String reason) {
    static Decision permit() {
      return new Decision(true, true, "permitted by policy");
    }

    static Decision deniedByPolicy() {
      return new Decision(false, true, "denied by policy");
    }

    static Decision unanswered(String reason) {
      return new Decision(false, false, reason);
    }
  }

  public Decision evaluate(Subject subject, String action, Resource resource, String requestId) {
    String body = JSONObjectUtils.toJSONString(requestBody(subject, action, resource, requestId));
    HttpRequest.Builder request =
        HttpRequest.newBuilder(evaluationEndpoint)
            .timeout(timeout)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
    if (requestId != null && !requestId.isBlank()) {
      request.header("X-Request-ID", requestId);
    }

    HttpResponse<String> response;
    try {
      response = http.send(request.build(), HttpResponse.BodyHandlers.ofString());
    } catch (IOException e) {
      return Decision.unanswered("the PDP at " + evaluationEndpoint + " could not be reached: " + e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return Decision.unanswered("interrupted while waiting for the PDP");
    }

    if (response.statusCode() != 200) {
      return Decision.unanswered("the PDP answered HTTP " + response.statusCode());
    }
    Object decision;
    try {
      decision = JSONObjectUtils.parse(response.body()).get("decision");
    } catch (ParseException e) {
      return Decision.unanswered("the PDP's answer is not JSON");
    }
    if (!(decision instanceof Boolean allowed)) {
      return Decision.unanswered("the PDP's answer has no boolean decision");
    }
    return allowed ? Decision.permit() : Decision.deniedByPolicy();
  }

  static Map<String, Object> requestBody(
      Subject subject, String action, Resource resource, String requestId) {
    Map<String, Object> subjectProperties = new LinkedHashMap<>(subject.properties());
    if (subject.roles() != null && !subject.roles().isEmpty()) {
      subjectProperties.put("roles", List.copyOf(subject.roles()));
    }

    Map<String, Object> subjectJson = new LinkedHashMap<>();
    subjectJson.put("type", subject.type());
    subjectJson.put("id", subject.id());
    if (!subjectProperties.isEmpty()) {
      subjectJson.put("properties", subjectProperties);
    }

    Map<String, Object> resourceJson = new LinkedHashMap<>();
    resourceJson.put("type", resource.type());
    resourceJson.put("id", resource.id());
    if (!resource.properties().isEmpty()) {
      resourceJson.put("properties", new LinkedHashMap<>(resource.properties()));
    }

    Map<String, Object> body = new LinkedHashMap<>();
    body.put("subject", subjectJson);
    body.put("action", Map.of("name", action));
    body.put("resource", resourceJson);
    if (requestId != null && !requestId.isBlank()) {
      body.put("context", Map.of("request_id", requestId));
    }
    return body;
  }
}
