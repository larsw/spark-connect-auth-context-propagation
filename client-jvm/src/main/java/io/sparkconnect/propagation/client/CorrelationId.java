package io.sparkconnect.propagation.client;

import java.util.Optional;
import java.util.UUID;

/**
 * Correlation-ID scoping for the client.
 *
 * <p>The default is session-scoped: one ID minted when the session opens and sent on every RPC, so
 * everything is correlatable without ceremony. Opening a scope narrows it.
 *
 * <p>The scope matters because a single user action spans several RPCs -- one
 * {@code spark.sql(...).show()} issues an AnalyzePlan and at least two ExecutePlans -- and a fresh
 * ID per RPC would make it impossible to follow one action through the logs.
 *
 * <pre>{@code
 * try (CorrelationId.Scope scope = CorrelationId.scope()) {
 *     spark.sql("SELECT * FROM polaris.shared.events").show();
 *     System.out.println("if that failed, quote " + scope.id());
 * }
 * }</pre>
 *
 * <p>This is a {@link ThreadLocal}, which is the honest Java equivalent of the Python client's
 * {@code contextvars}: it follows the thread that issues the RPC and does not propagate into
 * threads you hand work off to. A scope opened on one thread and a query run on another falls back
 * to the session default rather than picking up the wrong ID.
 */
public final class CorrelationId {

  private static final ThreadLocal<String> ACTIVE = new ThreadLocal<>();

  private CorrelationId() {}

  /**
   * A fresh correlation ID.
   *
   * <p>Deliberately a UUID4: Spark validates {@code operation_id} as a UUID4, so a correlation ID
   * stays usable anywhere an operation id is.
   */
  public static String newId() {
    return UUID.randomUUID().toString();
  }

  /** Scopes a freshly minted ID to a block of work. */
  public static Scope scope() {
    return scope(newId());
  }

  /** Scopes a caller-supplied ID to a block of work. Scopes nest and unwind. */
  public static Scope scope(String id) {
    if (id == null || id.isBlank()) {
      throw new IllegalArgumentException("correlation id must not be blank");
    }
    String previous = ACTIVE.get();
    ACTIVE.set(id);
    return new Scope(id, previous);
  }

  /** The ID to send on the next RPC: the innermost open scope, else the session default. */
  public static String current(String sessionDefault) {
    String active = ACTIVE.get();
    return active != null ? active : sessionDefault;
  }

  /** The innermost open scope's ID, or empty when no scope is open. */
  public static Optional<String> active() {
    return Optional.ofNullable(ACTIVE.get());
  }

  /** One open correlation scope. Closing it restores whatever was in force before. */
  public static final class Scope implements AutoCloseable {

    private final String id;
    private final String previous;
    private boolean closed;

    private Scope(String id, String previous) {
      this.id = id;
      this.previous = previous;
    }

    public String id() {
      return id;
    }

    @Override
    public void close() {
      if (closed) {
        return;
      }
      closed = true;
      if (previous == null) {
        ACTIVE.remove();
      } else {
        ACTIVE.set(previous);
      }
    }
  }
}
