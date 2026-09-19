package io.sparkconnect.propagation.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class CorrelationIdTest {

  @Test
  @DisplayName("the session default is used outside any scope")
  void sessionDefaultOutsideAScope() {
    assertEquals("session-cid", CorrelationId.current("session-cid"));
    assertTrue(CorrelationId.active().isEmpty());
  }

  @Test
  @DisplayName("an open scope overrides the session default")
  void scopeOverridesTheDefault() {
    try (CorrelationId.Scope scope = CorrelationId.scope("block-cid")) {
      assertEquals("block-cid", CorrelationId.current("session-cid"));
      assertEquals("block-cid", scope.id());
    }
    assertEquals("session-cid", CorrelationId.current("session-cid"));
  }

  @Test
  @DisplayName("scopes nest and unwind")
  void scopesNest() {
    try (CorrelationId.Scope outer = CorrelationId.scope("outer")) {
      assertEquals("outer", CorrelationId.current("session"));
      try (CorrelationId.Scope inner = CorrelationId.scope("inner")) {
        assertEquals("inner", CorrelationId.current("session"));
      }
      assertEquals("outer", CorrelationId.current("session"));
    }
    assertEquals("session", CorrelationId.current("session"));
  }

  @Test
  @DisplayName("closing twice is harmless")
  void closingTwiceIsHarmless() {
    CorrelationId.Scope scope = CorrelationId.scope("once");
    scope.close();
    scope.close();
    assertEquals("session", CorrelationId.current("session"));
  }

  @Test
  @DisplayName("generated ids are UUID4, so they are also valid Spark operation ids")
  void generatedIdsAreUuid4() {
    assertEquals(4, UUID.fromString(CorrelationId.newId()).version());
    assertNotEquals(CorrelationId.newId(), CorrelationId.newId());
  }

  @Test
  @DisplayName("a blank id is refused rather than silently sending nothing")
  void blankIdIsRefused() {
    assertThrows(IllegalArgumentException.class, () -> CorrelationId.scope(" "));
    assertThrows(IllegalArgumentException.class, () -> CorrelationId.scope(null));
  }

  @Test
  @DisplayName("ids do not leak between threads")
  void idsDoNotLeakBetweenThreads() throws Exception {
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try (CorrelationId.Scope scope = CorrelationId.scope("mine")) {
      Future<String> other = pool.submit(() -> CorrelationId.current("theirs"));
      assertEquals("theirs", other.get(), "a scope must not bleed into another thread");
      assertEquals("mine", CorrelationId.current("theirs"));
    } finally {
      pool.shutdownNow();
    }
  }
}
