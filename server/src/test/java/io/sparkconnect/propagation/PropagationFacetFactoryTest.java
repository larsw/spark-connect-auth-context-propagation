package io.sparkconnect.propagation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openlineage.client.OpenLineage;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The facet is the one place lineage and identity meet, and it hangs off an event format Spark
 * does not promise to keep stable, so it is worth pinning down.
 */
class PropagationFacetFactoryTest {

  private static final String USER = "alice-sub";
  private static final String SESSION = "session-1";
  private static final String OPERATION = "operation-1";

  private static final String CONNECT_TAG =
      "SparkConnect_OperationTag_User_" + USER + "_Session_" + SESSION + "_Operation_" + OPERATION;

  private PropagationFacetFactory.CorrelationRunFacetBuilder builder;
  private Map<String, OpenLineage.RunFacet> emitted;

  @BeforeEach
  void reset() {
    PropagatedIdentityHolder.clearForTests();
    builder = new PropagationFacetFactory.CorrelationRunFacetBuilder();
    emitted = new HashMap<>();
  }

  /** A job start carrying the given `spark.job.tags`, which is all the builder reads. */
  private static SparkListenerJobStart jobStartWithTags(String tags) {
    Properties properties = new Properties();
    if (tags != null) {
      properties.setProperty("spark.job.tags", tags);
    }
    return new SparkListenerJobStart(
        1, 0L, scala.collection.immutable.Seq$.MODULE$.empty(), properties);
  }

  private void build(Object event) {
    builder.build(event, emitted::put);
  }

  private void buildFromTags(String tags) {
    build(jobStartWithTags(tags));
  }

  private Map<String, Object> facetProperties() {
    OpenLineage.RunFacet facet = emitted.get("sparkConnectPropagation");
    return facet == null ? Map.of() : facet.getAdditionalProperties();
  }

  @Test
  @DisplayName("the identity the interceptor recorded reaches the lineage event")
  void identityReachesTheFacet() {
    PropagatedIdentityHolder.put(
        USER, SESSION, OPERATION,
        new PropagatedIdentity(USER, "alice", "a-token", "cid-1", 0L));

    buildFromTags(CONNECT_TAG);

    Map<String, Object> facet = facetProperties();
    assertEquals("cid-1", facet.get("correlationId"));
    assertEquals("alice", facet.get("principal"));
    assertEquals(USER, facet.get("subject"));
    assertEquals(SESSION, facet.get("sessionId"));
    assertEquals(OPERATION, facet.get("operationId"));
  }

  @Test
  @DisplayName("the bearer token never reaches a lineage event")
  void theTokenIsNeverEmitted() {
    PropagatedIdentityHolder.put(
        USER, SESSION, OPERATION,
        new PropagatedIdentity(USER, "alice", "eyJhbGciOiJSUzI1NiJ9.secret", "cid-1", 0L));

    buildFromTags(CONNECT_TAG);

    assertFalse(
        facetProperties().toString().contains("secret"),
        "a credential must never leave in an event that goes to a lineage backend");
  }

  @Test
  @DisplayName("the correlation ID still comes through when the identity is already forgotten")
  void fallsBackToTheCorrelationJobTag() {
    // Nothing in the holder: the session was released before the job start was processed.
    buildFromTags(CONNECT_TAG + ",cid:cid-from-tag");

    assertEquals("cid-from-tag", facetProperties().get("correlationId"));
    assertEquals(SESSION, facetProperties().get("sessionId"));
  }

  @Test
  @DisplayName("a job with no Connect tags produces no facet at all")
  void noTagsNoFacet() {
    buildFromTags("some-unrelated-tag,another");

    assertTrue(emitted.isEmpty(), "an unrelated Spark job must not gain an empty facet");
  }

  @Test
  @DisplayName("events that are not a job start are ignored")
  void otherEventsAreIgnored() {
    // The builder is typed as Object because OpenLineage's dispatch on the type parameter does
    // not fire; it really does get handed RDDs and SQL execution events.
    build("not even a Spark event");
    build(new Object());

    assertTrue(emitted.isEmpty());
  }

  @Test
  @DisplayName("a job start with no properties is survivable")
  void noPropertiesIsSurvivable() {
    buildFromTags(null);

    assertTrue(emitted.isEmpty());
  }
}
