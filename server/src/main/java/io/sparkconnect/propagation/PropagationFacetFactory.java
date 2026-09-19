package io.sparkconnect.propagation;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.function.BiConsumer;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Puts the correlation ID, and who caused the work, onto every OpenLineage run.
 *
 * <p>Registered through OpenLineage's own ServiceLoader SPI (see
 * {@code META-INF/services/io.openlineage.spark.api.OpenLineageEventHandlerFactory}), so the
 * listener picks it up with no extra configuration.
 *
 * <p><strong>Why this is not simply a ThreadLocal read.</strong> OpenLineage's listener runs on the
 * listener bus thread, which carries no Connect job tag — that is the whole of finding #14, and the
 * reason dataset resolution fails here. What rescues the correlation ID is that
 * {@link SparkListenerJobStart} carries the submitting thread's local properties along with it, and
 * {@code spark.job.tags} is one of them. So the tag that does not survive as a ThreadLocal does
 * survive inside the event, and that is enough to recover the Connect coordinates and look the
 * identity up in {@link PropagatedIdentityHolder}.
 *
 * <p>The consequence is worth stating plainly: only runs that submit a Spark job get this facet.
 * A DDL statement that the catalog satisfies without scheduling anything produces a lineage event
 * with no job start behind it, and therefore no correlation ID.
 *
 * <p>Nothing here emits a token. The Polaris credential stays in the holder.
 */
public final class PropagationFacetFactory implements OpenLineageEventHandlerFactory {

  private static final Logger LOG = LoggerFactory.getLogger(PropagationFacetFactory.class);

  /** The facet key that appears under {@code run.facets} in the emitted event. */
  static final String FACET_NAME = "sparkConnectPropagation";

  static final URI PRODUCER =
      URI.create("https://github.com/larsw/spark-connect-auth-context-propagation");

  /** {@code SparkContext.SPARK_JOB_TAGS}; see {@link PropagatedIdentityHolder}. */
  private static final String JOB_TAGS_PROPERTY = "spark.job.tags";

  private static final String JOB_TAGS_SEPARATOR = ",";

  /** The tag {@code PropagatingRestAuthManager} adds so the ID is visible in the Spark UI. */
  private static final String CORRELATION_TAG_PREFIX = "cid:";

  @Override
  public Collection<CustomFacetBuilder<?, ? extends OpenLineage.RunFacet>> createRunFacetBuilders(
      OpenLineageContext context) {
    LOG.info("registering the {} run-facet builder with OpenLineage", FACET_NAME);
    return Collections.singletonList(new CorrelationRunFacetBuilder());
  }

  /**
   * Builds the facet from a job start.
   *
   * <p>Typed as {@code Object} and filtered by hand, which looks lazy and is not. OpenLineage is
   * documented to dispatch on the builder's type parameter, but a
   * {@code CustomFacetBuilder<SparkListenerJobStart, ...>} is simply never called here -- no error,
   * no warning, just silence. Declaring {@code Object} and testing the type ourselves gets the same
   * events the generic was supposed to select: across one {@code make test} this is handed 8
   * SparkListenerJobStart, 16 SparkListenerSQLExecutionEnd, 13 SparkListenerSQLExecutionStart and
   * a scattering of RDDs.
   */
  static final class CorrelationRunFacetBuilder
      extends CustomFacetBuilder<Object, OpenLineage.RunFacet> {

    @Override
    protected void build(
        Object rawEvent, BiConsumer<String, ? super OpenLineage.RunFacet> consumer) {
      if (!(rawEvent instanceof SparkListenerJobStart event)) {
        return;
      }
      String rawTags = jobTagsOf(event);
      if (rawTags == null || rawTags.isEmpty()) {
        return;
      }

      OpenLineage.DefaultRunFacet facet = new OpenLineage.DefaultRunFacet(PRODUCER);
      boolean anything = false;

      for (String tag : rawTags.split(JOB_TAGS_SEPARATOR)) {
        Optional<PropagatedIdentityHolder.JobTag> parsed = PropagatedIdentityHolder.parseJobTag(tag);
        if (parsed.isEmpty()) {
          continue;
        }
        PropagatedIdentityHolder.JobTag coordinates = parsed.get();
        facet.getAdditionalProperties().put("sessionId", coordinates.sessionId());
        facet.getAdditionalProperties().put("operationId", coordinates.operationId());
        anything = true;

        // The authoritative correlation ID and principal, as the interceptor recorded them.
        Optional<PropagatedIdentity> identity = PropagatedIdentityHolder.identityFor(coordinates);
        if (identity.isPresent()) {
          putIfPresent(facet, "correlationId", identity.get().correlationId());
          putIfPresent(facet, "principal", identity.get().principalName());
          putIfPresent(facet, "subject", identity.get().subject());
        }
      }

      // Fallback for a run whose identity has already been forgotten: the AuthManager also puts
      // the correlation ID on the job tags directly, purely so it shows up in the Spark UI.
      if (!facet.getAdditionalProperties().containsKey("correlationId")) {
        for (String tag : rawTags.split(JOB_TAGS_SEPARATOR)) {
          if (tag.startsWith(CORRELATION_TAG_PREFIX)) {
            facet.getAdditionalProperties()
                .put("correlationId", tag.substring(CORRELATION_TAG_PREFIX.length()));
            anything = true;
            break;
          }
        }
      }

      if (anything) {
        LOG.debug("attaching {} to lineage run: {}", FACET_NAME, facet.getAdditionalProperties());
        consumer.accept(FACET_NAME, facet);
      }
    }

    private static void putIfPresent(OpenLineage.DefaultRunFacet facet, String key, String value) {
      if (value != null && !value.isBlank()) {
        facet.getAdditionalProperties().put(key, value);
      }
    }

    private static String jobTagsOf(SparkListenerJobStart event) {
      try {
        return event.properties() == null ? null : event.properties().getProperty(JOB_TAGS_PROPERTY);
      } catch (Throwable t) {
        // Lineage is observability: never let a facet builder be the reason a job fails.
        LOG.debug("could not read job tags off the listener event", t);
        return null;
      }
    }
  }
}
