package io.sparkconnect.propagation;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.rest.HTTPHeaders;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.ImmutableHTTPRequest;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * An Iceberg {@link AuthManager} that authenticates each outgoing REST call as whichever Spark
 * Connect user caused it, rather than as one static service identity.
 *
 * <p>Selected with {@code spark.sql.catalog.<name>.rest.auth.type=<this class>}. Iceberg builds it
 * reflectively via a constructor taking the manager name (see {@code AuthManagers.loadAuthManager}),
 * hence the single-argument constructor.
 *
 * <p>The sessions it hands out are intentionally stateless: {@link AuthSession#authenticate} runs
 * per outgoing request on the calling thread, which for Spark Connect is the per-operation
 * ExecutionThread. That is precisely where the job tag identifying the Connect session is
 * available, so the credential is resolved at call time rather than captured at catalog
 * construction time -- which also means a refreshed token takes effect immediately.
 */
public class PropagatingRestAuthManager implements AuthManager {

  private static final Logger LOG = LoggerFactory.getLogger(PropagatingRestAuthManager.class);

  private static final String AUTHORIZATION_HEADER = "Authorization";

  /** Polaris reads this into its MDC and audit events; see polaris.log.request-id-header-name. */
  private static final String REQUEST_ID_HEADER = "X-Request-ID";

  private static final String JOB_TAG_PREFIX = "cid:";

  private final String name;

  /** Required by Iceberg's reflective loader. */
  public PropagatingRestAuthManager(String name) {
    this.name = name;
    LOG.info("propagating auth manager '{}' installed", name);
  }

  @Override
  public AuthSession catalogSession(RESTClient sharedClient, Map<String, String> properties) {
    return new PerRequestAuthSession();
  }

  @Override
  public void close() {
    // Nothing owned; identities are held by PropagatedIdentityHolder for the session's lifetime.
  }

  @Override
  public String toString() {
    return "PropagatingRestAuthManager(" + name + ")";
  }

  /** Resolves the caller afresh on every outgoing request. */
  static final class PerRequestAuthSession implements AuthSession {

    @Override
    public HTTPRequest authenticate(HTTPRequest request) {
      Optional<PropagatedIdentity> maybeIdentity = PropagatedIdentityHolder.currentIdentity();
      if (maybeIdentity.isEmpty()) {
        // Reached when the calling thread carries no Connect operation job tag, e.g. during an
        // AnalyzePlan RPC. Sending the request unauthenticated produces a clear 401 from the
        // catalog rather than a silent fallback to some ambient credential.
        LOG.warn(
            "no propagated identity for this thread; sending an unauthenticated catalog request");
        return request;
      }

      PropagatedIdentity identity = maybeIdentity.get();
      publishToThreadContext(identity);

      Map<String, String> extra = new LinkedHashMap<>();
      extra.put(AUTHORIZATION_HEADER, "Bearer " + identity.polarisToken());
      if (identity.correlationId() != null && !identity.correlationId().isBlank()) {
        extra.put(REQUEST_ID_HEADER, identity.correlationId());
      }

      HTTPHeaders merged = request.headers().putIfAbsent(HTTPHeaders.of(extra));
      return merged.equals(request.headers())
          ? request
          : ImmutableHTTPRequest.builder().from(request).headers(merged).build();
    }

    /**
     * Makes the correlation ID visible in Spark's own logs and in the Spark UI.
     *
     * <p>The MDC entries are deliberately not cleared: Spark Connect gives each operation its own
     * thread and discards it afterwards, so there is nothing to leak into.
     */
    private void publishToThreadContext(PropagatedIdentity identity) {
      String correlationId = identity.correlationId();
      if (correlationId == null || correlationId.isBlank()) {
        return;
      }
      MDC.put(UserTokenServerInterceptor.MDC_CORRELATION_ID, correlationId);
      if (identity.principalName() != null) {
        MDC.put(UserTokenServerInterceptor.MDC_PRINCIPAL, identity.principalName());
      }
      try {
        if (SparkEnv.get() == null) {
          return;
        }
        SparkContext context = SparkContext.getOrCreate();
        String tags = context.getLocalProperty("spark.job.tags");
        String wanted = JOB_TAG_PREFIX + correlationId;
        if (tags == null || !tags.contains(wanted)) {
          // Job tags may not contain the separator; correlation IDs are UUIDs, so this is safe.
          context.addJobTag(wanted);
        }
      } catch (Throwable t) {
        LOG.debug("could not attach the correlation ID as a Spark job tag", t);
      }
    }

    @Override
    public void close() {
      // no resources
    }
  }
}
