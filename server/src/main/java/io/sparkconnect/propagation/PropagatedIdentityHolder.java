package io.sparkconnect.propagation;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The bridge across the thread boundary.
 *
 * <p>Spark Connect runs every operation on a fresh, deliberately unpooled ExecutionThread (see
 * {@code ExecuteThreadRunner}), so neither {@code io.grpc.Context} nor a ThreadLocal set by the
 * gRPC interceptor reaches the code that actually talks to Polaris. What <em>does</em> reach it is
 * the Spark job tag that {@code ExecuteThreadRunner} applies inside {@code withSession}:
 *
 * <pre>SparkConnect_OperationTag_User_&lt;userId&gt;_Session_&lt;sessionId&gt;_Operation_&lt;operationId&gt;</pre>
 *
 * <p>So the interceptor writes here keyed by (userId, sessionId), and the AuthManager recovers that
 * key by parsing the job tag off the calling thread's local properties.
 *
 * <p><strong>Why not key by operation?</strong> PySpark never populates
 * {@code ExecutePlanRequest.operation_id} -- every call site invokes the private
 * {@code _execute_plan_request_with_metadata()} with no argument -- so the interceptor cannot know
 * it. Two queries running concurrently in the <em>same</em> Connect session under different
 * correlation IDs may therefore observe each other's ID. Different sessions are unaffected.
 *
 * <p><strong>Classloader warning.</strong> This class holds static state, so the interceptor and
 * the AuthManager must be loaded by the same classloader. That is why the plugin jar is baked into
 * {@code $SPARK_HOME/jars} rather than supplied via {@code --jars} or {@code --packages}, either of
 * which would yield two independent copies of these maps and a token that silently "vanishes".
 */
public final class PropagatedIdentityHolder {

  private static final Logger LOG = LoggerFactory.getLogger(PropagatedIdentityHolder.class);

  /** Mirrors {@code ExecuteJobTag} in Spark's ExecuteHolder.scala. */
  private static final String TAG_PREFIX = "SparkConnect_OperationTag_User_";

  private static final String SESSION_MARKER = "_Session_";
  private static final String OPERATION_MARKER = "_Operation_";

  /** {@code SparkContext.SPARK_JOB_TAGS} and {@code SPARK_JOB_TAGS_SEP}. */
  private static final String JOB_TAGS_PROPERTY = "spark.job.tags";

  private static final String JOB_TAGS_SEPARATOR = ",";

  private static final ConcurrentMap<SessionKey, PropagatedIdentity> BY_SESSION =
      new ConcurrentHashMap<>();

  /**
   * sessionId to the subject that first claimed it. Spark Connect itself imposes no binding between
   * the authenticated caller and the (user_id, session_id) pair it trusts from the request body, so
   * we impose one here.
   */
  private static final ConcurrentMap<String, String> SESSION_OWNER = new ConcurrentHashMap<>();

  private PropagatedIdentityHolder() {}

  public static void put(String userId, String sessionId, PropagatedIdentity identity) {
    BY_SESSION.put(new SessionKey(userId, sessionId), identity);
  }

  public static void forget(String userId, String sessionId) {
    BY_SESSION.remove(new SessionKey(userId, sessionId));
    if (sessionId != null) {
      SESSION_OWNER.remove(sessionId);
    }
  }

  /**
   * Binds a Connect session to the authenticated subject that first used it.
   *
   * @return empty when the claim is accepted, otherwise the subject that already owns the session
   */
  public static Optional<String> claimSession(String sessionId, String subject) {
    if (sessionId == null || sessionId.isBlank() || subject == null) {
      return Optional.empty();
    }
    String owner = SESSION_OWNER.putIfAbsent(sessionId, subject);
    if (owner == null || owner.equals(subject)) {
      return Optional.empty();
    }
    return Optional.of(owner);
  }

  /** Looks up the identity for whichever Connect operation owns the calling thread. */
  public static Optional<PropagatedIdentity> currentIdentity() {
    return currentJobTag()
        .map(tag -> new SessionKey(tag.userId(), tag.sessionId()))
        .map(BY_SESSION::get);
  }

  /** The parsed Connect coordinates of the calling thread, if it is an ExecutionThread. */
  public static Optional<JobTag> currentJobTag() {
    String raw = jobTagsOfCurrentThread();
    if (raw == null || raw.isEmpty()) {
      return Optional.empty();
    }
    for (String tag : raw.split(JOB_TAGS_SEPARATOR)) {
      Optional<JobTag> parsed = parseJobTag(tag);
      if (parsed.isPresent()) {
        return parsed;
      }
    }
    return Optional.empty();
  }

  private static String jobTagsOfCurrentThread() {
    try {
      // Guarded by SparkEnv so that getOrCreate() can never be the thing that creates a context.
      if (SparkEnv.get() == null) {
        return null;
      }
      return SparkContext.getOrCreate().getLocalProperty(JOB_TAGS_PROPERTY);
    } catch (Throwable t) {
      LOG.debug("Could not read job tags from the current thread", t);
      return null;
    }
  }

  /**
   * Parses one job tag. Parsed from the right, because a userId may legitimately contain
   * underscores while the markers are fixed and the trailing identifiers are opaque.
   */
  static Optional<JobTag> parseJobTag(String tag) {
    if (tag == null || !tag.startsWith(TAG_PREFIX)) {
      return Optional.empty();
    }
    int operationAt = tag.lastIndexOf(OPERATION_MARKER);
    if (operationAt < 0) {
      return Optional.empty();
    }
    int sessionAt = tag.lastIndexOf(SESSION_MARKER, operationAt);
    if (sessionAt < TAG_PREFIX.length()) {
      return Optional.empty();
    }
    String userId = tag.substring(TAG_PREFIX.length(), sessionAt);
    String sessionId = tag.substring(sessionAt + SESSION_MARKER.length(), operationAt);
    String operationId = tag.substring(operationAt + OPERATION_MARKER.length());
    return Optional.of(new JobTag(userId, sessionId, operationId));
  }

  /** Visible for diagnostics. */
  public static Map<String, String> sessionOwnersSnapshot() {
    return Map.copyOf(SESSION_OWNER);
  }

  static void clearForTests() {
    BY_SESSION.clear();
    SESSION_OWNER.clear();
  }

  /** Identifies a Connect session as Spark keys it: {@code SessionKey(userId, sessionId)}. */
  record SessionKey(String userId, String sessionId) {}

  /** The three coordinates Spark Connect encodes into its operation job tag. */
  public record JobTag(String userId, String sessionId, String operationId) {}
}
