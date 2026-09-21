package io.sparkconnect.propagation;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
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
 * <p>So the interceptor writes here keyed by the operation when it knows it and by (userId,
 * sessionId) always, and the AuthManager recovers both keys by parsing the job tag off the calling
 * thread's local properties.
 *
 * <p><strong>Not every request gets an ExecutionThread.</strong> AnalyzePlan is answered
 * synchronously on the gRPC handler thread -- no {@code withSession}, so no job tag at all. A
 * client that asks for a schema before running anything (the Spark Connect JDBC driver does, on
 * every {@code DatabaseMetaData} call) therefore resolves the relation, and calls the catalog,
 * with nothing for the AuthManager to find. {@link #bindToCurrentThread} covers that case: the
 * interceptor binds the identity to the handler thread for the duration of the call, and
 * {@link #currentIdentity()} falls back to it when there is no job tag.
 *
 * <p><strong>Keying by operation.</strong> Stock PySpark leaves
 * {@code ExecutePlanRequest.operation_id} empty -- every call site invokes the private
 * {@code _execute_plan_request_with_metadata()} with no argument -- and the server then generates
 * one the interceptor never sees. Our client fills it in (see the Python {@code operation} module),
 * which is what lets two operations running concurrently in the <em>same</em> Connect session under
 * different correlation IDs stay apart. A client that does not gets the session entry instead:
 * still the right user, but possibly a sibling operation's correlation ID.
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

  /**
   * How many in-flight operations to remember. An operation id is used once and never again, so
   * these entries would otherwise accumulate for the life of the JVM. Bounded rather than tied to
   * ReleaseExecute because that RPC is exempt from authentication here (a client whose token has
   * expired must still be able to release server state), so there is no reliable end-of-operation
   * hook in this interceptor. Ageing an entry out is safe: the lookup falls back to the session
   * entry, which is the behaviour a client that mints no operation ids gets anyway.
   */
  private static final int MAX_TRACKED_OPERATIONS = 2048;

  private static final ConcurrentMap<SessionKey, PropagatedIdentity> BY_SESSION =
      new ConcurrentHashMap<>();

  /** Access-ordered, so the entries that age out are the ones nothing has looked up lately. */
  private static final Map<OperationKey, PropagatedIdentity> BY_OPERATION =
      Collections.synchronizedMap(
          new LinkedHashMap<>(256, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<OperationKey, PropagatedIdentity> eldest) {
              return size() > MAX_TRACKED_OPERATIONS;
            }
          });

  /**
   * sessionId to the subject that first claimed it. Spark Connect itself imposes no binding between
   * the authenticated caller and the (user_id, session_id) pair it trusts from the request body, so
   * we impose one here.
   */
  private static final ConcurrentMap<String, String> SESSION_OWNER = new ConcurrentHashMap<>();

  /**
   * The identity of the request being handled on this thread, for the RPCs Spark answers inline
   * rather than on an ExecutionThread. Deliberately NOT inheritable: a thread Spark spawns from
   * here is Spark's, and if it is one that talks to the catalog it must find its identity through
   * the job tag like everything else, or an identity could outlive the call that set it.
   */
  private static final ThreadLocal<PropagatedIdentity> CURRENT_THREAD_IDENTITY = new ThreadLocal<>();

  private PropagatedIdentityHolder() {}

  public static void put(String userId, String sessionId, PropagatedIdentity identity) {
    put(userId, sessionId, null, identity);
  }

  /**
   * Files an identity under the session, and additionally under the operation when the client
   * supplied an operation id. The session entry is always written: AnalyzePlan and Config carry no
   * operation id at all, and it is the fallback for anything the operation map has forgotten.
   */
  public static void put(
      String userId, String sessionId, String operationId, PropagatedIdentity identity) {
    BY_SESSION.put(new SessionKey(userId, sessionId), identity);
    if (operationId != null && !operationId.isBlank()) {
      BY_OPERATION.put(new OperationKey(userId, sessionId, operationId), identity);
    }
  }

  public static void forget(String userId, String sessionId) {
    BY_SESSION.remove(new SessionKey(userId, sessionId));
    synchronized (BY_OPERATION) {
      BY_OPERATION
          .keySet()
          .removeIf(
              key ->
                  Objects.equals(key.userId(), userId)
                      && Objects.equals(key.sessionId(), sessionId));
    }
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

  /**
   * Binds an identity to the calling thread until the returned scope is closed.
   *
   * <p>For the RPCs that never reach an ExecutionThread. The job tag still wins where there is
   * one: an ExecutionThread is a different thread from the gRPC handler, so the two never meet.
   */
  public static Scope bindToCurrentThread(PropagatedIdentity identity) {
    PropagatedIdentity previous = CURRENT_THREAD_IDENTITY.get();
    CURRENT_THREAD_IDENTITY.set(identity);
    return () -> {
      if (previous == null) {
        CURRENT_THREAD_IDENTITY.remove();
      } else {
        CURRENT_THREAD_IDENTITY.set(previous);
      }
    };
  }

  /** What {@link #bindToCurrentThread} hands back. Closing it is not optional. */
  public interface Scope extends AutoCloseable {
    @Override
    void close();
  }

  /**
   * Looks up the identity for whichever Connect operation owns the calling thread: the job tag of
   * an ExecutionThread, or failing that whatever the interceptor bound to this very thread.
   */
  public static Optional<PropagatedIdentity> currentIdentity() {
    Optional<PropagatedIdentity> fromJobTag = currentJobTag().flatMap(PropagatedIdentityHolder::identityFor);
    if (fromJobTag.isPresent()) {
      return fromJobTag;
    }
    return Optional.ofNullable(CURRENT_THREAD_IDENTITY.get());
  }

  /**
   * The identity for one set of Connect coordinates: the exact operation if we have it, otherwise
   * the session it belongs to.
   *
   * <p>The fallback is never wrong about <em>who</em> the caller is -- {@code userId} is pinned to
   * the authenticated subject by the interceptor, so a session entry cannot belong to anyone else.
   * It can only be stale in the correlation ID, which is exactly the limitation that populating
   * {@code operation_id} removes.
   */
  static Optional<PropagatedIdentity> identityFor(JobTag tag) {
    PropagatedIdentity perOperation =
        BY_OPERATION.get(new OperationKey(tag.userId(), tag.sessionId(), tag.operationId()));
    if (perOperation != null) {
      return Optional.of(perOperation);
    }
    return Optional.ofNullable(BY_SESSION.get(new SessionKey(tag.userId(), tag.sessionId())));
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
    BY_OPERATION.clear();
    SESSION_OWNER.clear();
    CURRENT_THREAD_IDENTITY.remove();
  }

  static int trackedOperationCount() {
    return BY_OPERATION.size();
  }

  /** Identifies a Connect session as Spark keys it: {@code SessionKey(userId, sessionId)}. */
  record SessionKey(String userId, String sessionId) {}

  /** Identifies one operation as Spark keys it: {@code ExecuteKey(userId, sessionId, opId)}. */
  record OperationKey(String userId, String sessionId, String operationId) {}

  /** The three coordinates Spark Connect encodes into its operation job tag. */
  public record JobTag(String userId, String sessionId, String operationId) {}
}
