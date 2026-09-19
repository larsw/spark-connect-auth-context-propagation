package io.sparkconnect.propagation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * The job tag is the one place this design depends on a format Spark does not promise to keep
 * stable, so it is worth pinning down precisely. If Spark ever changes {@code ExecuteJobTag},
 * these tests are what should fail.
 */
class PropagatedIdentityHolderTest {

  @BeforeEach
  void reset() {
    PropagatedIdentityHolder.clearForTests();
  }

  @Nested
  @DisplayName("job tag parsing")
  class JobTagParsing {

    @Test
    @DisplayName("parses the tag Spark actually writes")
    void parsesRealTag() {
      // Exactly the shape ExecuteThreadRunner applies, with the UUID user_id our client sends.
      String tag =
          "SparkConnect_OperationTag_User_d23af440-9c55-4cb0-9548-26a8805278cd"
              + "_Session_c2bfd738-c9d3-4847-95c8-503964d7e7a6"
              + "_Operation_b3ab0d92-9f5b-4fdb-967d-285b78caa180";

      PropagatedIdentityHolder.JobTag parsed = PropagatedIdentityHolder.parseJobTag(tag).orElseThrow();

      assertEquals("d23af440-9c55-4cb0-9548-26a8805278cd", parsed.userId());
      assertEquals("c2bfd738-c9d3-4847-95c8-503964d7e7a6", parsed.sessionId());
      assertEquals("b3ab0d92-9f5b-4fdb-967d-285b78caa180", parsed.operationId());
    }

    @Test
    @DisplayName("handles a user id containing underscores")
    void handlesUnderscoresInUserId() {
      // Spark falls back to $USER when the client sends no user_id, and usernames may contain
      // underscores -- which is why the parse works from the right rather than splitting.
      String tag =
          "SparkConnect_OperationTag_User_some_service_account"
              + "_Session_c2bfd738-c9d3-4847-95c8-503964d7e7a6"
              + "_Operation_b3ab0d92-9f5b-4fdb-967d-285b78caa180";

      PropagatedIdentityHolder.JobTag parsed = PropagatedIdentityHolder.parseJobTag(tag).orElseThrow();

      assertEquals("some_service_account", parsed.userId());
      assertEquals("c2bfd738-c9d3-4847-95c8-503964d7e7a6", parsed.sessionId());
    }

    @Test
    @DisplayName("ignores Spark's other job tags")
    void ignoresForeignTags() {
      // Session tags and user-supplied tags share the thread's job tag property.
      assertTrue(PropagatedIdentityHolder.parseJobTag(
          "SparkConnect_SessionTag_User_alice_Session_abc_Tag_mytag").isEmpty());
      assertTrue(PropagatedIdentityHolder.parseJobTag("cid:1234").isEmpty());
      assertTrue(PropagatedIdentityHolder.parseJobTag("").isEmpty());
      assertTrue(PropagatedIdentityHolder.parseJobTag(null).isEmpty());
    }

    @Test
    @DisplayName("rejects a tag missing its markers rather than guessing")
    void rejectsMalformed() {
      assertTrue(PropagatedIdentityHolder.parseJobTag(
          "SparkConnect_OperationTag_User_alice").isEmpty());
      assertTrue(PropagatedIdentityHolder.parseJobTag(
          "SparkConnect_OperationTag_User_alice_Operation_op1").isEmpty());
    }
  }

  @Nested
  @DisplayName("session ownership")
  class SessionOwnership {

    @Test
    @DisplayName("the first subject to use a session owns it")
    void firstClaimWins() {
      assertTrue(PropagatedIdentityHolder.claimSession("session-1", "alice-sub").isEmpty());
      // Same subject again is fine: every RPC in the session re-claims it.
      assertTrue(PropagatedIdentityHolder.claimSession("session-1", "alice-sub").isEmpty());
    }

    @Test
    @DisplayName("a different subject claiming the same session is refused")
    void impostorIsRefused() {
      PropagatedIdentityHolder.claimSession("session-1", "alice-sub");

      Optional<String> owner = PropagatedIdentityHolder.claimSession("session-1", "bob-sub");

      assertTrue(owner.isPresent(), "claiming another subject's session must be refused");
      assertEquals("alice-sub", owner.get());
    }

    @Test
    @DisplayName("separate sessions do not interfere")
    void sessionsAreIndependent() {
      assertTrue(PropagatedIdentityHolder.claimSession("session-1", "alice-sub").isEmpty());
      assertTrue(PropagatedIdentityHolder.claimSession("session-2", "bob-sub").isEmpty());
    }

    @Test
    @DisplayName("releasing a session frees its name")
    void forgetReleasesOwnership() {
      PropagatedIdentityHolder.claimSession("session-1", "alice-sub");
      PropagatedIdentityHolder.forget("alice-sub", "session-1");

      assertTrue(PropagatedIdentityHolder.claimSession("session-1", "bob-sub").isEmpty());
    }

    @Test
    @DisplayName("a blank session id is not claimable")
    void blankSessionIsIgnored() {
      assertTrue(PropagatedIdentityHolder.claimSession("", "alice-sub").isEmpty());
      assertTrue(PropagatedIdentityHolder.claimSession(null, "alice-sub").isEmpty());
      assertFalse(PropagatedIdentityHolder.sessionOwnersSnapshot().containsKey(""));
    }
  }

  @Nested
  @DisplayName("identity record")
  class IdentityRecord {

    @Test
    @DisplayName("never prints the bearer token")
    void redactsToken() {
      PropagatedIdentity identity =
          new PropagatedIdentity("sub-1", "alice", "eyJhbGciOiJSUzI1NiJ9.secret", "cid-1", 0L);

      String rendered = identity.toString();

      assertFalse(rendered.contains("secret"), "the token must never reach a log line");
      assertTrue(rendered.contains("alice"));
      assertTrue(rendered.contains("cid-1"));
    }
  }
}
