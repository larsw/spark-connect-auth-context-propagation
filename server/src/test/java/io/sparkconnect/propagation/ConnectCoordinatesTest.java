package io.sparkconnect.propagation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.spark.connect.proto.CloneSessionRequest;
import org.apache.spark.connect.proto.ExecutePlanRequest;
import org.apache.spark.connect.proto.UserContext;
import org.junit.jupiter.api.Test;

/** Which sessions a request names, as the interceptor reads them before its identity checks. */
class ConnectCoordinatesTest {

  private static UserContext user(String id) {
    return UserContext.newBuilder().setUserId(id).build();
  }

  @Test
  void aCloneNamesBothItsSourceAndItsNewSession() {
    CloneSessionRequest clone =
        CloneSessionRequest.newBuilder()
            .setUserContext(user("bob-sub"))
            .setSessionId("alices-session")
            .setNewSessionId("the-new-one")
            .build();

    UserTokenServerInterceptor.ConnectCoordinates coordinates =
        UserTokenServerInterceptor.coordinatesOf(clone);

    // Before this was read, a clone skipped the user_id and session-owner checks entirely.
    assertNotNull(coordinates);
    assertEquals("bob-sub", coordinates.userId());
    assertEquals("alices-session", coordinates.sessionId());
    assertEquals("the-new-one", coordinates.newSessionId());
  }

  @Test
  void aCloneWithoutANewSessionIdLeavesItToTheServer() {
    CloneSessionRequest clone =
        CloneSessionRequest.newBuilder().setUserContext(user("u")).setSessionId("s").build();

    assertNull(UserTokenServerInterceptor.coordinatesOf(clone).newSessionId());
  }

  @Test
  void otherRequestsNameNoSecondSession() {
    ExecutePlanRequest execute =
        ExecutePlanRequest.newBuilder()
            .setUserContext(user("u"))
            .setSessionId("s")
            .setOperationId("op")
            .build();

    UserTokenServerInterceptor.ConnectCoordinates coordinates =
        UserTokenServerInterceptor.coordinatesOf(execute);
    assertEquals("op", coordinates.operationId());
    assertNull(coordinates.newSessionId());
  }

  @Test
  void anUnknownMessageHasNoCoordinates() {
    // The interceptor refuses these, rather than passing them through unchecked.
    assertNull(UserTokenServerInterceptor.coordinatesOf("not a Connect request"));
  }
}
