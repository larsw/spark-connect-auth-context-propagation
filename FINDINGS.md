# Findings

What building this PoC turned up about Spark Connect, Iceberg, Polaris and Keycloak. Each item
was verified against source, published jars or a running system on 2026-09-18 — not recalled.
Versions: Spark 4.1.3, Iceberg 1.11.0, Polaris 1.7.0, Keycloak 26.7.0.

---

## 1. Spark Connect relocates gRPC, and its own documentation says otherwise

`spark.connect.grpc.interceptor.classes` is documented in `Connect.scala:54` as taking

> Comma separated list of class names that must implement the `io.grpc.ServerInterceptor` interface.

That is wrong for any released Spark 4.x. Inspecting the published artifact:

```
$ unzip -l spark-connect_2.13-4.1.3.jar | grep -c " io/grpc/"
0
$ unzip -l spark-connect_2.13-4.1.3.jar | grep -c "org/sparkproject/connect/grpc/"
1610
```

`sql/connect/server/pom.xml` relocates `io.grpc` to `${spark.shade.packageName}.connect.grpc`,
and `spark.shade.packageName` is `org.sparkproject`. A custom interceptor must therefore
implement **`org.sparkproject.connect.grpc.ServerInterceptor`** and use the relocated `Metadata`,
`ServerCall` and `ServerCallHandler`.

`com.google.protobuf` is relocated too, but `org.apache.spark.connect.proto.*` is not — so the
generated request classes keep their documented names.

The practical consequence: an implementation written against the documented interface compiles
happily against plain grpc-java and then fails at server start. Depending on
`org.apache.spark:spark-connect_2.13` with scope `provided` supplies the relocated classes
directly, so no build-time shading is needed on the plugin side.

**Worth reporting upstream**: the docstring should name the relocated interface, or the config
should accept both.

## 2. Custom interceptors must have a zero-argument constructor

`SparkConnectInterceptorRegistry.createInstance` does:

```scala
val ctorOpt = cls.getConstructors.find(_.getParameterCount == 0)
if (ctorOpt.isEmpty) throw new SparkException(errorClass = "CONNECT.INTERCEPTOR_CTOR_MISSING", ...)
```

There is no way to inject configuration. An interceptor must read `SparkEnv.get.conf` itself.
Secrets are better taken from the environment, since Spark conf values surface in the Spark UI's
environment tab.

## 3. Spark Connect has no binding between the authenticated caller and the session it serves

This is the most significant finding.

* `user_id` comes from the connection string, or falls back to `$SPARK_USER`/`$USER`
  (`core.py:711-715`).
* `session_id` is a client-generated UUID, or whatever the client puts in the connection string
  (`core.py:705-708`).
* `req.user_context.user_id = self._user_id` (`core.py:1193`), and the server keys
  `SessionKey(userId, sessionId)` straight off those values.

Nothing ties either to an authenticated identity. Spark's own
`PreSharedKeyAuthenticationInterceptor` cannot help: every client presents the *same* secret, so
it cannot distinguish users at all.

So on a shared Connect server, any client that can connect may assert another user's
`(user_id, session_id)` and attach to their live `SessionHolder` — inheriting temp views, cached
DataFrames, registered UDFs, and any credential a scheme like this one associates with the
session.

This PoC closes the gap in its interceptor: reject when `user_context.user_id` is not the JWT
subject, and pin `sessionId -> subject` on first use. `tests/test_propagation.py::test_bob_cannot_claim_alices_session`
demonstrates the attack being refused with `PERMISSION_DENIED`.

Anyone deploying multi-tenant Spark Connect should assume this is their problem to solve.

## 4. Crossing the thread boundary: job tags are the only public seam

Spark Connect executes every operation on a fresh, deliberately unpooled thread
(`ExecuteThreadRunner.scala:51`, `:332`), so `io.grpc.Context` and ThreadLocals set by an
interceptor do not reach the code that talks to the catalog.

What does reach it is the job tag applied inside `withSession` (`ETR.scala:201`):

```
SparkConnect_OperationTag_User_<userId>_Session_<sessionId>_Operation_<operationId>
```

`SparkContext.getJobTags()` has been public since 3.5, and the tags live in the thread-local
`spark.job.tags` property, so code running on the ExecutionThread can recover the Connect
coordinates and look up whatever the interceptor stashed. The tag *format* is internal, which is
the one fragile dependency in this design.

Per-session isolation is real: `SparkConnectSessionManager.newIsolatedSession()` calls
`SparkSession.newSession()`, giving each Connect session its own `CatalogManager` and therefore
its own catalog and auth manager instances.

## 5. `operation_id` is never populated by PySpark — and one statement is not one operation

All nine call sites in `core.py` invoke `self._execute_plan_request_with_metadata()` with no
argument, and `operation_id` is a parameter of that private method. The server generates the id,
so the client never learns it and an interceptor can key what it learns only by session. For a
token that is fine, since a token is per user. For a correlation ID it means two operations
running concurrently *in the same session* under different IDs can observe each other's.

**Now fixed here.** `connect()` patches `_execute_plan_request_with_metadata` on the client
*instance* — not the class, so nothing else in the process is affected — to mint a UUID4 per
request. Spark adopts it, which puts it in `ExecuteHolder`, in the Spark UI and, load-bearing for
this design, in the operation job tag that reaches the ExecutionThread. The interceptor then files
the identity under `(userId, sessionId, operationId)` and falls back to the session entry for a
client that sends no id. Verified: with the patch off, the captured ids come back `['', '']`; with
it on, each one appears in Spark's own logs as `opId=<uuid>`.

**The obvious shortcut does not work.** Setting `operation_id` *to the correlation ID*, which this
document previously suggested, fails immediately — one correlation ID is deliberately not one
operation:

* `spark.sql(x).collect()` issues **two** ExecutePlan requests (the command, then the result
  relation), and a `with correlation_id()` block is meant to span several statements.
* Spark keys `ExecuteHolder` by `(userId, sessionId, operationId)` and rejects a repeat with
  `INVALID_HANDLE.OPERATION_ALREADY_EXISTS`, or `OPERATION_ABANDONED` once the first has been
  reaped. Reusing one id failed on the very first statement, not on the second.

So the operation id is fresh per request and the correlation ID keeps its own header; the server
logs the pair, which is what makes one greppable from the other.

**How much this was really costing** is worth stating honestly: the cross-talk could not be
provoked on demand. Operations in one session do overlap — three sleeping queries showed two
running together — but each operation refreshes the session entry immediately before its own
catalog call, so it nearly always wins the race. Per-operation keying removes the dependence on
that timing rather than fixing an outage anyone had seen.

## 6. `UserContext.extensions` is usable, but leaks secrets into the Spark UI

`SparkConnectClient.add_threadlocal_user_context_extension` and
`add_global_user_context_extension` exist in PySpark 4.1.3 (`core.py:1928`, `:1935`), so in-band
propagation needs no fork.

It is still the wrong channel for a credential: `ExecuteThreadRunner` stringifies the entire
request proto into the job description and `callSite` (`ETR.scala:209`), so a token in the
request body would surface in the Spark UI. gRPC metadata does not.

## 7. Iceberg has no released runtime for Spark 4.2

At the time of writing, `iceberg-spark-runtime-4.2_2.13` returns 404 on Maven Central. Iceberg
1.11.0 tops out at `4.1_2.13`; only `1.12.0-SNAPSHOT` and `1.13.0-SNAPSHOT` build a 4.2 runtime.
Anyone pairing "latest Spark" with an Iceberg REST catalog is on Spark 4.1 until that ships.

## 8. Polaris requires principals to pre-exist, but will resolve them by name

`DefaultAuthenticator` states it plainly:

> **This authenticator is used in both internal and external authentication scenarios. For now,
> it does not support federated principals that are not managed by Polaris.**

`resolvePrincipalEntity` prefers `findPrincipalById` when a principal-id claim is present and
`> 0`, and otherwise falls back to `findPrincipalByName`.

This looks like a bootstrap circularity — the IdP would have to know Polaris's numeric principal
ids — but configuring only `polaris.oidc.principal-mapper.name-claim-path` (and deliberately
*not* `id-claim-path`) resolves by name instead, so the IdP only needs to emit a username.

Principal roles arrive from the token via `quarkus.oidc.roles.role-claim-path` mapped through
`polaris.oidc.principal-roles-mapper`, but are only *activated* if Polaris has granted them. IdP
role names must match Polaris principal role names exactly.

## 9. RFC 8693 `audience` filters; it never adds

Keycloak's own guide is explicit, and it is easy to miss:

> the `audience` parameter can be used to filter the audiences that are coming from the used
> client scopes. However, this parameter will not add more audiences.

So `audience=polaris` on a token exchange produces a token Polaris will reject, unless an
audience protocol mapper already puts `polaris` on the requester client's tokens. Two mappers are
needed for a two-hop chain: one adding `spark-connect` for the CLI client, one adding `polaris`
for the Connect server.

Standard token exchange (V2) is enabled by default in Keycloak 26.7; only the per-client
*Standard token exchange* switch needs turning on, and the requester must be confidential.

## 10. A realm import that declares `clientScopes` replaces the built-in ones

Providing a top-level `clientScopes` array in a realm JSON removed Keycloak's `basic` and
`profile` scopes. Every issued token then lacked `sub` and `preferred_username`, and `scope` came
back empty — the server rejected everything with `JWT missing required claims: [sub]`, which
points nowhere near the cause.

Attaching protocol mappers directly to clients, and not declaring `clientScopes` at all, lets
Keycloak create its standard scopes and assign them normally.

## 11. Credential vending against MinIO is real, temporary and sub-scoped

Polaris vends genuinely per-identity credentials. alice and bob receive different
`s3.access-key-id`, `s3.secret-access-key`, `s3.session-token` and expiry, and neither is the
MinIO root key.

MinIO's audit log shows what the executor actually presented:

```json
"requestClaims": {"accessKey": "TV4GL2GBHNDWRKP77OH3", "exp": 1789771028,
                  "parent": "minio_root", "sessionPolicy": "<base64>"}
```

and the decoded session policy is narrowed to a single table's prefix:

```json
{"Effect":"Allow","Action":["s3:GetObject","s3:GetObjectVersion"],
 "Resource":["arn:aws:s3:::warehouse/poc/shared/events/metadata/*"]}
```

Since no Spark container holds any S3 credential at all, a successful read from the worker JVM
can only have used credentials vended for the authenticated user.

## 12. The JVM Connect client shades gRPC to a *different* package than the server

Writing the same client twice, once in Python and once in Java, put the two clients' constraints
side by side. The JVM one is the easier of the two, with one trap.

**The relocation prefix is not the server's.** §1 established that `spark-connect` puts gRPC at
`org.sparkproject.connect.grpc`. `spark-connect-client-jvm` puts it somewhere else again:

```
$ unzip -l spark-connect-client-jvm_2.13-4.1.3.jar | grep -c " io/grpc/"
0
$ unzip -l spark-connect-client-jvm_2.13-4.1.3.jar | grep -oE 'org/sparkproject/io/[a-z]+/' | sort -u
org/sparkproject/io/grpc/
org/sparkproject/io/netty/
org/sparkproject/io/perfmark/
```

So a client interceptor implements `org.sparkproject.io.grpc.ClientInterceptor` while a server
interceptor implements `org.sparkproject.connect.grpc.ServerInterceptor`. The two cannot share a
superinterface, an abstract base or a `Metadata.Key`, even though both are "gRPC".

**Everything else is public API,** which PySpark cannot say:

* `SparkConnectClient.builder().interceptor(...)` and `SparkSession.builder().client(...)` are
  both public, so injecting an interceptor needs no custom channel builder.
* Scala's static forwarders make `SparkConnectClient.builder()` and
  `SparkSession.builder()` callable from plain Java, and `SparkConnectClient.Builder` resolves as
  an ordinary nested type — no `MODULE$` gymnastics.
* One `ClientInterceptor` covers every RPC shape. PySpark needs a separate implementation for
  unary-unary and unary-stream, and silently leaves half the traffic unauthenticated if you
  implement only one.
* `ExecutePlanRequest.operation_id` (§5) can be filled in by rewriting the outgoing message in
  `sendMessage`, which is ordinary interceptor work. On the Python side the same thing needs a
  patched private method.

**The trap is Arrow, not Connect.** Results come back as Arrow batches, and Arrow reaches into
`java.nio` reflectively, which Java 17+ encapsulates. Without

```
--add-opens=java.base/java.nio=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true
```

the first `collect()` fails with `Could not initialize class
org.sparkproject.org.apache.arrow.memory.util.MemoryUtil`, followed by a misleading "Memory was
leaked by query" on close. Neither message names the real problem or the flag that fixes it.

## 13. The Rust client is the only one that fills in `operation_id`, and the only one that cannot scope a correlation ID

Writing the client a third time, on [`spark-connect-rs`](https://crates.io/crates/spark-connect-rs)
0.0.2, sharpened §5 from both ends.

**It already does the thing PySpark will not.** `SparkConnectClient::execute_plan_request_with_metadata`
mints an id per call:

```rust
let operation_id = Uuid::new_v4().to_string();
self.operation_id = Some(operation_id.clone());
// ...
operation_id: Some(operation_id),
```

So of the three clients, the least mature one is the only one correct out of the box: PySpark
needs a patched private method, the JVM client needs an interceptor that rewrites the outgoing
message, and Rust needs nothing. The server-side per-operation keying works for it immediately,
which the integration test checks by asserting the server never logs
`operation <server-generated>` for this client.

**Its session type closes the generic, so headers cannot vary per RPC.** The crate is generic
right up until the point it matters:

```rust
pub struct SparkConnectClient<T> { /* ... */ }
pub type SparkClient = SparkConnectClient<HeadersMiddleware<Channel>>;
pub struct SparkSession { client: SparkClient, /* ... */ }
```

`HeadersMiddleware` holds a `HashMap<String, String>` captured when it is built, and
`SparkSession` accepts no other service type — so a tonic interceptor
(`InterceptedService<Channel, F>`) or a tower layer of our own is simply a different type and
cannot be used. `SparkSessionBuilder::create_client` is private and hardcodes the layer. Upstream
`main` has not changed this.

The consequences are exactly two, and they are the two dynamic things the other clients do: the
token cannot be re-read per RPC (a session lasts as long as the token that opened it), and the
correlation ID cannot be narrowed to a block. `Config` — which is public and does take `headers`,
`user_id` and `session_id` — is enough for everything static, so the client is built on that and
scopes correlation by session instead of pretending to offer a block.

**Two smaller things.** The published crate carries Spark **3.5** protos and still drives a 4.1.3
server for everything here, the Connect protocol being backwards compatible. And 0.0.2 renders the
gRPC status as `Unauthenicated` (sic) in its `Display` impl, so a caller matching on the rendered
string is matching a typo; it is fixed on `main` but not released, and matching the variant, or the
server's own message, avoids the question.

## 14. PySpark's session finaliser shuts down a process-wide thread pool, and can deadlock against gRPC

This one cost an afternoon of "flaky test" before it turned out to be a real, reproducible
deadlock with nothing to do with the test that kept hanging.

`SparkSession.__del__` calls `client.close()`, which calls
`ExecutePlanResponseReattachableIterator.shutdown()`:

```python
_release_thread_pool_instance: Optional[ThreadPoolExecutor] = None   # a CLASS variable

@classmethod
def shutdown(cls) -> None:
    with cls._lock:
        if cls._release_thread_pool_instance is not None:
            thread_pool = cls._release_thread_pool_instance
            cls._release_thread_pool_instance = None
            thread_pool.shutdown()          # joins every worker
```

Two facts collide there. The pool is a **class variable**, one executor shared by every session in
the process — so finalising *any* session, including one already stopped, tears it down for all of
them. And `shutdown()` **joins** those workers.

Now let the collector run that finaliser while the current thread is inside a gRPC call, holding
the channel state lock in `grpc._channel._blocking`. The workers being joined each need that same
lock to send their own ReleaseExecute. The joining thread waits in `join`, the pool threads wait
on the lock, and the process stops. `pytest --timeout-method=thread` shows it exactly:

```
MainThread:
  ... test .collect() -> grpc/_interceptor.py -> grpc/_channel.py:1136 _blocking
      -> pyspark/sql/connect/session.py:878 __del__        <- collector ran here
      -> client/reattach.py:84 shutdown -> thread.py:239 shutdown -> threading.py:1095 join
ThreadPoolExecutor-3_0..3:
  ... reattach.py:211 target -> grpc/_channel.py:1152 _blocking -> threading.py:304 __enter__
```

It hung this suite roughly one run in four, always in whichever test happened to be running when
the collection landed — which is why it looked like a concurrency bug in the one test that runs
concurrent queries. It was not; that test was just the slowest and so the likeliest to be caught.

**Stopping sessions explicitly is only half a fix.** A stopped session is still an object with a
`__del__`, so the finaliser still runs later, still calls the class-level `shutdown()`, and can
still land inside someone else's RPC. Tried that first: it went from 1 failure in 4 runs to 1 in
15, which is worse than either fixing it or leaving it alone, because it looks fixed.

What actually fixes it is denying the collector the opportunity: every short-lived session is
stopped *and* retained for the life of the process, so its finaliser runs at interpreter exit with
no RPC in flight. 45 consecutive clean runs, from about 1 in 4 hanging.

Anything long-lived that opens Spark Connect sessions and lets them go out of scope has this
hazard. It is not specific to tests.

## 15. Per-user catalog auth and out-of-band lineage collection are in direct conflict

Adding OpenLineage turned the thread-boundary problem of §4 around and pointed it back at us.

**Job-level lineage works.** OpenLineage 1.53.0 registers as an ordinary `SparkListener`, so Spark
Connect changes nothing for it: `make test` produces eight jobs in Marquez —
`create_table.restricted_salaries`, `append_data.polaris_restricted_salaries`, `drop_table` and so
on — with run ids, timings and success or failure.

**Dataset-level lineage does not, and cannot, in a stack built like this one.** Every event
carries empty `inputs` and `outputs`. The reason is not a missing facet or a version gap:

```
WARN [,] PropagatingRestAuthManager: no propagated identity for this thread;
                                     sending an unauthenticated catalog request
  ...
  at io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg.BaseCatalogTypeHandler.getIcebergTable
  at org.apache.iceberg.spark.SparkCatalog.loadTable
  at org.apache.spark.scheduler.AsyncEventQueue$$anon$2.run
org.apache.iceberg.exceptions.NotAuthorizedException: Not authorized:
```

To name a dataset, OpenLineage's Iceberg handler loads the table from the catalog. It does that on
the listener bus thread — note the empty `[,]` MDC, and `AsyncEventQueue` at the bottom of the
stack. §4 established that the *only* thing crossing into a Connect operation's thread is the job
tag, and the listener thread has no job tag, so our AuthManager has no identity and sends the call
unauthenticated, exactly as designed. Polaris answers 401 and the dataset goes unresolved. 38
times per test run.

**The identity itself, though, does cross — through the event rather than the thread.**
`SparkListenerJobStart` carries the submitting thread's local properties with it, and
`spark.job.tags` is one of them. So the tag that does not survive as a ThreadLocal *does* survive
inside the event, which is enough to recover the Connect coordinates and look the identity up.
`PropagationFacetFactory` does exactly that, and every job-backed run in Marquez now carries:

```json
"sparkConnectPropagation": {
  "correlationId": "22b19898-e641-4a08-8cb6-a8a9612e80c4",
  "principal": "alice",
  "subject": "9682af88-2cba-4543-9e4a-c2a37cbe78af",
  "sessionId": "56106cb6-a00e-4d11-9ef5-d38d81bd6468",
  "operationId": "aac54395-95f3-455d-845a-fa8d3c7ee36d"
}
```

Which is worth noticing: the *identity* crosses to the listener thread perfectly well, because
Spark hands it over in the event. It is the *catalog call* that cannot, because that needs a live
credential rather than a name. Only runs backed by a job start get the facet — a DDL statement the
catalog satisfies without scheduling anything has no job start behind it, and so no correlation ID.

**One trap in the SPI.** OpenLineage dispatches custom facet builders on the builder's type
parameter, so `CustomFacetBuilder<SparkListenerJobStart, RunFacet>` should be exactly right. It is
never called — no error, no warning, just silence. Declaring `CustomFacetBuilder<Object, RunFacet>`
and testing the type by hand receives precisely the events the generic was supposed to select:
across one `make test`, 8 `SparkListenerJobStart`, 16 `SparkListenerSQLExecutionEnd`, 13
`SparkListenerSQLExecutionStart` and a scattering of RDDs.

The dataset conflict is structural, not incidental. A catalog that authorises per user can only be
read by something that *is* a user; a lineage collector that runs beside the query, on its own
thread, deliberately is not one. Three ways out, none free:

* Give the collector a service identity for catalog reads. Cheapest, and it puts an ambient
  credential back into Spark — the one thing this PoC exists to show you do not need.
* Resolve datasets from the plan without a catalog round trip. Correct but upstream work, and it
  gives up whatever only the catalog knows.
* Carry the identity to the listener thread too, keyed by the run. That is §5's per-operation
  keying again, one thread further out, and it is the only option that keeps both properties.

Worth knowing before designing lineage into a system with per-user catalog authorisation: the two
features are not independent, and nothing in either project's documentation says so.

It is also loud. Each failed resolution is a 401 from Polaris, which Iceberg's
`org.apache.iceberg.rest.ErrorHandlers` logs with a full stack trace — around 76 of them per
`make test`, in a log whose selling point is that you can grep it. Turning the `io.openlineage`
loggers off does *not* fix that, which is worth knowing before trying: the traces are Iceberg's,
not OpenLineage's, and the only logger that would silence them is the one that also reports
genuine authorisation failures like bob's 403. Left noisy on purpose; the alternative hides our
own errors to tidy up after someone else's.

**Also, and separately: OpenLineage below 1.53.0 does not run on Spark 4.1.3 at all.** 1.34.0
registers, then throws on the first event and takes the SparkContext with it:

```
ERROR Utils: uncaught error in thread spark-listener-group-shared, stopping SparkContext
java.lang.NoSuchMethodError: 'org.apache.spark.sql.SparkSession
                              org.apache.spark.sql.execution.QueryExecution.sparkSession()'
```

Spark 4.1 moved `QueryExecution` behind the `classic` module and changed that return type, so a
listener compiled against 4.0 fails at link time — and a listener that throws does not merely stop
collecting lineage, it stops the query engine. The newest release module is still `spark40`; 1.53.0
works on 4.1.3 regardless, but pin it deliberately rather than by luck.

## 16. A SPARQL endpoint in front of this stack: what had to give

Putting [Ontop VKG](https://ontop-vkg.org) in front of the same tables meant making a SPARQL
caller's identity survive one more hop. Nine things turned up, in three groups: what Ontop assumes,
what Spark's own JDBC driver does and does not do, and one thing Polaris cannot express.

Versions: Ontop 5.6.0-SNAPSHOT (fork of `version5`), `spark-connect-client-jdbc_2.13` 4.1.3.

### Ontop

**Its connection pool is process-wide and its credentials are static.** `QueryContext` has carried
the request's HTTP headers, user, roles and groups since authorization was added, so Ontop knows
perfectly well who asked. The JDBC connection, though, comes from a pool built once from
`jdbc.url` / `jdbc.user` / `jdbc.password`, and the connection is taken *before* the query is even
prepared (`OntopVirtualRepository.getConnection()`, then `prepareQuery(..., httpHeaders)`). So the
database only ever sees one account. Behind a private back end that is the right design — Ontop is
then the only place access is decided. In front of a catalog that authorises end users itself it
throws that catalog's access control away.

The fork adds `getConnection(QueryContext)` down the chain, a `ContextPropagatingJDBCConnectionPool`
that builds each request's JDBC URL from the caller's context, and `ontop.queryIdHttpHeader` so
Ontop's own query id *is* the caller's correlation ID. All three are opt-in defaults; the shared
pools are untouched. See [docker/ontop/README.md](docker/ontop/README.md).

**It picks the SQL dialect by JDBC driver class name.** `sql-default.properties` maps
`org.apache.hive.jdbc.HiveDriver`, `com.simba.spark.jdbc.Driver` and
`com.databricks.client.jdbc.Driver` to the `SparkSQL*` factories. Spark's own
`SparkConnectDriver` — the one Spark itself ships — was not there, so Ontop fell through to the
generic dialect and generated SQL Spark does not speak, against a Spark it was otherwise talking
to perfectly well. Same dialect, different transport; four lines of properties.

### The Spark Connect JDBC driver

**Unknown connection-string parameters become gRPC metadata.** This is the seam the whole
integration hangs on. `SparkConnectClient.Builder.parseURIParams` recognises `user_id`, `use_ssl`,
`token`, `user_agent`, `session_id` and `grpc_max_message_size`; *everything else* is passed to
`option(k, v)`, which ends up in `Configuration.metadata` and is stamped onto every call by
`MetadataHeaderClientInterceptor`. So

```
jdbc:sc://spark-connect:15002/;user_id=<sub>;x-user-token=<jwt>;x-correlation-id=<uuid>
```

puts the user's token and the correlation ID exactly where `UserTokenServerInterceptor` reads them.
No fork of Spark, no custom driver.

**`token` is not the way to set the Authorization header, despite appearances.**
`Configuration.credentials()` reads: if SSL is not explicitly enabled *and* a token is defined
*and* the host is not local, use `TlsChannelCredentials`. And the header is only added to the
metadata map when the host *is* local. So against a remote plaintext server, `token=` silently
switches the channel to TLS and drops the header — the failure looks like `UNAVAILABLE: io
exception` with an `SslHandler` in the channel pipeline, which says nothing about tokens. Spelling
it out as ordinary metadata works, with the space percent-encoded because the connection string is
parsed as a `java.net.URI` (whose `getPath()` decodes it again):

```
;authorization=Bearer%20<shared-secret>
```

**Arrow needs `--add-opens` on Java 17.** The client decodes results as Arrow record batches, and
Arrow reaches into `java.nio`'s direct-buffer internals. Without
`--add-opens=java.base/java.nio=ALL-UNNAMED` the first query dies in a static initialiser
(`ArrowBuf.getDirectBuffer` → `ExceptionInInitializerError`) with no hint that a JVM flag is
missing. `bin/spark-submit` sets these for Spark's own processes; a third-party JDBC client of
Spark has to set them itself.

**It implements the JDBC surface strictly, and refuses the rest.** Where the Hive driver returns
empty result sets, this one throws:

| Call | Result |
|---|---|
| `getTables(..., {"TABLE","VIEW","SYSTEM"})` | `SQLException: The requested table types contains unsupported items: SYSTEM` |
| `getPrimaryKeys`, `getIndexInfo`, `getImportedKeys` | `SQLFeatureNotSupportedException` |
| `createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY)` | `SQLFeatureNotSupportedException` — only the no-argument form exists |
| `setFetchSize` | `SQLFeatureNotSupportedException` |

None of these change a row: Spark has no SYSTEM relations and no keys, and the last two ask for the
JDBC defaults and a buffering hint. But each surfaced as a bare
`OntopConnectionException: java.sql.SQLFeatureNotSupportedException`, naming neither the call nor
the driver. The fork attempts each and falls back.

### This PoC's own plugin

**AnalyzePlan never reaches an ExecutionThread, so §4's job tag does not exist.** Spark answers it
synchronously on the gRPC handler thread — no `withSession`, no tag. The Python, JVM and Rust
clients never exposed this because they only ever ran statements; a JDBC driver asks for a schema
first, on every `DatabaseMetaData` call. The catalog request then went out unauthenticated and
Polaris answered 401, with the same empty `[,]` MDC as in §15.

`PropagatedIdentityHolder.bindToCurrentThread` closes it: the interceptor binds the identity to the
handler thread for the duration of `onHalfClose`, and `currentIdentity()` falls back to it when
there is no job tag. The binding is deliberately *not* inheritable — a thread Spark spawns must
find its identity through the tag like everything else, or an identity could outlive its call.

### Polaris

**There is no "read the schema but not the data".** The obvious design for a SPARQL endpoint that
introspects at start-up is to give it a principal with `TABLE_READ_PROPERTIES` and no
`TABLE_READ_DATA`. It does not work, and not because of a missing privilege:
`spark.sql.catalog.polaris.header.X-Iceberg-Access-Delegation` is `vended-credentials` for the
whole server, so *every* `loadTable` asks Polaris to vend storage credentials, and Polaris
authorises that as `LOAD_TABLE_WITH_READ_DELEGATION` — which needs `TABLE_READ_DATA`. A principal
that may only look at column names cannot get past `loadTable` to see them.

Listing is separately awkward: `getTables(null, null, ...)` needs `NAMESPACE_LIST` **at the
catalog**, not on each namespace, and `LIST_VIEWS` is authorised apart from `LIST_TABLES`, so a
grant set that looks complete fails twice in a row on operations the caller never asked for by
name.

The endpoint here therefore pins its table definitions in a committed `db-metadata.json` and
introspects only when a human runs `make ontop-metadata`. Which is the better answer anyway: it
holds no credential at all.
