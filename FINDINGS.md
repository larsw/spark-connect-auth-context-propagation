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

## 5. `operation_id` is never populated by PySpark

All nine call sites in `core.py` invoke `self._execute_plan_request_with_metadata()` with no
argument, and `operation_id` is a parameter of that private method. The server generates the id.

An interceptor therefore cannot key anything per operation — only per session. For a token that
is fine, since a token is per user. For a correlation ID it means two queries running
concurrently *in the same session* under different IDs can observe each other's.

A client could set `operation_id` to the correlation ID by patching that private method (the
client validates it as a UUID4), which would make Spark carry the correlation ID as its own
operation id — visible in the job tag, in `ExecuteHolder` and in the Spark UI. Left undone here.

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

## 12. The Polaris console lives in a different repository

There is no UI anywhere in `apache/polaris` — the `main` tree has no `package.json`, no
`.tsx/.jsx/.vue/.svelte` files and no `ui` or `console` directory, and the running server serves
no Swagger UI, OpenAPI document or Quarkus Dev UI (all 404 in prod mode). The console is in
**`apache/polaris-tools`**, under `console/`: a Vite + React + TypeScript app.

Two details make it easy to integrate:

* Configuration is injected at container start. `index.html` loads `/config.js`, and
  `src/lib/config.ts` prefers `window.APP_CONFIG` over the values Vite inlined at build time, so
  one image can be pointed at different Polaris and identity-provider URLs without rebuilding.
* It supports OIDC authorization code with PKCE (S256), as well as Polaris's internal
  client-credentials path and even token exchange.

It publishes no release tags — the only tags in that repository belong to the catalog migrator —
so a build must pin a commit.

Two things are required to make it talk to Polaris and are silent failures otherwise:

* **CORS.** The console is a browser app calling Polaris from its own origin. Quarkus defaults
  `quarkus.http.cors.enabled` to `false`, so without opening it every API call dies in preflight.
* **Claims.** Its OIDC client needs the same audience and principal-claim mappers as any other
  Polaris client, or Polaris cannot map the token to a principal.

## 13. Smaller things that cost time

| Symptom | Cause |
|---|---|
| Healthcheck fails with `cannot create /dev/tcp/...` | `CMD-SHELL` runs `/bin/sh`; the Spark image's is dash, which has no `/dev/tcp`. Use `["CMD","bash","-c",...]` |
| Spark master healthcheck refused on `localhost:7077` | The master binds to whatever `--host` resolves to, not loopback |
| Master advertises the driver's hostname for its own UI | `spark.driver.host` in a shared `spark-defaults.conf` applies to master and worker too |
| Tables vanish but Polaris still lists them | MinIO container recreated without a named volume |
| A query produces no Polaris request at all | Iceberg caches loaded tables; disable `cache-enabled` if propagation must be observable |
| A container is unhealthy while the service plainly works | busybox `wget` resolves `localhost` to `::1` first; an nginx listening only on IPv4 then refuses the connection. Use `127.0.0.1`, or `curl`, which falls back |
| Every RPC fails with "No authentication token provided" | That is Spark's *channel-level* pre-shared key (`PreSharedKeyAuthenticationInterceptor`), not the user's token. The message names the wrong credential, so it sends you hunting in the OIDC path |
| One JWKS failure after restarting Keycloak | Recreating Keycloak rotates its signing keys; validators recover on refresh |
| Keycloak answers 400 to a login form POST | The flow started on one hostname and the form action is on another, so the session cookie is not sent. Keycloak renders the form action using its configured frontend hostname |
| `docker compose config` shows `$$1` and you cannot tell if it is right | It re-escapes on output. Only running a container shows the real value (`PRINCIPAL_ROLE:$1`) |
