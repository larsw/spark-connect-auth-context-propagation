# Spark Connect token + correlation-ID propagation PoC

**Goal:** thread a per-user auth token and a context correlation identifier through Apache
Spark Connect (PySpark client → Java server extensions), have the server exchange the token
for a downstream credential, and prove it reaches an Apache Polaris catalog and MinIO
object storage with per-user authorisation.

**Status:** design locked 2026-09-18. **Complete.** All milestones done, all four open risks resolved. 17 end-to-end checks pass against the live stack, plus 41 unit tests (15 Java, 26 Python) that need nothing running.

Legend: `[ ]` todo · `[~]` in progress · `[x]` done

---

## 1. Locked decisions

Each was chosen deliberately; the rationale matters as much as the choice. Do not re-open
without reading §2 first — several were forced by verified upstream facts.

| # | Decision | Rationale |
|---|---|---|
| 1 | **Spark 4.1.3**, not 4.2.0 | No released Iceberg runtime for Spark 4.2 exists (§2.1) |
| 2 | Server performs a **token exchange**, not pass-through | PoC must prove the server acts on behalf of the user |
| 3 | Exchange = **RFC 8693 standard token exchange at Keycloak** | Server never holds user secrets; outbound token provably ≠ inbound |
| 4 | Wire = **custom ChannelBuilder + client interceptor**, per-RPC headers | Dynamic/refreshable; no PySpark fork; avoids leaking token into Spark UI (§2.8) |
| 5 | Thread bridge = **job-tag lookup, keyed by operation then session** | Only mechanism that survives the gRPC-thread → ExecutionThread boundary without forking Spark (§2.5). Per-*operation* keying added 2026-09-19, once the client began supplying `operation_id` — see §2.15 |
| 6 | Correlation ID = **client-minted opaque UUID** → `X-Request-ID` → Polaris | Polaris already ingests this header into its MDC and audit events (§2.7) |
| 7 | Demo = **two users, divergent grants, run concurrently** | Sequential tests would pass even if per-operation keying were broken |
| 8 | Token acquisition = **pluggable provider**; device flow default, password grant for tests | Device flow is the real CLI UX; tests must stay headless |
| 9 | Catalog defined **server-side**, with **vended credentials** | A client that can redefine `spark.sql.catalog.*` makes the auth story decorative |
| 10 | JWT rides **`x-user-token`**; `Authorization` keeps Spark's **pre-shared key (enabled)** | Spark's built-in interceptor owns `Authorization` (§2.4); demonstrates both layers coexisting |
| 11 | Validate every RPC; **cache exchange by SHA-256 of inbound JWT**, TTL = `exp − 30s` | Mid-session client refresh then works with zero session state |
| 12 | **Single Maven module** + uv Python package + Makefile; **Java 21** | Jar must land in `$SPARK_HOME/jars` so interceptor and AuthManager share one classloader |
| 13 | Custom Spark image, **jars baked at build time** | `--packages`/`--jars` land in a child classloader and would split the static holder in two |
| 14 | **`/etc/hosts` aliases**, applied by an `install.sh` that prompts | One hostname everywhere ⇒ one `iss` ⇒ OIDC validation works for browser *and* containers |
| 15 | One catalog, two namespaces, graded grants; **alice seeds tables via Spark** | Exercises the write path and vended *write* credentials for free |
| 16 | **Fail fast at the interceptor**, precise gRPC codes, correlation ID in every error | Expired tokens fail cleanly at the edge, not mid-query |
| 17 | Custom `log4j2.properties` with `%X`, MDC at both hops, **plus a Spark UI job tag** | One `grep <uuid>` across three services; visible in the UI without reading logs |
| 18 | **pytest suite behind Makefile targets** | Log-grep assertions are what actually prove end-to-end threading |
| 19 | **Enforce subject↔session binding** at the interceptor | Spark Connect has no such binding; without it, propagating identity is moot (§2.9) |
| 20 | Correlation ID defaults to **session-scoped**, narrowed by `with correlation_id(...)` | One block spans the AnalyzePlan + ExecutePlan of a single user action |
| 21 | **Standalone master + worker** (overrides an earlier `local[*]` choice) | `local[*]` executors are threads; only real executor JVMs prove the data-plane claim |
| 22 | Executor proof = **no static S3 credentials anywhere in Spark** + MinIO access-log assertions | Success becomes structural proof; audit log gives direct evidence |
| 23 | Deliverables = **README.md + FINDINGS.md + a published page** | Findings have value independent of the code |

### Component versions

| Component | Version / image |
|---|---|
| Spark | `apache/spark:4.1.3-scala2.13-java21-python3-ubuntu` |
| Iceberg | `iceberg-spark-runtime-4.1_2.13:1.11.0` + `iceberg-aws-bundle:1.11.0` |
| Polaris | `apache/polaris:1.7.0` (+ `apache/polaris-admin-tool:1.7.0`) |
| Keycloak | `quay.io/keycloak/keycloak:26.7.0` |
| MinIO | `quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z` (+ `quay.io/minio/mc`) |
| Python client | `pyspark-client==4.1.3` (1.6 MB, pure Python) |
| JVM | Java 21, `maven.compiler.release=21` |
| JWT lib | `nimbus-jose-jwt` (Spark ships jjwt 0.12.6, so no clash) |

### Explicit non-goals

TLS between client and Connect server · token expiry *during* a single long-running query ·
Polaris persistence (in-memory; re-bootstrap on restart) · secrets management ·
upstreaming a patch to Spark.

---

## 2. Verified upstream facts

All verified 2026-09-18 by reading source/jars, not from memory. **Re-verify before
contradicting.**

### 2.1 No released Iceberg runtime for Spark 4.2
`iceberg-spark-runtime-4.2_2.13` returns 404 on Maven Central. Only `1.12.0-SNAPSHOT` and
`1.13.0-SNAPSHOT` exist on repository.apache.org. Iceberg 1.11.0 tops out at
`iceberg-spark-runtime-4.1_2.13`. This is the single fact that forced Spark 4.1.3.

### 2.2 Spark Connect relocates gRPC — the config docstring is stale
`spark-connect_2.13-4.1.3.jar` contains **0** `io/grpc/` entries and **1610**
`org/sparkproject/connect/grpc/` entries.
- Our interceptor MUST implement `org.sparkproject.connect.grpc.ServerInterceptor` and use
  the relocated `Metadata` / `ServerCall` / `ServerCallHandler`.
- `Connect.scala:54` documents `spark.connect.grpc.interceptor.classes` as requiring
  `io.grpc.ServerInterceptor`. **That docstring is wrong.**
- `com.google.protobuf` → `org.sparkproject.connect.protobuf`, but
  `org.apache.spark.connect.proto.*` is NOT relocated.
- Compile against `org.apache.spark:spark-connect_2.13:4.1.3` (`provided`) — the published
  artifact is already the shaded one.

### 2.3 Interceptors must have a no-arg constructor
`SparkConnectInterceptorRegistry.createInstance` looks for `getConstructors.find(_.getParameterCount == 0)`
and throws `CONNECT.INTERCEPTOR_CTOR_MISSING` otherwise. Config must be read from
`SparkEnv.get.conf` inside the constructor.

### 2.4 Spark's built-in auth owns the `Authorization` header
`PreSharedKeyAuthenticationInterceptor` does `Metadata.Key.of("Authorization", ASCII)` and
requires exactly `Bearer $token`. Config `spark.connect.authenticate.token`
(`Connect.scala:343`), env `SPARK_CONNECT_AUTHENTICATE_TOKEN`. A per-user JWT there collides.

### 2.5 The thread boundary and the job-tag bridge
- `ExecuteThreadRunner` runs each operation on a **fresh, deliberately unpooled**
  `ExecutionThread` (`ETR.scala:51`, `:332`) — gRPC `Context`/ThreadLocal do not reach it.
- Inside `sessionHolder.withSession`, `ETR.scala:201` calls
  `sparkContext.addJobTag(executeHolder.jobTag)`.
- Format (`ExecuteHolder.scala`, `object ExecuteJobTag`):
  `SparkConnect_OperationTag_User_<userId>_Session_<sessionId>_Operation_<operationId>`
- `SparkContext.getJobTags()` is public API (since 3.5). Job tags live in thread-local
  `localProperties`, so reading them on the ExecutionThread recovers the exact ExecuteKey.
- `AnalyzePlan` adds no job tag → session-level fallback required.

### 2.6 Per-session catalog isolation is real
`SparkConnectSessionManager.newIsolatedSession()` → `SparkSession.newSession()` (`:358-364`),
which builds a fresh `SessionState` → fresh `CatalogManager` → each Connect session gets its
own `RESTCatalog` and its own `AuthManager`. No cross-tenant contamination.

### 2.7 Iceberg + Polaris hooks
- `AuthProperties.AUTH_TYPE = "rest.auth.type"` accepts a fully-qualified custom class.
- `AuthSession.authenticate(HTTPRequest) -> HTTPRequest` runs **per outgoing REST call on
  the calling thread** — our injection point for both `Authorization` and `X-Request-ID`.
- These classes are NOT relocated in `iceberg-spark-runtime-4.1_2.13:1.11.0`.
- Polaris: `polaris.log.request-id-header-name=X-Request-ID`, log pattern includes
  `%X{requestId}`, and audit events carry `request_id`.
- Polaris external IdP config: `polaris.authentication."<realm>".type=external`,
  `quarkus.oidc.auth-server-url`, `quarkus.oidc.client-id`,
  `quarkus.oidc.roles.role-claim-path`, `polaris.oidc.principal-mapper.id-claim-path` /
  `.name-claim-path`, `polaris.oidc.principal-roles-mapper.mappings[0].regex|.replacement`.
- MinIO storage: `AwsStorageConfigInfo` with `storageType:S3`, `endpoint`,
  `endpointInternal`, `pathStyleAccess:true`, `region`; optional `--sts-endpoint`.
  Upstream `PolarisRestCatalogMinIOIT` runs with
  `header.X-Iceberg-Access-Delegation: vended-credentials` against MinIO — vending works.

### 2.8 UserContext extensions exist, but would leak the token
`SparkConnectClient.add_threadlocal_user_context_extension()` and
`add_global_user_context_extension()` exist in PySpark 4.1.3 (`core.py:1928`, `:1935`) — no
fork needed. **But** `ETR.scala:209` stringifies the whole request proto into the job
description and `callSite`, so a JWT in the request body would surface in the Spark UI.
Headers avoid this. This is why decision #4 is correct.

### 2.9 Spark Connect has no subject↔session binding (security finding)
- `user_id` comes from `sc://…;user_id=X` or falls back to `$SPARK_USER`/`$USER`
  (`core.py:711-715`) — fully client-controlled.
- `session_id` is a client-generated UUID or supplied in the connection string (`:705-708`).
- `req.user_context.user_id = self._user_id` (`:1193`); server keys
  `SessionKey(userId, sessionId)` off these.
- ⇒ any client passing the pre-shared key can claim another user's `(user_id, session_id)`
  and attach to their live `SessionHolder` — temp views, cached DataFrames, and our
  session-level credential entry. The pre-shared key cannot distinguish users.
- Decision #19 closes this.

### 2.10 Keycloak 26.7 specifics
- Standard token exchange **V2 is enabled by default**; no `--features` flag.
- The requester client must be **confidential** with the *Standard token exchange* switch on.
- The `audience` parameter **only filters, never adds** → audience protocol mappers are
  required: `spark-cli` must add `spark-connect`, `spark-connect` must add `polaris`.
- Device grant: client attribute `oauth2.device.authorization.grant.enabled=true`
  (`OAuth2DeviceConfig.java:41`); defaults 600s device-code lifespan, 5s poll interval;
  endpoint `…/protocol/openid-connect/auth/device`; returns a refresh token.

### 2.11 Spark runtime environment
- Spark 4.1.3 supports **Java 17/21 only** (`docs/index.md:37`); build targets release 17.
  No `apache/spark:4.1.3-…java25…` image exists. JDK 25 is not viable.
- Ships `jjwt 0.12.6` (not nimbus) and `hadoop-aws 3.4.2` but **no AWS SDK** →
  `iceberg-aws-bundle` supplies it with nothing to clash against.
- `spark.log.structuredLogging.enabled` defaults to **false**; the stock log4j2 pattern
  (`%d %p %c{1}: %m%n%ex`) contains no `%X`, so MDC is invisible without our own config.
- Connect entrypoint class: `org.apache.spark.sql.connect.service.SparkConnectServer`.
  `sbin/start-connect-server.sh` daemonises via `spark-daemon.sh` — wrong for a container;
  `exec spark-submit` in the foreground instead.
- Docker Hub `minio/minio` is **404**; images moved to `quay.io/minio/minio`.

### 2.12 Polaris principals must pre-exist, but resolve by name
`DefaultAuthenticator` states plainly: *"it does not support federated principals that are
not managed by Polaris"*. `resolvePrincipalEntity` prefers `findPrincipalById` when a
principal-id claim is present and `> 0`, and otherwise falls back to `findPrincipalByName`.

⇒ Configure **only** `polaris.oidc.principal-mapper.name-claim-path` (deliberately **no**
`id-claim-path`), create the principals by name during bootstrap, and Keycloak never has to
learn Polaris's numeric principal IDs. That removes the apparent bootstrap circularity.

Principal roles come from the token (`quarkus.oidc.roles.role-claim-path=principal_roles`,
mapped `(.+)` → `PRINCIPAL_ROLE:$1`), but are only *activated* if Polaris has actually
granted them — so Keycloak realm role names must match Polaris principal role names exactly.

### 2.13 Compose `$` escaping is real
Verified empirically: `"PRINCIPAL_ROLE:$$1"` in `compose.yaml` arrives in the container as
`PRINCIPAL_ROLE:$1`. `docker compose config` re-escapes on output, so its display cannot be
used to confirm this — only running a container can.

### 2.14 The whole server-side chain is verified (2026-09-18, no Java involved)
Stood up keycloak + minio + polaris and drove it with curl from inside the compose network.

**Token exchange.** Subject token from `spark-cli` for alice carries `aud=spark-connect` and
NOT the Polaris claims. The RFC 8693 exchange as `spark-connect` returns a *different* token
with `aud=polaris`, `azp=spark-connect`, `principal_name=alice`,
`principal_roles=["data_engineer"]`. The Polaris claims appear only on the exchanged token,
because those mappers live on `spark-connect`'s client scopes — the separation we wanted.

**Polaris authorisation**, using the exchanged token:

| probe | alice | bob |
|---|---|---|
| `GET /config` | 200 | 200 |
| `GET namespaces` (list all) | 200 | 403 |
| `namespaces/shared` | 200 | 200 |
| `namespaces/restricted` | 200 | **403** |
| `tables in shared` | 200 | 200 |
| `tables in restricted` | 200 | **403** |

Denial message: *"Principal 'bob' with activated PrincipalRoles '[analyst]' and activated
grants via '[shared_reader, analyst]' is not authorized..."* — confirming claim → principal
→ role → grant resolution all worked.

**Correlation ID.** Sending `X-Request-ID: cid-probe-bob` puts `[cid-probe-bob,POLARIS]` into
Polaris's MDC on *every* line for that request — access log, `PolarisAuthorizerImpl`
denial, and `IcebergExceptionMapper`. Decision #6 is proven at the Polaris end.

⚠️ **Watch during milestone D:** bob gets 403 on *listing all namespaces*, because his
`NAMESPACE_LIST` grant is scoped to `shared` rather than the catalog. Fully-qualified reads
(`polaris.shared.events`) are unaffected, but `SHOW NAMESPACES IN polaris` will fail for bob.
Decide then whether that is desirable demo behaviour or an extra catalog-level grant.

### 2.15 operation_id is not available to the interceptor (design correction, since resolved)
All nine call sites in `core.py` invoke `self._execute_plan_request_with_metadata()` with no
argument, and `operation_id` is a parameter of that private method with no public seam. So
PySpark never populates `ExecutePlanRequest.operation_id`; the server generates it.

⇒ The gRPC interceptor could not know the operation id, so the holder was keyed by
`(userId, sessionId)`, not by `ExecuteKey`. The execution thread parsed the job tag, but only
the `User_`/`Session_` segments were used for lookup.

**Consequence, stated honestly:** decision #5's original claim that job-tag keying "makes
concurrent queries in one session correct" did not hold for the *correlation ID*. Two
queries running concurrently **in the same Connect session** under *different*
`with correlation_id(...)` blocks could observe each other's ID. The token was unaffected
(it is per-user/per-session by nature), and concurrency across *different* sessions — which
is what the two-user demo exercises — was always correct.

**Resolved 2026-09-19.** `connect()` now patches `_execute_plan_request_with_metadata` on the
client *instance* (not the class) to mint a UUID4 per request, `per_operation_ids=False` opts
out, and the holder keys `(userId, sessionId, operationId)` with the session entry as fallback.
The operation map is a bounded access-ordered LRU (2048): ReleaseExecute is exempt from
authentication here, so there is no reliable end-of-operation hook, and an evicted entry simply
degrades to the old session lookup.

Two things the original plan got wrong, both verified against 4.1.3:

* **`operation_id` cannot be the correlation ID.** `spark.sql(x).collect()` issues *two*
  ExecutePlan requests, and a `correlation_id()` block spans several statements. Reusing an id
  fails on the first statement with `INVALID_HANDLE.OPERATION_ALREADY_EXISTS`, or
  `OPERATION_ABANDONED` once the first holder has been reaped.
* **The cross-talk would not reproduce.** Operations in one session do overlap, but each
  refreshes the session entry immediately before its own catalog call and so nearly always wins
  the race. The keying is pinned down in `PropagatedIdentityHolderTest.PerOperationKeying`
  instead; the end-to-end test asserts that Spark adopts the client's id (`opId=<uuid>` in its
  logs), which does fail without the patch.

### 2.16 The Java plugin's contracts, confirmed by bytecode
`mvn package` produces `spark-connect-propagation-0.1.0.jar` (904 KB). `javap` confirms:

* `UserTokenServerInterceptor implements org.sparkproject.connect.grpc.ServerInterceptor`
  — the *shaded* interface, as §2.2 requires — with a `public UserTokenServerInterceptor()`
  zero-arg constructor, as §2.3 requires.
* `public PropagatingRestAuthManager(java.lang.String)` — matches the
  `DynConstructors...impl(impl, String.class)` lookup in `AuthManagers.loadAuthManager`.
* nimbus-jose-jwt: 0 unrelocated `com/nimbusds/` entries, 612 relocated under
  `io/sparkconnect/propagation/shaded/nimbusds/`. Nothing can perturb the Spark JVM.

Compiling against `org.apache.spark:spark-connect_2.13:4.1.3` (scope `provided`) supplies both
the relocated gRPC classes and the unrelocated `org.apache.spark.connect.proto.*` requests, so
no build-time shading gymnastics were needed on our side.

### 2.17 End-to-end result (verified 2026-09-18)
`13 passed` in `tests/test_propagation.py`, run from the compose `client` service. A further 26
client unit tests and 15 Java unit tests run with no stack at all:

| Claim | Evidence |
|---|---|
| Token crosses gRPC thread → ExecutionThread | `SHOW NAMESPACES IN polaris` → `['shared','restricted']` |
| Identity is the *user's* at the catalog | alice reads `restricted`; bob gets `ForbiddenException: Principal 'bob' ... not authorized for op LOAD_TABLE_WITH_READ_DELEGATION` |
| No cross-session leakage under concurrency | 3 interleaved alice/bob pairs; alice always 2 rows, bob always denied |
| Write path works | alice CREATEs and INSERTs into both namespaces through Spark Connect |
| Correlation ID spans services | one UUID in `[cid,alice] UserTokenServerInterceptor` and `[cid,POLARIS] access-log` |
| Bad tokens rejected at the edge | missing / malformed / downstream-audience → `UNAUTHENTICATED`, correlation ID in the message |
| Session spoofing blocked | bob claiming alice's `user_id`+`session_id` → `PERMISSION_DENIED` |

The interceptor loaded first try: `UserTokenServerInterceptor: propagation interceptor active`,
confirming §2.2 and §2.3 in practice rather than only in bytecode.

### 2.18 Credential vending is genuinely per-identity and sub-scoped
Polaris hands alice and bob *completely different* temporary credentials — different
`s3.access-key-id`, `s3.secret-access-key`, `s3.session-token` and expiry — and neither is
`minio_root`.

MinIO's audit log (captured via `MINIO_AUDIT_WEBHOOK_*` into a small collector) shows what the
executor actually presented:

```json
"requestClaims": {"accessKey": "TV4GL2GBHNDWRKP77OH3", "exp": 1789771028,
                  "parent": "minio_root", "sessionPolicy": "<base64>"}
```

The decoded session policy is narrowed per table:

```json
{"Effect":"Allow","Action":["s3:GetObject","s3:GetObjectVersion"],
 "Resource":["arn:aws:s3:::warehouse/poc/shared/events/metadata/*"]}
```

Combined with the structural fact that no Spark container holds any S3 credential (verified: no
`AWS_*` env vars, none in `spark-defaults.conf`), a successful read from the worker JVM can only
have happened with credentials vended for the authenticated user.

### 2.19 Operational gotchas found while building
* **Realm import replaces built-in client scopes.** Declaring a top-level `clientScopes` array
  removed Keycloak's `basic`/`profile` scopes, silently stripping `sub` and `preferred_username`
  from every token — the server then rejected everything with "JWT missing required claims:
  [sub]". Fix: put protocol mappers on the clients and let Keycloak create its standard scopes.
* **`CMD-SHELL` healthchecks use `/bin/sh`**, which in the Spark image is dash and has no
  `/dev/tcp`. Use `["CMD","bash","-c",...]`.
* **The Spark master binds to whatever `--host` resolves to**, not loopback, so its healthcheck
  must target `spark-master:7077`.
* **`spark.driver.host` in the shared `spark-defaults.conf`** also applies to the master and
  worker, making the master advertise the driver's hostname for its own UI. Set it per role.
* **MinIO with no volume** loses every table's data on container recreation while Polaris keeps
  serving metadata pointing at the missing files. Named volume added.
* **Iceberg caches loaded tables**, so a repeated query can produce no Polaris request at all.
  Disabled in this PoC so propagation stays observable.

---

## 3. Architecture — the path a query takes

1. `demo.py` → **device flow** at Keycloak → browser login → token `aud=spark-connect`,
   cached under `~/.cache/spark-connect-poc/<user>.json`. `TokenProvider` is pluggable;
   pytest substitutes password grant to stay headless.
2. `PropagatingChannelBuilder(DefaultChannelBuilder)` + a client interceptor implementing
   **both** `UnaryUnaryClientInterceptor` and `UnaryStreamClientInterceptor` stamps every
   RPC with `authorization: Bearer <pre-shared>`, `x-user-token: <JWT>`,
   `x-correlation-id: <uuid>`.
3. Spark's `PreSharedKeyAuthenticationInterceptor` clears the channel. Ours then:
   validates the JWT (nimbus, cached JWKS); **enforces `user_context.user_id == subject` and
   pins `sessionId → subject`**; performs the RFC 8693 exchange for `aud=polaris`
   (cached by SHA-256 of inbound JWT, TTL `exp − 30s`); stores
   `{polarisToken, correlationId, subject}` in a static holder keyed by `ExecuteKey` and by
   `SessionKey`; sets MDC.
4. `ExecuteThreadRunner` runs the plan on a fresh `ExecutionThread`, having applied the
   `SparkConnect_OperationTag_…` job tag.
5. The per-session `RESTCatalog` uses `rest.auth.type=<PropagatingRestAuthManager>`;
   `authenticate(HTTPRequest)` reads `getJobTags()`, recovers the key, looks up the holder,
   and stamps `Authorization: Bearer <exchanged>` + `X-Request-ID: <cid>`.
6. Polaris (realm `external`) validates the exchanged token, maps claims → principal/roles,
   authorises, and logs `%X{requestId}` = our correlation ID.
7. Polaris vends STS-scoped MinIO credentials (`X-Iceberg-Access-Delegation: vended-credentials`).
8. Credentials serialise into the task; the **standalone worker** reads from MinIO.
   Spark holds **no static S3 credentials anywhere** — only Polaris has root.

---

## 4. Repo layout

```
install.sh              # detect → show → confirm; never silent sudo
Makefile                # up / bootstrap / demo / test / logs / down
compose.yaml            # keycloak, polaris, minio, spark-master, spark-worker, spark-connect
docker/spark/           # Dockerfile, spark-defaults.conf, log4j2.properties, entrypoint.sh
docker/keycloak/spark-realm.json
docker/polaris/         # bootstrap scripts (catalog, namespaces, roles, grants)
server/                 # single Maven module → one jar into $SPARK_HOME/jars
  UserTokenServerInterceptor.java    # shaded-gRPC world
  PropagatedIdentityHolder.java      # the static bridge
  TokenExchangeService.java          # nimbus + java.net.http
  PropagatingRestAuthManager.java    # Iceberg world
client/                 # uv package: channel.py, auth.py, context.py
demo/demo.py
tests/
README.md  FINDINGS.md
```

---

## 5. Task list

### Milestone A — infrastructure stands up
- [x] A1 repo skeleton + `.gitignore`
- [x] A2 `install.sh`: detect docker/compose/JDK21/maven/uv + `/etc/hosts` aliases
      (`127.0.0.1 keycloak polaris minio spark-connect spark-master`); print each privileged
      command and require explicit y/N; idempotent; offer "print only" mode
- [x] A3 `docker/keycloak/spark-realm.json`: realm `spark`; users alice/bob; clients
      `spark-cli` (public, device grant + password grant, audience mapper → `spark-connect`),
      `spark-connect` (confidential, standard token exchange on, audience mapper → `polaris`),
      `polaris`; claim mappers `principal_id`, `principal_name`, `principal_roles`
- [x] A4 `compose.yaml`: keycloak, minio, mc bucket setup, polaris (realm `external` → Keycloak),
      spark-master, spark-worker, spark-connect; healthchecks + `depends_on` conditions
- [x] A5 `docker/spark/Dockerfile`: FROM apache/spark:4.1.3-scala2.13-java21-python3-ubuntu;
      `mvn dependency:copy` Iceberg runtime + aws-bundle + our jar into `$SPARK_HOME/jars`
- [x] A6 `docker/spark/spark-defaults.conf`: interceptor class, pre-shared token, catalog
      (`rest.auth.type`, `X-Iceberg-Access-Delegation: vended-credentials`),
      `spark.driver.host=spark-connect`; **no S3 credentials anywhere**
- [x] A7 `docker/spark/log4j2.properties` with `[%X{correlationId},%X{principal}]`
- [x] A8 `Makefile`: up / bootstrap / demo / test / logs / down
- [x] A9 Polaris bootstrap: catalog `poc_catalog` → `s3://warehouse`, namespaces
      `shared` + `restricted`, principal roles `data_engineer` (alice) / `analyst` (bob),
      catalog roles + grants

### Milestone B — Java server plugin
- [x] B1 `server/pom.xml`: Java 21, `spark-connect_2.13:4.1.3` + `spark-sql_2.13:4.1.3` +
      `iceberg-spark-runtime-4.1_2.13:1.11.0` as `provided`; nimbus-jose-jwt shaded in
- [x] B2 `PropagatedIdentityHolder` — static map keyed by ExecuteKey + SessionKey, TTL eviction
- [x] B3 `UserTokenServerInterceptor` — no-arg ctor, `org.sparkproject.connect.grpc.*`,
      header extraction, message-type switch for session/user/operation ids, JWT validation,
      subject↔session binding, MDC
- [x] B4 `TokenExchangeService` — RFC 8693 via `java.net.http`, cache by SHA-256(JWT),
      TTL = `exp − 30s`
- [x] B5 `PropagatingRestAuthManager` / `AuthSession` — job-tag parse, session fallback,
      stamp `Authorization` + `X-Request-ID`, set job tag `cid:<uuid>`
- [x] B6 Error mapping: UNAUTHENTICATED vs PERMISSION_DENIED, correlation ID in every message;
      exempt lifecycle RPCs from strict checks

### Milestone C — Python client
- [x] C1 `client/pyproject.toml` (uv), dep `pyspark-client==4.1.3`
- [x] C2 `context.py` — contextvars correlation ID, session-scoped default, `with` block
- [x] C3 `auth.py` — `TokenProvider` protocol, `DeviceCodeTokenProvider` (cache + refresh),
      `PasswordGrantTokenProvider`
- [x] C4 `channel.py` — `PropagatingChannelBuilder` + interceptor (UnaryUnary + UnaryStream)
- [x] C5 `demo/demo.py` — interactive two-user walkthrough

### Milestone D — verification
- [x] D1 alice + bob both read `shared.events`
- [x] D2 alice reads `restricted.salaries`; bob gets FORBIDDEN
- [x] D3 concurrent interleaved execution, order-independent
- [x] D4 one correlation ID greppable in pyspark + spark-connect + polaris logs
- [x] D5 negative tokens: missing / expired / wrong audience → correct gRPC status
- [x] D6 spoofing: bob claiming alice's `user_id`/`session_id` is rejected
- [x] D7 MinIO access log: executor GETs use temporary STS keys, different per user, never root

### Milestone E — deliverables
- [x] E1 `README.md` — quickstart + mermaid sequence diagram
- [x] E2 `FINDINGS.md` — §2 written up as upstream findings
- [x] E3 published page: https://claude.ai/code/artifact/a12709f8-3cfa-4167-9507-bd9121b64265

---

## 5b. Milestone F — Apache Polaris console (added 2026-09-19)

- [x] F1 `docker/polaris-console/Dockerfile` — builds the console from `apache/polaris-tools`
      at pinned commit `8ffe1e5`, node:22-alpine build stage, nginx:alpine runtime (49 MB).
      Fetches the source rather than vendoring it.
- [x] F2 `entrypoint.sh` generates `window.APP_CONFIG` at container start; `nginx.conf` provides
      SPA fallback, no-cache for `/config.js`, asset caching and `/health`.
- [x] F3 Keycloak client `polaris-console`: public, standard flow, PKCE S256, redirect URIs and
      web origins for both `polaris-console:3000` and `localhost:3000`, carrying the same
      audience and principal-claim mappers as `spark-connect`.
- [x] F4 Polaris CORS opened for the console origin (`quarkus.http.cors.*`, default is off).
- [x] F5 `polaris-console` added to the `/etc/hosts` aliases in `install.sh`, plus a port check.
- [x] F6 `tests/test_console.py` — drives the real auth-code + PKCE flow headlessly and asserts
      alice sees `restricted` while bob gets 403, plus the CORS preflight.

**Why it matters here:** the console signs in through the same realm as Spark, so it renders the
authenticated user's view of the catalog rather than an admin's. It is the same authorisation
decision the Spark path makes, visible in a UI.

**Where the console actually lives:** `apache/polaris-tools`, not `apache/polaris` — see §12 of
FINDINGS.md. The main Polaris repo has no UI at all.

---

## 6. Open risks

All four original risks are resolved. Recorded here with how.

1. ~~Polaris principal bootstrap ordering~~ — **RESOLVED**, §2.12. Name-based resolution.
2. ~~Keycloak audience mappers~~ — **RESOLVED**, §2.14. Exchanged token carries `aud=polaris`.
3. ~~Catalog init timing vs. the job tag~~ — **RESOLVED**, §2.17. The catalog initialises inside
   the tagged region; `SHOW NAMESPACES` and table reads all authenticate correctly.
4. ~~MinIO STS sub-scoping~~ — **RESOLVED**, §2.18. Polaris issues per-table prefix-scoped STS
   session policies, confirmed from MinIO's own audit log.

### Known limitations, deliberately not fixed

* Per-operation correlation IDs within a single session can interleave (§2.15).
* Iceberg's async scan-report `POST /metrics` can carry a stale correlation ID for the same
  reason.
* `spark.sql.catalog.polaris.cache-enabled=false` is set so every catalog operation is visible.
  That is right for a PoC about observability and wrong for anything performance-sensitive.
* `SHOW NAMESPACES IN polaris` succeeds for bob only because he holds `NAMESPACE_LIST` on
  `shared`; listing at catalog scope returns 403. Acceptable, and arguably correct.
* Recreating the Keycloak container rotates its signing keys; Polaris and our validator log one
  JWKS failure and recover on refresh.
