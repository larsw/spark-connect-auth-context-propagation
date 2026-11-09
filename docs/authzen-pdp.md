# Authorization through AuthZEN, decided by Cerbos

On this branch, two parts of the stack stop deciding for themselves who may do what, and ask a
Policy Decision Point (PDP) instead, over the [OpenID AuthZEN Authorization API 1.0][authzen]:

- **Polaris**, through the fork [larsw/polaris @ `feat/authzen-pdp-support`][fork], asks about every
  catalog operation: may bob load this table, may alice create one in that namespace.
- **The Spark Connect plugin** asks about every RPC except ReleaseSession and ReleaseExecute, before
  Spark does any work: may bob run a query, may he upload an artifact.

The PDP is [Cerbos][cerbos]. Its policies are YAML files in [`docker/cerbos/policies`](../docker/cerbos/policies),
and they are the only place either decision is made.

Keycloak still issues every token and still does the token exchange. It is no longer a PDP, so it
runs without `--features=authzen` and the realm has no authorization-services client.

[authzen]: https://openid.net/specs/authorization-api-1_0.html
[fork]: https://github.com/larsw/polaris/tree/feat/authzen-pdp-support
[cerbos]: https://www.cerbos.dev/

## Running it

```bash
make polaris-image     # clones the fork at a pinned commit and builds it in a JDK 21 container
make build && make up
make demo
make test-container    # the suite inside the compose network
```

`make polaris-image` needs only git and docker. Polaris's own `make build-server` refuses any `java`
that is not exactly 21, and Gradle needs a full JDK, not the Java 21 runtime some distributions
install. So the script runs Gradle directly in `eclipse-temurin:21-jdk`. The Gradle cache is a
docker volume, so the second build is much faster than the first.

The fork is a 1.8.0 snapshot of Polaris main, where `main` of this repo runs the 1.7.0 release. If
something in Polaris behaves differently, check whether 1.7.0 does the same before blaming a policy.

## Who asks what

```
                      AuthZEN                                AuthZEN
  Spark Connect  ───────────────►  Cerbos  ◄───────────────  Polaris (fork)
  plugin                          policies                   every catalog operation
  every checked RPC               docker/cerbos/policies
```

### Polaris

```yaml
polaris.authorization.type: "authzen"
polaris.authorization.authzen.pdp-uri: "http://cerbos:3592"
polaris.authorization.authzen.auth.type: "none"
```

The fork's defaults suit Cerbos. `resource.type` is the bare Polaris entity type (`TABLE_LIKE`,
`NAMESPACE`, `CATALOG`, `ROOT`), which Cerbos uses as the resource kind, so each type the users touch
has its own policy file. Other types, such as `PRINCIPAL_ROLE` and `CATALOG_ROLE`, have none and are
allowed only to root. Each request also carries the entity's `name` and its `parents` in
`resource.properties`, so a policy can name a namespace without anything being registered in
advance:

```json
{
  "subject":  {"type": "user", "id": "bob", "properties": {"roles": ["analyst"], "realm": "POLARIS"}},
  "action":   {"name": "LOAD_TABLE_WITH_READ_DELEGATION"},
  "resource": {"type": "TABLE_LIKE", "id": "poc_catalog/shared/events",
               "properties": {"name": "events",
                              "parents": [{"type": "CATALOG", "name": "poc_catalog"},
                                          {"type": "NAMESPACE", "name": "shared"}]}},
  "context":  {"request_id": "5ca6e59a-..."}
}
```

Polaris refuses anything but a plain yes. A `false`, an error status, a batch answer with fewer
decisions than it asked for, or an answer with no decision is a 403. A PDP that cannot be reached
fails the request with a 500. Each request is logged at `DEBUG` in the Polaris container, under
`org.apache.polaris.extension.auth.authzen`.

### The Spark Connect plugin

```properties
spark.connect.propagation.authzen.url         http://cerbos:3592
spark.connect.propagation.authzen.timeout-ms  2000
```

After the interceptor has checked the token and that the caller owns the session they name, it
sends one evaluation per request message. That is one per RPC, except a streamed AddArtifacts
upload, which is asked about once per message:

```json
{
  "subject":  {"type": "user", "id": "bob", "properties": {"sub": "<uuid>", "roles": ["analyst"]}},
  "action":   {"name": "AddArtifacts"},
  "resource": {"type": "spark_connect", "id": "<session id>", "properties": {"user_id": "<uuid>"}},
  "context":  {"request_id": "<correlation id>"}
}
```

`operation_id` is added to the resource's properties when the request carries one.

The subject's name and roles come from the token the server has just exchanged, which carries
`principal_name` and `principal_roles`, so the plugin and Polaris describe the same user the same
way. The token the client sent carries the roles only as Keycloak's `realm_access.roles`.

The two session checks stay in code. They are about whether the caller is who they claim to be in
this session, and a policy should not be able to switch them off. The PDP decides what an
authenticated user may do. A request type the interceptor does not recognise is refused, so a new
Spark Connect RPC cannot pass unchecked. That is how CloneSession used to skip the session checks
(FINDINGS §19).

Anything but a clear yes is a no. A `false` decision closes the call with `PERMISSION_DENIED`. A PDP
that answers with an error, answers something other than a boolean `decision`, or does not answer
within the timeout closes it with `UNAVAILABLE`, so the two cases can be told apart. Leave
`authzen.url` unset and the plugin asks nothing, as on `main`.

## The policies

| File | Kind | What it allows |
|---|---|---|
| `derived_roles.yaml` | derived roles | turns `principal.attr.roles` into the roles `data_engineer` and `analyst` |
| `polaris_root_principal.yaml` | principal policy | Polaris's `root`, which the bootstrap runs as, may do anything to Polaris's resource kinds, for requests that carry Polaris's realm |
| `polaris_root.yaml` | `ROOT` | both roles may list catalogs |
| `polaris_catalog.yaml` | `CATALOG` | data engineers anything; analysts look and list namespaces |
| `polaris_namespace.yaml` | `NAMESPACE` | data engineers anything; analysts read `shared` only |
| `polaris_table.yaml` | `TABLE_LIKE` | data engineers anything; analysts read tables in `shared` only |
| `spark_connect.yaml` | `spark_connect` | data engineers every RPC; analysts every RPC except `AddArtifacts` |

Anything not allowed is denied. A new Polaris operation or Spark Connect RPC is refused to analysts
and to callers with no role until a policy names it. Data engineers get it at once on `CATALOG`,
`NAMESPACE`, `TABLE_LIKE` and `spark_connect` through their `*` rules, and root gets everything on
Polaris's kinds through its principal policy. A resource type no policy covers is refused to every
caller except root.

Cerbos watches the directory, so a policy edited with the stack running applies to the next
request.

`bootstrap.sh` still creates Polaris's catalog-role grants, which nothing reads now. It also assigns
principal roles to the principals, and those still matter: Polaris only accepts the roles a token
asks for if the principal has been granted them, and those are the roles it sends to Cerbos.

## What each user gets

| | alice (data_engineer) | bob (analyst) | spark-job's service account (analyst) | carol (no role) |
|---|---|---|---|---|
| read `shared.events` | yes | yes | yes | refused by Spark Connect |
| read `restricted.salaries` | yes | 403 from Polaris | 403 from Polaris | refused by Spark Connect |
| write | yes | no: the vended key is read-only | no | refused by Spark Connect |
| `addArtifacts` | yes | refused by Spark Connect | refused by Spark Connect | refused by Spark Connect |

## Things worth knowing

**Cerbos does not treat `subject.properties.roles` as the principal's roles.** Over AuthZEN it puts
the subject's properties into `principal.attr`. It takes roles only from a property literally named
`cerbos.roles`, and without one it uses the subject's type, so every caller here has
`roles: ["user"]`. A rule with `roles: ["analyst"]` never matches what these two callers send, and
every decision comes back `false` with no warning; only the decision log's `roles: ["user"]` shows
why. Polaris cannot send `cerbos.roles`, so `derived_roles.yaml` reads the roles back out of the
attributes, and the rules use `derivedRoles`. svipul's Cerbos policies take another route,
`roles: ["*"]` with a condition, for the same reason (FINDINGS §20).

**The policy decides how much the vended S3 key may do.** On a delegated load, Polaris asks for
`LOAD_TABLE_WITH_WRITE_DELEGATION` first and only falls back to read when that is refused. A policy
that allows an analyst write delegation hands them `s3:PutObject` while every query still works.
`polaris_table.yaml` leaves it out on purpose, and two tests check it: one asks Cerbos directly, the
other asks Polaris for the key it vends bob and reads the session policy MinIO embedded in it.

**Polaris caches vended credentials by STS session name**, which is `polaris` for everyone unless
`INCLUDE_PRINCIPAL_NAME_IN_SUBSCOPED_CREDENTIAL` is on. Without it, two users whose decisions match
share one cache entry and get the same key. It is on in `compose.yaml`.

**Even the bootstrap asks the PDP.** With an external PDP, Polaris's root principal is just another
subject, so `make bootstrap` fails on its first call unless a policy allows `root`. That policy
matches on the name, so it is limited to Polaris's resource kinds and to requests that carry
Polaris's realm, which the plugin never sends. A Keycloak user called root gets nothing from it in
Spark Connect.

**Cerbos is on the path of everything.** If it is down, the plugin answers `UNAVAILABLE` (after up to
`authzen.timeout-ms` if it hangs) and Polaris fails catalog calls. Polaris still starts, because it
retries discovery on first use. Whoever answers at `cerbos:3592` decides for both callers, and here
that is plain HTTP with no authentication; write access to the policy directory is admin access.

## Asking Cerbos directly

Cerbos is published on 3592, so a policy can be tried without Polaris or Spark in the way. This is
the quickest way to tell a policy problem from anything else:

```bash
curl -s localhost:3592/access/v1/evaluation -H 'Content-Type: application/json' -d '{
  "subject":  {"type": "user", "id": "bob", "properties": {"roles": ["analyst"]}},
  "action":   {"name": "LOAD_TABLE_WITH_READ_DELEGATION"},
  "resource": {"type": "TABLE_LIKE", "id": "poc_catalog/restricted/salaries",
               "properties": {"name": "salaries",
                              "parents": [{"type": "CATALOG", "name": "poc_catalog"},
                                          {"type": "NAMESPACE", "name": "restricted"}]}}}'
# {"decision":false}
```

Every decision Cerbos makes is in `docker compose logs cerbos`, with the caller's correlation ID, so
`make cid CID=<uuid>` shows it beside the Spark Connect and Polaris lines for the same request.
