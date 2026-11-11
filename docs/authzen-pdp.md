# Authorization through AuthZEN, decided by OpenFGA

On this branch, two parts of the stack stop deciding for themselves who may do what, and ask a
Policy Decision Point (PDP) instead, over the [OpenID AuthZEN Authorization API 1.0][authzen]:

- **Polaris**, through the fork [larsw/polaris @ `feat/authzen-pdp-support`][fork], asks about every
  catalog operation: may bob load this table, may alice create one in that namespace.
- **The Spark Connect plugin** asks about every RPC except ReleaseSession and ReleaseExecute, before
  Spark does any work: may bob run a query, may he upload an artifact.

The PDP is [OpenFGA][openfga], with its experimental AuthZEN API turned on. The policy, a model and
its tuples, is in [`docker/openfga/setup.py`](../docker/openfga/setup.py), and it is the only place
either decision is made. The branch is cut from `feat/authzen-cerbos` and replaces Cerbos; the
callers did not change, apart from the request details the next sections describe.

Keycloak still issues every token and still does the token exchange. It is not a PDP.

[authzen]: https://openid.net/specs/authorization-api-1_0.html
[fork]: https://github.com/larsw/polaris/tree/feat/authzen-pdp-support
[openfga]: https://openfga.dev/

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
something in Polaris behaves differently, check whether 1.7.0 does the same before blaming the
policy.

## Who asks what

```
                      AuthZEN                                 AuthZEN
  Spark Connect  ───────────────►  OpenFGA  ◄───────────────  Polaris (fork)
  plugin                          store                       every catalog operation
  every checked RPC               docker/openfga/setup.py
```

### How OpenFGA reads an AuthZEN request

OpenFGA turns each evaluation into a Check:

| AuthZEN | OpenFGA Check |
|---|---|
| `subject.type`, `subject.id` | user `user:bob` |
| `action.name` | relation `LOAD_TABLE` |
| `resource.type`, `resource.id` | object `TABLE_LIKE:TABLE_LIKE` |
| `subject.properties.roles` | condition context `subject_roles` |
| `resource.properties.name`, `.parents` | `resource_name`, `resource_parents` |
| `context.request_id` | `request_id` |

Two consequences shape everything else.

**The endpoints are per store.** Every path is `/stores/<store id>/access/v1/...`, and the id is
made when the store is. `openfga-setup` makes the store, and writes its id to the `openfga-store`
volume, which Polaris and Spark Connect read when they start.

**OpenFGA decides from tuples, one per object.** It has no wildcard object, and nothing writes a
tuple when a table or a session is created. So both callers send a fixed `resource.id`, one object
per resource type, and say which table or session they mean in `resource.properties`. Conditions
then decide from those properties.

### Polaris

```yaml
polaris.authorization.type: "authzen"
QUARKUS_CONFIG_LOCATIONS: "file:/openfga/polaris.properties"
polaris.authorization.authzen.auth.type: "none"
polaris.authorization.authzen.mapping.resource-id-format: "type"
```

`/openfga/polaris.properties` holds the two endpoints, with the store id in them:

```properties
polaris.authorization.authzen.access-evaluation-endpoint=http://openfga:8080/stores/01M7.../access/v1/evaluation
polaris.authorization.authzen.access-evaluations-endpoint=http://openfga:8080/stores/01M7.../access/v1/evaluations
```

Polaris finds a PDP's endpoints at `<pdp-uri>/.well-known/authzen-configuration`, and OpenFGA's
discovery document is at `/.well-known/authzen-configuration/<store id>`, so discovery is not used.

With `resource-id-format: type`, `resource.id` is the entity type, so every table is the object
`TABLE_LIKE:TABLE_LIKE`. The table's name and its catalog and namespace are in the properties:

```json
{
  "subject":  {"type": "user", "id": "bob", "properties": {"roles": ["analyst"], "realm": "POLARIS"}},
  "action":   {"name": "LOAD_TABLE_WITH_READ_DELEGATION"},
  "resource": {"type": "TABLE_LIKE", "id": "TABLE_LIKE",
               "properties": {"name": "events",
                              "parents": [{"type": "CATALOG", "name": "poc_catalog"},
                                          {"type": "NAMESPACE", "name": "shared"}]}},
  "context":  {"request_id": "5ca6e59a-...", "realm": "POLARIS"}
}
```

Polaris asks one Access Evaluation per question, and uses Access Evaluations only when one
authorization has several. It refuses anything but a plain yes. A `false`, an error status, a batch answer with fewer decisions than it
asked for, or an answer with no decision is a 403. A PDP that cannot be reached fails the request
with a 500. Each request is logged at `DEBUG` in the Polaris container, under
`org.apache.polaris.extension.auth.authzen`.

### The Spark Connect plugin

```properties
spark.connect.propagation.authzen.url          http://openfga:8080/stores/<store id>   # set by entrypoint.sh
spark.connect.propagation.authzen.resource-id  spark-connect
spark.connect.propagation.authzen.timeout-ms   2000
```

The Spark image's entrypoint reads the store id from `/openfga/store-id` when `OPENFGA_URL` is set,
and refuses to start without it, so the server never runs with no PDP by accident.

After the interceptor has checked the token and that the caller owns the session they name, it
sends one evaluation per request message. That is one per RPC, except a streamed AddArtifacts
upload, which is asked about once per message:

```json
{
  "subject":  {"type": "user", "id": "bob", "properties": {"sub": "<uuid>", "roles": ["analyst"]}},
  "action":   {"name": "AddArtifacts"},
  "resource": {"type": "spark_connect", "id": "spark-connect",
               "properties": {"session_id": "<session id>", "user_id": "<uuid>"}},
  "context":  {"request_id": "<correlation id>"}
}
```

`operation_id` is added to the resource's properties when the request carries one. Without
`authzen.resource-id`, `resource.id` is the session id, as it was for Cerbos.

`roles` is always sent, as `[]` when there are none. OpenFGA answers HTTP 400, not `false`, when a
condition's parameter is missing, and the plugin reports that as `UNAVAILABLE`, so carol would get
the wrong error.

The subject's name and roles come from the token the server has just exchanged, which carries
`principal_name` and `principal_roles`, so the plugin and Polaris describe the same user the same
way. The token the client sent carries the roles only as Keycloak's `realm_access.roles`.

The two session checks stay in code. They are about whether the caller is who they claim to be in
this session, and a policy should not be able to switch them off. The PDP decides what an
authenticated user may do. A request type the interceptor does not recognise is refused, so a new
Spark Connect RPC cannot pass unchecked (FINDINGS §19).

Anything but a clear yes is a no. A `false` decision closes the call with `PERMISSION_DENIED`. A PDP
that answers with an error, answers something other than a boolean `decision`, or does not answer
within the timeout closes it with `UNAVAILABLE`, so the two cases can be told apart. Leave
`authzen.url` unset and the plugin asks nothing, as on `main`.

## The policy

`setup.py` builds the model from a short table, `POLICY`. Each resource type has one object, a few
**holders**, and its operations. A holder is a relation filled by one tuple, and an operation is the
union of the holders that may do it:

```
type TABLE_LIKE
  relations
    define root:              [user with from_polaris]                  # tuple: user:root
    define data_engineer:     [user:* with data_engineer]               # tuple: user:*
    define analyst_in_shared: [user:* with analyst_in_shared_table]     # tuple: user:*
    define LOAD_TABLE:        root or data_engineer or analyst_in_shared
    define DROP_TABLE:        root or data_engineer
    ...                                                                 # all 119 operations
```

(That is the shape in OpenFGA's DSL. The real model is JSON, because the DSL cannot declare the
`list<map<string>>` parameter that `parents` needs. `python3 docker/openfga/setup.py --print-model`
prints it.)

The conditions are CEL over the request's context:

| Condition | True when |
|---|---|
| `data_engineer` | `"data_engineer" in subject_roles` |
| `analyst` | `"analyst" in subject_roles` |
| `analyst_in_shared_namespace` | analyst, and the namespace is `shared` or inside it |
| `analyst_in_shared_table` | analyst, and the table's parents include the namespace `shared` |
| `from_polaris` | `subject_realm == "POLARIS"`, which only Polaris sends |

| Object | Who may do what |
|---|---|
| `ROOT:ROOT` | root anything; both roles `LIST_CATALOGS` |
| `CATALOG:CATALOG` | root and data engineers anything; analysts look and list namespaces |
| `NAMESPACE:NAMESPACE` | root and data engineers anything; analysts read `shared` only |
| `TABLE_LIKE:TABLE_LIKE` | root and data engineers anything; analysts read tables in `shared` only |
| `PRINCIPAL`, `PRINCIPAL_ROLE`, `CATALOG_ROLE`, `POLICY` | root only |
| `spark_connect:spark-connect` | data engineers every RPC; analysts every RPC except `AddArtifacts` |

Anything not allowed is denied. OpenFGA has no wildcard relation, so "anything" means every
operation in `docker/openfga/polaris-operations.txt`, the 119 in the fork's
`PolarisAuthorizableOperation`. A new Polaris operation is refused to everyone, root included, until
it is added there and `openfga-setup` is run again.

To change the policy with the stack running, edit `setup.py` and run
`docker compose run --rm openfga-setup`. It writes a new model, which OpenFGA uses from the next
request, and makes the stored tuples match. The store is in memory, so restarting the `openfga`
container loses it, and Polaris and Spark Connect keep the old store id. After that, `make up` again,
or restart all three.

`bootstrap.sh` still creates Polaris's catalog-role grants, which nothing reads now. It also assigns
principal roles to the principals, and those still matter: Polaris only accepts the roles a token
asks for if the principal has been granted them, and those are the roles it sends to OpenFGA.

## What each user gets

| | alice (data_engineer) | bob (analyst) | spark-job's service account (analyst) | carol (no role) |
|---|---|---|---|---|
| read `shared.events` | yes | yes | yes | refused by Spark Connect |
| read `restricted.salaries` | yes | 403 from Polaris | 403 from Polaris | refused by Spark Connect |
| write | yes | no: the vended key is read-only | no | refused by Spark Connect |
| `addArtifacts` | yes | refused by Spark Connect | refused by Spark Connect | refused by Spark Connect |

## Things worth knowing

**OpenFGA answers some questions with an error instead of `false`** (FINDINGS §21). A condition
whose parameter is missing from the request is HTTP 400, and so is an action or resource type the
model does not have. Inside a batch, the item is `decision: false` with the error in its `context`.
Both callers deny on either, so nothing gets through, but the error tells you something different
from a refusal: the request's shape does not match the model.

**The policy decides how much the vended S3 key may do.** On a delegated load, Polaris asks for
`LOAD_TABLE_WITH_WRITE_DELEGATION` first and only falls back to read when that is refused. A policy
that allows an analyst write delegation hands them `s3:PutObject` while every query still works. The
analyst's list on `TABLE_LIKE` leaves it out on purpose, and two tests check it: one asks OpenFGA
directly, the other asks Polaris for the key it vends bob and reads the session policy MinIO
embedded in it.

**Polaris caches vended credentials by STS session name**, which is `polaris` for everyone unless
`INCLUDE_PRINCIPAL_NAME_IN_SUBSCOPED_CREDENTIAL` is on. Without it, two users whose decisions match
share one cache entry and get the same key. It is on in `compose.yaml`.

**Even the bootstrap asks the PDP.** With an external PDP, Polaris's root principal is just another
subject, so `make bootstrap` fails on its first call unless the policy allows `root`. The tuple
names `user:root`, so it exists only on Polaris's types, and its condition needs Polaris's realm,
which the plugin never sends. A Keycloak user called root gets nothing from it in Spark Connect.

**OpenFGA is on the path of everything.** If it is down, the plugin answers `UNAVAILABLE` (after up
to `authzen.timeout-ms` if it hangs) and Polaris fails catalog calls. Whoever answers at
`openfga:8080` decides for both callers, and here that is plain HTTP with no authentication, with
the write API (models and tuples) on the same port as the AuthZEN API.

## Asking OpenFGA directly

OpenFGA is published on 8093, so a decision can be tried without Polaris or Spark in the way. This
is the quickest way to tell a policy problem from anything else:

```bash
STORE=$(docker compose exec -T spark-connect cat /openfga/store-id)
curl -s localhost:8093/stores/$STORE/access/v1/evaluation -H 'Content-Type: application/json' -d '{
  "subject":  {"type": "user", "id": "bob", "properties": {"roles": ["analyst"], "realm": "POLARIS"}},
  "action":   {"name": "LOAD_TABLE_WITH_READ_DELEGATION"},
  "resource": {"type": "TABLE_LIKE", "id": "TABLE_LIKE",
               "properties": {"name": "salaries",
                              "parents": [{"type": "CATALOG", "name": "poc_catalog"},
                                          {"type": "NAMESPACE", "name": "restricted"}]}}}'
# {"decision":false}
```

OpenFGA has no decision log, and ignores the caller's X-Request-ID. It logs every request at info,
as JSON with the request and the answer, in `docker compose logs openfga`. Both callers put the
correlation ID in the AuthZEN context, so it is in that log, and `make cid CID=<uuid>` shows OpenFGA's
decisions beside the Spark Connect and Polaris lines for the same request.
