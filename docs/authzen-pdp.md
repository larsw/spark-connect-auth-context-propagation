# Trying the AuthZEN PDP fork of Polaris

This branch swaps Apache Polaris for **[larsw/polaris @ `feat/authzen-pdp-support`][fork]**, one
commit on top of Polaris `main` that lets Polaris delegate every authorization decision to a
Policy Decision Point speaking the [OpenID AuthZEN Authorization API 1.0][authzen]. Keycloak — the
same instance that already issues the tokens — becomes that PDP, via its experimental `authzen`
feature.

It is a branch, not a merge: the point is to find out what changes. The short answer is that the
authorization decision moves, and two settings have to move with it or the vended credentials stop
being per-user.

[fork]: https://github.com/larsw/polaris/tree/feat/authzen-pdp-support
[authzen]: https://openid.net/specs/authorization-api-1_0.html

## Running it

```bash
make polaris-image     # clones the fork at a pinned commit and builds it (gradle + quarkus, ~2 min)
make build && make up
make bootstrap
make demo
```

`make polaris-image` needs `java21` and `docker`, which `install.sh` already checks for. There is
no published image for the fork, so it is built from source — the same approach
`docker/polaris-console/` takes for the Polaris console.

## What changed

**Keycloak** (`compose.yaml`, `docker/keycloak/spark-realm.json`)

- started with `--features=authzen`, which exposes
  `/realms/spark/.well-known/authzen-configuration` and the evaluation endpoints under it
- a new confidential client, `polaris-pdp`, with Authorization Services enabled. **Its resources,
  scopes and permissions are now the policy** — what alice and bob may do is decided here, not by
  Polaris grants
- a `root` user, with no credentials, which exists only so a policy can name it. The bootstrap
  runs as Polaris's internal root principal, and with an external PDP even that goes to Keycloak:
  without this, `make bootstrap` fails on its first call with `AuthZEN PDP denied authorization`

**Polaris** (`compose.yaml`)

```yaml
polaris.authorization.type: "authzen"
polaris.authorization.authzen.pdp-uri: "http://keycloak:8080/realms/spark"
polaris.authorization.authzen.auth.type: "client-credentials"
polaris.authorization.authzen.mapping.resource-id-format: "name"
```

`resource-id-format: name` rather than the default `path`, so `resource.id` is the leaf — `events`,
`salaries` — which is what the realm registers and what lets one policy tell the shared table from
the restricted one.

## The policy, in Keycloak terms

Keycloak matches `action.name` against **scopes** and `resource.id` against **resources**, and both
must be registered in advance — an unregistered resource id is indistinguishable from a deny, so
`docker/keycloak/polaris-operations.txt` carries all 119 Polaris operations and every entity the
bootstrap touches is registered.

| Policy | Subject | Resources | Scopes |
|---|---|---|---|
| `alice-may-do-anything` | alice | all | all 119 |
| `bob-may-read-shared` | bob | `POLARIS`, `poc_catalog`, `shared`, `events` | the 30 read-only ones (no `*WRITE*`) |
| `root-may-bootstrap` | root | all | all 119 |

The resource server is set to `AFFIRMATIVE`. Keycloak defaults it to `UNANIMOUS`, which requires
*every* permission covering a (resource, scope) pair to grant — so as soon as there is more than
one role, they contradict each other and everyone is denied.

## It works, and you can see it

`make demo` is unchanged, and so is its outcome — except for where the refusal comes from:

```
  OK      alice: SELECT * FROM polaris.restricted.salaries
  DENIED  bob  : SELECT * FROM polaris.restricted.salaries
            (org.apache.iceberg.exceptions.ForbiddenException) Forbidden: AuthZEN PDP denied
            authorization
```

That sentence used to come from Polaris's own grant evaluation. It now comes from a policy
decision made in Keycloak.

## Per-user credentials: two settings, both easy to get wrong

Getting to "alice and bob hold different, differently-scoped credentials" took two fixes. Neither
is a defect in the fork — both are configuration — but both fail *silently*, and the first draft of
this branch shipped with both wrong and concluded the fork was at fault. It was not.

**1. Polaris caches vended credentials, and the cache key includes the STS session name.**

`AwsStorageCredentialCacheKey` is keyed on the realm, the storage config, the allowed read/list/
write locations, the session name and the session tags. The session name is `polaris` for
*everyone* unless you say otherwise, so two principals whose location grants match collapse onto
one cache entry and are handed a byte-identical access key and session token.

```yaml
polaris.features."INCLUDE_PRINCIPAL_NAME_IN_SUBSCOPED_CREDENTIAL": "true"
```

makes it `polaris-alice` / `polaris-bob`. Polaris's own documentation frames this as a cost —
"degradation in temporary credential caching as catalog will no longer be able to reuse
credentials for multiple principals" — which is precisely the property wanted here.

**2. A delegated load asks for WRITE first, so a too-generous read policy grants writes.**

`IcebergCatalogHandler` decides the credential's scope like this:

```java
Set<PolarisStorageActions> actionsRequested = new HashSet<>(Set.of(READ, LIST));
try {
  authorize(... LOAD_TABLE_WITH_WRITE_DELEGATION ...);
  actionsRequested.add(WRITE);
} catch (ForbiddenException e) {
  authorize(... LOAD_TABLE_WITH_READ_DELEGATION ...);
}
```

Write delegation is requested *first*, and read is only a fallback for when the PDP refuses. So
the credential's scope is decided by the policy, and a reader who is granted
`LOAD_TABLE_WITH_WRITE_DELEGATION` receives `s3:PutObject`.

The realm generator originally picked bob's scopes with a name heuristic — anything starting
`LIST_`, `LOAD_` or `GET_` — and `LOAD_TABLE_WITH_WRITE_DELEGATION` starts with `LOAD_`. bob was a
read-only analyst holding a write credential, and every test still passed. The generator now
excludes any operation containing `WRITE` from the read set.

**The result**, read back out of the session policy MinIO embeds in each token:

```
alice: access-key=F0OEK0HW91LQ... token-len=1183
bob  : access-key=CZBNUKU1JKZ5... token-len=970
same access key: False    same session token: False

alice: s3:DeleteObject, s3:PutObject, s3:ListBucket, s3:GetObject, ...
bob  : s3:ListBucket, s3:GetBucketLocation, s3:GetObject, s3:GetObjectVersion
```

`test_the_vended_credential_is_scoped_to_what_the_user_may_do` now asserts exactly that, because
the previous test — which only compared tokens for inequality — would have passed throughout the
second bug.

## Status

| | |
|---|---|
| Host suite | 45 passed, 1 skipped |
| `make bootstrap` | works, once Keycloak has a `root` user |
| `make demo` | unchanged output; refusal now from the PDP |
| Polaris version | 1.8.0-SNAPSHOT (fork of `main`), against 1.7.0 on `main` |

The PDP itself can be exercised without Polaris in the picture, which is the quickest way to tell a
policy problem from a Polaris problem:

```bash
TOKEN=$(curl -s -d grant_type=client_credentials -d client_id=polaris-pdp \
  -d client_secret=polaris-pdp-secret \
  http://keycloak:8080/realms/spark/protocol/openid-connect/token | jq -r .access_token)

curl -s -X POST http://keycloak:8080/realms/spark/authzen/access/v1/evaluation \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"subject":{"type":"user","id":"username:bob"},
       "action":{"name":"LOAD_TABLE_WITH_READ_DELEGATION"},
       "resource":{"type":"polaris:TABLE_LIKE","id":"salaries"}}'
# {"decision":false}
```
