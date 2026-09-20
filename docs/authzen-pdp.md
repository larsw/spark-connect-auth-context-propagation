# Trying the AuthZEN PDP fork of Polaris

This branch swaps Apache Polaris for **[larsw/polaris @ `feat/authzen-pdp-support`][fork]**, one
commit on top of Polaris `main` that lets Polaris delegate every authorization decision to a
Policy Decision Point speaking the [OpenID AuthZEN Authorization API 1.0][authzen]. Keycloak — the
same instance that already issues the tokens — becomes that PDP, via its experimental `authzen`
feature.

It is a branch, not a merge: the point is to find out what changes, and one thing does.

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
| `bob-may-read-shared` | bob | `POLARIS`, `poc_catalog`, `shared`, `events` | the 31 read-ish ones |
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

## What it costs: the vended credential stops being per-user

**One test fails, and it is left failing on purpose** (`xfail(strict=True)`, so it will shout if
the fork fixes it): `test_polaris_vends_distinct_temporary_credentials_per_user`.

Ask Polaris to load `shared.events` with `X-Iceberg-Access-Delegation: vended-credentials` as each
user, and compare:

```
alice: access-key=2JBZC2XRQJEN... token-len=1183
bob  : access-key=2JBZC2XRQJEN... token-len=1183
same access key:    True
same session token: True
```

Byte-identical. And the session policy embedded in both grants **write**:

```json
{"Effect": "Allow",
 "Action": ["s3:DeleteObject", "s3:PutObject"],
 "Resource": ["arn:aws:s3:::warehouse/poc/shared/events/*"]}
```

bob is a read-only analyst. On `main` he receives his own token, scoped to reads. Here he gets
alice's, and it can write.

The catalog decision is still right — bob is refused `restricted.salaries`, which is what the demo
shows. It is the **data plane** that stops differentiating. That follows from what an external PDP
is: it answers *allow or deny*, and Polaris previously shaped the vended credential from the
principal's resolved privilege *set*, which the PDP never returns. The identical token suggests the
vended credential is being cached per table rather than per principal, but that is inference from
the outside; the difference in behaviour is the measured part.

This matters for this PoC specifically, because its headline claim is that the only credentials on
the data path are the ones Polaris vended *for whichever user made the request*. On this branch
that is still true of the request, and no longer true of the user.

## Status

| | |
|---|---|
| Host suite | 43 passed, 1 skipped, 1 xfailed |
| `make bootstrap` | works, once Keycloak has a `root` user |
| `make demo` | unchanged output, refusal now from the PDP |
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
