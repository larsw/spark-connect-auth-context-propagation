# AuthZEN with Cerbos: what this branch does, and what it turned up

Branch `feat/authzen-cerbos`, 2026-11-09. Not merged to `main`. It builds on the earlier
`feat/authzen-pdp-support`, which sent Polaris's decisions to Keycloak as the PDP, and replaces
Keycloak in that role with [Cerbos](https://www.cerbos.dev/).

Two components now ask Cerbos for authorization decisions, over the
[OpenID AuthZEN Authorization API 1.0](https://openid.net/specs/authorization-api-1_0.html):

- **Polaris**, through the fork [larsw/polaris @ `feat/authzen-pdp-support`](https://github.com/larsw/polaris/tree/feat/authzen-pdp-support),
  for every catalog operation.
- **The Spark Connect plugin**, for every RPC except ReleaseSession and ReleaseExecute, after its own
  token and session checks.

The policies are seven YAML files in `docker/cerbos/policies`. Keycloak still issues every token and
still does the RFC 8693 exchange; it is no longer a PDP. The wiring is in
[docs/authzen-pdp.md](docs/authzen-pdp.md), and the slide deck in
`docs/presentations/authzen-with-cerbos` walks through it.

## Test results and what each user gets

Inside the compose network, all 77 Python tests pass: 43 against the running stack and 34 client
unit tests. Of the 43, eight ask Cerbos directly and the rest go through Spark Connect or Polaris.
The Java plugin's 38 unit tests also pass. The JVM and Rust integration suites were not rerun for
this branch.

| | alice (data_engineer) | bob (analyst) | spark-job (analyst) | carol (no role) |
|---|---|---|---|---|
| read `shared.events` | yes | yes | yes | refused by Spark Connect |
| read `restricted.salaries` | yes | 403 from Polaris | 403 from Polaris | refused by Spark Connect |
| vended S3 key can write | yes | no | no | refused by Spark Connect |
| `addArtifacts` | yes | refused by Spark Connect | refused by Spark Connect | refused by Spark Connect |

Every one of those decisions is in Cerbos's decision log, under the caller's correlation ID, so
`make cid` now finds the PDP's decision beside the Spark Connect and Polaris log lines.

## What it turned up

### Cerbos ignores `subject.properties.roles` (FINDINGS §20)

Cerbos puts the subject's properties in `principal.attr`. It takes roles only from a property named
`cerbos.roles`, and otherwise uses the subject's type, so every caller here has `roles: ["user"]`. A
rule with `roles: ["analyst"]` never matches: the answer is a plain `false`, logged as an ordinary
deny. Polaris cannot send `cerbos.roles`, so the policies use derived roles that read `P.attr.roles`.

### The policy decides how much the vended S3 key may do

On a delegated load Polaris asks for write delegation first and falls back to read only when that is
refused. A policy that allows an analyst `LOAD_TABLE_WITH_WRITE_DELEGATION` hands them
`s3:PutObject`, and every query still works. The earlier Keycloak branch shipped that mistake. Two
tests guard against it here: one asks Cerbos directly, one reads the session policy MinIO embedded
in the key Polaris vends bob.

### The Python client never authenticated `addArtifacts` (FINDINGS §18)

Its interceptor covered two of grpc-python's four call shapes, and `AddArtifacts` is
client-streaming, so it went out with no token at all. This was on `main` too, and is fixed on both.

### CloneSession skipped the session checks (FINDINGS §19)

The interceptor passed request types it did not recognise straight through, and Spark 4.1's
CloneSession was one, so a clone skipped the `user_id` and session-owner checks. It now reads
CloneSession, claims the new session id for the caller, and refuses any request type it does not
know. This gap is on `main` as well.

### The fork needs exactly Java 21, as a full JDK

Polaris's own `make build-server` refuses any `java` that is not exactly 21, and Gradle needs a JDK
with `javac`, not the Java 21 runtime some distributions install. `make polaris-image` now runs
Gradle in an `eclipse-temurin:21-jdk` container, so the host needs only git and docker.

### quay.io/minio refuses anonymous pulls

Since November 2026, `quay.io/minio` refuses anonymous pulls. MinIO and `mc` now come from
Chainguard's public images, pinned by digest, on `main` and here.

## Compared with Keycloak as the PDP

| | Keycloak (`feat/authzen-pdp-support`) | Cerbos (this branch) |
|---|---|---|
| Enforcement points | Polaris only | Polaris and Spark Connect |
| Policy | authorization services in the realm, about 4,800 lines of JSON | seven YAML files |
| Who a rule names | individual users: alice, bob, root | roles: data_engineer, analyst |
| Resources | registered in advance, so decisions are per registered name or entity type | nothing registered; decisions per namespace or table, from `parents` |
| Calling the PDP | client credentials, as `polaris-pdp` | no credentials, inside the compose network |
| Traps | `UNANIMOUS` by default; root needed a Keycloak user | roles have to be read from attributes |

## Costs and open questions

- Polaris moves from the 1.7.0 release to a 1.8.0 snapshot of the fork, and its AuthZEN support is a
  preview feature.
- Cerbos is on the path of every checked RPC and every catalog operation, and both callers fail
  closed, so it has to be as available as Keycloak. Here it is plain HTTP with no authentication,
  and write access to the policy directory is admin access.
- The plugin caches no decisions.
- `bootstrap.sh` still creates catalog-role grants that nothing reads. Its principal-role assignments
  still matter.
- We looked at replacing Keycloak with Dex and kept Keycloak. Dex implements the RFC 8693 grant only
  for tokens an upstream OIDC provider issued, not its own, so the server would have to forward the
  user's token instead of exchanging it.
