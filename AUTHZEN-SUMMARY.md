# AuthZEN with OpenFGA: what this branch does, and what it turned up

Branch `feat/authzen-openfga`, 2026-11-10. Not merged to `main`. It is cut from
`feat/authzen-cerbos` and replaces [Cerbos](https://www.cerbos.dev/) as the policy decision point
with [OpenFGA](https://openfga.dev/) 1.22.1, using its experimental AuthZEN API.

Two components ask OpenFGA for authorization decisions, over the
[OpenID AuthZEN Authorization API 1.0](https://openid.net/specs/authorization-api-1_0.html):

- **Polaris**, through the fork [larsw/polaris @ `feat/authzen-pdp-support`](https://github.com/larsw/polaris/tree/feat/authzen-pdp-support),
  for every catalog operation.
- **The Spark Connect plugin**, for every RPC except ReleaseSession and ReleaseExecute, after its own
  token and session checks.

The policy is `docker/openfga/setup.py`, which builds the model, makes the store and writes the
tuples on every `make up`. Keycloak still issues every token and still does the RFC 8693 exchange.
The wiring is in [docs/authzen-pdp.md](docs/authzen-pdp.md). The slide deck in
`docs/presentations/authzen-with-cerbos` still describes the Cerbos branch.

## Test results and what each user gets

Inside the compose network, all 78 Python tests pass: 44 against the running stack and 34 client
unit tests. Of the 44, nine ask OpenFGA directly and the rest go through Spark Connect or Polaris. The
Java plugin's 38 unit tests also pass. The JVM and Rust integration suites were not rerun for this
branch.

| | alice (data_engineer) | bob (analyst) | spark-job (analyst) | carol (no role) |
|---|---|---|---|---|
| read `shared.events` | yes | yes | yes | refused by Spark Connect |
| read `restricted.salaries` | yes | 403 from Polaris | 403 from Polaris | refused by Spark Connect |
| vended S3 key can write | yes | no | no | refused by Spark Connect |
| `addArtifacts` | yes | refused by Spark Connect | refused by Spark Connect | refused by Spark Connect |

The same as on the Cerbos branch. Every one of those decisions is in OpenFGA's request log, which
carries the caller's correlation ID in the AuthZEN context, so `make cid` finds the PDP's decision
beside the Spark Connect and Polaris log lines.

## What changed to make OpenFGA fit (FINDINGS §21)

### The callers send a fixed resource id

OpenFGA decides from tuples, one per object, and has no wildcard object. Tables, namespaces and
Spark Connect sessions exist only at runtime, and nothing on the request path writes tuples. So
Polaris now sends `resource-id-format: type` (every table is `TABLE_LIKE:TABLE_LIKE`), and the
plugin has a new `authzen.resource-id` (every session is `spark_connect:spark-connect`, with the
session id moved to `properties.session_id`). Which table is meant comes from the properties, so the
decisions are made by CEL conditions on `user:*` tuples, plus one tuple naming root.

### The store id has to reach both callers

OpenFGA's AuthZEN paths are `/stores/<id>/access/v1/...`, the id is made at runtime, and its
discovery document is not where Polaris looks for one. `openfga-setup` writes the id to a shared
volume. Polaris reads both endpoints from a properties file there, through
`QUARKUS_CONFIG_LOCATIONS`, and the Spark image's entrypoint builds the plugin's URL from it.

### Some questions get an error instead of `false`

A condition parameter missing from the request, or an action the model does not have, is HTTP 400.
Polaris leaves `roles` out for a principal with none, and treats the 400 as a deny. The plugin now
always sends `roles`, so carol gets `PERMISSION_DENIED`, not `UNAVAILABLE`. OpenFGA has no wildcard
relation either, so "may do anything" is all 119 Polaris operations as relations on each type, and a
new Polaris operation is refused to everyone until it is added to `polaris-operations.txt`.

### The model is JSON, not the DSL

The table and namespace conditions need a `list<map<string>>` parameter for Polaris's `parents`. The
API accepts it; the DSL and `fga model transform` do not. So `setup.py` builds the model as JSON from
a short table of who holds what.

## Compared with Cerbos

| | Cerbos (`feat/authzen-cerbos`) | OpenFGA (this branch) |
|---|---|---|
| Policy | seven YAML files | one Python file that builds a JSON model and 18 tuples |
| Roles | read back out of `principal.attr` by derived roles | conditions on `subject_roles` |
| Resources | decided from `parents`, ids as Polaris sends them | one fixed object per type; decided from `parents` by conditions |
| "Anything" | `actions: ["*"]` | every operation listed, 119 per Polaris type |
| Missing attribute, unknown action | `false` | HTTP 400 |
| Endpoint | one URL, with discovery | per store; the id is passed to both callers at startup |
| Policy reload | watched directory | `docker compose run --rm openfga-setup` |
| Decision log | yes, with X-Request-ID | no; the request log has the AuthZEN context |
| State | none | the store is in memory; restarting OpenFGA alone loses it |

The relationship graph, which is what OpenFGA is for, does no work here. The callers send roles and
attributes, not relationships, so every decision is a CEL condition. Using it would mean writing
tuples as resources and memberships change, for example role membership synchronised from Keycloak,
or tables registered when Polaris creates them. Nothing in this stack does that today.

## Costs and open questions

- Polaris moves from the 1.7.0 release to a 1.8.0 snapshot of the fork, its AuthZEN support is a
  preview feature, and OpenFGA's AuthZEN API is experimental.
- OpenFGA is on the path of every checked RPC and every catalog operation, and both callers fail
  closed. Here it is plain HTTP with no authentication, and the API that writes models and tuples is
  on the same port as AuthZEN.
- The store lives in memory. A persistent datastore (Postgres, MySQL or SQLite) would survive a
  restart, and the store id would then stay put.
- The plugin caches no decisions.
- `bootstrap.sh` still creates catalog-role grants that nothing reads. Its principal-role assignments
  still matter.
