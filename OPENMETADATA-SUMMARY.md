# OpenMetadata branch — what was done, and what it turned up

Branch `worktree-openmetadata-iceberg`, 2026-09-20, cut from `feat/authzen-pdp-support`. Not merged.

Adds [OpenMetadata 2.0.2][om] to the stack and catalogues the Polaris Iceberg tables in it. The
connector that would have done that does not exist any more, so most of the work was reviving it.

The mechanics — how to run it, how the identities line up, what the connector changed — are in
**[docs/openmetadata-iceberg.md](docs/openmetadata-iceberg.md)**. This is the shorter story.

[om]: https://docs.open-metadata.org/v2.0.x/quick-start/local-docker-deployment

## The connector had been deleted, and it does not come back as a service type

OpenMetadata [PR #26365][pr] removed the built-in Iceberg service in 2.0 — 36 files spanning the
Python ingestion, the JSON Schemas, the Java converters and the UI. It is gone from every 2.0.x
release, not just the tip of `main`.

Restoring the *service type* would mean rebuilding the server and the UI, because the schema and
converters went with it. The PR's own migration path is the way in: existing Iceberg services were
moved to `CustomDatabase`, which takes a Python class the ingestion framework imports by name. So
`openmetadata-connector/` is the original ingestion logic — same topology, same models, same column
parsing — with its configuration re-rooted from the deleted JSON Schema onto `connectionOptions`,
and with the Glue, Hive and DynamoDB catalogs dropped.

[pr]: https://github.com/open-metadata/OpenMetadata/pull/26365

### Brushing it up found three bugs

Reviving code is a chance to read it properly:

- **A `NameError` inside the retry handler.** `_load_iceberg_table` binds the exception as `e` and
  logs `exc` in two of four branches. Every exhausted retry and every non-network error therefore
  raised `NameError` *in the handler*, which the outer catch reported as `Could not load iceberg
  table properly: name 'exc' is not defined`. The actual failure was never logged, and the table
  was skipped silently.
- **Nested namespaces corrupted table names.** The helper drops only the first element of the
  identifier tuple, so a table in namespace `a.b` was ingested as `b.tbl`.
- **Two SigV4 property names had been renamed under it.** PyIceberg moved
  `rest.signing_region`/`rest.signing_name` to hyphens. The connector pinned `pyiceberg==0.5.1`
  from 2023 and would have passed keys nothing reads.

It now runs on **PyIceberg 0.12**. Every call the old code makes still exists; a test asserts our
property-name constants equal PyIceberg's, so the next rename fails a test rather than a request.

## It reaches Polaris with OAuth 2.0 client credentials, via Keycloak

Not a human user and not Polaris's own token endpoint: a confidential Keycloak client,
`openmetadata`, using the client credentials grant. Supplying a token URL is what selects
PyIceberg's OAuth2 `AuthManager` over the legacy `credential` flow that PyIceberg drops in 1.0.

Five names have to agree, and none of them is a free choice — Keycloak names a service account
`service-account-<clientId>`, Polaris resolves its principal by name from `principal_name` and
requires it to pre-exist, and Keycloak-as-PDP resolves `subject.id` back to one of its own users:

```
  Keycloak client                openmetadata
  service account user           service-account-openmetadata
  principal_name claim           service-account-openmetadata
  Polaris principal              service-account-openmetadata
  AuthZEN subject.id             username:service-account-openmetadata
```

### What it may do, checked against the live PDP

```
  LIST_TABLES                       restricted  -> true
  LOAD_TABLE                        salaries    -> true
  LOAD_TABLE_WITH_READ_DELEGATION   salaries    -> true
  LOAD_TABLE_WITH_WRITE_DELEGATION  salaries    -> false
  UPDATE_TABLE / DROP_TABLE         salaries    -> false
  CREATE_TABLE_DIRECT               shared      -> false
```

That is the contrast worth having: the crawler describes the *shape* of `restricted.salaries` while
writing nothing anywhere, and bob still cannot read it at all. A catalogue needs to see more than
any one user and less than any writer.

The 30 read-only scopes are copied out of `bob-may-read-shared` by the realm generator rather than
restated, so the two cannot drift — and the generator refuses to run if that policy ever gains a
WRITE scope. That is the `LOAD_TABLE_WITH_WRITE_DELEGATION`-starts-with-`LOAD_` bug from the
AuthZEN branch wired shut.

## Two things that cost real time

**1. Polaris caches its PDP token and never refreshes it.** Changing the realm means recreating the
Keycloak container, which regenerates the realm signing keys. Polaris's cached `polaris-pdp` token
is then rejected, and the AuthZEN client treats *any* non-200 as a deny:

```
WARN AuthzenPdpClient: AuthZEN PDP ... returned unexpected HTTP status 401, treating as deny
INFO IcebergExceptionMapper: Handling runtimeException AuthZEN PDP denied authorization
```

Every request fails closed with a message indistinguishable from a real policy decision, while the
PDP asked directly with curl answers `true` for the same subject and action. That contradiction is
the tell. **After touching the realm, restart Polaris as well.** Failing closed on an unreachable
PDP is defensible; not retrying once on a 401 turns a credential expiry into an outage.

**2. In Keycloak, "allowed on everything" is an enumeration.** `alice-may-do-anything` and
`root-may-bootstrap` list their resources explicitly. Adding three resources without extending
those lists left root able to create the principal and the role but not to connect them —
`FAIL (403) service-account-openmetadata -> metadata_reader`. A registered resource that no
permission covers denies exactly like an unregistered one. The generator now rewrites both
permissions to cover every registered resource.

**3. Elasticsearch needs twice the memory its heap suggests.** At `mem_limit: 1g` around a 512 MB
heap, the container sat at 98% and the migration crawled — one index per 30 seconds, still
unfinished after fifteen minutes, with the health check timing out. Lucene lives off-heap. At 2 GB
the same migration finished in **60 seconds**. On a host whose disk is also fairly full,
`cluster.routing.allocation.disk.threshold_enabled=false` is needed as well, or Elasticsearch
refuses to allocate shards above 90% and marks indices read-only above 95% — which presents as a
hung migration rather than a disk problem.

## Deployment choices

- **No Airflow.** The quickstart runs an Airflow scheduler as its "ingestion" service; this sets
  `PIPELINE_SERVICE_CLIENT_ENABLED=false` and drives ingestion from the `metadata` CLI in a one-shot
  container. One container and ~3 GB less. The cost: the *Ingestion* tab cannot schedule anything,
  so `make om-ingest` is how it runs.
- **The crawler image is `python:3.10-slim` + `openmetadata-ingestion`**, not Collate's ingestion
  image, which is 5.9 GB on this host because it bundles Airflow and every connector they ship.
- **Everything is behind compose profiles**, so `make up` is unchanged.
- **OpenMetadata's own login is local basic auth**, deliberately not the Keycloak realm. The realm
  models Polaris principals; keeping OpenMetadata's login separate stops the two authorization
  stories being mistaken for one another. The part that uses Keycloak is the part this PoC is about.

## Licence, which is not Apache-2.0

The OpenMetadata repository root is Apache-2.0, but `ingestion/` — where the revived files come
from — is under the **Collate Community License Agreement 1.0**. It grants use, modification,
derivative works and distribution, but not an "Excluded Purpose": offering a SaaS that competes
with Collate. Fine for this PoC. Original file headers are intact and the provenance, including the
exact upstream commit, is in `openmetadata-connector/NOTICE`.

## Status

| | |
|---|---|
| `make test-connector` | 18 passed |
| Keycloak client credentials | token carries the right `principal_name`, `principal_roles`, `aud` |
| AuthZEN decisions | read everywhere, write nowhere; bob unchanged |
| `make bootstrap` | clean, including the new principal, role, grants and bindings |
| `make om-ingest` | **Workflow Success 100%**, 8 records, 0 errors, 4.4s |
| Polaris | 1.8.0-SNAPSHOT (AuthZEN fork), unchanged from the parent branch |
| OpenMetadata | 2.0.2, mysql + elasticsearch + server, no Airflow |

What lands in the catalogue:

```
  service   polaris  (CustomDatabase)
  database  polaris.poc_catalog
  schemas   polaris.poc_catalog.shared, polaris.poc_catalog.restricted
  tables    shared.events        id: LONG,     kind: STRING
            restricted.salaries  person: STRING, amount: LONG
```

Both connection steps — `GetNamespaces` and `GetTables` — pass, which is the part that had to
route around the missing `TestConnectionDefinition`.

## If you pick this up again

- `make om-up` then `make om-ingest`. The first `om-up` migrates the database through every schema
  version since 1.5 and takes several minutes before the server is healthy.
- `make test-connector` needs only PyIceberg, so it runs on the host in about a second and is the
  fastest way to check the connector after a change.
- The two sharp edges above both present as a 403 that looks like policy. If the PDP answers `true`
  to curl and Polaris still refuses, restart Polaris.
