# Cataloguing Polaris in OpenMetadata

This branch adds [OpenMetadata][om] to the stack and points it at the Polaris Iceberg catalog, so
the tables the demo creates show up with their schemas in a data catalogue.

The connector that would have done this no longer exists. OpenMetadata
[PR #26365][pr] deleted the built-in Iceberg service type in 2.0 — 36 files across the Python
ingestion, the JSON Schemas, the Java converters and the UI — and migrated existing Iceberg
services to `CustomDatabase`. `openmetadata-connector/` is that ingestion code revived as a custom
connector, brushed up, and aimed at Polaris.

[om]: https://docs.open-metadata.org/v2.0.x/quick-start/local-docker-deployment
[pr]: https://github.com/open-metadata/OpenMetadata/pull/26365

## Running it

```bash
make om-up        # mysql + elasticsearch + server, behind a compose profile (~4 GB)
make om-ingest    # build the crawler image and crawl Polaris once
```

Then <http://localhost:8585>, logging in as `admin@open-metadata.org` / `admin`. The catalogue is
under **Settings → Services → Databases → polaris**:

```
  service   polaris  (CustomDatabase)
  database  polaris.poc_catalog
  schemas   polaris.poc_catalog.shared, polaris.poc_catalog.restricted
  tables    shared.events        id: LONG,       kind: STRING
            restricted.salaries  person: STRING, amount: LONG
```

The first `make om-up` migrates the database through every schema version since 1.5 and seeds the
entity types, so allow several minutes before the server is healthy. `make om-ingest` itself takes
about five seconds.

`make om-down` stops just those three containers and leaves the rest of the stack running.

The OpenMetadata services are on the `openmetadata` compose profile, so `make up` does not start
them. That is on purpose: they are another four containers and roughly 4 GB, and nothing else in
the PoC needs them.

## How it reaches Polaris

**OAuth 2.0, client credentials, issued by Keycloak** — the same Keycloak that issues alice's and
bob's tokens and that acts as Polaris's PDP.

```
  openmetadata-ingest                Keycloak                      Polaris
         |                              |                             |
         |-- client_credentials ------->|                             |
         |    client_id=openmetadata    |                             |
         |<-- access_token -------------|                             |
         |    principal_name=service-account-openmetadata             |
         |    principal_roles=[metadata_reader], aud=polaris          |
         |                              |                             |
         |-- GET /v1/.../namespaces (Bearer) ----------------------->  |
         |                              |<-- AuthZEN evaluation ------|
         |                              |--- decision: true --------->|
         |<-- namespaces --------------------------------------------|
```

Three names have to line up, and they are not free choices:

| | |
|---|---|
| Keycloak client | `openmetadata` |
| Keycloak service account user | `service-account-openmetadata` |
| `principal_name` claim | `service-account-openmetadata` |
| Polaris principal | `service-account-openmetadata` |
| AuthZEN `subject.id` | `username:service-account-openmetadata` |

Keycloak always names a client's service account user `service-account-<clientId>`; Polaris
resolves its principal **by name** from the `principal_name` claim and requires it to pre-exist
(§8 of [FINDINGS.md](../FINDINGS.md)); and Keycloak-as-PDP resolves `subject.id` back to one of its
own users. Making the claim anything prettier breaks one of the three.

`docker/keycloak/add-openmetadata-client.py` adds all of the Keycloak side and is idempotent;
`docker/polaris/bootstrap.sh` creates the matching Polaris principal, role and grants.

### What it may do

`metadata_reader` gets the same 30 read-only scopes bob has — the set with every `*WRITE*`
operation excluded — but on **every** resource rather than just `shared`:

```
  service-account-openmetadata  LIST_TABLES                       restricted  -> true
  service-account-openmetadata  LOAD_TABLE                        salaries    -> true
  service-account-openmetadata  LOAD_TABLE_WITH_READ_DELEGATION   salaries    -> true
  service-account-openmetadata  LOAD_TABLE_WITH_WRITE_DELEGATION  salaries    -> false
  service-account-openmetadata  UPDATE_TABLE                      salaries    -> false
  service-account-openmetadata  DROP_TABLE                        salaries    -> false
```

That is the interesting contrast with bob: the crawler can describe the *shape* of
`restricted.salaries` while being unable to write anything anywhere, and bob still cannot read it
at all. A catalogue needs to see more than any one user, and less than a writer.

The scope set is copied out of `bob-may-read-shared` by the generator rather than restated, so the
two cannot drift — and the script refuses to run if that policy ever gains a WRITE scope. That is
the bug from the AuthZEN branch (`LOAD_TABLE_WITH_WRITE_DELEGATION` starts with `LOAD_`) wired
shut.

## The connector

`openmetadata-connector/`, installed into the crawler image, registered as:

```yaml
sourcePythonClass: polaris_iceberg.metadata.PolarisIcebergSource
```

The topology — database, schema, table, columns, partitions — is the original code. What changed:

- **Configuration comes from `connectionOptions`.** The service type's JSON Schema
  (`IcebergConnection`, `IcebergCatalog`, `RestCatalogConnection`) was deleted with the connector,
  so `polaris_iceberg/config.py` is its replacement: it validates the untyped string map the custom
  service hands over. Option names are matched ignoring case and separators, so `clientId`,
  `client_id` and `client-id` all work.
- **REST only.** The Glue, Hive and DynamoDB catalogs and the factory that chose between them are
  gone; they existed so one service type could cover every Iceberg deployment.
- **PyIceberg 0.12** instead of the pinned 0.5.1, which is three years old. Every call the
  connector makes still exists; two SigV4 property names did not.
- **Three fixed bugs**, described in §18 of [FINDINGS.md](../FINDINGS.md).

Anything with a dot in its name is handed to PyIceberg untouched — `s3.endpoint`,
`header.X-Correlation-Id`, the `auth.*` keys — so a catalogue needing something this connector does
not model stays usable without a code change.

`make test-connector` runs the unit tests; they need only PyIceberg, not the OpenMetadata
framework, so they run on the host in about a second. One of them builds PyIceberg's real OAuth2
auth manager from our configuration, so a renamed key fails a test rather than a request.

## Notes

- **No Airflow.** The quickstart runs an Airflow scheduler as its "ingestion" service; this stack
  sets `PIPELINE_SERVICE_CLIENT_ENABLED=false` and drives ingestion from the `metadata` CLI in a
  one-shot container. That drops a container and about 3 GB. The cost is that the *Ingestion* tab
  in the OpenMetadata UI cannot schedule anything — run `make om-ingest` instead.
- **The crawler image is built from `python:3.10-slim`**, not from
  `docker.getcollate.io/openmetadata/ingestion`, which is 5.9 GB because it bundles Airflow and
  every connector Collate ships.
- **OpenMetadata's own login is local basic auth**, deliberately not wired to the Keycloak realm.
  The realm models Polaris principals; giving OpenMetadata its own login keeps the two
  authorization stories from being mistaken for one another. The part that *does* use Keycloak is
  the crawler's connection to Polaris, which is the part this PoC is about.
- **Changing the realm no longer requires restarting Polaris.** It used to: Polaris cached its PDP
  token and treated the resulting 401 as a deny, so recreating Keycloak killed authorization until
  Polaris was bounced. Fixed by a patch carried on top of the pinned fork commit —
  `docker/polaris-authzen/patches/`, and §16 of [FINDINGS.md](../FINDINGS.md).
- **Elasticsearch gets 2 GB for a 512 MB heap**, because Lucene is off-heap. At 1 GB it sat at 98%
  and the migration took more than fifteen minutes without finishing; at 2 GB it takes 60 seconds.
  Its disk watermarks are disabled too: this host's disk is 94% full, and above 90% Elasticsearch
  stops allocating shards, which looks like a hung migration rather than a full disk.
- **The ingestion-bot's JWT comes from `/api/v1/users/auth-mechanism/{id}`.** The more obvious
  `/api/v1/users/{id}?fields=authenticationMechanism` is accepted on 2.0.2 and answers with `null`
  for every field. Set `OM_JWT_TOKEN` to skip the lookup entirely.

## Licence

The revived code is derived from OpenMetadata's `ingestion/` tree, which is under the **Collate
Community License 1.0**, not Apache-2.0 like the repository root. It permits use, modification,
derivative works and distribution, but not building a competing SaaS. The original file headers are
kept and the provenance is recorded in `openmetadata-connector/NOTICE`.
