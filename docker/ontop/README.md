# Ontop VKG, with the caller's identity attached

A SPARQL endpoint over the same two Iceberg tables. The point is not that SPARQL works — it is
that a SPARQL question asked by bob is refused by Polaris for the same reason his SQL was, because
the query reaches Spark Connect as *bob* and not as the endpoint.

```
alice ──HTTP──▶ Ontop ──gRPC──▶ Spark Connect ──REST──▶ Polaris ──STS──▶ MinIO
      Authorization: Bearer <alice's token>
      X-Correlation-ID: <uuid>
                  │
                  └── jdbc:sc://spark-connect:15002/;user_id=<alice's sub>
                          ;x-user-token=<alice's token>;x-correlation-id=<the same uuid>
```

## Why stock Ontop cannot do this

Ontop compiles SPARQL into SQL and runs it over JDBC. It knows perfectly well who asked — its
`QueryContext` has carried the request's HTTP headers, user, roles and groups for a while — but the
database never finds out. The connection comes from a process-wide pool built once from
`jdbc.url`, `jdbc.user` and `jdbc.password`, so every caller arrives as the same account.

Behind a private database that is fine: Ontop is then the only place access is decided, and the
mapping and the lenses are where you decide it. In front of a catalog that authorises end users
itself, it throws that catalog's access control away. Every SPARQL caller would be one service
account to Polaris, and the answer to "who read the salaries" would be "the SPARQL endpoint".

## What the fork changes

Branch [`version5-auth-context`](https://github.com/larsw/ontop/tree/version5-auth-context), three
changes, all opt-in and all default methods so nothing downstream has to move:

1. **The caller's context reaches connection acquisition.** `getConnection(QueryContext)` on
   `DBConnector`, `OntopQueryEngine` and `JDBCConnectionPool`, plus
   `OntopRepository.getConnection(httpHeaders)` for the endpoint — the connection is opened before
   the query is prepared, so the headers are all there is at that moment. `QuestStatement` then
   evaluates the query under that same context instead of minting a second one, which is what keeps
   the query id in Ontop's log equal to the one on the connection.

2. **`ContextPropagatingJDBCConnectionPool`**, which opens a connection per request from a
   template: `{bearer}`, `{claim:sub}`, `{header:...}`, `{queryId}`, `{user}`, `{roles}`,
   `{groups}`. An unresolved placeholder fails the request rather than expanding to an empty
   string, and a value containing a separator is rejected rather than escaped. It deliberately does
   not pool — a pooled connection carries the credentials of whoever opened it.

3. **`ontop.queryIdHttpHeader`**, which lets the `QueryContext` adopt the caller's correlation ID
   as its query id, so one UUID covers the client, Ontop's query log, Spark Connect and Polaris.

Full write-up: `documentation/context-propagation.md` in the fork.

## What makes it work on this side

The [Spark Connect JDBC driver](https://mvnrepository.com/artifact/org.apache.spark/spark-connect-client-jdbc)
(`jdbc:sc://`, new in Spark 4.1). Its connection string takes arbitrary parameters and turns every
one it does not recognise into **gRPC metadata** — which is exactly where
`UserTokenServerInterceptor` reads `x-user-token` and `x-correlation-id` from. The parameters it
*does* recognise matter too:

| Parameter | Meaning |
|---|---|
| `user_id` | the Connect session's owner; the server refuses it unless it equals the authenticated subject, which is why the template reads `{claim:sub}` |
| `token` | **not used here.** It looks like the way to set Spark's pre-shared key and is not: the Scala client only puts it on the wire when the target is local, and otherwise switches the channel to TLS, which this plaintext server does not speak. The key goes on `authorization=Bearer%20…` as ordinary metadata instead |
| anything else | gRPC metadata |

So `{claim:sub}` is not an authorisation decision — it addresses the session. Spark Connect
verifies the very token it was read from, and refuses the call if the two disagree.

## The endpoint holds no credential

Ontop needs a table's columns before it can compile SPARQL into SQL, and it does that at start-up,
when no caller exists. Left to introspect, it would need a standing identity of its own — and in
this stack that identity would need real read rights, because Spark asks Polaris to vend storage
credentials on *every* `loadTable`, and Polaris authorises that as `LOAD_TABLE_WITH_READ_DELEGATION`.
There is no "schema only" to grant. (Tried it: a principal with `TABLE_READ_PROPERTIES` and no
`TABLE_READ_DATA` is refused before it can see a single column.)

So the definitions are pinned in `db-metadata.json` instead. Given one, Ontop opens no connection
at start-up at all, and the only token that ever reaches Spark Connect is the caller's own.
Introspection still happens — deliberately, by someone who already has the rights, and the result
is reviewed and committed:

```bash
make ontop-metadata     # re-extracts it live over Spark Connect, as alice
```

`jdbc.url` is still configured, for anything that might take a connection outside a request, and
deliberately carries no user token: Spark Connect refuses it rather than serving it as somebody.

## Running it

```bash
make build           # builds this image from the pinned fork ref (slow the first time)
make up
make seed            # alice creates the tables, through Spark Connect
make demo-sparql     # alice and bob ask the same questions
```

The endpoint is on <http://localhost:8090>; `/sparql` wants `Authorization: Bearer <token>` and
honours `X-Correlation-ID`. It starts whether or not the tables exist — it never looks — but a
query before `make seed` fails at Spark, saying the table is not there.

To build against a different fork or ref:

```bash
ONTOP_REF=my-branch docker compose build ontop   # a branch or a commit
```
