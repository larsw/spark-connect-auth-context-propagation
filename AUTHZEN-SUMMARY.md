# AuthZEN PDP branch — what was done, and what it turned up

Branch `feat/authzen-pdp-support`, 2026-09-20. Not merged to `main`.

Swaps Apache Polaris 1.7.0 for **[larsw/polaris @ `feat/authzen-pdp-support`][fork]** (one commit
on top of Polaris `main`, pinned at `c71717a`) and turns on Keycloak's experimental `authzen`
feature, so the same Keycloak that issues the tokens also decides what they may do.

The mechanics — configuration, policy model, how to run it — are in
**[docs/authzen-pdp.md](docs/authzen-pdp.md)**. This file is the shorter story: what happened, and
what is worth knowing before deciding whether to merge it.

[fork]: https://github.com/larsw/polaris/tree/feat/authzen-pdp-support

## It works

`make demo` prints exactly what it always did — both users read `shared.events`, alice reads
`restricted.salaries`, bob does not. The only visible difference is where the refusal comes from:

```
DENIED  bob  : SELECT * FROM polaris.restricted.salaries
          (org.apache.iceberg.exceptions.ForbiddenException) Forbidden: AuthZEN PDP denied
          authorization
```

That sentence used to be Polaris evaluating its own grants. It is now a policy decision made in
Keycloak and returned over the OpenID AuthZEN Authorization API.

There is no published image for the fork, so `make polaris-image` builds it from the pinned
commit — the same approach `docker/polaris-console/` already uses for the Polaris console. Gradle
plus Quarkus, about two minutes on a warm cache.

## Three things the Keycloak side needed

1. **All 119 Polaris operations registered as scopes**, and every entity the bootstrap touches
   registered as a resource. Keycloak matches `action.name` against scopes and `resource.id`
   against resources, and an unregistered resource id is indistinguishable from a deny — so this
   was front-loaded rather than discovered one failure at a time.
2. **The resource server set to `AFFIRMATIVE`.** Keycloak defaults it to `UNANIMOUS`, where two
   permissions covering the same (resource, scope) pair contradict each other and deny everyone.
   The fork's documentation warns about this, and it is real.
3. **A credential-less `root` user in the realm.** The bootstrap runs as Polaris's internal root
   principal, and with an external PDP even that decision goes to Keycloak. Without a Keycloak
   user for a policy to name, `make bootstrap` dies on its first call with
   `AuthZEN PDP denied authorization`.

## Per-user credentials: two settings that fail silently

Credentials are per-user and correctly scoped — alice can write the table, bob can only read it —
but getting there took two fixes, and **an earlier draft of this branch shipped with both wrong and
blamed the fork for it. That was incorrect.** Both are configuration.

**1. `INCLUDE_PRINCIPAL_NAME_IN_SUBSCOPED_CREDENTIAL` was off.** Polaris caches vended credentials
on a key that includes the STS session name, and that name is `polaris` for everyone by default. Two
principals with matching location grants therefore share a cache entry and are handed a
byte-identical access key and session token. Turning it on makes the name `polaris-alice` /
`polaris-bob`; Polaris's own docs describe the loss of cache reuse as the cost, which is exactly the
property wanted here.

**2. The Keycloak read policy granted `LOAD_TABLE_WITH_WRITE_DELEGATION`.** On a delegated load
Polaris asks the PDP for *write* delegation first and only falls back to read when that is refused,
so the credential's scope is decided by the policy. The realm generator picked bob's scopes with a
name heuristic — anything starting `LIST_`, `LOAD_` or `GET_` — and that prefix matches
`LOAD_TABLE_WITH_WRITE_DELEGATION`. bob held a write credential for a table he may only read, and
every test still passed, because the only test looking at credentials compared tokens for
*inequality*.

The result, read out of the session policy MinIO embeds in each token:

```
alice: access-key=F0OEK0HW91LQ...   s3:PutObject, s3:DeleteObject, s3:GetObject, ...
bob  : access-key=CZBNUKU1JKZ5...   s3:GetObject, s3:GetObjectVersion, s3:ListBucket
same access key: False    same session token: False
```

`test_the_vended_credential_is_scoped_to_what_the_user_may_do` now asserts the scope rather than
just the distinctness, which is what would have caught the second bug.

The general lesson, which is not specific to this fork: moving the authorization decision to an
external PDP also moves the decision about how much the vended credential may do. A policy that is
merely *generous* about read-ish operation names silently becomes a policy that hands out write
credentials.

## Status

| | |
|---|---|
| Host suite | 45 passed, 1 skipped |
| `make bootstrap` | works, once Keycloak has a `root` user |
| `make demo` | unchanged output; refusal now from the PDP |
| Polaris | 1.8.0-SNAPSHOT (fork of `main`), against 1.7.0 on `main` |
| Keycloak | 26.7.0 with `--features=authzen` |

## If you pick this up again

- `make polaris-image` before `make up`; the image is built locally and is not on any registry.
- The PDP can be queried without Polaris in the picture, which is the quickest way to tell a
  policy problem from a Polaris problem. The curl recipe is at the end of
  [docs/authzen-pdp.md](docs/authzen-pdp.md).
- The two credential settings above are the sharp edges. Both fail quietly: the first produces
  identical credentials, the second produces over-privileged ones, and neither shows up as an
  error anywhere.
