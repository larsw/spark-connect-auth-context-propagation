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

## What it costs: the vended credential stops being per-user

One test is left **failing on purpose**, as `xfail(strict=True)` so it will shout if the fork
fixes it: `test_polaris_vends_distinct_temporary_credentials_per_user`.

Loading the same table as each user returns byte-identical credentials:

```
alice: access-key=2JBZC2XRQJEN... token-len=1183
bob  : access-key=2JBZC2XRQJEN... token-len=1183
same access key:    True
same session token: True
```

and the session policy embedded in both grants **write**:

```json
{"Effect": "Allow",
 "Action": ["s3:DeleteObject", "s3:PutObject"],
 "Resource": ["arn:aws:s3:::warehouse/poc/shared/events/*"]}
```

bob is a read-only analyst. On `main` he receives his own token, scoped to reads.

The catalog decision is still correct — bob is refused `restricted.salaries`, which is what the
demo shows. It is the **data plane** that stops differentiating, and that follows from what a PDP
is: it answers *allow or deny*, while Polaris previously shaped the vended credential from the
principal's resolved privilege **set**, which an external PDP never returns. The identical token
suggests the credential is cached per table rather than per principal, but that is inference from
the outside; the behaviour difference is the measured part.

This matters for this repository in particular, because its headline claim is that the only
credentials on the data path are the ones Polaris vended *for whichever user made the request*. On
this branch that is still true of the request, and no longer true of the user.

## Status

| | |
|---|---|
| Host suite | 43 passed, 1 skipped, 1 xfailed |
| `make bootstrap` | works, once Keycloak has a `root` user |
| `make demo` | unchanged output; refusal now from the PDP |
| Polaris | 1.8.0-SNAPSHOT (fork of `main`), against 1.7.0 on `main` |
| Keycloak | 26.7.0 with `--features=authzen` |

## If you pick this up again

- `make polaris-image` before `make up`; the image is built locally and is not on any registry.
- The PDP can be queried without Polaris in the picture, which is the quickest way to tell a
  policy problem from a Polaris problem. The curl recipe is at the end of
  [docs/authzen-pdp.md](docs/authzen-pdp.md).
- The open question worth answering before merging: whether per-principal credential scoping can
  be restored — either by the PDP returning more than a boolean, or by Polaris keeping its own
  privilege resolution for vending while delegating the access decision.
