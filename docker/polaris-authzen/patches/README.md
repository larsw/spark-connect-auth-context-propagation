# Patches applied on top of the pinned fork commit

**There are currently none.** This directory is the mechanism, kept because the last patch proved
it earns its keep.

`build.sh` resets the checkout to `POLARIS_FORK_REF` and then applies every `*.patch` here, in name
order, before building. Patches live here rather than in the fork so that it stays obvious which
part is [larsw/polaris][fork]'s and which is this PoC's — and so that moving the pin is a decision
rather than an accident.

To add one: change the checkout under `$POLARIS_BUILD_DIR`, `git diff > patches/NNNN-name.patch`,
and re-run `make polaris-image`. To refresh one after moving the pin, apply it to a fresh checkout,
fix the rejects and regenerate it.

[fork]: https://github.com/larsw/polaris/tree/feat/authzen-pdp-support

## History

### 0001 — refresh the PDP token when the PDP rejects it (now upstream)

Carried here while it was being written and tested; folded into the fork and the pin moved from
`c71717a` to [`24702b2`][commit], so the patch is gone rather than applied twice.

**The bug.** Polaris fetched an OAuth2 client-credentials token to call the AuthZEN PDP and cached
it, refreshing on a schedule derived from the token's expiry. Nothing else could invalidate it. So
when the PDP's signing keys were rotated — which recreating the Keycloak container does, and which
is how a realm change is applied here — Polaris kept presenting a token that had not expired and
was no longer accepted. Every evaluation came back `401`, and `AuthzenPdpClient` treated *any*
non-200 as a deny:

```
WARN  AuthzenPdpClient: AuthZEN PDP at http://keycloak:8080/... returned unexpected
      HTTP status 401, treating as deny: {"error":"HTTP 401 Unauthorized"}
INFO  IcebergExceptionMapper: Handling runtimeException AuthZEN PDP denied authorization
```

Authorization was then dead until Polaris was restarted, and every request failed closed with a
message indistinguishable from a real policy decision — while the same question put to the PDP
directly with `curl` answered `true`. It presented as `make bootstrap` dying on its first call.

**Why it mattered beyond a container restart.** Key rotation, a revoked client session and clock
skew all produce the same thing. Failing closed when the PDP cannot be consulted is a defensible
choice; never recovering is not, because it converts an ordinary credential lifecycle event into a
permanent outage that only a restart clears.

**The fix**, as merged:

- `BearerTokenProvider` grows `invalidate()`, defaulting to a no-op — the way a consumer says "what
  you gave me was refused", which a schedule based on expiry cannot know.
- `ClientCredentialsTokenProvider` implements it: fetch a replacement now, then re-aim the
  scheduled refresh at the new expiry. Guarded so a rejection seen by many in-flight requests at
  once causes one token request, and rate-limited to one forced fetch per `refreshRetryInterval` so
  a credential that is simply *wrong* cannot become a flood at the token endpoint.
- `AuthzenPdpClient` distinguishes `401` from every other non-200 and, on one, replaces the token
  and asks again exactly once. If the fresh token is refused too it fails closed as before. `403`
  is deliberately **not** retried: a PDP may legitimately use it to say this client may not ask at
  all, and retrying would not fix that.

Both the evaluation calls and endpoint discovery go through the same retry.

**Tests.** `StubAuthzenPdp` gained a queue of one-shot responses, so a `401` can be followed by a
`200` — its single sticky response per endpoint could not express the case the retry exists for.
`AuthzenPdpClientTest` asserts the token is replaced once, that the two attempts carry *different*
`Authorization` headers, that a permanently-refused credential fails closed after exactly one
retry, and that a `403` is not retried at all. The first two fail against the old behaviour and
pass with the fix.

**End to end**, independently of the unit tests: recreate the Keycloak container and then use the
stack without restarting anything. `make test-container` passes, and the Polaris log shows one
recovery cycle rather than a wall of denials.

[commit]: https://github.com/larsw/polaris/commit/24702b214ae07fe738eca968e5165dffce3662ed
