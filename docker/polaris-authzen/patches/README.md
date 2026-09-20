# Patches applied on top of the pinned fork commit

`build.sh` resets the checkout to `POLARIS_FORK_REF` and then applies every `*.patch` here, in
name order, before building. They live here rather than in the fork so that it stays obvious which
part is [larsw/polaris][fork]'s and which is this PoC's — and so that moving the pin is a decision
rather than an accident.

To drop one, delete the file. To refresh one after moving the pin, apply it to a fresh checkout,
fix the rejects, and regenerate with `git diff`.

[fork]: https://github.com/larsw/polaris/tree/feat/authzen-pdp-support

## 0001 — refresh the PDP token when the PDP rejects it

**The bug.** Polaris fetches its own OAuth2 client-credentials token to call the AuthZEN PDP and
caches it, refreshing on a schedule derived from the token's expiry. Nothing else can invalidate
it. So when the PDP's signing keys are rotated — which recreating the Keycloak container does,
because `--import-realm` only imports into an empty database — Polaris keeps presenting a token
that is not yet expired and is no longer accepted. Every evaluation comes back `401`, and
`AuthzenPdpClient` treated *any* non-200 as a deny:

```
WARN  AuthzenPdpClient: AuthZEN PDP at http://keycloak:8080/... returned unexpected
      HTTP status 401, treating as deny: {"error":"HTTP 401 Unauthorized"}
INFO  IcebergExceptionMapper: Handling runtimeException AuthZEN PDP denied authorization
```

Authorization is then dead until Polaris is restarted, and every request fails closed with a
message indistinguishable from a real policy decision — while the same question put to the PDP
directly with `curl` answers `true`. In this PoC it presented as `make bootstrap` dying on its
first call.

**Why it matters beyond a container restart.** Key rotation, a revoked client session and a
clock skew all produce the same thing. Failing closed when the PDP cannot be consulted is a
defensible choice; never recovering is not, because it converts an ordinary credential lifecycle
event into a permanent outage that only a restart clears.

**The fix.**

- `BearerTokenProvider` grows `invalidate()`, defaulting to a no-op — the way a consumer says "what
  you gave me was refused", which a schedule based on expiry cannot know.
- `ClientCredentialsTokenProvider` implements it: fetch a replacement now, then re-aim the
  scheduled refresh at the new expiry. Guarded so that a rejection seen by many in-flight requests
  at once causes one token request, and rate-limited to one forced fetch per `refreshRetryInterval`
  so that a credential which is simply *wrong* cannot turn into a flood at the token endpoint.
- `AuthzenPdpClient` distinguishes `401` from every other non-200 and, on one, replaces the token
  and asks again exactly once. If the fresh token is refused too, it fails closed as before.
  `403` is deliberately **not** retried: a PDP may legitimately use it to say this client may not
  ask at all, and retrying would not fix that.

Both the evaluation calls and endpoint discovery go through the same retry.

**If this is upstreamed**, it should carry a unit test. The natural place is
`AuthzenPdpClientTest`, and it needs `StubAuthzenPdp` to grow a one-shot scripted response (it
currently holds one sticky response per endpoint) so a `401` can be followed by a `200`. That was
left out here because the PoC verifies the behaviour end to end instead: recreate the Keycloak
container, then call Polaris, and watch it recover on its own.
