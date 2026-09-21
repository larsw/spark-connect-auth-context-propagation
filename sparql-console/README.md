# SPARQL console

A browser front end for the Ontop endpoint. React, React Router and YASGUI, with Blueprint for the
widgets, built by bun and Vite and served by nginx.

<http://localhost:3002> once the stack is up. Sign in as `alice`/`alice` or `bob`/`bob`.

## What it is for

The same demonstration as `make demo-sparql`, with a cursor in it. Sign in as alice, ask for the
salaries, get two rows. Sign out, sign in as bob, ask the *same question in the same console*, and
Polaris refuses him by name:

```
Forbidden: Principal 'bob' with activated PrincipalRoles '[analyst]' ...
is not authorized for op LOAD_TABLE_WITH_READ_DELEGATION
```

Nothing about the console changed between those two queries. What changed is the token on the
connection Ontop opened.

## How it holds nothing

It is a public OIDC client: no secret, authorization code + PKCE, and the access token lives in
`sessionStorage` rather than `localStorage` so one tab's sign-in does not outlive it in another.
The token goes onto each request as `Authorization: Bearer …` and nowhere else. Ontop then puts it
on the JDBC connection for that request, Spark Connect validates it and exchanges it for one
addressed to Polaris.

So there are three things in a row that hold no credential for the data — this console, the Ontop
endpoint, and Spark — and one that does: Polaris, on the user's behalf.

## Correlation

Every query carries a fresh `X-Correlation-ID`, minted in the browser. The navbar shows its first
eight characters; click to copy the whole thing, then:

```bash
make cid CID=<the value>
```

prints every line mentioning it from Spark Connect and Polaris. Ontop adopts it as its own query
id (`ontop.queryIdHttpHeader`), which is why it survives the hop rather than being replaced.

It has to be a UUID: Ontop only adopts the header when it parses as one, and otherwise mints its
own, which breaks the chain in the middle.

## Two things worth knowing

**Open it on `localhost`, never on `sparql-console:3002`.** PKCE S256 hashes its verifier with
`crypto.subtle`, which browsers expose only in a secure context — HTTPS, or plain HTTP on
`localhost`. The test is on the hostname you typed, so a loopback alias does not qualify. The
container says so on the page rather than letting Sign in fail with
`Cannot read properties of undefined (reading 'digest')`; see `entrypoint.sh`.

**Sign out really signs out.** It is an RP-initiated logout that ends the Keycloak session, not
just a local token drop. Dropping the token alone looks tidier and is useless here: the SSO
session survives, the next sign-in returns the same user without showing a login form, and
comparing alice with bob is the whole point.

## Running it

```bash
make up                 # the console comes up with everything else, on :3002
make seed               # alice creates the tables the mapping needs
make console            # or: the Vite dev server on the same port, with HMR
```

`make console` needs bun on the host (`./install.sh` reports it). It reads the same defaults as
the container, so it talks to the same endpoint and realm.

## Layout

```
src/config.ts              runtime settings, from window.APP_CONFIG with dev fallbacks
src/auth.ts                OIDC client settings
src/correlation.ts         the per-query id, and the store the navbar subscribes to
src/components/Shell.tsx   navbar, sign in/out, correlation tag
src/components/YasguiPanel.tsx   YASGUI, and the per-request header callback
src/routes/                query, callback and about pages
entrypoint.sh              writes /config.js at container start, plus the secure-context guard
```

The interesting file is `YasguiPanel.tsx`. YASGUI takes `requestConfig.headers` as a *function*,
evaluated per request, which is what lets a silently-renewed token and a fresh correlation ID both
be right on every query rather than only on the first.
