/**
 * Where this console points, resolved at runtime rather than baked in at build time.
 *
 * The container writes `window.APP_CONFIG` on every start (see docker/entrypoint.sh), which is
 * what lets one image serve any deployment. `bun run dev` has no such file, so the Vite env
 * fills in, and failing that the defaults below match the compose stack.
 *
 * Every URL here is resolved by the BROWSER, not by the container serving this page. That is why
 * they are localhost and published ports rather than compose service names.
 */
declare global {
  interface Window {
    APP_CONFIG?: Record<string, string | undefined>;
  }
}

function setting(name: string, fallback: string): string {
  const runtime = window.APP_CONFIG?.[name];
  if (runtime !== undefined && runtime !== "") return runtime;

  const build = import.meta.env[name as keyof ImportMetaEnv] as string | undefined;
  if (build !== undefined && build !== "") return build;

  return fallback;
}

export const config = {
  /** The Ontop VKG endpoint. Queries are POSTed to `${sparqlEndpoint}`. */
  sparqlEndpoint: setting("VITE_SPARQL_ENDPOINT", "http://localhost:8090/sparql"),

  /** Keycloak realm. The issuer must match what the token carries, byte for byte. */
  oidcIssuer: setting("VITE_OIDC_ISSUER_URL", "http://keycloak:8080/realms/spark"),
  oidcClientId: setting("VITE_OIDC_CLIENT_ID", "sparql-console"),
  oidcRedirectUri: setting("VITE_OIDC_REDIRECT_URI", "http://localhost:3002/auth/callback"),
  oidcScope: setting("VITE_OIDC_SCOPE", "openid profile email"),

  /** Shown in the UI so a correlation ID can be pasted straight into `make cid`. */
  correlationHeader: setting("VITE_CORRELATION_HEADER", "x-correlation-id"),
} as const;
