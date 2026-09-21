import { Callout, HTMLTable } from "@blueprintjs/core";
import { useAuth } from "react-oidc-context";
import { config } from "../config";

/** What this console is pointed at, and what it is holding. Useful when something is refused. */
export function AboutPage() {
  const auth = useAuth();
  const profile = auth.user?.profile as Record<string, unknown> | undefined;

  return (
    <div className="sparql-console-about">
      <Callout icon="key" title="This console holds no credential of its own">
        It is a public OIDC client: it has no secret, and the only token it ever sends is the one
        you signed in with. Neither does the endpoint behind it — Ontop forwards your token to
        Spark Connect, which exchanges it for one addressed to Polaris. Nothing in this chain can
        read data as anyone but you.
      </Callout>

      <HTMLTable className="sparql-console-table" striped>
        <tbody>
          <tr>
            <td>SPARQL endpoint</td>
            <td><code>{config.sparqlEndpoint}</code></td>
          </tr>
          <tr>
            <td>OIDC issuer</td>
            <td><code>{config.oidcIssuer}</code></td>
          </tr>
          <tr>
            <td>Client ID</td>
            <td><code>{config.oidcClientId}</code></td>
          </tr>
          <tr>
            <td>Redirect URI</td>
            <td><code>{config.oidcRedirectUri}</code></td>
          </tr>
          <tr>
            <td>Correlation header</td>
            <td><code>{config.correlationHeader}</code></td>
          </tr>
          <tr>
            <td>Signed in as</td>
            <td>
              <code>
                {auth.isAuthenticated
                  ? ((profile?.preferred_username as string | undefined) ?? "unknown")
                  : "nobody"}
              </code>
            </td>
          </tr>
          <tr>
            <td>Token audience</td>
            <td>
              <code>{auth.isAuthenticated ? audienceOf(auth.user?.access_token) : "-"}</code>
            </td>
          </tr>
        </tbody>
      </HTMLTable>
    </div>
  );
}

/**
 * The `aud` claim, read without verifying anything — this is a diagnostic, not a check.
 *
 * Worth surfacing because it is the single likeliest thing to be wrong: Spark Connect refuses a
 * token that is not addressed to it, and the realm needs an audience mapper on this client to
 * produce one that is.
 */
function audienceOf(jwt: string | undefined): string {
  if (!jwt) return "-";
  try {
    const payload = jwt.split(".")[1];
    const claims = JSON.parse(atob(payload.replace(/-/g, "+").replace(/_/g, "/")));
    const aud = claims.aud;
    return Array.isArray(aud) ? aud.join(", ") : String(aud ?? "-");
  } catch {
    return "unreadable";
  }
}
