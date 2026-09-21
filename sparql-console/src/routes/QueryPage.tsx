import { Callout, NonIdealState, Spinner } from "@blueprintjs/core";
import { useAuth } from "react-oidc-context";
import { YasguiPanel } from "../components/YasguiPanel";
import { config } from "../config";

const SAMPLE = `PREFIX : <http://example.org/poc#>
SELECT ?event ?kind WHERE { ?event a :Event ; :kind ?kind }`;

export function QueryPage() {
  const auth = useAuth();

  if (auth.isLoading) {
    return <NonIdealState icon={<Spinner />} title="Checking your session" />;
  }

  if (auth.error) {
    return (
      <NonIdealState
        icon="error"
        title="Could not sign you in"
        description={auth.error.message}
      />
    );
  }

  return (
    <>
      {!auth.isAuthenticated && (
        <Callout
          className="sparql-console-callout"
          intent="warning"
          icon="user"
          title="Not signed in"
        >
          Queries will be sent without a token and the endpoint will refuse them — it holds no
          credential of its own to fall back on. Sign in to query as yourself.
        </Callout>
      )}

      <Callout className="sparql-console-callout" icon="info-sign" title="What this is">
        Every query below is compiled to Spark SQL by Ontop and run over Spark Connect{" "}
        <strong>as you</strong>. Apache Polaris decides what you may read, so
        <code> alice</code> sees the restricted namespace and <code>bob</code> does not — same
        console, same mapping, same query. Endpoint: <code>{config.sparqlEndpoint}</code>.
        <pre className="sparql-console-sample">{SAMPLE}</pre>
      </Callout>

      <YasguiPanel />
    </>
  );
}
