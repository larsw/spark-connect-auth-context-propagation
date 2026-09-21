import { useEffect, useRef } from "react";
import Yasgui from "@triply/yasgui";
import { useAuth } from "react-oidc-context";
import { config } from "../config";
import { nextCorrelationId } from "../correlation";

import "@triply/yasgui/build/yasgui.min.css";

/**
 * YASGUI, wired to send the signed-in user's own token.
 *
 * Two things make this more than an embed:
 *
 *   * `headers` is a **function**, evaluated per request rather than captured once. The access
 *     token is renewed silently every few minutes, so a captured one would start failing mid
 *     session — and a fresh correlation ID has to be minted per query, not per page load.
 *
 *   * the token is read from a ref rather than closed over, because YASGUI is constructed once
 *     for the life of the component and React would otherwise hand it a stale `auth`.
 */
export function YasguiPanel() {
  const auth = useAuth();
  const container = useRef<HTMLDivElement>(null);
  const yasgui = useRef<Yasgui | null>(null);

  const accessToken = useRef<string | undefined>(undefined);
  accessToken.current = auth.user?.access_token;

  useEffect(() => {
    if (!container.current || yasgui.current) return;

    yasgui.current = new Yasgui(container.current, {
      requestConfig: {
        endpoint: config.sparqlEndpoint,
        method: "POST",
        headers: () => {
          const headers: Record<string, string> = {
            [config.correlationHeader]: nextCorrelationId(),
          };
          // Anonymous requests are left unsigned deliberately: the endpoint refuses them, and
          // seeing that refusal is more useful than a console that hides the button.
          if (accessToken.current) {
            headers.Authorization = `Bearer ${accessToken.current}`;
          }
          return headers;
        },
      },
      // One endpoint, fixed by the deployment: this console is not a general SPARQL client, and
      // pointing it elsewhere would send the user's token to whatever was typed in.
      copyEndpointOnNewTab: false,
      endpointCatalogueOptions: { getData: () => [] },
      persistenceId: "ontop-sparql-console",
    });

    return () => {
      yasgui.current?.destroy();
      yasgui.current = null;
    };
  }, []);

  return <div className="sparql-console-yasgui" ref={container} />;
}
