import { useEffect } from "react";
import { NonIdealState, Spinner } from "@blueprintjs/core";
import { useNavigate } from "react-router";
import { useAuth } from "react-oidc-context";

/**
 * Where Keycloak sends the browser back.
 *
 * react-oidc-context consumes the `code` and `state` from the URL on its own, so there is nothing
 * to do here but wait for it and get out of the way. The route exists because the redirect URI
 * has to be registered with the realm as an exact path.
 */
export function CallbackPage() {
  const auth = useAuth();
  const navigate = useNavigate();

  useEffect(() => {
    if (auth.isLoading) return;
    void navigate("/", { replace: true });
  }, [auth.isLoading, auth.isAuthenticated, navigate]);

  if (auth.error) {
    return (
      <NonIdealState icon="error" title="Sign-in failed" description={auth.error.message} />
    );
  }

  return <NonIdealState icon={<Spinner />} title="Completing sign-in" />;
}
