import { WebStorageStateStore, type UserManagerSettings } from "oidc-client-ts";
import { config } from "./config";

/**
 * OIDC settings for the authorization-code + PKCE flow.
 *
 * The console is a public client: it holds no secret, and the token it receives is the *user's*,
 * which is the entire point — it is forwarded to the SPARQL endpoint, which forwards it to Spark
 * Connect, which decides what that user may read.
 */
export const oidcConfig: UserManagerSettings = {
  authority: config.oidcIssuer,
  client_id: config.oidcClientId,
  redirect_uri: config.oidcRedirectUri,
  post_logout_redirect_uri: new URL("/", config.oidcRedirectUri).toString(),
  scope: config.oidcScope,
  response_type: "code",

  // sessionStorage, not localStorage: a token is a bearer credential, and one browser tab's
  // sign-in has no business outliving it in another.
  userStore: new WebStorageStateStore({ store: window.sessionStorage }),

  // Keycloak's default access token lives five minutes. Renew quietly rather than dropping a
  // query the user is halfway through writing.
  automaticSilentRenew: true,
  monitorSession: false,
};

/** Strips `?code=...&state=...` once the redirect has been consumed. */
export function onSigninCallback(): void {
  window.history.replaceState({}, document.title, "/");
}
