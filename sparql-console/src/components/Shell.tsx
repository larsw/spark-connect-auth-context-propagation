import {
  Alignment,
  AnchorButton,
  Button,
  Navbar,
  Tag,
  Tooltip,
} from "@blueprintjs/core";
import { Link, Outlet, useLocation } from "react-router";
import { useAuth } from "react-oidc-context";
import { useCorrelationId } from "../correlation";

/** The name to show for the signed-in user, preferring what Polaris will know them as. */
function displayName(profile: Record<string, unknown> | undefined): string {
  if (!profile) return "signed in";
  return (
    (profile.preferred_username as string | undefined) ??
    (profile.name as string | undefined) ??
    (profile.sub as string | undefined) ??
    "signed in"
  );
}

export function Shell() {
  const auth = useAuth();
  const location = useLocation();
  const correlationId = useCorrelationId();

  return (
    <div className="sparql-console-shell">
      <Navbar>
        <Navbar.Group align={Alignment.START}>
          <Navbar.Heading>
            <Link className="sparql-console-brand" to="/">
              SPARQL console
            </Link>
          </Navbar.Heading>
          <Navbar.Divider />
          <Link to="/">
            <Button
              variant="minimal"
              icon="search-template"
              text="Query"
              active={location.pathname === "/"}
            />
          </Link>
          <Link to="/about">
            <Button
              variant="minimal"
              icon="info-sign"
              text="About"
              active={location.pathname === "/about"}
            />
          </Link>
        </Navbar.Group>

        <Navbar.Group align={Alignment.END}>
          {correlationId && (
            <Tooltip
              content={`Trace it: make cid CID=${correlationId}`}
              placement="bottom"
            >
              <Tag
                minimal
                interactive
                icon="link"
                onClick={() => void navigator.clipboard?.writeText(correlationId)}
              >
                {correlationId.slice(0, 8)}
              </Tag>
            </Tooltip>
          )}
          <Navbar.Divider />

          {auth.isAuthenticated ? (
            <>
              <Tag minimal intent="success" icon="user">
                {displayName(auth.user?.profile as Record<string, unknown> | undefined)}
              </Tag>
              <Navbar.Divider />
              <Button
                variant="minimal"
                icon="log-out"
                text="Sign out"
                // A full RP-initiated logout, which ends the Keycloak session too, not just
                // removeUser(). Dropping only this console's copy of the token looks tidier and
                // is useless here: the SSO session survives, so the next sign-in returns the
                // same user without ever showing a login form -- and comparing what alice may
                // read with what bob may read is the entire point of this console.
                onClick={() => void auth.signoutRedirect()}
              />
            </>
          ) : (
            <Button
              variant="minimal"
              icon="log-in"
              intent="primary"
              text="Sign in"
              loading={auth.isLoading}
              onClick={() => void auth.signinRedirect()}
            />
          )}
          <Navbar.Divider />
          <AnchorButton
            variant="minimal"
            icon="git-repo"
            href="https://ontop-vkg.org"
            target="_blank"
            rel="noreferrer"
          />
        </Navbar.Group>
      </Navbar>

      <main className="sparql-console-main">
        <Outlet />
      </main>
    </div>
  );
}
