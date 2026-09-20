"""Connector configuration, read from a CustomDatabaseConnection.

The native Iceberg service type carried a JSON Schema -- ``IcebergConnection``,
``IcebergCatalog``, ``RestCatalogConnection`` -- and the ingestion code received
those as validated pydantic models. OpenMetadata PR #26365 deleted that schema
along with the connector, so a custom connector has nothing to be handed but the
free-form ``connectionOptions`` string map. This module is the replacement for
that schema: it is where the untyped map becomes a validated object, so the rest
of the connector can go on reading attributes the way it always did.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, model_validator

# PyIceberg property names. Kept as constants because two of them changed
# spelling between the 0.5.1 the original connector pinned and the 0.12 used here.
URI = "uri"
WAREHOUSE = "warehouse"
CREDENTIAL = "credential"
TOKEN = "token"
SCOPE = "scope"
OAUTH2_SERVER_URI = "oauth2-server-uri"
SIGV4 = "rest.sigv4-enabled"
SIGV4_REGION = "rest.signing-region"  # was `rest.signing_region` in the original
SIGV4_SERVICE = "rest.signing-name"  # was `rest.signing_name` in the original

_TRUTHY = {"1", "true", "yes", "on"}


def _normalise(key: str) -> str:
    """Fold a connectionOptions key to a canonical form.

    connectionOptions is typed by hand in the UI, so `clientId`, `client_id` and
    `client-id` all turn up. Folding avoids making the user guess which spelling
    this connector happens to expect.
    """
    return key.replace("_", "").replace("-", "").lower()


def options_to_dict(options: Any) -> Dict[str, Any]:
    """Flatten connectionOptions into a plain dict.

    It has had three shapes across OpenMetadata releases -- a bare dict, a
    RootModel wrapping one, and a BaseModel with ``extra="allow"`` -- and the
    connector should not care which one this server builds.
    """
    if options is None:
        return {}
    if isinstance(options, dict):
        return dict(options)
    root = getattr(options, "root", None)
    if isinstance(root, dict):
        return dict(root)
    if hasattr(options, "model_dump"):
        return dict(options.model_dump(exclude_none=True))
    return dict(vars(options))


class PolarisIcebergConfig(BaseModel):
    """Everything the connector needs to reach one Iceberg REST catalog."""

    uri: str
    warehouse: Optional[str] = None

    # OAuth2. Supplying `oauth2ServerUri` alongside the client credentials moves
    # the token request to that server -- Keycloak, in this PoC -- instead of
    # Polaris's own /v1/oauth/tokens endpoint.
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    credential: Optional[str] = None
    token: Optional[str] = None
    scope: Optional[str] = None
    oauth2_server_uri: Optional[str] = None

    # The Iceberg catalog has no concept of a database above the namespace, so
    # OpenMetadata needs a name to hang the schemas off. The original connector
    # took this from `catalog.databaseName`.
    database_name: str = "default"
    ownership_property: str = "owner"

    # TLS
    ca_cert_path: Optional[str] = None
    client_cert_path: Optional[str] = None
    private_key_path: Optional[str] = None

    # SigV4, for catalogs behind AWS API Gateway. Unused against Polaris.
    sigv4: bool = False
    signing_region: Optional[str] = None
    signing_name: Optional[str] = None

    # Anything with a dot in its name goes to PyIceberg untouched. That covers
    # `s3.endpoint`, `header.X-Correlation-Id`, and the `auth.*` keys of the
    # AuthManager rework, none of which this model should have to enumerate.
    passthrough: Dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _check_auth(self) -> "PolarisIcebergConfig":
        if self.client_id and not self.client_secret:
            raise ValueError("clientId was given without clientSecret")
        if self.client_secret and not self.client_id:
            raise ValueError("clientSecret was given without clientId")
        return self

    @classmethod
    def from_options(cls, options: Any) -> "PolarisIcebergConfig":
        raw = options_to_dict(options)

        # Dotted keys are PyIceberg's own namespace and must survive verbatim;
        # everything else is matched against the fields above, case- and
        # separator-insensitively.
        passthrough = {k: v for k, v in raw.items() if "." in k}
        flat = {_normalise(k): v for k, v in raw.items() if "." not in k}

        def pick(*names: str) -> Optional[Any]:
            for name in names:
                value = flat.get(_normalise(name))
                if value not in (None, ""):
                    return value
            return None

        def flag(*names: str) -> bool:
            value = pick(*names)
            if value is None:
                return False
            return str(value).strip().lower() in _TRUTHY

        uri = pick("uri", "catalogUri", "restUri")
        if not uri:
            raise ValueError(
                "connectionOptions is missing 'uri' -- the Iceberg REST catalog "
                "endpoint, e.g. http://polaris:8181/api/catalog"
            )

        return cls(
            uri=str(uri).rstrip("/"),
            warehouse=pick("warehouse", "warehouseLocation", "catalogName"),
            client_id=pick("clientId"),
            client_secret=pick("clientSecret"),
            credential=pick("credential"),
            token=pick("token"),
            scope=pick("scope"),
            oauth2_server_uri=pick("oauth2ServerUri", "tokenUrl", "authorizationUrl"),
            database_name=pick("databaseName") or "default",
            ownership_property=pick("ownershipProperty") or "owner",
            ca_cert_path=pick("caCertPath"),
            client_cert_path=pick("clientCertPath"),
            private_key_path=pick("privateKeyPath"),
            sigv4=flag("sigv4", "sigv4Enabled"),
            signing_region=pick("signingRegion"),
            signing_name=pick("signingName"),
            passthrough=passthrough,
        )

    def pyiceberg_properties(self) -> Dict[str, Any]:
        """Render the PyIceberg property map for ``load_rest``.

        Keys whose value is None are dropped rather than passed through. The
        original connector passed them, which 0.5.1 tolerated; since then
        PyIceberg tests membership (``if CREDENTIAL in self.properties``) rather
        than truthiness, so a present-but-None key reads as "configured" and then
        fails on use.
        """
        properties: Dict[str, Any] = {URI: self.uri}

        if self.warehouse:
            properties[WAREHOUSE] = self.warehouse

        credential = self.credential
        if not credential and self.client_id and self.client_secret:
            credential = f"{self.client_id}:{self.client_secret}"

        if self.oauth2_server_uri and self.client_id and self.client_secret:
            # The AuthManager path (RFC 6749 client credentials) rather than the
            # legacy `credential` flow, which PyIceberg deprecates and drops in
            # 1.0. This is what lets an external IdP issue the token.
            oauth2: Dict[str, Any] = {
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "token_url": self.oauth2_server_uri,
            }
            if self.scope:
                oauth2["scope"] = self.scope
            properties["auth"] = {"type": "oauth2", "oauth2": oauth2}
        else:
            if credential:
                properties[CREDENTIAL] = credential
            if self.token:
                properties[TOKEN] = self.token
            if self.scope:
                properties[SCOPE] = self.scope
            if self.oauth2_server_uri:
                properties[OAUTH2_SERVER_URI] = self.oauth2_server_uri

        if self.ca_cert_path or self.client_cert_path or self.private_key_path:
            ssl: Dict[str, Any] = {}
            if self.ca_cert_path:
                ssl["cabundle"] = self.ca_cert_path
            if self.client_cert_path:
                client: Dict[str, Any] = {"cert": self.client_cert_path}
                if self.private_key_path:
                    client["key"] = self.private_key_path
                ssl["client"] = client
            properties["ssl"] = ssl

        if self.sigv4:
            properties[SIGV4] = "true"
            if self.signing_region:
                properties[SIGV4_REGION] = self.signing_region
            if self.signing_name:
                properties[SIGV4_SERVICE] = self.signing_name

        # Last, so an explicit `s3.endpoint` or `header.*` always wins.
        properties.update(self.passthrough)
        return properties
