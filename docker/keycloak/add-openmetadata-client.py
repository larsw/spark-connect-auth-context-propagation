#!/usr/bin/env python3
"""Add the OpenMetadata ingestion identity to the Keycloak realm export.

OpenMetadata's Iceberg ingestion reaches Polaris as an OAuth2 client using the
client credentials grant, so it needs a confidential Keycloak client rather than
one of the interactive users. Everything it needs is added here:

  realm role   metadata_reader
  client       openmetadata            (confidential, service account only)
  user         service-account-openmetadata
  PDP          resources, a user policy and a read-only scope permission

The result is written back into spark-realm.json, which is what Keycloak imports
at start-up. Running it twice changes nothing.

Why the principal is called `service-account-openmetadata`: Polaris resolves the
principal by name from the `principal_name` claim, Keycloak's AuthZEN endpoint
resolves `subject.id` back to a Keycloak user, and a client's service account
user is always named `service-account-<clientId>`. Making the claim anything
else would break one side or the other.

    python3 docker/keycloak/add-openmetadata-client.py
"""
from __future__ import annotations

import json
import pathlib
import sys

REALM_FILE = pathlib.Path(__file__).with_name("spark-realm.json")

CLIENT_ID = "openmetadata"
CLIENT_SECRET = "openmetadata-secret"  # PoC only; matches compose.yaml
SERVICE_ACCOUNT_USER = f"service-account-{CLIENT_ID}"
PRINCIPAL_ROLE = "metadata_reader"
CATALOG_ROLE = "catalog_reader"

# The permission is written against every resource the realm knows about, so the
# crawler can describe both namespaces. That is the point of the contrast with
# bob: a metadata crawler may see the shape of `restricted.salaries` while still
# being unable to read a row of it, because the scope set below has no WRITE and
# no data-plane privilege beyond read delegation.
PDP_CLIENT = "polaris-pdp"
READ_POLICY_TO_COPY = "bob-may-read-shared"

# These two are meant to cover everything, and their resource list was written
# out in full when the realm was generated. Adding a resource without adding it
# here leaves root unable to touch it, which surfaces only as a 403 from the
# bootstrap -- a registered resource that no permission covers denies exactly
# like an unregistered one.
BLANKET_PERMISSIONS = ["alice-may-do-anything", "root-may-bootstrap"]


def load() -> dict:
    return json.loads(REALM_FILE.read_text())


def upsert(collection: list, item: dict, key: str = "name") -> str:
    """Insert item, or replace the existing entry with the same key."""
    for index, existing in enumerate(collection):
        if existing.get(key) == item[key]:
            collection[index] = item
            return "updated"
    collection.append(item)
    return "added"


def add_realm_role(realm: dict) -> str:
    roles = realm.setdefault("roles", {}).setdefault("realm", [])
    return upsert(
        roles,
        {
            "name": PRINCIPAL_ROLE,
            "description": "Read every catalog's metadata; read no data.",
        },
    )


def add_client(realm: dict) -> str:
    return upsert(
        realm.setdefault("clients", []),
        {
            "clientId": CLIENT_ID,
            "name": "OpenMetadata ingestion",
            "description": (
                "Crawls the Polaris Iceberg catalog. Client credentials only: "
                "there is no human behind it and no browser flow to protect."
            ),
            "enabled": True,
            "publicClient": False,
            "secret": CLIENT_SECRET,
            "standardFlowEnabled": False,
            "implicitFlowEnabled": False,
            "directAccessGrantsEnabled": False,
            "serviceAccountsEnabled": True,
            "protocolMappers": [
                {
                    # Polaris validates the audience against its own client id.
                    "name": "polaris-audience",
                    "protocol": "openid-connect",
                    "protocolMapper": "oidc-audience-mapper",
                    "config": {
                        "included.client.audience": "polaris",
                        "access.token.claim": "true",
                    },
                },
                {
                    # Same mapper the interactive clients use, so the claim is
                    # the service account's username and Keycloak can resolve it
                    # back from `username:<name>` when it acts as the PDP.
                    "name": "principal-name",
                    "protocol": "openid-connect",
                    "protocolMapper": "oidc-usermodel-property-mapper",
                    "config": {
                        "user.attribute": "username",
                        "claim.name": "principal_name",
                        "jsonType.label": "String",
                        "access.token.claim": "true",
                    },
                },
                {
                    "name": "principal-roles",
                    "protocol": "openid-connect",
                    "protocolMapper": "oidc-usermodel-realm-role-mapper",
                    "config": {
                        "claim.name": "principal_roles",
                        "jsonType.label": "String",
                        "multivalued": "true",
                        "access.token.claim": "true",
                    },
                },
            ],
        },
        key="clientId",
    )


def add_service_account_user(realm: dict) -> str:
    return upsert(
        realm.setdefault("users", []),
        {
            "username": SERVICE_ACCOUNT_USER,
            "enabled": True,
            "serviceAccountClientId": CLIENT_ID,
            "realmRoles": [PRINCIPAL_ROLE],
        },
        key="username",
    )


def add_pdp_entries(realm: dict) -> list[str]:
    pdp = next(c for c in realm["clients"] if c["clientId"] == PDP_CLIENT)
    settings = pdp["authorizationSettings"]
    resources = settings["resources"]
    policies = settings["policies"]

    # Every resource carries the full scope list; reuse it rather than rebuild it.
    all_scopes = resources[0]["scopes"]

    changes = []
    for name, type_ in [
        (SERVICE_ACCOUNT_USER, "polaris:PRINCIPAL"),
        (PRINCIPAL_ROLE, "polaris:PRINCIPAL_ROLE"),
        (CATALOG_ROLE, "polaris:CATALOG_ROLE"),
    ]:
        changes.append(
            f"resource {name}: "
            + upsert(
                resources,
                {
                    "name": name,
                    "type": type_,
                    "ownerManagedAccess": False,
                    "attributes": {},
                    "uris": [],
                    "scopes": all_scopes,
                },
            )
        )

    changes.append(
        "policy is-openmetadata: "
        + upsert(
            policies,
            {
                "name": "is-openmetadata",
                "type": "user",
                "logic": "POSITIVE",
                "decisionStrategy": "UNANIMOUS",
                "config": {"users": json.dumps([SERVICE_ACCOUNT_USER])},
            },
        )
    )

    # Take the read-only scope set from bob's permission so the two cannot drift.
    # That set already excludes every operation containing WRITE, which is what
    # keeps LOAD_TABLE_WITH_WRITE_DELEGATION -- and so a writable vended
    # credential -- out of a read-only principal's hands.
    read_scopes = next(p for p in policies if p["name"] == READ_POLICY_TO_COPY)["config"]["scopes"]
    if "WRITE" in read_scopes:
        raise SystemExit(
            f"{READ_POLICY_TO_COPY} now grants a WRITE scope; refusing to copy it"
        )

    changes.append(
        "permission openmetadata-may-read-all-metadata: "
        + upsert(
            policies,
            {
                "name": "openmetadata-may-read-all-metadata",
                "type": "scope",
                "logic": "POSITIVE",
                "decisionStrategy": "AFFIRMATIVE",
                "config": {
                    "resources": json.dumps([r["name"] for r in resources]),
                    "scopes": read_scopes,
                    "applyPolicies": json.dumps(["is-openmetadata"]),
                },
            },
        )
    )

    every_resource = json.dumps([r["name"] for r in resources])
    for name in BLANKET_PERMISSIONS:
        permission = next((p for p in policies if p["name"] == name), None)
        if permission is None:
            raise SystemExit(f"expected permission {name} in the realm")
        if permission["config"]["resources"] != every_resource:
            permission["config"]["resources"] = every_resource
            changes.append(f"permission {name}: extended to every resource")

    return changes


def main() -> int:
    realm = load()

    report = [
        f"realm role {PRINCIPAL_ROLE}: {add_realm_role(realm)}",
        f"client {CLIENT_ID}: {add_client(realm)}",
        f"user {SERVICE_ACCOUNT_USER}: {add_service_account_user(realm)}",
        *add_pdp_entries(realm),
    ]

    REALM_FILE.write_text(json.dumps(realm, indent=2) + "\n")
    for line in report:
        print(f"  {line}")
    print(f"\nwrote {REALM_FILE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
