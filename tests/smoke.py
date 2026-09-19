"""First end-to-end check: does a user's token reach Polaris through Spark Connect?"""
from __future__ import annotations

import os
import sys

from spark_connect_propagation import (
    Endpoints,
    PasswordGrantTokenProvider,
    connect,
    correlation_id,
)

REMOTE = os.environ.get("SPARK_REMOTE", "sc://spark-connect:15002")
ISSUER = os.environ.get("KEYCLOAK_ISSUER", "http://keycloak:8080/realms/spark")


def provider(user: str) -> PasswordGrantTokenProvider:
    return PasswordGrantTokenProvider(
        endpoints=Endpoints(ISSUER), client_id="spark-cli", username=user, password=user
    )


def main() -> int:
    print(f"connecting to {REMOTE} as alice")
    spark = connect(REMOTE, provider("alice"))

    with correlation_id() as cid:
        print(f"  correlation id: {cid}")
        print("  1. plain query (no catalog involved)")
        print("     ->", spark.sql("SELECT 1 AS n").collect())

        print("  2. list namespaces via the Polaris catalog")
        rows = spark.sql("SHOW NAMESPACES IN polaris").collect()
        print("     ->", [r[0] for r in rows])

    print("\nsmoke OK")
    print(f"trace it with:  make cid CID={cid}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
