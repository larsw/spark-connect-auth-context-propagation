"""Exercises the write path and vended credentials from a real executor JVM."""
from __future__ import annotations

import os

from spark_connect_propagation import Endpoints, PasswordGrantTokenProvider, connect, correlation_id

REMOTE = os.environ.get("SPARK_REMOTE", "sc://spark-connect:15002")
ISSUER = os.environ.get("KEYCLOAK_ISSUER", "http://keycloak:8080/realms/spark")


def provider(user: str):
    return PasswordGrantTokenProvider(
        endpoints=Endpoints(ISSUER), client_id="spark-cli", username=user, password=user
    )


alice = connect(REMOTE, provider("alice"))

with correlation_id() as cid:
    print("correlation id:", cid)

    print("\n[1] alice CREATEs shared.events  (write path + vended write credentials)")
    alice.sql("DROP TABLE IF EXISTS polaris.shared.events").collect()
    alice.sql(
        "CREATE TABLE polaris.shared.events (id BIGINT, kind STRING) USING iceberg"
    ).collect()
    alice.sql(
        "INSERT INTO polaris.shared.events VALUES (1,'login'),(2,'logout'),(3,'purchase')"
    ).collect()
    print("    created and populated")

    print("\n[2] alice CREATEs restricted.salaries")
    alice.sql("DROP TABLE IF EXISTS polaris.restricted.salaries").collect()
    alice.sql(
        "CREATE TABLE polaris.restricted.salaries (person STRING, amount BIGINT) USING iceberg"
    ).collect()
    alice.sql(
        "INSERT INTO polaris.restricted.salaries VALUES ('alice',100),('bob',90)"
    ).collect()
    print("    created and populated")

    print("\n[3] alice READs both  (executor reads Parquet from MinIO with vended credentials)")
    print("    shared.events     ->", alice.sql("SELECT * FROM polaris.shared.events ORDER BY id").collect())
    print("    restricted.salaries ->", alice.sql("SELECT * FROM polaris.restricted.salaries").collect())

print("\nscenario OK")
