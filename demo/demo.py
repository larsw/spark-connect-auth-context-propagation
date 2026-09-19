"""Interactive walkthrough: two users, one Spark Connect server, different data.

Run from the host after ./install.sh has added the /etc/hosts aliases:

    make demo

Each user signs in with the OAuth 2.0 device flow -- the same shape as `gh auth login`. Tokens
cache under ~/.cache/spark-connect-poc, so the second run is silent.
"""
from __future__ import annotations

import os
import sys
import textwrap

from spark_connect_propagation import (
    DeviceCodeTokenProvider,
    Endpoints,
    connect,
    correlation_id,
)

REMOTE = os.environ.get("SPARK_REMOTE", "sc://spark-connect:15002")
ISSUER = os.environ.get("KEYCLOAK_ISSUER", "http://keycloak:8080/realms/spark")

BOLD, DIM, GREEN, RED, RESET = "\033[1m", "\033[2m", "\033[32m", "\033[31m", "\033[0m"


def heading(text: str) -> None:
    print(f"\n{BOLD}{text}{RESET}")
    print(f"{DIM}{'-' * len(text)}{RESET}")


def sign_in(user: str):
    heading(f"Sign in as {user}")
    print(f"{DIM}  (password is '{user}' in this sandbox realm){RESET}")
    provider = DeviceCodeTokenProvider(
        endpoints=Endpoints(ISSUER), client_id="spark-cli", label=user
    )
    provider.token()  # trigger the flow now, so the prompts are not interleaved later
    return connect(REMOTE, provider)


def attempt(session, user: str, sql: str) -> None:
    try:
        rows = session.sql(sql).collect()
        print(f"  {GREEN}OK{RESET}      {user}: {sql}")
        for row in rows[:4]:
            print(f"            {row}")
    except Exception as error:
        first_line = str(error).strip().splitlines()[0]
        print(f"  {RED}DENIED{RESET}  {user}: {sql}")
        print(textwrap.indent(textwrap.fill(first_line, 92), "            "))


def main() -> int:
    print(textwrap.dedent(f"""
        {BOLD}Spark Connect token + correlation-ID propagation{RESET}

        One Spark Connect server, one Polaris catalog, two users. Nothing about the
        server changes between them -- only the token each client presents.
    """))

    alice = sign_in("alice")
    bob = sign_in("bob")

    with correlation_id() as cid:
        heading("alice seeds the tables (she has CATALOG_MANAGE_CONTENT)")
        alice.sql("DROP TABLE IF EXISTS polaris.shared.events").collect()
        alice.sql("CREATE TABLE polaris.shared.events (id BIGINT, kind STRING) USING iceberg").collect()
        alice.sql("INSERT INTO polaris.shared.events VALUES (1,'login'),(2,'logout')").collect()
        alice.sql("DROP TABLE IF EXISTS polaris.restricted.salaries").collect()
        alice.sql("CREATE TABLE polaris.restricted.salaries (person STRING, amount BIGINT) USING iceberg").collect()
        alice.sql("INSERT INTO polaris.restricted.salaries VALUES ('alice',100),('bob',90)").collect()
        print("  done")

        heading("Both users read the shared namespace")
        attempt(alice, "alice", "SELECT * FROM polaris.shared.events ORDER BY id")
        attempt(bob, "bob  ", "SELECT * FROM polaris.shared.events ORDER BY id")

        heading("Only alice may read the restricted namespace")
        attempt(alice, "alice", "SELECT * FROM polaris.restricted.salaries")
        attempt(bob, "bob  ", "SELECT * FROM polaris.restricted.salaries")

    heading("Follow the whole thing through the logs")
    print(f"  Every RPC above carried correlation id {BOLD}{cid}{RESET}")
    print(f"  {DIM}make cid CID={cid}{RESET}")
    print()
    print(f"  {DIM}Spark holds no S3 credentials at all; the only ones on the data path{RESET}")
    print(f"  {DIM}were vended by Polaris for whichever user made the request.{RESET}")
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
