#!/usr/bin/env bash
#
# Preflight for the Spark Connect propagation PoC.
#
# Verifies the local toolchain and the /etc/hosts aliases the stack needs.
# Anything requiring root is PRINTED and CONFIRMED before it runs -- this
# script never invokes sudo on your behalf without an explicit yes.
#
#   ./install.sh              interactive
#   ./install.sh --check      report only, change nothing (exit 1 if work remains)
#   ./install.sh --print-only show the privileged commands, run none of them
#   ./install.sh --yes        assume yes for privileged steps (for CI)
#
set -euo pipefail

MODE="interactive"
for arg in "$@"; do
  case "$arg" in
    --check)      MODE="check" ;;
    --print-only) MODE="print" ;;
    --yes|-y)     MODE="yes" ;;
    --help|-h)    sed -n '2,14p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown argument: $arg (try --help)" >&2; exit 2 ;;
  esac
done

if [ -t 1 ]; then
  BOLD=$'\033[1m'; RED=$'\033[31m'; GRN=$'\033[32m'; YEL=$'\033[33m'; DIM=$'\033[2m'; RST=$'\033[0m'
else
  BOLD=""; RED=""; GRN=""; YEL=""; DIM=""; RST=""
fi

PENDING=0
FAILED=0

ok()   { printf '  %s✓%s %s\n' "$GRN" "$RST" "$1"; }
bad()  { printf '  %s✗%s %s\n' "$RED" "$RST" "$1"; FAILED=$((FAILED+1)); }
todo() { printf '  %s!%s %s\n' "$YEL" "$RST" "$1"; PENDING=$((PENDING+1)); }
hint() { printf '    %s%s%s\n' "$DIM" "$1" "$RST"; }
# Reported but not counted: for tooling only one of the three clients needs. Both FAILED and
# PENDING exit 1, and a missing Rust toolchain must not fail the preflight for someone building
# the Python or JVM path.
note() { printf '  %s·%s %s\n' "$DIM" "$RST" "$1"; }
head2(){ printf '\n%s%s%s\n' "$BOLD" "$1" "$RST"; }

# ---------------------------------------------------------------- privileged --
# Print a privileged command, then run it only with explicit consent.
run_privileged() {
  local description="$1"; shift
  local cmd="$*"

  printf '\n  %sThis step needs root:%s %s\n' "$BOLD" "$RST" "$description"
  printf '  %s%s%s\n\n' "$DIM" "$cmd" "$RST"

  case "$MODE" in
    check|print)
      printf '  Not running it (%s mode). Run the line above yourself if you prefer.\n' "$MODE"
      return 1 ;;
    yes)
      printf '  Running (--yes).\n' ;;
    interactive)
      local reply
      read -r -p "  Run it now with sudo? [y/N/p=print only] " reply </dev/tty || reply="n"
      case "$reply" in
        [yY]*) : ;;
        [pP]*) printf '  Skipped. Run the line above yourself, then re-run this script.\n'; return 1 ;;
        *)     printf '  Skipped.\n'; return 1 ;;
      esac ;;
  esac

  if sudo bash -c "$cmd"; then
    printf '  %sDone.%s\n' "$GRN" "$RST"
    return 0
  fi
  printf '  %sFailed.%s\n' "$RED" "$RST"
  return 1
}

# ------------------------------------------------------------------- toolchain --
have() { command -v "$1" >/dev/null 2>&1; }

# Compare dotted versions: version_ge 21.0.12 21 -> true
version_ge() {
  [ "$(printf '%s\n%s\n' "$2" "$1" | sort -V | head -n1)" = "$2" ]
}

head2 "Toolchain"

if have docker; then
  if docker info >/dev/null 2>&1; then
    ok "docker $(docker --version | sed 's/Docker version //; s/,.*//') (daemon reachable)"
  else
    bad "docker is installed but the daemon is not reachable"
    hint "try: sudo systemctl start docker   (or add yourself to the 'docker' group)"
  fi
else
  bad "docker not found"
  hint "https://docs.docker.com/engine/install/"
fi

if docker compose version >/dev/null 2>&1; then
  ok "docker compose $(docker compose version --short 2>/dev/null || echo v2)"
else
  bad "docker compose v2 plugin not found"
  hint "https://docs.docker.com/compose/install/"
fi

# The PoC builds its jar locally; Spark 4.1 supports Java 17/21 and we target release 21.
if have java; then
  JAVA_VER="$(java -version 2>&1 | head -1 | sed -E 's/.*version "([0-9]+).*/\1/')"
  if [ "${JAVA_VER:-0}" -ge 21 ] 2>/dev/null; then
    ok "java $JAVA_VER"
    [ "$JAVA_VER" -gt 21 ] && hint "newer than 21 is fine; the module compiles with --release 21"
  else
    bad "java $JAVA_VER found, need 21+ (Spark 4.1 supports 17/21; we target 21)"
  fi
else
  bad "java not found (need JDK 21)"
fi

if have mvn; then
  ok "maven $(mvn -v 2>/dev/null | head -1 | awk '{print $3}')"
else
  bad "maven not found"
  hint "sudo apt install maven   # or https://maven.apache.org/install.html"
fi

if have uv; then
  ok "uv $(uv --version | awk '{print $2}')"
else
  bad "uv not found"
  hint "curl -LsSf https://astral.sh/uv/install.sh | sh"
fi

if have python3; then
  PY_VER="$(python3 -c 'import sys; print("%d.%d" % sys.version_info[:2])')"
  if version_ge "$PY_VER" "3.10"; then
    ok "python $PY_VER"
  else
    bad "python $PY_VER found, need 3.10+ (pyspark-client requirement)"
  fi
else
  bad "python3 not found"
fi

# ------------------------------------------------------- Rust client (optional) --
#
# One of three clients, and nothing else in the stack depends on it: `make up`, `make demo` and
# `make test` all run without any of this. So these are reported and never counted.
#
head2 "Rust client (optional -- make client-rust / test-rust / demo-rust)"

if have cargo; then
  ok "cargo $(cargo --version 2>/dev/null | awk '{print $2}')"
else
  note "cargo not found"
  hint "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
fi

if have rustc; then
  ok "rustc $(rustc --version 2>/dev/null | awk '{print $2}')"
else
  note "rustc not found (rustup installs it alongside cargo)"
fi

# protoc is a BUILD dependency of the spark-connect-rs crate: its build.rs compiles Spark's .proto
# files through tonic-build. Without it `cargo test` fails inside the build script, and the error
# names the crate and the .proto files but never protoc itself -- which is a long way to go to
# learn you are missing a package.
if have protoc; then
  PROTOC_VER="$(protoc --version 2>/dev/null | awk '{print $2}')"
  # build.rs passes --experimental_allow_proto3_optional, which exists from 3.12 on.
  if version_ge "${PROTOC_VER:-0}" "3.12"; then
    ok "protoc $PROTOC_VER"
  else
    note "protoc $PROTOC_VER is older than 3.12, which spark-connect-rs needs for proto3 optional"
    hint "sudo apt install protobuf-compiler"
  fi
else
  note "protoc not found -- spark-connect-rs compiles Spark's .proto files at build time"
  hint "sudo apt install protobuf-compiler"
fi

# ----------------------------------------------------------------- /etc/hosts --
#
# Why this is needed: Keycloak stamps a single issuer URL into every token. The
# browser (device flow), the PySpark client, Polaris fetching JWKS and our Java
# validator must all reach Keycloak at that SAME hostname, or OIDC validation
# fails on an issuer mismatch. Using the compose service names everywhere, with
# these aliases pointing at the published ports, keeps one issuer for everyone.
#
HOST_ALIASES="keycloak polaris polaris-console minio spark-connect spark-master"
HOSTS_MARKER="# spark-connect-propagation PoC"

head2 "/etc/hosts aliases"

MISSING_ALIASES=""
for alias in $HOST_ALIASES; do
  if getent hosts "$alias" >/dev/null 2>&1; then
    ok "$alias resolves"
  else
    todo "$alias does not resolve"
    MISSING_ALIASES="$MISSING_ALIASES $alias"
  fi
done

if [ -n "$MISSING_ALIASES" ]; then
  hint "needed so the browser, the client and the containers agree on one Keycloak issuer"
  HOSTS_CMD="printf '%s\n127.0.0.1 %s\n' '$HOSTS_MARKER' '$(echo $HOST_ALIASES)' >> /etc/hosts"
  if run_privileged "append loopback aliases to /etc/hosts" "$HOSTS_CMD"; then
    PENDING=0
    for alias in $HOST_ALIASES; do
      getent hosts "$alias" >/dev/null 2>&1 || PENDING=$((PENDING+1))
    done
  fi
fi

# ---------------------------------------------------------------------- ports --
head2 "Host ports"

port_busy() { ss -ltn "sport = :$1" 2>/dev/null | tail -n +2 | grep -q . ; }

for spec in "8080 keycloak" "8181 polaris" "3000 polaris-console" "9000 minio" "9001 minio-console" \
            "15002 spark-connect" "4040 spark-ui" "7077 spark-master" "8081 spark-worker-ui" \
            "3001 marquez-web" "5000 marquez-api" "5001 marquez-admin" "8090 ontop"; do
  set -- $spec
  if port_busy "$1"; then
    todo "port $1 ($2) is already in use"
  else
    ok "port $1 free ($2)"
  fi
done

# --------------------------------------------------------------------- verdict --
head2 "Result"

if [ "$FAILED" -gt 0 ]; then
  printf '  %s%d prerequisite(s) missing.%s Install them and re-run.\n\n' "$RED" "$FAILED" "$RST"
  exit 1
fi
if [ "$PENDING" -gt 0 ]; then
  printf '  %s%d item(s) still pending.%s Re-run after handling them.\n\n' "$YEL" "$PENDING" "$RST"
  exit 1
fi
printf '  %sReady.%s Next: %smake up%s\n\n' "$GRN" "$RST" "$BOLD" "$RST"
printf '  %sOnce it is up:%s\n' "$DIM" "$RST"
printf '    Polaris console   http://localhost:3000   %s(localhost, not the service name)%s\n' "$DIM" "$RST"
printf '    Marquez lineage   http://localhost:3001\n'
printf '    OpenLineage API   http://localhost:5000/api/v1/lineage\n\n'
