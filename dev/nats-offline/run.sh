#!/bin/bash
#
# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
#
# Runs the Python and Java NATS transport tests offline: the test containers join an internal
# network that has no route to the internet, next to a JetStream-enabled NATS server.
#
# Usage: dev/nats-offline/run.sh [python|java|all]   (default: all)
# RUNTIME=container (Apple container, default) or RUNTIME=docker
set -euo pipefail

RUNTIME="${RUNTIME:-container}"
SUITE="${1:-all}"
NETWORK="ol-nats-offline"
NATS="ol-nats-offline-server"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
HERE="$ROOT/dev/nats-offline"

# Apple container's builder silently sees an empty context outside the home directory (for example
# under /tmp or /var/folders), so stage it in the user cache
mkdir -p "${XDG_CACHE_HOME:-$HOME/.cache}"
CONTEXT="$(mktemp -d "${XDG_CACHE_HOME:-$HOME/.cache}/ol-nats-offline.XXXXXX")"

cleanup() {
  "$RUNTIME" stop "$NATS" >/dev/null 2>&1 || true
  "$RUNTIME" rm "$NATS" >/dev/null 2>&1 || true
  "$RUNTIME" network rm "$NETWORK" >/dev/null 2>&1 || "$RUNTIME" network delete "$NETWORK" >/dev/null 2>&1 || true
}
trap 'cleanup; rm -rf "$CONTEXT"' EXIT

# Build from the files git knows about (tracked or new, not ignored), so local virtualenvs and
# build output stay out of the image
git -C "$ROOT" ls-files -co --exclude-standard -- spec client/python client/java \
  | rsync -a --files-from=- "$ROOT/" "$CONTEXT/"

if [ "$RUNTIME" = container ]; then
  container system status >/dev/null 2>&1 || container system start
fi

build() {
  echo "==> building $1 test image (network access)"
  "$RUNTIME" build -t "ol-nats-$1-test" -f "$HERE/$1.Containerfile" "$CONTEXT"
}

nats_url() {
  if [ "$RUNTIME" = container ]; then
    # Apple container has no name resolution between containers; use the server's address
    container ls --format json | python3 -c '
import json, sys
for c in json.load(sys.stdin):
    if c["id"] == sys.argv[1]:
        print("nats://" + c["status"]["networks"][0]["ipv4Address"].split("/")[0] + ":4222")
' "$NATS"
  else
    echo "nats://$NATS:4222"
  fi
}

# Proves the test container really is offline before trusting its results
PREFLIGHT='if timeout 5 bash -c "exec 3<>/dev/tcp/1.1.1.1/443" 2>/dev/null; then echo "internet reachable - not offline" >&2; exit 97; fi; echo "==> offline: internet unreachable"'

run_suite() {
  local suite="$1" url="$2" command
  case "$suite" in
    python) command="python -m pytest -p no:cacheprovider tests/test_nats.py" ;;
    java) command="./gradlew --offline --no-daemon --console=plain --continue :transports-nats:test --rerun :transports-nats:shadowJarTest --rerun" ;;
  esac
  echo "==> running $suite tests offline against $url"
  "$RUNTIME" run --rm --network "$NETWORK" -e "NATS_URL=$url" "ol-nats-$suite-test" \
    bash -c "$PREFLIGHT; $command"
}

case "$SUITE" in
  all) SUITES=(python java) ;;
  python | java) SUITES=("$SUITE") ;;
  *) echo "usage: $0 [python|java|all]" >&2; exit 2 ;;
esac

for suite in "${SUITES[@]}"; do build "$suite"; done

cleanup
if [ "$RUNTIME" = container ]; then
  container network create --internal "$NETWORK" >/dev/null
else
  docker network create --internal "$NETWORK" >/dev/null
fi
"$RUNTIME" run -d --name "$NATS" --network "$NETWORK" docker.io/library/nats:2 -js >/dev/null
URL="$(nats_url)"

status=0
for suite in "${SUITES[@]}"; do run_suite "$suite" "$URL" || status=$?; done
exit "$status"
