#!/usr/bin/env bash
# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

# init-workspace.sh — openlineage-base-resource
#
# Creates go.work at byool/terraform wiring together the Go modules:
#   ./openlineage-base-resource
#   ./terraform-provider-openlineage-file
#   ../../client/go
#
# go.work is git-ignored — safe to re-run at any time. Its go version is pinned to this
# module's, so tools built for that version (golangci-lint in CI) keep working with a newer
# local Go. There is no `go work sync`: it would copy the workspace's dependency versions
# into every module's go.mod, including client/go.
#
# Usage:
#   ./init-workspace.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TERRAFORM_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

BASE_RESOURCE="./openlineage-base-resource"
EXAMPLE_PROVIDER="./terraform-provider-openlineage-file"
CLIENT_GO="../../client/go"

echo "init-workspace: byool/terraform"
echo "  terraform dir : $TERRAFORM_DIR"
echo "  modules       :"
echo "    $BASE_RESOURCE"
echo "    $EXAMPLE_PROVIDER"
echo "    $CLIENT_GO"
echo ""

cd "$TERRAFORM_DIR"

# Validate all modules exist before touching anything
for rel in "$BASE_RESOURCE" "$EXAMPLE_PROVIDER" "$CLIENT_GO"; do
  if [ ! -f "$rel/go.mod" ]; then
    echo "ERROR: no go.mod found at $TERRAFORM_DIR/$rel"
    exit 1
  fi
done

GO_VERSION="$(go mod edit -json "$BASE_RESOURCE/go.mod" | sed -n 's/^[[:space:]]*"Go": "\(.*\)",*$/\1/p')"

# Always recreate go.work so the file stays minimal and reproducible
rm -f go.work go.work.sum

go work init "$BASE_RESOURCE" "$EXAMPLE_PROVIDER" "$CLIENT_GO"
go work edit -go="$GO_VERSION"

echo ""
echo "Done. go.work at $TERRAFORM_DIR:"
cat go.work
