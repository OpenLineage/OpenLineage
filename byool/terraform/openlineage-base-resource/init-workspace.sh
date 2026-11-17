#!/usr/bin/env bash
# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

# init-workspace.sh — openlineage-base-resource
#
# Creates go.work at byool/terraform wiring together two Go modules:
#   ./openlineage-base-resource
#   ../../client/go
#
# go.work is git-ignored — safe to re-run at any time.
#
# Usage:
#   ./init-workspace.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TERRAFORM_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

BASE_RESOURCE="./openlineage-base-resource"
CLIENT_GO="../../client/go"

echo "init-workspace: byool/terraform"
echo "  terraform dir : $TERRAFORM_DIR"
echo "  modules       :"
echo "    $BASE_RESOURCE"
echo "    $CLIENT_GO"
echo ""

cd "$TERRAFORM_DIR"

# Validate all modules exist before touching anything
for rel in "$BASE_RESOURCE" "$CLIENT_GO"; do
  if [ ! -f "$rel/go.mod" ]; then
    echo "ERROR: no go.mod found at $TERRAFORM_DIR/$rel"
    exit 1
  fi
done

# Always recreate go.work so the file stays minimal and reproducible
rm -f go.work go.work.sum

go work init "$BASE_RESOURCE" "$CLIENT_GO"
go work sync

echo ""
echo "Done. go.work at $TERRAFORM_DIR:"
cat go.work
