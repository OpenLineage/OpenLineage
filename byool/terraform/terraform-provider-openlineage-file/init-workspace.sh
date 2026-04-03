#!/usr/bin/env bash
# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

# init-workspace.sh — terraform-provider-openlineage-file
#
# Creates go.work at byool/terraform wiring together all three Go modules:
#   ./openlineage-base-resource
#   ./terraform-provider-openlineage-file
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
FILE_PROVIDER="./terraform-provider-openlineage-file"
CLIENT_GO="../../client/go"

echo "init-workspace: byool/terraform"
echo "  terraform dir : $TERRAFORM_DIR"
echo "  modules       :"
echo "    $BASE_RESOURCE"
echo "    $FILE_PROVIDER"
echo "    $CLIENT_GO"
echo ""

cd "$TERRAFORM_DIR"

# Validate all modules exist before touching anything
for rel in "$BASE_RESOURCE" "$FILE_PROVIDER" "$CLIENT_GO"; do
  if [ ! -f "$rel/go.mod" ]; then
    echo "ERROR: no go.mod found at $TERRAFORM_DIR/$rel"
    exit 1
  fi
done

# Always recreate go.work so the file stays minimal and reproducible
rm -f go.work go.work.sum

go work init "$BASE_RESOURCE" "$FILE_PROVIDER" "$CLIENT_GO"
go work sync

echo ""
echo "Done. go.work at $TERRAFORM_DIR:"
cat go.work
