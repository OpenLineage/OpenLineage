#!/usr/bin/env bash
#
# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

# jv cli from: https://github.com/santhosh-tekuri/jsonschema

set -e

# The facet schemas $ref the root schema by its absolute openlineage.io URL, which
# jv would otherwise fetch over the network. Map that prefix onto the in-repo
# published mirror so the hook is hermetic and validates the refs against the
# working tree rather than against the last release.
SPEC_MIRROR="https://openlineage.io/spec/=website/static/spec/"

while [ "$1" ]; do
  echo "Checking $1 schema"
  jv --map "$SPEC_MIRROR" "$1"
  shift
done
