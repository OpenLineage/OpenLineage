#!/usr/bin/env bash
#
# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

# jv cli from: https://github.com/santhosh-tekuri/jsonschema

set -e

# See check-spec.sh: resolve the absolute openlineage.io $refs from the in-repo
# published mirror instead of over the network.
SPEC_MIRROR="https://openlineage.io/spec/=website/static/spec/"

while [ "$1" ]; do
  event_type=$(basename "$1" .json)
  shopt -s nullglob
  test_events=("spec/tests/$event_type"/*.json)
  if [ ${#test_events[@]} -gt 0 ]; then
    for event in "${test_events[@]}"; do
      echo "Validating ${event} against $1"
      jv --map "$SPEC_MIRROR" "$1" "${event}" --assert-format
    done
  fi
  shift
done