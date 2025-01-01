#!/usr/bin/env bash
#
# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

# jv cli from: https://github.com/santhosh-tekuri/jsonschema

set -e

while [ "$1" ]; do
  event_type=$(basename "$1" .json)
  shopt -s nullglob
  test_events=("spec/tests/$event_type"/*.json)
  if [ ${#test_events[@]} -gt 0 ]; then
    for event in "${test_events[@]}"; do
      echo "Validating ${event} against $1"
      jv "$1" "${event}" --assert-format
    done
  fi
  shift
done