#!/usr/bin/env bash
#
# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

# jv cli from: https://github.com/santhosh-tekuri/jsonschema

set -e

while [ "$1" ]; do
  echo "Checking $1 schema"
  jv $1
  shift
done
