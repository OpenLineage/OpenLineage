#!/bin/bash

# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

set -e

JAVA_VERSION=${JAVA_VERSION:-17}
LOCATION=.

# Parse command-line arguments
for arg in "$@"; do
  case $arg in
    --java-version)
      JAVA_VERSION=$2
      shift 2
      ;;
    --location)
      LOCATION=$2
      shift 2
      ;;
  esac
done

echo "Using Java version: $JAVA_VERSION"
source "$(dirname "${BASH_SOURCE[0]}")/set-java-version.sh"
set_java_version "$JAVA_VERSION"

echo "Changing directory to: $LOCATION"
cd "$LOCATION"
./gradlew spotlessApply

