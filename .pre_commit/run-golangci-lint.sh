#!/bin/bash

# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

set -e

# Default configuration file
CONFIG_FILE=${CONFIG_FILE:-client/go/.golangci.yml}
WORKING_DIR=${WORKING_DIR:-client/go}

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --config-file)
      CONFIG_FILE="$2"
      shift 2
      ;;
    --working-dir)
      WORKING_DIR="$2"
      shift 2
      ;;
    *)
      break
      ;;
  esac
done

echo "Using golangci-lint config: $CONFIG_FILE"
echo "Working directory: $WORKING_DIR"

# Check if golangci-lint is installed
if ! command -v golangci-lint &> /dev/null; then
    echo "golangci-lint not found. Installing..."
    # Install golangci-lint
    curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b "$(go env GOPATH)/bin"
fi

# Change to the working directory
cd "$WORKING_DIR"

# If specific files are provided, run on those files
if [ $# -gt 0 ]; then
    echo "Running golangci-lint on specific files..."
    # Run golangci-lint on the provided files
    golangci-lint run --config "../../$CONFIG_FILE" "$@"
else
    echo "Running golangci-lint on all files..."
    # Run golangci-lint on all files
    golangci-lint run --config "../../$CONFIG_FILE" ./...
fi

