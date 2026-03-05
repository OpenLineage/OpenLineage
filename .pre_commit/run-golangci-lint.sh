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

# Determine golangci-lint binary location
GOLANGCI_LINT_BIN=""

# Check if golangci-lint is already in PATH
if command -v golangci-lint &> /dev/null; then
    GOLANGCI_LINT_BIN="golangci-lint"
else
    # Try to find it in common locations
    GOPATH_RAW="${GOPATH:-$(go env GOPATH 2>/dev/null)}"

    # Handle GOPATH with multiple paths (colon-separated)
    # Take the first path only
    GOPATH="${GOPATH_RAW%%:*}"

    if [ -n "$GOPATH" ]; then
        if [ -x "$GOPATH/bin/golangci-lint" ]; then
            GOLANGCI_LINT_BIN="$GOPATH/bin/golangci-lint"
        fi
    fi

    # If still not found, install it
    if [ -z "$GOLANGCI_LINT_BIN" ]; then
        echo "golangci-lint not found. Installing..."
        INSTALL_DIR="${GOPATH:-$HOME/go}/bin"
        mkdir -p "$INSTALL_DIR"

        echo "Installing to: $INSTALL_DIR"
        if curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b "$INSTALL_DIR"; then
            GOLANGCI_LINT_BIN="$INSTALL_DIR/golangci-lint"

            # Add to PATH for current session
            export PATH="$INSTALL_DIR:$PATH"

            # Verify installation
            if [ ! -x "$GOLANGCI_LINT_BIN" ]; then
                echo "Error: Failed to install golangci-lint - binary not found at $GOLANGCI_LINT_BIN"
                exit 1
            fi

            echo "Successfully installed golangci-lint to $GOLANGCI_LINT_BIN"
        else
            echo "Error: Failed to download and install golangci-lint"
            exit 1
        fi
    fi
fi

echo "Using golangci-lint binary: $GOLANGCI_LINT_BIN"

# Change to the working directory
cd "$WORKING_DIR"

# If specific files are provided, run on those files
if [ $# -gt 0 ]; then
    echo "Running golangci-lint on specific files..."
    # Run golangci-lint on the provided files
    "$GOLANGCI_LINT_BIN" run --config "../../$CONFIG_FILE" "$@"
else
    echo "Running golangci-lint on all files..."
    # Run golangci-lint on all files
    "$GOLANGCI_LINT_BIN" run --config "../../$CONFIG_FILE" ./...
fi

