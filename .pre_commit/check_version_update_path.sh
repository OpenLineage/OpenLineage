#!/bin/bash
# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

set -e

# Get project root
project_root=$(git rev-parse --show-toplevel)
cd "${project_root}/"

echo "Checking paths referenced in new-version.sh..."

# File to check
NEW_VERSION_SCRIPT="${project_root}/new-version.sh"

if [ ! -f "$NEW_VERSION_SCRIPT" ]; then
  echo "Error: new-version.sh file not found at $NEW_VERSION_SCRIPT"
  exit 1
fi

# Extract gradle.properties paths
GRADLE_PROPERTIES_PATHS=$(grep -E "perl -i -pe.*gradle.properties" "$NEW_VERSION_SCRIPT" | grep -o "\./[^ ]*gradle.properties" | sort | uniq)

# Extract README paths
README_PATHS=$(grep -E "perl -i -pe.*README.md" "$NEW_VERSION_SCRIPT" | grep -o "\./[^ ]*README.md" | sort | uniq)

# Extract version.properties paths
VERSION_PROPERTIES_PATHS=$(grep -E "echo \"version.*> " "$NEW_VERSION_SCRIPT" | grep -o "[^ >]*version.properties" | sort | uniq)

# Check if all paths exist
EXIT_CODE=0

check_paths() {
  local paths=$1
  local file_type=$2
  
  if [ -z "$paths" ]; then
    echo "Warning: No $file_type paths found in new-version.sh"
    return
  fi
  
  echo "Checking $file_type files..."
  while IFS= read -r path; do
    # Remove leading ./ if present
    clean_path="${path#./}"
    
    if [ ! -f "$clean_path" ]; then
      echo "Error: $file_type file not found: $clean_path"
      EXIT_CODE=1
    else
      echo "  ✓ Found: $clean_path"
    fi
  done <<< "$paths"
}

# Check all extracted paths
check_paths "$GRADLE_PROPERTIES_PATHS" "gradle.properties"
check_paths "$README_PATHS" "README"
check_paths "$VERSION_PROPERTIES_PATHS" "version.properties"

if [ $EXIT_CODE -ne 0 ]; then
  echo "Pre-commit check failed. Please fix the above errors."
else
  echo "All path checks passed successfully!"
fi

exit $EXIT_CODE 