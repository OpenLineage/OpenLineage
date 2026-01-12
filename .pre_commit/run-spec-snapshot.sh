#!/bin/bash

# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

# Copy changed spec JSON files to website static folder
# This is necessary to keep within repo history of all the spec versions
# Initialize CHANGE_DONE to 0 (no changes detected by default)
CHANGE_DONE=0

# If arguments are passed, use them; otherwise, use git diff
if [ "$#" -gt 0 ]; then
  FILES=("$@")
else
  FILES=($(git diff --name-only origin/main -- 'spec/OpenLineage.json' 'spec/facets/*.json' 'spec/OpenLineage.yml'))
fi

# Use process substitution to avoid subshell problem
for LINE in "${FILES[@]}"; do
  # Ignore registry files
  if [[ $LINE =~ registry.json ]]; then
    continue
  fi

  # Only process .json files or OpenLineage.yml
  if [[ ! $LINE =~ \.json$ ]] && [[ $LINE != "spec/OpenLineage.yml" ]]; then
    continue
  fi

  if [[ $LINE == "spec/OpenLineage.yml" ]]; then
    LOC="website/static/spec/OpenLineage.yml"
  else
    # Extract target file name from $id field in spec files using jq
    URL=$(jq -r '.["$id"]' < "$LINE")

    # Extract target location in website repo
    LOC="website/static/${URL#*//*/}"
  fi
  LOC_DIR="${LOC%/*}"

  # Create dir if necessary, and copy files
  mkdir -p "$LOC_DIR"
  cp "$LINE" "$LOC"
  echo "$LOC"
  # Check if the file is tracked by Git
  if git ls-files --error-unmatch "$LOC" &>/dev/null; then
    # The file is tracked by Git
    # Check if the copied file differs from the committed version and is not staged
    if ! git diff --quiet HEAD -- "$LOC"; then
      # Check if the differences are not already staged
      if git diff --quiet --cached -- "$LOC"; then
        echo "Change detected in $LINE: $LOC differs from committed version but is not staged"
        CHANGE_DONE=1  # Mark as change detected
      fi
    fi
  else
    # The file is untracked by Git
    echo "Change detected in $LINE: $LOC is untracked"
    CHANGE_DONE=1  # Mark as change detected
  fi
done

# Exit with the value of CHANGE_DONE (0 if no changes, 1 if there were changes)
exit $CHANGE_DONE