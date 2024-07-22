#!/usr/bin/env bash
#
# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

set -e

# check if there are any changes in spec in the latest commit
if git diff --name-only --exit-code HEAD^ HEAD -- 'spec/*.json' 'spec/OpenLineage.yml' >> /dev/null; then
  echo "no changes in spec detected, skipping publishing spec"
  exit 0
fi

# Copy changed spec JSON files to target location
git diff --name-only HEAD^ HEAD -- 'spec/*.json' | while read LINE; do

  #ignore registry files
  if [[ $LINE =~ "registry.json" ]]; then
      continue
  fi

  # extract target file name from $id field in spec files
  URL=$(cat $LINE | jq -r '.["$id"]')

  # extract target location in website repo
  LOC="website/static/${URL#*//*/}"
  LOC_DIR="${LOC%/*}"

  # create dir if necessary, and copy files
  mkdir -p $LOC_DIR
  cp $LINE $LOC
done

# verify if there are any changes
if [[ $(git status --porcelain | wc -l) -gt 0 ]]; then
  git config user.name github-actions
  git config user.email github-actions@github.com
  git fetch
  git checkout main
  git add website/static/spec/*
  git commit -m "[generated] adding spec changes"
  git push
fi
