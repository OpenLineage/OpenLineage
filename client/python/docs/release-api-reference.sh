#!/usr/bin/env bash
#
# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
#

set -e

function git-website() {
  command git --git-dir "$WEBSITE_DIR/.git" --work-tree "$WEBSITE_DIR" $@
}

git config --global user.email "openlineage-bot-key@openlineage.io"
git config --global user.name "OpenLineage deploy bot"

WEBSITE_DIR=${WEBSITE_DIR:-$HOME/build/website}
REFERENCE_DIR=/docs/development/developing/python/api-reference
REPO="git@github.com:OpenLineage/docs"
DEFAULT_BRANCH=main
BRANCH_NAME=${BRANCH_NAME:-$DEFAULT_BRANCH}

if [[ -d $WEBSITE_DIR ]]; then
  # Check if we're in git repository and the repository points at website
  if [[ $(git-website rev-parse --is-inside-work-tree) == "true" && $(git-website config --get remote.origin.url) == "$REPO" ]]; then
    # Make sure we're at the head of the main branch
    git checkout main
    git reset --hard origin/master
  else
    echo "$WEBSITE_DIR is not empty - failing"
    exit 1
  fi
else
  git clone -b $BRANCH_NAME --depth 1 $REPO $WEBSITE_DIR
fi

# check if there are any changes in javadoc in the latest commit
if [[ $(diff -qr --exclude _category_.json $WEBSITE_DIR$REFERENCE_DIR './converted' | wc -l) -eq 0 ]]; then
  echo "no changes in python client reference detected, skipping publishing"
  exit 0
fi

echo "Changes detected, updating python client reference..."
mv ./converted/* $WEBSITE_DIR$REFERENCE_DIR/

# commit new spec and push
git-website add -A $WEBSITE_DIR$REFERENCE_DIR
git --git-dir "$WEBSITE_DIR/.git" --work-tree "$WEBSITE_DIR" commit -s -m "openlineage python client reference update"
git-website push
