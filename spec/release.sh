#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0

set -e

function git-website() {
  command git --git-dir "$WEBSITE_DIR/.git" --work-tree "$WEBSITE_DIR" $@
}

git config --global user.email "openlineage-bot-key@openlineage.io"
git config --global user.name "OpenLineage deploy bot"

WEBSITE_DIR=${WEBSITE_DIR:-$HOME/build/website}
REPO="git@github.com:OpenLineage/website"

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
  git clone --depth 1 $REPO $WEBSITE_DIR
fi

WEBSITE_COMMIT_FILE="$WEBSITE_DIR/.last_spec_commit_id"

# Check on which commit we deployed spec last
if [[ -f $WEBSITE_COMMIT_FILE ]]; then
  PREV_SPEC_COMMIT=$(cat "$WEBSITE_COMMIT_FILE")
else
  # Before lifecycle state facet
  PREV_SPEC_COMMIT="d66c41872f3cc7f7cd5c99664d401e070e60ff48"
fi

# check if there are any changes in spec in the latest commit
if git diff --name-only --exit-code $PREV_SPEC_COMMIT HEAD 'spec/*.json' >> /dev/null; then
  echo "no changes in spec detected, skipping publishing spec"
  exit 0
fi

echo "Copying spec files from commit $PREV_SPEC_COMMIT"

# Mark last commit on which we finished copying spec
echo "$CIRCLE_SHA1" > "$WEBSITE_COMMIT_FILE"

# Copy changed spec fils to target location
git diff --name-only $PREV_SPEC_COMMIT HEAD 'spec/*.json' | while read LINE; do
  # extract target file name from $id field in spec files
  URL=$(cat $LINE | jq -r '.["$id"]')

  # extract target location in website repo
  LOC="${WEBSITE_DIR}/static/${URL#*//*/}"
  LOC_DIR="${LOC%/*}"

  # create dir if necessary, and copy files
  mkdir -p $LOC_DIR
  cp $LINE $LOC
done

# commit new spec and push
git-website add -A .last_spec_commit_id
git-website add -A spec/
git --git-dir "$WEBSITE_DIR/.git" --work-tree "$WEBSITE_DIR" commit -m "openlineage specification update"
git-website push
