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

# check if there are any changes in javadoc in the latest commit
if [[ $(diff -qr $WEBSITE_DIR/static/apidocs/javadoc './build/docs/javadoc' | wc -l) -eq 0 ]]; then
  echo "no changes in javadoc detected, skipping publishing javadoc"
  exit 0
fi

echo "Changes detected, updating javadoc..."
rm -rf $WEBSITE_DIR/static/apidocs/javadoc
mv ./build/docs/javadoc $WEBSITE_DIR/static/apidocs

# commit new spec and push
git-website add -A static/apidocs/javadoc
git --git-dir "$WEBSITE_DIR/.git" --work-tree "$WEBSITE_DIR" commit -m "openlineage javadoc update"
git-website push
