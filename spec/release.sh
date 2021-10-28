#!/usr/bin/env bash
set -e

function git-website() {
  command git --git-dir "$WEBSITE_DIR/.git" --work-tree "$WEBSITE_DIR" $@
}

# check if there are any changes in spec in the latest commit
if git diff --name-only --exit-code HEAD~1 HEAD 'spec/*.json' >> /dev/null; then
  echo "no changes in spec detected, skipping publishing spec"
  exit 0
fi

WEBSITE_DIR=${WEBSITE_DIR:-$HOME/build/website}
REPO="git@github.com:OpenLineage/OpenLineage.github.io"

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

# Copy changed spec fils to target location
git diff --name-only HEAD~1 HEAD 'spec/*.json' | while read LINE; do
  # extract target file name from $id field in spec files
  URL=$(cat $LINE | jq -r '.["$id"]')

  # extract target location in website repo
  LOC="${WEBSITE_DIR}/${URL#*//*/}"
  LOC_DIR="${LOC%/*}"

  # create dir if necessary, and copy files
  mkdir -p $LOC_DIR
  cp $LINE $LOC
done

# commit new spec and push
git-website add -A spec/
git --git-dir "$WEBSITE_DIR/.git" --work-tree "$WEBSITE_DIR" commit -m "openlineage specification update"
git-website push
