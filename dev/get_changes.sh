#!/bin/bash

export INPUT_CHANGELOG_FILENAME=CHANGES.md
export GITHUB_REPOSITORY=OpenLineage/OpenLineage

git clone git@github.com:merobi-hub/changelog-ci.git

python3 changelog-ci/scripts/main.py

rm -rf changelog-ci 