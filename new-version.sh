#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# NOTE: This script was inspired by https://github.com/MarquezProject/marquez/blob/main/new-version.sh
#
# Requirements:
#   * You're on the 'main' branch
#   * You've installed 'bump2version'
#
# Usage: $ ./new-version.sh --release-version RELEASE_VERSION --next-version NEXT_VERSION

set -e

title() {
  echo -e "\033[1m${1}\033[0m"
}

usage() {
  echo "Usage: ./$(basename -- "${0}") --release-version RELEASE_VERSION --next-version NEXT_VERSION"
  echo "A script used to release OpenLineage"
  echo
  title "EXAMPLES:"
  echo "  # Bump version ('-SNAPSHOT' will automatically be appended to '0.0.2')"
  echo "  $ ./new-version.sh -r 0.0.1 -n 0.0.2"
  echo
  echo "  # Bump version (with '-SNAPSHOT' already appended to '0.0.2')"
  echo "  $ ./new-version.sh -r 0.0.1 -n 0.0.2-SNAPSHOT"
  echo
  echo "  # Bump release candidate"
  echo "  $ ./new-version.sh -r 0.0.1-rc.1 -n 0.0.2-rc.2"
  echo
  echo "  # Bump release candidate without push"
  echo "  $ ./new-version.sh -r 0.0.1-rc.1 -n 0.0.2-rc.2 -p"
  echo
  title "ARGUMENTS:"
  echo "  -r, --release-version string     the release version (ex: X.Y.Z, X.Y.Z-rc.*)"
  echo "  -n, --next-version string        the next version (ex: X.Y.Z, X.Y.Z-SNAPSHOT)"
  echo
  title "FLAGS:"
  echo "  -p, --no-push     local changes are not automatically pushed to the remote repository"
  exit 1
}

readonly SEMVER_REGEX="^[0-9]+(\.[0-9]+){2}((-rc\.[0-9]+)?|(-SNAPSHOT)?)$" # X.Y.Z
                                                                           # X.Y.Z-rc.*
                                                                           # X.Y.Z-SNAPSHOT

# Change working directory to project root
project_root=$(git rev-parse --show-toplevel)
cd "${project_root}/"

# Verify bump2version is installed
if [[ ! $(type -P bump2version) ]]; then
  echo "bump2version not installed! Please see https://github.com/c4urself/bump2version#installation"
  exit 1;
fi

if [[ $# -eq 0 ]] ; then
  usage
fi

PUSH="true"
while [ $# -gt 0 ]; do
  case $1 in
    -r|--release-version)
       shift
       RELEASE_VERSION="${1}"
       ;;
    -n|--next-version)
       shift
       NEXT_VERSION="${1}"
       ;;
    -p|--no-push)
       PUSH="false"
       ;;
    -h|--help)
       usage
       ;;
    *) exit 1
       ;;
  esac
  shift
done

branch=$(git symbolic-ref --short HEAD)
if [[ "${branch}" != "main" ]]; then
  echo "error: you may only release on 'main'!"
  exit 1;
fi

# Ensure no unstaged changes are present in working directory
if [[ -n "$(git status --porcelain --untracked-files=no)" ]] ; then
  echo "error: you have unstaged changes in your working directory!"
  exit 1;
fi

# Append '-SNAPSHOT' to 'NEXT_VERSION' if not a release candidate, or missing
if [[ ! "${NEXT_VERSION}" == *-rc.? &&
      ! "${NEXT_VERSION}" == *-SNAPSHOT ]]; then
  NEXT_VERSION="${NEXT_VERSION}-SNAPSHOT"
fi

# Ensure valid versions
VERSIONS=($RELEASE_VERSION $NEXT_VERSION)
for VERSION in "${VERSIONS[@]}"; do
  if [[ ! "${VERSION}" =~ ${SEMVER_REGEX} ]]; then
    echo "Error: Version '${VERSION}' must match '${SEMVER_REGEX}'"
    exit 1
  fi
done

# Ensure python module version matches X.Y.Z or X.Y.ZrcN (see: https://www.python.org/dev/peps/pep-0440/),
PYTHON_RELEASE_VERSION=${RELEASE_VERSION}
if [[ "${RELEASE_VERSION}" == *-rc.? ]]; then
  RELEASE_CANDIDATE=${RELEASE_VERSION##*-}
  PYTHON_RELEASE_VERSION="${RELEASE_VERSION%-*}${RELEASE_CANDIDATE//.}"
fi

# (1) Bump python module versions
PYTHON_MODULES=(client/python/ integration/common/ integration/airflow/)
for PYTHON_MODULE in "${PYTHON_MODULES[@]}"; do
  (cd "${PYTHON_MODULE}" && bump2version manual --new-version "${PYTHON_RELEASE_VERSION}" --allow-dirty)
done

# (2) Bump java module versions
sed -i "" "s/^version=.*/version=${RELEASE_VERSION}/g" ./client/java/gradle.properties
sed -i "" "s/^version=.*/version=${RELEASE_VERSION}/g" ./integration/spark/gradle.properties

# (3) Bump version in docs
sed -i "" "s/<version>.*/<version>${RELEASE_VERSION}<\/version>/g" ./integration/spark/README.md
sed -i "" "s/openlineage-spark:.*/openlineage-spark:${RELEASE_VERSION}/g" ./integration/spark/README.md

# (4) Prepare release commit
git commit -sam "Prepare for release ${RELEASE_VERSION}"

# (5) Pull latest tags, then prepare release tag
git fetch --all --tags
git tag -a "${RELEASE_VERSION}" -m "openlineage ${RELEASE_VERSION}"

# (6) Prepare next development version
sed -i  "" "s/^version=.*/version=${NEXT_VERSION}/g" integration/spark/gradle.properties
sed -i  "" "s/^version=.*/version=${NEXT_VERSION}/g" client/java/gradle.properties

# (7) Prepare next development version commit
git commit -sam "Prepare next development version"

# (8) Push commits and tag
if [[ ! ${PUSH} = "false" ]]; then
  git push origin main && git push origin "${RELEASE_VERSION}"
else
  echo "...skipping push; to push manually, use 'git push origin main && git push origin "${RELEASE_VERSION}"'"
fi

echo "DONE!"
