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
  echo "  # Bump version ('-SNAPSHOT' and '-devN' will automatically be appended to '0.0.2')"
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
  echo "  -r, --release-version string    the release version (ex: X.Y.Z, X.Y.Z-rc.*)"
  echo "  -n, --next-version string       the next version (ex: X.Y.Z, X.Y.Z-SNAPSHOT)"
  echo "  -d, --next-python-dev string    the next python dev version (ex: 1, 2, 3 - will"
  echo "                                  be added to end of next version, creating X.Y.Z-devN"
  echo
  title "FLAGS:"
  echo "  -p, --no-push     local changes are not automatically pushed to the remote repository"
  exit 1
}

readonly SEMVER_REGEX="^[0-9]+(\.[0-9]+){2}((-rc\.[0-9]+)?(-SNAPSHOT)?)$" # X.Y.Z
                                                                          # X.Y.Z-rc.*
                                                                          # X.Y.Z-rc.*-SNAPSHOT
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
PYTHON_DEV_VERSION=0
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
    -d|--next-dev-version)
       shift
       PYTHON_DEV_VERSION="${1}"
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

# (1) Bump python module versions for release candidates
# This is only necessary when we are publishing a release candidate
# This is necessary to get bump2version to bump from x.y.z to x.y.z-rc1
if [ -n "$RELEASE_CANDIDATE" ]; then
  PYTHON_MODULES=(client/python/ integration/common/ integration/airflow/ integration/dbt/)
  for PYTHON_MODULE in "${PYTHON_MODULES[@]}"; do
    (cd "${PYTHON_MODULE}" && bump2version manual --new-version "${PYTHON_RELEASE_VERSION}" --allow-dirty)
  done
fi

# (2) Bump java module versions
if [[ $OSTYPE == 'darwin'* ]]; then
  sed -i "" "s/^version=.*/version=${RELEASE_VERSION}/g" ./client/java/gradle.properties
  sed -i "" "s/^version=.*/version=${RELEASE_VERSION}/g" ./integration/spark/gradle.properties
else
  sed -i "s/^version=.*/version=${RELEASE_VERSION}/g" ./client/java/gradle.properties
  sed -i "s/^version=.*/version=${RELEASE_VERSION}/g" ./integration/spark/gradle.properties
fi

# (3) Bump version in docs
if [[ $OSTYPE == 'darwin'* ]]; then
  sed -i \
    -e "s/<version>.*/<version>${RELEASE_VERSION}<\/version>/g" \
    -e "s/openlineage-spark:[[:alnum:]\.-]*/openlineage-spark:${RELEASE_VERSION}/g" \
    -e "s/openlineage-spark-.*jar/openlineage-spark-${RELEASE_VERSION}.jar/g" ./integration/spark/README.md
else
  sed -i \
    -e "s/<version>.*/<version>${RELEASE_VERSION}<\/version>/g" \
    -e "s/openlineage-spark:[[:alnum:]\.-]*/openlineage-spark:${RELEASE_VERSION}/g" \
    -e "s/openlineage-spark-.*jar/openlineage-spark-${RELEASE_VERSION}.jar/g" ./integration/spark/README.md
fi

# (4) Prepare release commit
git commit -sam "Prepare for release ${RELEASE_VERSION}"

# (5) Pull latest tags, then prepare release tag
git fetch --all --tags
git tag -a "${RELEASE_VERSION}" -m "openlineage ${RELEASE_VERSION}"

# (6) Append SNAPSHOT and dev versions
# Java: append '-SNAPSHOT' to 'NEXT_VERSION' if a release candidate, or missing
# (ex: '-SNAPSHOT' will be appended to X.Y.Z or X.Y.Z-rc.N)
if [[ "${NEXT_VERSION}" == *-rc.? ||
      ! "${NEXT_VERSION}" == *-SNAPSHOT ]]; then
  JAVA_NEXT_VERSION="${NEXT_VERSION}-SNAPSHOT"
fi

# Python: append "-devN" to 'NEXT_VERSION' if missing
# If version ends with SNAPSHOT or rc.N, remove it
if [[ "${NEXT_VERSION}" == *-SNAPSHOT ]]; then
  PYTHON_NEXT_VERSION="${NEXT_VERSION%-SNAPSHOT}-dev${PYTHON_DEV_VERSION}"
elif [[ "${NEXT_VERSION}" == *-rc.? ]]; then
  PYTHON_NEXT_VERSION="${NEXT_VERSION%-rc.?}-dev${PYTHON_DEV_VERSION}"
else
  PYTHON_NEXT_VERSION="${NEXT_VERSION}-dev${PYTHON_DEV_VERSION}"
fi

# (7) Prepare next development version
PYTHON_MODULES=(client/python/ integration/common/ integration/airflow/ integration/dbt/)
for PYTHON_MODULE in "${PYTHON_MODULES[@]}"; do
  (cd "${PYTHON_MODULE}" && bump2version manual --new-version "${PYTHON_NEXT_VERSION}" --allow-dirty)
done


if [[ $OSTYPE == 'darwin'* ]]; then
  sed -i "" "s/^version=.*/version=${JAVA_NEXT_VERSION}/g" integration/spark/gradle.properties
  sed -i "" "s/^version=.*/version=${JAVA_NEXT_VERSION}/g" client/java/gradle.properties
else
  sed -i "s/^version=.*/version=${JAVA_NEXT_VERSION}/g" integration/spark/gradle.properties
  sed -i "s/^version=.*/version=${JAVA_NEXT_VERSION}/g" client/java/gradle.properties
fi
echo "version ${JAVA_NEXT_VERSION}" > integration/spark/src/test/resources/io/openlineage/spark/agent/client/version.properties

# (8) Prepare next development version commit
git commit -sam "Prepare next development version"

# (9) Push commits and tag
if [[ ! ${PUSH} = "false" ]]; then
  git push origin main && git push origin "${RELEASE_VERSION}"
else
  echo "...skipping push; to push manually, use 'git push origin main && git push origin "${RELEASE_VERSION}"'"
fi

echo "DONE!"
