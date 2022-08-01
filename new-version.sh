#!/bin/bash
#
# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
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

# Update the python package version only if the current_version is different from the new_version
# We do this check because bumpversion screws up the search/replace if the current_version and
# new_version are the same
function update_py_version_if_needed() {
  export $(bump2version manual --new-version $1 --allow-dirty --list --dry-run | grep version | xargs)
  if [ "$new_version" != "$current_version" ]; then
    bump2version manual --new-version $1 --allow-dirty
  fi
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

# (1) Bump python module versions. Do this before the release in case the current release is not
# the same version as what was expected the last time we released. E.g., if the next expected
# release was a patch version, but a new minor version is being released, we need to update to the
# actual release version prior to committing/tagging
PYTHON_MODULES=(client/python/ integration/common/ integration/airflow/ integration/dbt/ integration/dagster/ integration/sql)
for PYTHON_MODULE in "${PYTHON_MODULES[@]}"; do
  (cd "${PYTHON_MODULE}" && update_py_version_if_needed "${PYTHON_RELEASE_VERSION}")
done

# (2) Bump java module versions
perl -i -pe"s/^version=.*/version=${RELEASE_VERSION}/g" ./client/java/gradle.properties
perl -i -pe"s/^version=.*/version=${RELEASE_VERSION}/g" ./integration/spark/gradle.properties
perl -i -pe"s/^version=.*/version=${RELEASE_VERSION}/g" ./integration/flink/gradle.properties
perl -i -pe"s/^version=.*/version=${RELEASE_VERSION}/g" ./proxy/gradle.properties

# (3) Bump version in docs
perl -i -pe"s/<version>.*/<version>${RELEASE_VERSION}<\/version>/g" ./integration/spark/README.md
perl -i -pe"s/openlineage-spark:[[:alnum:]\.-]*/openlineage-spark:${RELEASE_VERSION}/g" ./integration/spark/README.md
perl -i -pe"s/openlineage-spark-.*jar/openlineage-spark-${RELEASE_VERSION}.jar/g" ./integration/spark/README.md
perl -i -pe"s/<version>.*/<version>${RELEASE_VERSION}<\/version>/g" ./client/java/README.md
perl -i -pe"s/openlineage-java:[[:alnum:]\.-]*/openlineage-java:${RELEASE_VERSION}/g" ./client/java/README.md

# (4) Prepare release commit
git commit -sam "Prepare for release ${RELEASE_VERSION}"

# (5) Pull latest tags, then prepare release tag
git fetch --all --tags
git tag -a "${RELEASE_VERSION}" -m "openlineage ${RELEASE_VERSION}"

# (6) Prepare next development version
PYTHON_MODULES=(client/python/ integration/common/ integration/airflow/ integration/dbt/ integration/dagster/ integration/sql/)
for PYTHON_MODULE in "${PYTHON_MODULES[@]}"; do
  (cd "${PYTHON_MODULE}" && bump2version manual --new-version "${NEXT_VERSION}" --allow-dirty)
done

# Append '-SNAPSHOT' to 'NEXT_VERSION' if a release candidate, or missing
# (ex: '-SNAPSHOT' will be appended to X.Y.Z or X.Y.Z-rc.N)
if [[ "${NEXT_VERSION}" == *-rc.? ||
      ! "${NEXT_VERSION}" == *-SNAPSHOT ]]; then
  NEXT_VERSION="${NEXT_VERSION}-SNAPSHOT"
fi

perl -i -pe"s/^version=.*/version=${NEXT_VERSION}/g" ./integration/spark/gradle.properties
perl -i -pe"s/^version=.*/version=${NEXT_VERSION}/g" ./client/java/gradle.properties
perl -i -pe"s/^version=.*/version=${NEXT_VERSION}/g" ./proxy/gradle.properties
echo "version ${NEXT_VERSION}" > integration/spark/spark2/src/test/resources/io/openlineage/spark/agent/version.properties
echo "version ${NEXT_VERSION}" > integration/spark/spark3/src/test/resources/io/openlineage/spark/agent/version.properties
echo "version ${NEXT_VERSION}" > integration/flink/src/test/resources/io/openlineage/flink/client/version.properties

# (7) Prepare next development version commit
git commit -sam "Prepare next development version ${NEXT_VERSION}"

# (8) Check for commits in log
COMMITS=false
MESSAGE_1=$(git log -1 --grep="Prepare for release ${RELEASE_VERSION}" --pretty=format:%s)
MESSAGE_2=$(git log -1 --grep="Prepare next development version ${NEXT_VERSION}" --pretty=format:%s)

if [[ $MESSAGE_1 ]] && [[ $MESSAGE_2 ]]; then
  COMMITS=true
else
  echo "one or both commits failed; exiting..."
  exit 0
fi

# (9) Push commits and tag
if [[ $COMMITS = "true" ]] && [[ ! ${PUSH} = "false" ]]; then
  git push origin main && git push origin "${RELEASE_VERSION}"
else
  echo "...skipping push; to push manually, use 'git push origin main && git push origin "${RELEASE_VERSION}"'"
fi

echo "DONE!"
