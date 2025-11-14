#!/bin/bash
#
# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
#
# NOTE: This script was inspired by https://github.com/MarquezProject/marquez/blob/main/new-version.sh
#
# Requirements:
#   * You're on the 'main' branch
#   * You have the following dependencies installed: git, uv, bump-my-version
#     (The script will check for these and provide installation instructions if missing)
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
  echo "  -d, --dry-run     show what would be changed without making any actual changes"
  exit 1
}

# Update the python package version only if the current_version is different from the new_version
# We do this check because bumpversion screws up the search/replace if the current_version and
# new_version are the same
function update_py_version_if_needed() {
  # shellcheck disable=SC2086,SC2046
  current_version=$(uv run bump-my-version show --config-file pyproject.toml --format json current_version | grep -o '"current_version": "[^"]*"' | cut -d'"' -f4)
  # shellcheck disable=SC2154
  if [ "$1" != "$current_version" ]; then
    # shellcheck disable=SC2086
    if [[ "${DRY_RUN}" == "true" ]]; then
      uv run bump-my-version bump "$bump_type" --new-version $1 --config-file pyproject.toml --allow-dirty --dry-run --verbose
    else
      uv run bump-my-version bump "$bump_type" --new-version $1 --config-file pyproject.toml --allow-dirty
    fi
  fi
}

readonly SEMVER_REGEX="^[0-9]+(\.[0-9]+){2}((-rc\.[0-9]+)?(-SNAPSHOT)?)$" # X.Y.Z
                                                                          # X.Y.Z-rc.*
                                                                          # X.Y.Z-rc.*-SNAPSHOT
                                                                          # X.Y.Z-SNAPSHOT

# Detect OS for installation instructions
detect_os() {
  if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "macos"
  elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    echo "linux"
  else
    echo "unknown"
  fi
}

# Check for required dependencies
check_dependencies() {
  local missing_deps=()
  local os_type
  os_type=$(detect_os)

  # Check for git
  if ! command -v git &> /dev/null; then
    missing_deps+=("git")
  fi

  # Check for uv
  if ! command -v uv &> /dev/null; then
    missing_deps+=("uv")
  fi

  # Check for bump-my-version (via uv)
  if command -v uv &> /dev/null; then
    if ! uv run which bump-my-version &> /dev/null; then
      missing_deps+=("bump-my-version")
    fi
  fi

  # If dependencies are missing, print installation instructions
  if [ ${#missing_deps[@]} -gt 0 ]; then
    echo "Error: Missing required dependencies: ${missing_deps[*]}"
    echo
    echo "Please install the missing dependencies:"
    echo

    for dep in "${missing_deps[@]}"; do
      case $dep in
        git)
          if [[ "$os_type" == "macos" ]]; then
            echo "  git:"
            echo "    • Using Homebrew: brew install git"
            echo "    • Or install Xcode Command Line Tools: xcode-select --install"
          elif [[ "$os_type" == "linux" ]]; then
            echo "  git:"
            echo "    • Debian/Ubuntu: sudo apt-get install git"
            echo "    • Fedora/RHEL: sudo dnf install git"
            echo "    • Arch: sudo pacman -S git"
          fi
          echo
          ;;
        uv)
          if [[ "$os_type" == "macos" ]]; then
            echo "  uv:"
            echo "    • Using Homebrew: brew install uv"
            echo "    • Or using curl: curl -LsSf https://astral.sh/uv/install.sh | sh"
          elif [[ "$os_type" == "linux" ]]; then
            echo "  uv:"
            echo "    • Using curl: curl -LsSf https://astral.sh/uv/install.sh | sh"
            echo "    • Or download from: https://github.com/astral-sh/uv/releases"
          fi
          echo "    • Documentation: https://docs.astral.sh/uv/getting-started/installation/"
          echo
          ;;
        bump-my-version)
          echo "  bump-my-version:"
          echo "    • Install via uv (recommended): uv tool install bump-my-version"
          echo "    • Or install via pip: pip install bump-my-version"
          echo "    • Documentation: https://github.com/callowayproject/bump-my-version#installation"
          echo
          ;;
      esac
    done

    exit 1
  fi
}

# Verify all required dependencies are installed
check_dependencies

# Change working directory to project root
project_root=$(git rev-parse --show-toplevel)
cd "${project_root}/"


if [[ $# -eq 0 ]] ; then
  usage
fi

PUSH="true"
DRY_RUN="false"
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
    -d|--dry-run)
       DRY_RUN="true"
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
if [[ "${branch}" != "main" ]] && [[ "${DRY_RUN}" != "true" ]]; then
  echo "error: you may only release on 'main'!"
  exit 1;
fi

# Ensure no unstaged changes are present in working directory
if [[ -n "$(git status --porcelain --untracked-files=no)" ]] && [[ "${DRY_RUN}" != "true" ]]; then
  echo "error: you have unstaged changes in your working directory!"
  exit 1;
fi

# Ensure valid versions
# shellcheck disable=SC2086,SC2206
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
PYTHON_MODULES=(client/python/ integration/common/ integration/airflow/ integration/dbt/ integration/sql/iface-py/)

if [[ "${DRY_RUN}" == "true" ]]; then
  echo "=== DRY RUN: Would bump Python modules to ${PYTHON_RELEASE_VERSION} ==="
fi

for PYTHON_MODULE in "${PYTHON_MODULES[@]}"; do
  if [[ "${DRY_RUN}" == "true" ]]; then
    echo "  - ${PYTHON_MODULE}"
  fi
  (cd "${PYTHON_MODULE}" && update_py_version_if_needed "${PYTHON_RELEASE_VERSION}")
done

# (2) Bump java module versions
if [[ "${DRY_RUN}" == "true" ]]; then
  echo ""
  echo "=== DRY RUN: Would bump Java/Gradle modules to ${RELEASE_VERSION} ==="
  echo "  - client/java/gradle.properties"
  echo "  - integration/sql/iface-java/gradle.properties"
  echo "  - integration/spark/gradle.properties"
  echo "  - integration/spark-extension-interfaces/gradle.properties"
  echo "  - integration/flink/gradle.properties"
  echo "  - integration/flink/examples/flink1-test-apps/gradle.properties"
  echo "  - integration/flink/examples/flink2-test-apps/gradle.properties"
  echo "  - integration/hive/gradle.properties"
  echo "  - integration/hive/hive-openlineage-hook/src/main/resources/io/openlineage/hive/client/version.properties"
  # Skip actual changes in dry-run
  echo ""
  echo "=== DRY RUN: Would update documentation ==="
  echo "  - integration/spark/README.md"
  echo "  - integration/flink/README.md"
  echo "  - client/java/README.md"
  echo ""
  echo "=== DRY RUN: Would create commit: 'Prepare for release ${RELEASE_VERSION}' ==="
  echo "=== DRY RUN: Would create tag: '${RELEASE_VERSION}' ==="
  echo ""
  echo "=== DRY RUN: Would bump Python modules to ${NEXT_VERSION} for next development version ==="
  for PYTHON_MODULE in "${PYTHON_MODULES[@]}"; do
    echo "  - ${PYTHON_MODULE}"
  done
  echo ""
  echo "=== DRY RUN: Would bump Java/Gradle modules to ${NEXT_VERSION}-SNAPSHOT ==="
  echo ""
  echo "=== DRY RUN: Would create commit: 'Prepare next development version ${NEXT_VERSION}-SNAPSHOT' ==="
  if [[ "${PUSH}" == "true" ]]; then
    echo "=== DRY RUN: Would push to origin: main and tag ${RELEASE_VERSION} ==="
  else
    echo "=== DRY RUN: Would NOT push (--no-push flag set) ==="
  fi
  echo ""
  echo "DONE (dry run)"
  exit 0
fi

perl -i -pe"s/^version=.*/version=${RELEASE_VERSION}/g" ./client/java/gradle.properties
perl -i -pe"s/^version=.*/version=${RELEASE_VERSION}/g" ./integration/sql/iface-java/gradle.properties
perl -i -pe"s/^version=.*/version=${RELEASE_VERSION}/g" ./integration/spark/gradle.properties
perl -i -pe"s/^version=.*/version=${RELEASE_VERSION}/g" ./integration/spark-extension-interfaces/gradle.properties
perl -i -pe"s/^version=.*/version=${RELEASE_VERSION}/g" ./integration/flink/gradle.properties
perl -i -pe"s/^version=.*/version=${RELEASE_VERSION}/g" ./integration/flink/examples/flink1-test-apps/gradle.properties
perl -i -pe"s/^version=.*/version=${RELEASE_VERSION}/g" ./integration/flink/examples/flink2-test-apps/gradle.properties
perl -i -pe"s/^version=.*/version=${RELEASE_VERSION}/g" ./integration/hive/gradle.properties
perl -i -pe"s/^version=.*/version=${RELEASE_VERSION}/g" ./integration/hive/hive-openlineage-hook/src/main/resources/io/openlineage/hive/client/version.properties

# (3) Bump version in docs
perl -i -pe"s/<version>.*/<version>${RELEASE_VERSION}<\/version>/g" ./integration/spark/README.md
perl -i -pe"s/openlineage-spark:[[:alnum:]\.-]*/openlineage-spark:${RELEASE_VERSION}/g" ./integration/spark/README.md
perl -i -pe"s/openlineage-spark-.*jar/openlineage-spark-${RELEASE_VERSION}.jar/g" ./integration/spark/README.md
perl -i -pe"s/<version>.*/<version>${RELEASE_VERSION}<\/version>/g" ./integration/flink/README.md
perl -i -pe"s/<version>.*/<version>${RELEASE_VERSION}<\/version>/g" ./client/java/README.md
perl -i -pe"s/openlineage-java:[[:alnum:]\.-]*/openlineage-java:${RELEASE_VERSION}/g" ./client/java/README.md

# (4) Prepare release commit
git commit --no-verify -sam "Prepare for release ${RELEASE_VERSION}"

# (5) Pull latest tags, then prepare release tag
git fetch --all --tags
git tag -a "${RELEASE_VERSION}" -m "openlineage ${RELEASE_VERSION}"

# (6) Prepare next development version
for PYTHON_MODULE in "${PYTHON_MODULES[@]}"; do
  (cd "${PYTHON_MODULE}" && uv run bump-my-version bump minor --new-version "${NEXT_VERSION}" --config-file pyproject.toml --allow-dirty)
done

# Append '-SNAPSHOT' to 'NEXT_VERSION' if a release candidate, or missing
# (ex: '-SNAPSHOT' will be appended to X.Y.Z or X.Y.Z-rc.N)
if [[ "${NEXT_VERSION}" == *-rc.? ||
      ! "${NEXT_VERSION}" == *-SNAPSHOT ]]; then
  NEXT_VERSION="${NEXT_VERSION}-SNAPSHOT"
fi

perl -i -pe"s/^version=.*/version=${NEXT_VERSION}/g" ./client/java/gradle.properties
perl -i -pe"s/^version=.*/version=${NEXT_VERSION}/g" ./integration/sql/iface-java/gradle.properties
perl -i -pe"s/^version=.*/version=${NEXT_VERSION}/g" ./integration/spark/gradle.properties
perl -i -pe"s/^version=.*/version=${NEXT_VERSION}/g" ./integration/spark-extension-interfaces/gradle.properties
perl -i -pe"s/^version=.*/version=${NEXT_VERSION}/g" ./integration/flink/gradle.properties
perl -i -pe"s/^version=.*/version=${NEXT_VERSION}/g" ./integration/flink/examples/flink1-test-apps/gradle.properties
perl -i -pe"s/^version=.*/version=${NEXT_VERSION}/g" ./integration/flink/examples/flink2-test-apps/gradle.properties
perl -i -pe"s/^version=.*/version=${NEXT_VERSION}/g" ./integration/hive/gradle.properties
perl -i -pe"s/^version=.*/version=${NEXT_VERSION}/g" ./integration/hive/hive-openlineage-hook/src/main/resources/io/openlineage/hive/client/version.properties
echo "version ${NEXT_VERSION}" > integration/spark/spark3/src/test/resources/io/openlineage/spark/agent/version.properties
echo "version ${NEXT_VERSION}" > integration/spark-extension-interfaces/src/test/resources/io/openlineage/spark/shade/extension/v1/lifecycle/plan/version.properties
echo "version ${NEXT_VERSION}" > integration/flink/shared/src/test/resources/io/openlineage/flink/client/version.properties
echo "version ${NEXT_VERSION}" > integration/flink/flink1/src/test/resources/io/openlineage/flink/client/version.properties
echo "version ${NEXT_VERSION}" > integration/flink/flink2/src/test/resources/io/openlineage/flink/client/version.properties

# (7) Prepare next development version commit
git commit --no-verify -sam "Prepare next development version ${NEXT_VERSION}"

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
  echo "...skipping push; to push manually, use 'git push origin main && git push origin \"${RELEASE_VERSION}\"'"
fi

echo "DONE!"
