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
  exit 1
}

# Determine the bump type (major, minor, or patch) based on version comparison
# Arguments: current_version new_version
# Returns: "major", "minor", or "patch"
function determine_bump_type() {
  local current="$1"
  local new="$2"

  # Strip any suffixes like -SNAPSHOT, -rc.X for comparison
  local current_base
  local new_base
  current_base=$(echo "$current" | sed -E 's/(-rc\.[0-9]+)?(-SNAPSHOT)?$//')
  new_base=$(echo "$new" | sed -E 's/(-rc\.[0-9]+)?(-SNAPSHOT)?$//')

  # Parse version components
  local current_major current_minor current_patch
  local new_major new_minor new_patch

  IFS='.' read -r current_major current_minor current_patch <<< "$current_base"
  IFS='.' read -r new_major new_minor new_patch <<< "$new_base"

  # Determine bump type
  if [ "$new_major" != "$current_major" ]; then
    echo "major"
  elif [ "$new_minor" != "$current_minor" ]; then
    echo "minor"
  elif [ "$new_patch" != "$current_patch" ]; then
    echo "patch"
  else
    echo "error: current and new versions are the same: $current vs $new" >&2
    return 1
  fi
}

# Update the python package version only if the current_version is different from the new_version
# We do this check because bumpversion screws up the search/replace if the current_version and
# new_version are the same
function update_py_version_if_needed() {
  # shellcheck disable=SC2086,SC2046
  current_version=$(uv run bump-my-version show --config-file pyproject.toml --format json current_version | grep -o '"current_version": "[^"]*"' | cut -d'"' -f4)
  # shellcheck disable=SC2154
  if [ "$1" != "$current_version" ]; then
    # Determine the appropriate bump type
    bump_type=$(determine_bump_type "$current_version" "$1")
    # shellcheck disable=SC2086
    uv run bump-my-version bump "$bump_type" --new-version $1 --config-file pyproject.toml --allow-dirty
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
for PYTHON_MODULE in "${PYTHON_MODULES[@]}"; do
  (cd "${PYTHON_MODULE}" && update_py_version_if_needed "${PYTHON_RELEASE_VERSION}")
done

# (2) Bump java module versions
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
git commit --no-verify -sam "Prepare for release ${RELEASE_VERSION}" --signoff

# (5) Pull latest tags, then prepare release tag
git fetch --all --tags
git tag -a "${RELEASE_VERSION}" -m "openlineage ${RELEASE_VERSION}"

# (6) Prepare next development version
# Determine bump type based on release version and next version
NEXT_BUMP_TYPE=$(determine_bump_type "${PYTHON_RELEASE_VERSION}" "${NEXT_VERSION}")
for PYTHON_MODULE in "${PYTHON_MODULES[@]}"; do
  (cd "${PYTHON_MODULE}" && uv run bump-my-version bump "${NEXT_BUMP_TYPE}" --new-version "${NEXT_VERSION}" --config-file pyproject.toml --allow-dirty)
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
git commit --no-verify -sam "Prepare next development version ${NEXT_VERSION}" --signoff

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
