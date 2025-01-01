#!/bin/bash

# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

set -e

source "$(dirname "${BASH_SOURCE[0]}")/set-java-version.sh"
JAVA_VERSION=${JAVA_VERSION:-17}
RULESET_FILE=${RULESET_FILE:-client/java/pmd-openlineage.xml}

# Parse command-line arguments
for arg in "$@"; do
  case $arg in
    --java-version)
      JAVA_VERSION=$2
      shift 2
      ;;
    --ruleset-file)
      RULESET_FILE=$2
      shift
      ;;
  esac
done

echo "Using Java version: $JAVA_VERSION"

source "$(dirname "${BASH_SOURCE[0]}")/set-java-version.sh"
set_java_version "$JAVA_VERSION"

PMD_RELEASE="https://github.com/pmd/pmd/releases/download/pmd_releases%2F6.46.0/pmd-bin-6.46.0.zip"

mkdir -p ./.pmd_cache
cd ./.pmd_cache

# check if there is a pmd folder
if [ ! -d "pmd" ]; then
  curl -fsSL -o pmd.zip "$PMD_RELEASE" > /dev/null 2>&1 \
    && unzip pmd.zip > /dev/null 2>&1 \
    && rm pmd.zip > /dev/null 2>&1 \
    && mv pmd-bin* pmd > /dev/null 2>&1 \
    && chmod -R +x pmd > /dev/null 2>&1
fi

idx=1
for (( i=1; i <= "$#"; i++ )); do
    if [[ ${!i} == *.java ]]; then
        idx=${i}
        break
    fi
done

# populate list of files to analyse
files=""
prefix="../"
for arg in "${@:idx}"; do
  files="$files $prefix$arg"
done

# Remove leading space (optional)
files="${files:1}"
eol=$'\n'
echo "${files// /$eol}" > /tmp/list

./pmd/bin/run.sh pmd -f textcolor -min 5 --file-list /tmp/list -R ../"${RULESET_FILE}"