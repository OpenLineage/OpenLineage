#!/bin/bash
#
# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
#
# Usage: $ ./build-and-push-proxy.sh <version>

set -eu

readonly SEMVER_REGEX="^[0-9]+(\.[0-9]+){2}(-rc\.[0-9]+)?$" # X.Y.Z
readonly ORG="openlineage"

# Change working directory to proxy module
project_root=$(git rev-parse --show-toplevel)
cd "${project_root}/proxy/backend"

# Version X.Y.Z of proxy image to build
version="${1}"

# Ensure valid version
if [[ ! "${version}" =~ ${SEMVER_REGEX} ]]; then
  echo "Version must match ${SEMVER_REGEX}"
  exit 1
fi

echo "Building image (tag: ${version})..."

# Build, tag and push proxy image
docker build --no-cache --tag "${ORG}/proxy:${version}" .
docker tag "${ORG}/proxy:${version}" "${ORG}/proxy:latest"

docker push "${ORG}/proxy:${version}"
docker push "${ORG}/proxy:latest"

echo "DONE!"

