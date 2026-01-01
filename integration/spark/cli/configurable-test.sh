#!/usr/bin/env bash
#
# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
#
# Requirements:
#   * You're in the root of openlineage directory
#   * You've running docker environment
#
# Usage: $  ./integration/spark/cli/configurable-test.sh --spark SPARK_CONF_YML --test.dir TEST_DIR
#
# Script has to be called from the root directory in OpenLineage path so that the docker built can
# access all the required subprojects.
#
# Please note that in case of source code being change, prune flag has to be used to force building new docker image.

set -e

title() {
  echo -e "\033[1m${1}\033[0m"
}

usage() {
  echo "A script used to run a user defined integration test for OpenLineage Spark integration"
  echo
  title "USAGE:"
  echo "  ./$(basename -- "${0}") --spark SPARK_CONFIG_FILE --test TEST_DIR"
  echo
  title "EXAMPLES:"
  echo "  $ ./integration/spark/cli/configurable-test.sh --spark ./integration/spark/cli/spark-conf.yml --test ./integration/spark/cli/tests "
  echo
  echo "  $ ./integration/spark/cli/configurable-test.sh --spark ./integration/spark/cli/spark-conf-docker.yml --test ./integration/spark/cli/tests "
  echo
  echo "  $ ./integration/spark/cli/configurable-test.sh --help"
  echo
  echo
  title "ARGUMENTS:"
  echo "  --spark   string       spark configuration yaml, see example or docs to check yaml format required, OpenLineage conf entries should not overwrite transport configuration"
  echo "  --test    string       directory location for particular test. Directory should contain a single .sql file and one or more .json files with expected events to be emitted"
  echo "  -h|--help              print help"
  echo
  title "FLAGS:"
  echo "  -c, --clean     clear existing docker image and volumes"
  exit 1
}

if ! docker info > /dev/null 2>&1; then
  echo "This script uses docker, and it isn't running - please start docker and try again!"
  exit 1
fi

if [[ $# -eq 0 ]] ; then
  usage
fi

# (1) Parse arguments
CLEAN="false"
while [ $# -gt 0 ]; do
  case $1 in
    --spark)
       shift
       SPARK_CONF_YML="${1}"
       ;;
    --test)
       shift
       TEST_DIR="${1}"
       ;;
    -c|--clean)
          CLEAN="true"
          ;;
    -h|--help)
       usage
       ;;
    *) usage
       ;;
  esac
  shift
done

# (2) Validate input params
echo "SPARK_CONF_YML=$SPARK_CONF_YML;TEST_DIR=$TEST_DIR;CLEAN=$CLEAN"

if test -z "$SPARK_CONF_YML"
then
    echo "SPARK_CONF argument can't be empty"
    exit 1
fi

if test -z "$TEST_DIR"
then
    echo "TEST_DIR argument can't be empty"
    exit 1
fi

# (3) Validate current directory
if [ ! -d "./integration/spark" ]; then
  echo "Subdirectory './integration/spark' does not exist. Probably running script within incorrect working dir."
  exit 1
fi

# (4) Get OpenLineage version to compute image tag and container tag
SCALA_BINARY_VERSION=$(grep "^scalaBinaryVersion:" "$SPARK_CONF_YML" | cut -d':' -f2 | xargs)
SPARK_VERSION=$(grep "^sparkVersion:" "$SPARK_CONF_YML" | cut -d':' -f2 | xargs)
if test -z "$SPARK_VERSION"
then
  SPARK_VERSION='3.3.4'
fi
OPENLINEAGE_VERSION=$(grep "^version=" "integration/spark/gradle.properties" | cut -d'=' -f2)
DOCKER_IMAGE_TAG="openlineage-test:$OPENLINEAGE_VERSION"
CONTAINER_NAME="openlineage-test-$OPENLINEAGE_VERSION"
NETWORK_NAME=openlineage

echo "Spark version is '$SPARK_VERSION'"

# (5) Remove container if exists
if docker ps -a | grep -q "$CONTAINER_NAME" 2> /dev/null; then
    echo "Removing container: $CONTAINER_NAME"
    docker rm -f "$CONTAINER_NAME"
fi

# (6) Clear docker image
if [[ ${CLEAN} = "true" ]]; then
  echo "Clearing docker image: $DOCKER_IMAGE_TAG";
  docker image rm "$DOCKER_IMAGE_TAG"
  docker volume rm cargo
  docker volume rm gradle-cache
  docker volume rm m2-cache
  docker network rm "$NETWORK_NAME"
fi

# (7) Build docker images if not present
if [ -z "$(docker images -q "$DOCKER_IMAGE_TAG" 2> /dev/null)" ]; then
  echo "Building new image: $DOCKER_IMAGE_TAG";
  docker build -f ./integration/spark/cli/Dockerfile . --tag "$DOCKER_IMAGE_TAG"
else
   echo "Reusing existing image: $DOCKER_IMAGE_TAG";
fi

# (8) Make sure network is created
if [ -z "$(docker network ls --filter name=^${NETWORK_NAME}$ --format="{{ .Name }}")" ]; then
    echo "Creating network: $NETWORK_NAME"
    docker network create ${NETWORK_NAME} ;
else
   echo "Reusing existing network: $NETWORK_NAME";
fi

# (9) Run docker
prefix="./integration/spark/"
docker run --name "$CONTAINER_NAME" \
 --mount=type=bind,source=./integration/spark,target=/usr/lib/openlineage/integration/spark \
 -v gradle-cache:/root/.gradle \
 -v cargo:/root/.cargo \
 -v m2-cache:/root/.m2 \
 -v /var/run/docker.sock:/var/run/docker.sock \
 --network openlineage \
 -it \
 --rm \
 --env SCALA_BINARY_VERSION="$SCALA_BINARY_VERSION" \
 --env SPARK_VERSION="$SPARK_VERSION" \
 --env SPARK_CONF_YML=/usr/lib/openlineage/integration/spark/"${SPARK_CONF_YML#"$prefix"}" \
 --env TEST_DIR=/usr/lib/openlineage/integration/spark/"${TEST_DIR#"$prefix"}" \
 --env HOST_DIR="$(pwd)" \
 --add-host=host.docker.internal:host-gateway \
 "$DOCKER_IMAGE_TAG"