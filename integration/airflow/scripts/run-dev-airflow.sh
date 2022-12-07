#!/bin/bash
#
# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
#
# Usage: $ ./run-dev-airflow.sh

set -e

# Change working directory to integration
project_root=$(git rev-parse --show-toplevel)
current_dir=$(pwd)

YAML_LOCATION="${project_root}"/integration/airflow/tests/integration/tests

REBUILD="false"
RUN_DEFAULT="true"
DETACHED="true"

function help() {
  echo "Usage:"
  echo
  echo "IMPORTANT: Set AIRFLOW_IMAGE variable first."
  echo "./run-dev-airflow.sh [-b] [-r] [--shutdown] [--compose-exec]"
  echo
  echo "options:"
  echo "-b|--block-thread         Run docker-compose without --detach flag."
  echo "-r|--rebuild              Force rebuilding all images."
  echo "--shutdown                Stop and remove all containers and networks."
  echo "--compose-exec            Run arbitrary command using docker-compose"
  echo "-i|--attach-integration   Attach terminal to integration container from which you can run integration tests."
  echo "-a|--attach-worker        Attach terminal to Airflow worker from which you can run unit tests."
  echo "-h|--help                 Prints help."
  echo
}

function set_write_permissions () {
  mkdir -p airflow/logs
  chmod a+rw -R airflow/logs
  chmod a+w -R $project_root/integration
  chmod a+w -R $project_root/client/python
}

function compose_up() {

  # string operator: from variable AIRFLOW_IMAGE
  #  ##   <-- greedy front trim
  #  *    <-- matches anything
  #  :    <-- until the last ':'
  AIRFLOW_VERSION=${AIRFLOW_IMAGE##*:}

  # Remove -python3.7 from the tag
  export AIRFLOW_VERSION=${AIRFLOW_VERSION::-10}
  export BIGQUERY_PREFIX=$(echo "$AIRFLOW_VERSION" | tr "-" "_" | tr "." "_")
  export DBT_DATASET_PREFIX=$(echo "$AIRFLOW_VERSION" | tr "-" "_" | tr "." "_")_dbt
  export DAGS_ARE_PAUSED_AT_CREATION=True

  if [[ "$(id -u)" == "0" ]]; then
    set_write_permissions
    export AIRFLOW_UID=50000
  else
    export AIRFLOW_UID=$(id -u)
  fi

  UP_ARGS=""
  if [[ "${REBUILD}" == "true" ]]; then
    UP_ARGS+="--build --force-recreate "
  fi

  if [[ "${DETACHED}" == "true" ]]; then
    UP_ARGS+="-d"
  fi

  docker-compose -f $YAML_LOCATION/docker-compose.yml -f $YAML_LOCATION/docker-compose-dev.yml down -v
  docker-compose -f $YAML_LOCATION/docker-compose.yml -f $YAML_LOCATION/docker-compose-dev.yml --profile dev up $UP_ARGS
}

while [[ $# -gt 0 ]]
do
  case $1 in
    -h|--help)
      help
      exit
      ;;
    -b|--block-thread)
      shift
      DETACHED="false"
      RUN_DEFAULT="true"
      ;;
    -r|--rebuild)
       shift
       REBUILD="true"
       RUN_DEFAULT="true"
       ;;
    --shutdown)
      docker-compose -f $YAML_LOCATION/docker-compose.yml -f $YAML_LOCATION/docker-compose-dev.yml down
      exit
      ;;
    --compose-exec)
      shift
      docker-compose -f $YAML_LOCATION/docker-compose.yml -f $YAML_LOCATION/docker-compose-dev.yml "$@"
      exit
      ;;
    -i|--attach-integration)
      docker-compose -f $YAML_LOCATION/docker-compose.yml -f $YAML_LOCATION/docker-compose-dev.yml exec integration /bin/bash
      exit
      ;;
    -a|--attach-worker)
      docker-compose -f $YAML_LOCATION/docker-compose.yml -f $YAML_LOCATION/docker-compose-dev.yml exec --workdir /app/openlineage/integration/airflow airflow_worker /bin/bash
      exit
      ;;
     *) echo "Unknown argument: $1"
      exit 1
      ;;
  esac
done

if [[ "${RUN_DEFAULT}" == "true" ]]; then
  compose_up
fi