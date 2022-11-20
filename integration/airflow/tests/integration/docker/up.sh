#!/bin/bash
#
# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
#
# Usage: $ ./up.sh

set -e

# Change working directory to integration
project_root=$(git rev-parse --show-toplevel)
cd "${project_root}"/integration/airflow/tests/integration

if [[ ! -f gcloud/gcloud-service-key.json ]]; then
  mkdir -p gcloud
fi
if [[ -n "$CI" ]]; then
  echo $GCLOUD_SERVICE_KEY > gcloud/gcloud-service-key.json
  chmod 644 gcloud/gcloud-service-key.json
fi

# this makes it easier to keep proper permissions on logs
mkdir -p tests/airflow/logs
chmod a+rwx -R tests/airflow/logs

# string operator: from variable AIRFLOW_IMAGE
#  ##   <-- greedy front trim
#  *    <-- matches anything
#  :    <-- until the last ':'
AIRFLOW_VERSION=${AIRFLOW_IMAGE##*:}

# Remove -python3.7 from the tag
export AIRFLOW_VERSION=${AIRFLOW_VERSION::-10}
export AWS_ATHENA_SUFFIX=$(echo "$AIRFLOW_VERSION" | tr "-" "_" | tr "." "_")
export BIGQUERY_PREFIX=$(echo "$AIRFLOW_VERSION" | tr "-" "_" | tr "." "_")
export DBT_DATASET_PREFIX=$(echo "$AIRFLOW_VERSION" | tr "-" "_" | tr "." "_")_dbt
# just a hack to have same docker-compose for dev and CI env
export PWD='.'

docker-compose -f tests/docker-compose.yml down -v
docker-compose -f tests/docker-compose.yml build
docker-compose -f tests/docker-compose.yml run integration
docker-compose -f tests/docker-compose.yml logs > tests/airflow/logs/docker.log
docker-compose -f tests/docker-compose.yml down
