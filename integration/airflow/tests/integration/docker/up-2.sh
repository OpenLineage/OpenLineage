#!/bin/bash
#
# SPDX-License-Identifier: Apache-2.0.
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
  mkdir -p tests/airflow/logs
  chmod a+rwx -R tests/airflow/logs
fi

# Add revision to requirements.txt
cat > requirements.txt <<EOL
airflow-provider-great-expectations==0.0.8
apache-airflow-providers-snowflake==2.5.1
apache-airflow-providers-postgres==3.0.0
apache-airflow-providers-google==6.7.0
great-expectations==0.13.42
dbt-core==1.0.1
dbt-bigquery==1.0.0
dbt-snowflake==1.0.0
EOL

# Add revision to integration-requirements.txt
cat > integration-requirements.txt <<EOL
requests==2.24.0
psycopg2-binary==2.9.2
mysqlclient>=1.3.6
httplib2>=0.18.1
retrying==1.3.3
pytest==6.2.2
jinja2==3.0.2
python-dateutil==2.8.2
EOL

# string operator: from variable AIRFLOW_IMAGE
#  ##   <-- greedy front trim
#  *    <-- matches anything
#  :    <-- until the last ':'
AIRFLOW_VERSION=${AIRFLOW_IMAGE##*:}

# Remove -python3.7 from the tag
export AIRFLOW_VERSION=${AIRFLOW_VERSION::${#AIRFLOW_VERSION}-10}
export BIGQUERY_PREFIX=$(echo "$AIRFLOW_VERSION" | tr "-" "_" | tr "." "_")
export DBT_DATASET_PREFIX=$(echo "$AIRFLOW_VERSION" | tr "-" "_" | tr "." "_")_dbt

./docker/volumes.sh tests

FAILED=0
docker-compose -f tests/docker-compose-2.yml down
docker-compose -f tests/docker-compose-2.yml up -V --build --abort-on-container-exit airflow_init postgres
docker-compose -f tests/docker-compose-2.yml up --build --exit-code-from integration --scale airflow_init=0 || FAILED=1

docker-compose -f tests/docker-compose-2.yml -f failures/docker-compose.yml down
docker-compose -f tests/docker-compose-2.yml -f failures/docker-compose.yml up -V --build --abort-on-container-exit airflow_init postgres
OPENLINEAGE_URL="http://backend:5000/error/" docker-compose -f tests/docker-compose-2.yml -f failures/docker-compose.yml up --build --exit-code-from integration --scale airflow_init=0 || FAILED=1
docker-compose -f tests/docker-compose-2.yml -f failures/docker-compose.yml down

docker-compose -f tests/docker-compose-2.yml -f failures/docker-compose.yml up -V --build --abort-on-container-exit airflow_init postgres
OPENLINEAGE_URL="http://backend:5000/timeout/" docker-compose -f tests/docker-compose-2.yml -f failures/docker-compose.yml up --build --exit-code-from integration --scale airflow_init=0 || FAILED=1
docker-compose -f tests/docker-compose-2.yml -f failures/docker-compose.yml down

docker-compose -f tests/docker-compose-2.yml -f failures/docker-compose.yml up -V --build --abort-on-container-exit airflow_init postgres
OPENLINEAGE_URL="http://network-partition:5000/" docker-compose -f tests/docker-compose-2.yml -f failures/docker-compose.yml up --build --exit-code-from integration --scale airflow_init=0 || FAILED=1
docker-compose -f tests/docker-compose-2.yml -f failures/docker-compose.yml down

docker create --name openlineage-volume-helper -v tests_airflow_logs:/opt/airflow/logs busybox
docker cp openlineage-volume-helper:/opt/airflow/logs tests/airflow/
docker rm openlineage-volume-helper

exit $FAILED