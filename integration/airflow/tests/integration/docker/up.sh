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
apache-airflow[celery]==1.10.15
airflow-provider-great-expectations==0.0.8
great-expectations==0.13.42
dbt-bigquery==0.20.1
EOL


# Add revision to integration-requirements.txt
cat > integration-requirements.txt <<EOL
requests==2.24.0
setuptools==34.0.0
psycopg2-binary==2.9.2
mysqlclient>=1.3.6
httplib2>=0.18.1
retrying==1.3.3
pytest==6.2.2
jinja2==3.0.2
python-dateutil==2.8.2
EOL

./docker/volumes.sh tests

docker-compose -f tests/docker-compose.yml down

# Run airflow-init first, because rest of airflow containers can die if the database is not prepared.
docker-compose -f tests/docker-compose.yml up  -V --build --abort-on-container-exit airflow_init postgres
docker-compose -f tests/docker-compose.yml up --build --exit-code-from integration --scale airflow_init=0 || FAILED=1

docker create --name openlineage-volume-helper $AIRFLOW_VOLUME:/opt/airflow busybox
docker cp openlineage-volume-helper:/opt/airflow/logs tests/airflow/
docker rm openlineage-volume-helper

exit $FAILED