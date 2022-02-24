#!/bin/bash
#
# SPDX-License-Identifier: Apache-2.0.
#
# Usage: $ ./up.sh

set -e

# Change working directory to integration
project_root=$(git rev-parse --show-toplevel)
cd "${project_root}"/integration/airflow/tests/integration

if [[ "$(docker images -q openlineage-airflow-base:latest 2> /dev/null)" == "" ]]; then
  echo "Please run 'docker build -f Dockerfile.tests -t openlineage-airflow-base .' at base folder"
  exit 1
fi

if [[ ! -f gcloud/gcloud-service-key.json ]]; then
  mkdir -p gcloud
fi
if [[ -n "$CI" ]]; then
  echo $GCLOUD_SERVICE_KEY > gcloud/gcloud-service-key.json
  chmod 644 gcloud/gcloud-service-key.json
fi

OPENLINEAGE_AIRFLOW_WHL=$(docker run openlineage-airflow-base:latest sh -c "ls /whl/openlineage*")

# Add revision to integration-requirements.txt
cat > integration-requirements.txt <<EOL
requests==2.24.0
setuptools==34.0.0
psycopg2-binary==2.9.2
httplib2>=0.18.1
retrying==1.3.3
pytest==6.2.2
jinja2==3.0.2
python-dateutil==2.8.2
${OPENLINEAGE_AIRFLOW_WHL}
EOL

docker-compose down -v
docker-compose up -V --build --force-recreate --exit-code-from integration
