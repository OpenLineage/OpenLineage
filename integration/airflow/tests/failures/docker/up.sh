#!/bin/bash
#
# SPDX-License-Identifier: Apache-2.0.
#
# Usage: $ ./up.sh

set -e

# Change working directory to integration
project_root=$(git rev-parse --show-toplevel)
cd "${project_root}"/integration/airflow/tests/failures

if [[ "$(docker images -q openlineage-airflow-base:latest 2> /dev/null)" == "" ]]; then
  echo "Please run 'docker build -f Dockerfile.tests -t openlineage-airflow-base .' at base folder"
  exit 1
fi

if [[ -n "$CI" ]]; then
  mkdir -p airflow/logs
  chmod a+rwx -R airflow/logs
fi

# maybe overkill
OPENLINEAGE_AIRFLOW_WHL=$(docker run openlineage-airflow-base:latest sh -c "ls /whl/openlineage*")
OPENLINEAGE_AIRFLOW_WHL_ALL=$(docker run openlineage-airflow-base:latest sh -c "ls /whl/*")

# Add revision to requirements.txt
cat > requirements.txt <<EOL
${OPENLINEAGE_AIRFLOW_WHL}
EOL

# Add revision to integration-requirements.txt
cat > integration-requirements.txt <<EOL
requests==2.24.0
psycopg2-binary==2.9.2
httplib2>=0.18.1
retrying==1.3.3
pytest==6.2.2
jinja2==3.0.2
python-dateutil==2.8.2
${OPENLINEAGE_AIRFLOW_WHL}
EOL

docker-compose -f docker-compose-2.yml down
docker-compose -f docker-compose-2.yml up --build --abort-on-container-exit airflow_init postgres
OPENLINEAGE_URL="http://backend:5000/error/" docker-compose -f docker-compose-2.yml up --build --exit-code-from integration --scale airflow_init=0
docker-compose -f docker-compose-2.yml down

docker-compose -f docker-compose-2.yml up --build --abort-on-container-exit airflow_init postgres
OPENLINEAGE_URL="http://backend:5000/timeout/" docker-compose -f docker-compose-2.yml up --build --exit-code-from integration --scale airflow_init=0
docker-compose -f docker-compose-2.yml down

docker-compose -f docker-compose-2.yml up --build --abort-on-container-exit airflow_init postgres
OPENLINEAGE_URL="http://network-partition:5000/" docker-compose -f docker-compose-2.yml up --build --exit-code-from integration --scale airflow_init=0
docker-compose -f docker-compose-2.yml down
