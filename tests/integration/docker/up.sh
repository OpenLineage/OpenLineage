#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Usage: $ ./up.sh

set -e

# Change working directory to integration
project_root=$(git rev-parse --show-toplevel)
cd "${project_root}"/integrations/airflow/tests/integration

if [[ "$(docker images -q marquez-airflow-base:latest 2> /dev/null)" == "" ]]; then
  echo "Please run 'docker build -f Dockerfile.tests -t marquez-airflow-base .' at base folder"
  exit 1
fi

# maybe overkill
MARQUEZ_AIRFLOW_WHL=$(docker run marquez-airflow-base:latest sh -c "ls /whl/marquez_airflow*")

# Add revision to requirements.txt
echo "${MARQUEZ_AIRFLOW_WHL}" > requirements.txt

# Add revision to integration-requirements.txt
cat > integration-requirements.txt <<EOL
apache-airflow==1.10.12
apache-airflow[gcp]==1.10.12
apache-airflow[gcp_api]==1.10.12
apache-airflow[google]==1.10.12
apache-airflow[postgres]==1.10.12
requests==2.24.0
psycopg2-binary==2.8.6
httplib2>=0.18.1
google-cloud-bigquery>=1.28.0
google-auth-httplib2>=0.0.4
google-api-core>=1.22.2
google-api-python-client>=1.12.2
pandas-gbq>=0.13.2
google-cloud-storage>=1.31.2
retrying==1.3.3
${MARQUEZ_AIRFLOW_WHL}
EOL

docker-compose up --build --exit-code-from integration
